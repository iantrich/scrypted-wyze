"""Scrypted camera device for a Wyze "lake" (Agora) camera.

getVideoStream hands Scrypted an FFmpeg input that reads MPEG-TS (H.265, timestamped
here on arrival from Agora) from a localhost TCP port. Each TCP client (normally the Rebroadcast
plugin's single ffmpeg) gets its own Agora session: fresh Wyze credentials
(which also wake the camera), join, forward frames from the first keyframe on,
and leave when the client disconnects or frames stop.
"""

import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import scrypted_sdk

from ts import CLOCK, TsMuxer

if TYPE_CHECKING:
    from main import WyzePlugin

RESEND_AFTER = 3  # seconds of wrong-size frames before re-requesting the resolution
JOIN_ATTEMPTS = 3  # sessions to try for the requested size before accepting whatever arrives
SIZE_WAIT = 8  # seconds to wait for a keyframe at the requested size before accepting any keyframe
FRAME_TIMEOUT = 15  # seconds without a frame before giving up on a session
SESSION_MAX = 55 * 60  # Wyze's rtc_token is issued for 3600s; end before it expires
QUEUE_MAX = 300  # ~15s at 20fps; beyond this the reader is stuck, so drop frames


class WyzeAgoraCamera(scrypted_sdk.ScryptedDeviceBase, scrypted_sdk.VideoCamera):
    def __init__(self, nativeId: str, plugin: "WyzePlugin"):
        super().__init__(nativeId)
        self.plugin = plugin
        self.server: Optional[asyncio.AbstractServer] = None
        self.port: Optional[int] = None
        self.server_lock = asyncio.Lock()
        self.sessions = 0
        self.writers = set()

    def close_streams(self) -> None:
        """Drop every active client; their Agora sessions end as the writes fail."""
        for w in list(self.writers):
            try:
                w.close()
            except Exception:
                pass

    def _stream_options(self) -> Dict[str, Any]:
        hd = self.plugin.resolution() % 2 == 1
        width, height = (2304, 1296) if hd else (640, 360)
        return {
            "id": "agora",
            "name": "Agora (H.265 %sp)" % height,
            "video": {"codec": "h265", "width": width, "height": height},
            "audio": None,
        }

    async def getVideoStreamOptions(self) -> List[Dict[str, Any]]:
        return [self._stream_options()]

    async def getVideoStream(self, options: Dict[str, Any] = None) -> Any:
        await self._ensure_server()
        url = "tcp://127.0.0.1:%d" % self.port
        ffmpeg_input = {
            "url": url,
            "inputArguments": [
                "-f", "mpegts",
                "-i", url,
            ],
            "mediaStreamOptions": self._stream_options(),
        }
        return await scrypted_sdk.mediaManager.createFFmpegMediaObject(ffmpeg_input)

    async def _ensure_server(self) -> None:
        async with self.server_lock:
            if self.server:
                return
            self.server = await asyncio.start_server(self._handle_client, "127.0.0.1", 0)
            self.port = self.server.sockets[0].getsockname()[1]
            self.print("Stream server listening on 127.0.0.1:%d" % self.port)

    async def _resend(self, sid: int, camera) -> None:
        try:
            await self.plugin.wyze.request_resolution(camera, self.plugin.resolution())
            self.print("[%d] resolution re-request sent" % sid)
        except Exception as e:
            self.print("[%d] resolution re-request failed: %r" % (sid, e))

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        import agora_rx  # lazy: keeps a native-lib problem from breaking device creation

        if self.plugin.streaming_paused:
            writer.close()
            return
        self.writers.add(writer)
        self.sessions += 1
        sid = self.sessions
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX)
        dropped = 0

        def enqueue(item):
            nonlocal dropped
            try:
                queue.put_nowait(item)
            except asyncio.QueueFull:
                dropped += 1

        def on_frame(data: bytes, is_key: bool, codec: str, width: int, height: int):
            # Timestamp on arrival from Agora (SDK thread), before any queueing delay.
            loop.call_soon_threadsafe(enqueue, (data, is_key, width, height, codec, time.monotonic()))

        session = None
        written = 0
        started = time.time()
        reason = "unknown"
        try:
            camera = await self.plugin.wait_for_camera(self.nativeId)
            self.print("[%d] client connected; requesting stream for %s" % (sid, camera.nickname))
            want = self._stream_options()["video"]
            want_size = (want["width"], want["height"])
            attempt = 0
            seen_key = False
            resent = False
            attempt_started = time.time()

            async def join():
                nonlocal session, attempt, resent, attempt_started
                attempt += 1
                resent = False
                attempt_started = time.time()
                info = await self.plugin.wyze.stream_info(camera, self.plugin.resolution())
                session = agora_rx.AgoraSession(
                    info, on_frame, lambda m: self.print("[%d.%d]%s" % (sid, attempt, m)))
                await asyncio.to_thread(session.start)

            await join()
            while True:
                if session.failed:
                    reason = session.failed
                    break
                if time.time() - started > SESSION_MAX:
                    reason = "session max age"
                    break
                try:
                    data, is_key, width, height, codec, arrived = await asyncio.wait_for(queue.get(), FRAME_TIMEOUT)
                except asyncio.TimeoutError:
                    reason = "no frames for %ds" % FRAME_TIMEOUT
                    break
                if not seen_key:
                    if not is_key:
                        continue
                    waited = time.time() - attempt_started
                    if (width, height) != want_size:
                        if not resent and waited > RESEND_AFTER:
                            resent = True
                            self.print("[%d.%d] got %sx%s, re-requesting resolution %s"
                                       % (sid, attempt, width, height, self.plugin.resolution()))
                            asyncio.ensure_future(self._resend(sid, camera))
                        if waited < SIZE_WAIT:
                            continue  # camera still sending its previous resolution
                        if attempt < JOIN_ATTEMPTS:
                            # The camera alternates SD/HD between sessions: leave and rejoin.
                            self.print("[%d.%d] still %sx%s after %.0fs, rejoining"
                                       % (sid, attempt, width, height, waited))
                            old, session = session, None
                            await asyncio.to_thread(old.stop)
                            while not queue.empty():
                                queue.get_nowait()
                            await asyncio.sleep(1)
                            await join()
                            continue
                    seen_key = True
                    muxer = TsMuxer(codec)
                    t0 = arrived
                    last_pts = -1
                    self.print("[%d.%d] streaming from %sx%s keyframe after %.1fs (wanted %sx%s)"
                               % (sid, attempt, width, height, time.time() - started, want_size[0], want_size[1]))
                if writer.is_closing():
                    reason = "client closed"
                    break
                pts = CLOCK + int((arrived - t0) * CLOCK)  # start at 1s so PCR (pts - 100ms) stays positive
                if pts <= last_pts:
                    pts = last_pts + 1
                last_pts = pts
                writer.write(muxer.mux(data, pts, is_key))
                await writer.drain()
                written += 1
        except (ConnectionResetError, BrokenPipeError):
            reason = "client disconnected"
        except Exception as e:
            reason = "error: %r" % e
            self.print("[%d] stream error: %r" % (sid, e))
        finally:
            self.writers.discard(writer)
            if session:
                try:
                    await asyncio.to_thread(session.stop)
                except Exception as e:
                    self.print("[%d] session stop error: %r" % (sid, e))
            try:
                writer.close()
            except Exception:
                pass
            self.print("[%d] stream ended (%s): %d frames written, %d dropped, %.0fs"
                       % (sid, reason, written, dropped, time.time() - started))
