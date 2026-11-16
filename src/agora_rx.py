"""Receive a Wyze "lake" camera's encoded video over Agora RTC.

Stage 3a: join the channel with the credentials from wyze.py, collect encoded
frames for a while, and report what arrived (codec, resolution, keyframes), plus
save the raw elementary stream to disk for offline inspection.

All Agora calls are blocking ctypes calls; run this module's functions in a worker
thread. Agora's callbacks fire on SDK threads and must not call back into the SDK.
"""

import base64
import os
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import agora  # noqa: F401  (loads native libs; agora_probe restores the TLS default afterwards)
from agora.rtc.agora_base import (
    ChannelProfileType,
    ClientRoleType,
    EncryptionConfig,
    RtcConnectionPublishConfig,
    RTCConnConfig,
    VideoStreamType,
    VideoSubscriptionOptions,
)
from agora.rtc.agora_service import AgoraService, AgoraServiceConfig
from agora.rtc.local_user_observer import IRTCLocalUserObserver
from agora.rtc.rtc_connection_observer import IRTCConnectionObserver
from agora.rtc.video_encoded_frame_observer import IVideoEncodedFrameObserver

CODECS = {1: "VP8", 2: "H264", 3: "H265", 5: "VP9", 6: "GENERIC", 7: "GENERIC_H264", 12: "AV1", 20: "JPEG"}
FRAME_TYPES = {0: "blank", 1: "drop", 3: "key", 4: "delta", 5: "B"}
FILE_EXT = {"H264": "h264", "H265": "h265"}

_service: Optional[AgoraService] = None
_service_app_id: Optional[str] = None
_service_lock = threading.Lock()


def _data_dir() -> str:
    d = os.environ.get("SCRYPTED_PLUGIN_VOLUME") or "/tmp"
    path = os.path.join(d, "agora")
    os.makedirs(path, exist_ok=True)
    return path


def get_service(app_id: str) -> AgoraService:
    """One AgoraService per process; it is bound to a single app id."""
    global _service, _service_app_id
    with _service_lock:
        if _service is not None:
            if _service_app_id != app_id:
                raise RuntimeError("Agora service already initialized with a different app_id")
            return _service
        config = AgoraServiceConfig()
        config.appid = app_id
        config.enable_video = 1
        config.enable_audio_device = 0
        config.log_path = os.path.join(_data_dir(), "agorasdk.log")
        config.log_file_size_kb = 1024
        service = AgoraService()
        ret = service.initialize(config)
        if ret != 0:
            raise RuntimeError("AgoraService.initialize failed: %s" % ret)
        _service, _service_app_id = service, app_id
        return service


class _Events:
    """Timestamped event log shared by the observers (thread-safe)."""

    def __init__(self, log: Callable[[str], None]):
        self.start = time.time()
        self.lines: List[str] = []
        self.lock = threading.Lock()
        self.log = log

    def add(self, msg: str) -> None:
        line = "+%.1fs %s" % (time.time() - self.start, msg)
        with self.lock:
            self.lines.append(line)
        self.log("  agora: " + line)


class _ConnObserver(IRTCConnectionObserver):
    def __init__(self, ev: _Events):
        self.ev = ev
        self.connected = threading.Event()
        self.failed: Optional[str] = None

    def on_connected(self, conn, info, reason):
        self.ev.add("connected (reason=%s)" % reason)
        self.connected.set()

    def on_connecting(self, conn, info, reason):
        self.ev.add("connecting (reason=%s)" % reason)

    def on_disconnected(self, conn, info, reason):
        self.ev.add("disconnected (reason=%s)" % reason)

    def on_reconnecting(self, conn, info, reason):
        self.ev.add("reconnecting (reason=%s)" % reason)

    def on_connection_lost(self, conn, info):
        self.ev.add("connection lost")

    def on_connection_failure(self, conn, info, reason):
        self.failed = "connection failure (reason=%s)" % reason
        self.ev.add(self.failed)

    def on_user_joined(self, conn, user_id):
        self.ev.add("remote user joined: %s" % user_id)

    def on_user_left(self, conn, user_id, reason):
        self.ev.add("remote user left: %s (reason=%s)" % (user_id, reason))

    def on_token_privilege_did_expire(self, conn):
        self.ev.add("token expired")

    def on_encryption_error(self, conn, error_type):
        self.ev.add("ENCRYPTION ERROR type=%s" % error_type)

    def on_error(self, conn, error_code, error_msg):
        self.ev.add("error %s: %s" % (error_code, error_msg))


class _UserObserver(IRTCLocalUserObserver):
    def __init__(self, ev: _Events):
        self.ev = ev

    def on_user_video_track_subscribed(self, local_user, user_id, info, track):
        self.ev.add("video track subscribed from %s" % user_id)

    def on_user_video_track_state_changed(self, local_user, user_id, track, state, reason, elapsed):
        self.ev.add("video track %s state=%s reason=%s" % (user_id, state, reason))

    def on_video_subscribe_state_changed(self, local_user, channel, user_id, old_state, new_state, elapsed):
        self.ev.add("video subscribe %s: %s -> %s" % (user_id, old_state, new_state))

    def on_first_remote_video_frame(self, local_user, user_id, width, height, elapsed):
        self.ev.add("first remote video frame from %s %sx%s" % (user_id, width, height))

    def on_user_audio_track_subscribed(self, local_user, user_id, track):
        self.ev.add("audio track available from %s" % user_id)


class _FrameCollector(IVideoEncodedFrameObserver):
    def __init__(self, ev: _Events, save_path_base: str, save_seconds: float):
        self.ev = ev
        self.lock = threading.Lock()
        self.frames = 0
        self.keyframes = 0
        self.bytes = 0
        self.first_frame_at: Optional[float] = None
        self.first_key_at: Optional[float] = None
        self.codec: Optional[str] = None
        self.size: Optional[str] = None
        self.uids = set()
        self.sizes_by_uid: Dict[int, set] = {}
        self.size_history: List[str] = []  # "WxH@+Ns" each time the size changes
        self.annexb: Optional[bool] = None
        self.save_path_base = save_path_base
        self.save_until: Optional[float] = None
        self.save_seconds = save_seconds
        self.save_path: Optional[str] = None
        self._file = None

    def on_encoded_video_frame(self, uid, image_buffer, length, info):
        try:
            now = time.time()
            codec = CODECS.get(info.codec_type, str(info.codec_type))
            is_key = info.frame_type == 3
            with self.lock:
                self.frames += 1
                self.bytes += length
                self.uids.add(uid)
                if self.first_frame_at is None:
                    self.first_frame_at = now
                    self.codec = codec
                    self.ev.add("first encoded frame: uid=%s codec=%s %sx%s type=%s %dB"
                                % (uid, codec, info.width, info.height,
                                   FRAME_TYPES.get(info.frame_type, info.frame_type), length))
                if info.width and info.height:
                    size = "%sx%s" % (info.width, info.height)
                    if size != self.size:
                        self.size_history.append("%s@+%.1fs" % (size, now - self.ev.start))
                    self.size = size
                    self.sizes_by_uid.setdefault(uid, set()).add(self.size)
                if is_key:
                    self.keyframes += 1
                    if self.first_key_at is None:
                        self.first_key_at = now
                        self.annexb = bytes(image_buffer[:4]) in (b"\x00\x00\x00\x01",) or bytes(image_buffer[:3]) == b"\x00\x00\x01"
                        self.ev.add("first keyframe: %dB, head=%s" % (length, bytes(image_buffer[:16]).hex()))
                        ext = FILE_EXT.get(codec)
                        if ext:
                            self.save_path = "%s.%s" % (self.save_path_base, ext)
                            self._file = open(self.save_path, "wb")
                            self.save_until = now + self.save_seconds
                # Save from the first keyframe on, so the file is decodable.
                if self._file:
                    if now <= self.save_until:
                        self._file.write(image_buffer)
                    else:
                        self._file.close()
                        self._file = None
        except Exception as e:  # never let an exception escape into the SDK thread
            self.ev.add("frame callback error: %r" % e)
        return 1

    def close(self):
        with self.lock:
            if self._file:
                self._file.close()
                self._file = None


def receive_test(info: Dict[str, Any], seconds: float, log: Callable[[str], None]) -> Dict[str, Any]:
    """Join the camera's Agora channel, collect encoded video for `seconds`, then leave.

    `info` is wyzeapy's get_stream_info output for a lake camera.
    Blocking; call from a worker thread.
    """
    ev = _Events(log)
    service = get_service(info["app_id"])

    conn_config = RTCConnConfig(
        auto_subscribe_audio=0,
        auto_subscribe_video=1,
        client_role_type=ClientRoleType.CLIENT_ROLE_BROADCASTER,
        channel_profile=ChannelProfileType.CHANNEL_PROFILE_LIVE_BROADCASTING,
    )
    publish_config = RtcConnectionPublishConfig(is_publish_audio=False, is_publish_video=False)
    conn = service.create_rtc_connection(conn_config, publish_config)
    if conn is None:
        raise RuntimeError("create_rtc_connection returned None")

    conn_obs = _ConnObserver(ev)
    user_obs = _UserObserver(ev)
    collector = _FrameCollector(ev, os.path.join(_data_dir(), "sample"), save_seconds=10)
    try:
        conn.register_observer(conn_obs)
        conn.register_local_user_observer(user_obs)

        key = info.get("encryption_key")
        if key:
            salt = base64.b64decode(info["encryption_salt"]) if info.get("encryption_salt") else None
            mode = int(info.get("encryption_mode") or 7)
            ret = conn.enable_encryption(1, EncryptionConfig(
                encryption_mode=mode, encryption_key=key, encryption_kdf_salt=bytearray(salt or b"")))
            ev.add("enable_encryption(mode=%s, salt=%dB) -> %s" % (mode, len(salt or b""), ret))

        local_user = conn.get_local_user()
        ret = local_user.subscribe_all_video(
            VideoSubscriptionOptions(type=VideoStreamType.VIDEO_STREAM_HIGH, encodedFrameOnly=True))
        ev.add("subscribe_all_video(encodedFrameOnly) -> %s" % ret)
        ret = conn.register_video_encoded_frame_observer(collector)
        ev.add("register_video_encoded_frame_observer -> %s" % ret)

        ret = conn.connect(info["rtc_token"], info["channel"], str(info["uid"]))
        ev.add("connect(channel, uid=%s) -> %s" % (info["uid"], ret))

        deadline = time.time() + seconds
        while time.time() < deadline and not conn_obs.failed:
            time.sleep(0.5)
    finally:
        collector.close()
        try:
            conn.disconnect()
        finally:
            conn.release()
        ev.add("left channel")

    elapsed = (time.time() - collector.first_frame_at) if collector.first_frame_at else 0
    return {
        "ok": collector.keyframes > 0,
        "connected": conn_obs.connected.is_set(),
        "failure": conn_obs.failed,
        "frames": collector.frames,
        "keyframes": collector.keyframes,
        "fps": round(collector.frames / elapsed, 1) if elapsed > 1 else None,
        "kbps": round(collector.bytes * 8 / 1000 / elapsed) if elapsed > 1 else None,
        "codec": collector.codec,
        "size": collector.size,
        "uids": sorted(collector.uids),
        "sizes_by_uid": {u: sorted(v) for u, v in collector.sizes_by_uid.items()},
        "size_history": collector.size_history,
        "annexb": collector.annexb,
        "first_frame_after_s": round(collector.first_frame_at - ev.start, 1) if collector.first_frame_at else None,
        "saved": collector.save_path,
        "events": ev.lines,
    }


class _FrameForwarder(IVideoEncodedFrameObserver):
    """Hands each encoded frame to `on_frame(data, is_key, codec)` (called on the SDK thread)."""

    def __init__(self, ev: _Events, on_frame: Callable[[bytes, bool, str, int, int], None]):
        self.ev = ev
        self.on_frame = on_frame
        self.frames = 0
        self.last_size = None

    def on_encoded_video_frame(self, uid, image_buffer, length, info):
        try:
            self.frames += 1
            size = (info.width, info.height)
            if self.frames == 1:
                self.ev.add("first frame: codec=%s %sx%s" % (
                    CODECS.get(info.codec_type, info.codec_type), info.width, info.height))
            elif size != self.last_size and all(size):
                self.ev.add("frame size changed %sx%s -> %sx%s at frame %d (%s)" % (
                    self.last_size[0], self.last_size[1], size[0], size[1], self.frames,
                    FRAME_TYPES.get(info.frame_type, info.frame_type)))
            if all(size):
                self.last_size = size
            self.on_frame(image_buffer, info.frame_type == 3, CODECS.get(info.codec_type, str(info.codec_type)),
                          info.width, info.height)
        except Exception as e:
            self.ev.add("frame callback error: %r" % e)
        return 1


class AgoraSession:
    """A live Agora subscription to one camera. start()/stop() block; call them from a worker thread."""

    def __init__(self, info: Dict[str, Any], on_frame: Callable[[bytes, bool, str, int, int], None], log: Callable[[str], None]):
        self.info = info
        self.ev = _Events(log)
        self.conn = None
        self.conn_obs = _ConnObserver(self.ev)
        self.user_obs = _UserObserver(self.ev)
        self.forwarder = _FrameForwarder(self.ev, on_frame)

    def start(self) -> None:
        info = self.info
        service = get_service(info["app_id"])
        conn = service.create_rtc_connection(
            RTCConnConfig(
                auto_subscribe_audio=0,
                auto_subscribe_video=1,
                client_role_type=ClientRoleType.CLIENT_ROLE_BROADCASTER,
                channel_profile=ChannelProfileType.CHANNEL_PROFILE_LIVE_BROADCASTING,
            ),
            RtcConnectionPublishConfig(is_publish_audio=False, is_publish_video=False),
        )
        if conn is None:
            raise RuntimeError("create_rtc_connection returned None")
        self.conn = conn
        conn.register_observer(self.conn_obs)
        conn.register_local_user_observer(self.user_obs)
        if info.get("encryption_key"):
            salt = base64.b64decode(info["encryption_salt"]) if info.get("encryption_salt") else b""
            conn.enable_encryption(1, EncryptionConfig(
                encryption_mode=int(info.get("encryption_mode") or 7),
                encryption_key=info["encryption_key"],
                encryption_kdf_salt=bytearray(salt)))
        conn.get_local_user().subscribe_all_video(
            VideoSubscriptionOptions(type=VideoStreamType.VIDEO_STREAM_HIGH, encodedFrameOnly=True))
        conn.register_video_encoded_frame_observer(self.forwarder)
        ret = conn.connect(info["rtc_token"], info["channel"], str(info["uid"]))
        self.ev.add("connect -> %s" % ret)
        if ret != 0:
            raise RuntimeError("Agora connect failed: %s" % ret)

    def stop(self) -> None:
        conn, self.conn = self.conn, None
        if conn is None:
            return
        try:
            conn.disconnect()
        finally:
            conn.release()
        self.ev.add("left channel after %d frames" % self.forwarder.frames)

    @property
    def failed(self) -> Optional[str]:
        return self.conn_obs.failed
