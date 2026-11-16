"""Wyze for Scrypted, prototype.

Stage 1: Agora SDK load probe (agora_probe.py), runs at startup.
Stage 2: Wyze login and Agora stream credentials for "lake" cameras (wyze.py),
         run from the Settings page.

Everything is surfaced through this plugin's Settings and console.
"""

import asyncio
import traceback
from typing import Any, Dict, List, Optional

import scrypted_sdk
from scrypted_sdk import Setting, SettingValue

from agora_probe import run_probe
from camera import WyzeAgoraCamera
from wyze import NeedsTwoFactor, WyzeClient, summarize_stream_info

CREDENTIAL_KEYS = ["email", "password", "key_id", "api_key"]
DEFAULT_RESOLUTION = 1  # 2K on the Solar Cam Pan; Wyze's web app sends 2 (360p)


class WyzePlugin(scrypted_sdk.ScryptedDeviceBase, scrypted_sdk.Settings, scrypted_sdk.DeviceProvider):
    def __init__(self, nativeId: str = None):
        super().__init__(nativeId)
        self.probe_result: Optional[Dict[str, Any]] = None
        self.busy: Optional[str] = None
        self.wyze = WyzeClient(self.storage, self.print)
        self.logged_in = False
        self.needs_2fa = False
        self.cameras: List[Any] = []
        self.stream_summary: Optional[Dict[str, str]] = None
        self.last_error: Optional[str] = None
        self.rx_result: Optional[Dict[str, Any]] = None
        self.devices: Dict[str, WyzeAgoraCamera] = {}
        self.streaming_paused = False
        self.sweep_result: Optional[List[str]] = None
        asyncio.get_event_loop().create_task(self._startup())

    async def _startup(self) -> None:
        await self.run_agora_probe()
        if self._creds():
            await self.wyze_login()

    # ---- helpers -------------------------------------------------------

    async def _settings_changed(self) -> None:
        await self.onDeviceEvent(scrypted_sdk.ScryptedInterface.Settings.value, None)

    async def _task(self, label: str, coro) -> None:
        if self.busy:
            self.print("Busy with %s, ignoring %s" % (self.busy, label))
            return
        self.busy = label
        self.last_error = None
        await self._settings_changed()
        try:
            await coro
        except Exception as e:
            self.last_error = "%s failed: %r" % (label, e)
            self.print(self.last_error)
            self.print(traceback.format_exc())
        finally:
            self.busy = None
            await self._settings_changed()

    def _creds(self) -> Optional[List[str]]:
        values = [self.storage.getItem(k) or "" for k in CREDENTIAL_KEYS]
        return values if all(values) else None

    def _camera_by_mac(self, mac: str):
        return next((c for c in self.cameras if c.mac == mac), None)

    # ---- stage 1 ---------------------------------------------------------

    async def run_agora_probe(self) -> None:
        async def go():
            self.print("Agora probe starting...")
            self.probe_result = await asyncio.to_thread(run_probe)
            r = self.probe_result
            self.print("==== Agora probe: %s (%ss) ====" % ("PASS" if r["ok"] else "FAIL", r["seconds"]))
            for k, v in r["env"].items():
                self.print("  %s: %s" % (k, v))
            self.print("  sdk_url: %s" % r.get("sdk_url", "-"))
            for s in r["steps"]:
                self.print("  " + s)
            if r.get("traceback"):
                self.print(r["traceback"])

        await self._task("Agora probe", go())

    # ---- stage 2 ---------------------------------------------------------

    async def wyze_login(self) -> None:
        async def go():
            creds = self._creds()
            if not creds:
                raise Exception("Fill in email, password, key ID and API key first")
            self.logged_in = False
            try:
                await self.wyze.login(*creds)
            except NeedsTwoFactor:
                self.needs_2fa = True
                self.print("Wyze: two-factor code required; enter it in Settings")
                return
            self.needs_2fa = False
            await self._after_login()

        await self._task("Wyze login", go())

    async def wyze_2fa(self, code: str) -> None:
        async def go():
            await self.wyze.complete_two_factor(code.strip())
            self.needs_2fa = False
            await self._after_login()

        await self._task("Wyze 2FA", go())

    async def _after_login(self) -> None:
        self.logged_in = True
        self.cameras = await self.wyze.cameras()
        self.print("Wyze: %d camera(s)" % len(self.cameras))
        for c in self.cameras:
            self.print(
                "  %s  model=%s  mac=%s  %s"
                % (c.nickname, c.product_model, c.mac, "AGORA (lake)" if WyzeClient.is_lake(c) else "kinesis")
            )
        lake = [c for c in self.cameras if WyzeClient.is_lake(c)]
        if lake and not self._camera_by_mac(self.storage.getItem("camera") or ""):
            self.storage.setItem("camera", lake[0].mac)
        for c in lake:
            await scrypted_sdk.deviceManager.onDeviceDiscovered({
                "nativeId": c.mac,
                "name": c.nickname,
                "type": scrypted_sdk.ScryptedDeviceType.Camera.value,
                "interfaces": [scrypted_sdk.ScryptedInterface.VideoCamera.value],
                "info": {"manufacturer": "Wyze", "model": c.product_model, "mac": c.mac},
            })

    # ---- stage 3b: devices ---------------------------------------------

    async def getDevice(self, nativeId: str) -> Any:
        if nativeId not in self.devices:
            self.devices[nativeId] = WyzeAgoraCamera(nativeId, self)
        return self.devices[nativeId]

    async def releaseDevice(self, id: str, nativeId: str) -> None:
        self.devices.pop(nativeId, None)

    async def wait_for_camera(self, mac: str, timeout: float = 60):
        """The Wyze camera object for `mac`, waiting for the startup login if needed."""
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            camera = self._camera_by_mac(mac) if self.logged_in else None
            if camera:
                return camera
            if asyncio.get_event_loop().time() > deadline:
                raise Exception("Wyze login not ready, or camera %s not on the account" % mac)
            await asyncio.sleep(1)

    async def fetch_stream_info(self) -> None:
        async def go():
            if not self.logged_in:
                raise Exception("Log in to Wyze first")
            camera = self._camera_by_mac(self.storage.getItem("camera") or "")
            if not camera:
                raise Exception("Pick a camera first")
            self.print("Wyze: requesting lake stream for %s (this wakes the camera)..." % camera.nickname)
            info = await self.wyze.stream_info(camera)
            self.stream_summary = summarize_stream_info(info)
            self.print("Wyze: stream info (secrets redacted):")
            for k, v in self.stream_summary.items():
                self.print("  %s: %s" % (k, v))
            # Sanity checks from PR #336: key is 32 chars, salt is base64 of 32 bytes.
            key, salt = info.get("encryption_key"), info.get("encryption_salt")
            self.print("  check: provider=%s key_len=%s salt_len=%s" % (
                info.get("provider"), len(key) if key else None, len(salt) if salt else None))

        await self._task("Fetch stream info", go())

    # ---- stage 3a ------------------------------------------------------

    async def receive_test(self, seconds: float = 20) -> None:
        async def go():
            if not self.logged_in:
                raise Exception("Log in to Wyze first")
            camera = self._camera_by_mac(self.storage.getItem("camera") or "")
            if not camera:
                raise Exception("Pick a camera first")
            self.print("Receive test: fetching fresh credentials for %s (wakes the camera)..." % camera.nickname)
            info = await self.wyze.stream_info(camera, self.resolution())
            self.print("Receive test: joining Agora channel for %ss..." % seconds)
            import agora_rx  # imported lazily so a load failure can't break the rest of the plugin

            r = await asyncio.to_thread(agora_rx.receive_test, info, seconds, self.print)
            self.rx_result = r
            self.print("==== Receive test: %s ====" % ("PASS" if r["ok"] else "FAIL"))
            for k, v in r.items():
                if k != "events":
                    self.print("  %s: %s" % (k, v))

        await self._task("Receive test", go())

    def resolution(self) -> int:
        """create-connection resolution. Sweep on ME_WCO3: odd = 2304x1296, even = 640x360."""
        v = self.storage.getItem("resolution")
        return int(v) if v not in (None, "", "default") else DEFAULT_RESOLUTION

    async def resolution_sweep(self, values=None, seconds: float = 10) -> None:
        """Try each create-connection resolution value and record what the camera actually sends."""
        async def go():
            camera = await self.wait_for_camera(self.storage.getItem("camera") or "")
            import agora_rx

            seq = values or [int(v) for v in (self.storage.getItem("sweep_values") or "0,1,2,3,4,5").split(",") if v.strip()]
            self.print("Resolution sweep: %s" % seq)

            self.streaming_paused = True
            for d in self.devices.values():
                d.close_streams()
            await asyncio.sleep(3)  # let live sessions leave the channel
            lines = []
            try:
                for i, r in enumerate(seq):
                    try:
                        info = await self.wyze.stream_info(camera, r)
                    except Exception as e:
                        lines.append("resolution=%s: Wyze error %r" % (r, e))
                        self.print(lines[-1])
                        continue
                    rx = await asyncio.to_thread(agora_rx.receive_test, info, seconds, lambda m: None)
                    lines.append("#%d resolution=%s: %s sizes=%s fps=%s kbps=%s first_frame=%ss" % (
                        i + 1, r, "ok" if rx["ok"] else "NO VIDEO", " -> ".join(rx["size_history"]) or "-",
                        rx["fps"], rx["kbps"], rx["first_frame_after_s"]))
                    self.print(lines[-1])
                    await asyncio.sleep(2)
            finally:
                self.streaming_paused = False
                self.sweep_result = lines
                self.print("==== Resolution sweep done ====")
                for line in lines:
                    self.print("  " + line)

        await self._task("Resolution sweep", go())

    # ---- Settings ------------------------------------------------------

    async def getSettings(self) -> List[Setting]:
        s: List[Setting] = []
        status = "Busy: %s" % self.busy if self.busy else (self.last_error or "Idle")
        s.append({"key": "status", "title": "Status", "value": status, "readonly": True})

        g = "Wyze account"
        s.append({"group": g, "key": "email", "title": "Email", "value": self.storage.getItem("email") or ""})
        s.append({"group": g, "key": "password", "title": "Password", "type": "password", "value": self.storage.getItem("password") or ""})
        s.append({"group": g, "key": "key_id", "title": "API Key ID", "value": self.storage.getItem("key_id") or "",
                  "description": "From developer-api-console.wyze.com"})
        s.append({"group": g, "key": "api_key", "title": "API Key", "type": "password", "value": self.storage.getItem("api_key") or ""})
        s.append({"group": g, "key": "login", "title": "Log in", "type": "button"})
        if self.needs_2fa:
            s.append({"group": g, "key": "two_factor", "title": "Two-factor code", "value": "",
                      "description": "Enter the code Wyze sent you, then save"})
        s.append({"group": g, "key": "logged_in", "title": "Logged in", "value": "yes" if self.logged_in else "no", "readonly": True})

        if self.cameras:
            g = "Stream credentials"
            choices = ["%s | %s | %s" % (c.mac, c.nickname, c.product_model) for c in self.cameras]
            mac = self.storage.getItem("camera") or ""
            current = next((ch for ch in choices if ch.startswith(mac + " |")), None) if mac else None
            s.append({"group": g, "key": "camera", "title": "Camera", "choices": choices, "value": current})
            s.append({"group": g, "key": "fetch", "title": "Fetch Agora credentials", "type": "button",
                      "description": "Runs get-streams(lake) → wakeup → create-connection"})
            if self.stream_summary:
                s.append({"group": g, "key": "stream_summary", "title": "Last result (redacted)", "type": "textarea", "readonly": True,
                          "value": "\n".join("%s: %s" % kv for kv in self.stream_summary.items())})
            s.append({"group": g, "key": "resolution", "title": "Requested resolution",
                      "choices": ["0", "1", "2", "3", "4", "5"], "value": str(self.resolution()),
                      "description": "create-connection 'resolution' value. On the Solar Cam Pan odd = 2304x1296, even = 640x360 (Wyze's web app sends 2)."})
            s.append({"group": g, "key": "sweep_values", "title": "Sweep sequence",
                      "value": self.storage.getItem("sweep_values") or "0,1,2,3,4,5",
                      "description": "Comma-separated resolution values, 10s each"})
            s.append({"group": g, "key": "sweep", "title": "Run resolution sweep", "type": "button",
                      "description": "Pauses the live stream and tries each value in the sequence"})
            if self.sweep_result:
                s.append({"group": g, "key": "sweep_result", "title": "Sweep result", "type": "textarea", "readonly": True,
                          "value": "\n".join(self.sweep_result)})
            s.append({"group": g, "key": "receive_test", "title": "Receive test (20s)", "type": "button",
                      "description": "Joins the Agora channel and records what video arrives"})
            if self.rx_result:
                r = self.rx_result
                summary = ["%s: %s" % (k, v) for k, v in r.items() if k != "events"] + [""] + r["events"]
                s.append({"group": g, "key": "rx_result", "title": "Receive test result (%s)" % ("PASS" if r["ok"] else "FAIL"),
                          "type": "textarea", "readonly": True, "value": "\n".join(summary)})

        g = "Agora probe"
        r = self.probe_result
        s.append({"group": g, "key": "probe_status", "title": "Result", "readonly": True,
                  "value": "Not run" if not r else ("PASS" if r["ok"] else "FAIL")})
        if r:
            s.append({"group": g, "key": "probe_steps", "title": "Steps", "type": "textarea", "readonly": True,
                      "value": "\n".join(["%s: %s" % kv for kv in r["env"].items()] + r["steps"])})
        s.append({"group": g, "key": "rerun_probe", "title": "Run probe again", "type": "button"})
        return s

    async def putSetting(self, key: str, value: SettingValue) -> None:
        if key in CREDENTIAL_KEYS:
            self.storage.setItem(key, str(value or "").strip())
            if key in ("email", "password"):
                self.wyze.forget_token()
            await self._settings_changed()
        elif key == "login":
            asyncio.ensure_future(self.wyze_login())
        elif key == "two_factor":
            asyncio.ensure_future(self.wyze_2fa(str(value)))
        elif key == "camera":
            self.storage.setItem("camera", str(value).split(" | ")[0])
            await self._settings_changed()
        elif key == "fetch":
            asyncio.ensure_future(self.fetch_stream_info())
        elif key == "resolution":
            self.storage.setItem("resolution", str(value))
            for d in self.devices.values():
                d.close_streams()  # Rebroadcast reconnects with the new value
            await self._settings_changed()
        elif key == "sweep_values":
            self.storage.setItem("sweep_values", str(value or "").replace(" ", ""))
            await self._settings_changed()
        elif key == "sweep":
            asyncio.ensure_future(self.resolution_sweep())
        elif key == "receive_test":
            asyncio.ensure_future(self.receive_test())
        elif key == "rerun_probe":
            asyncio.ensure_future(self.run_agora_probe())


def create_scrypted_plugin():
    return WyzePlugin()
