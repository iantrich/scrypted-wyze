"""Wyze account login and Agora ("lake") stream credentials, via wyzeapy.

wyzeapy is pinned to the commit from SecKatie/wyzeapy PR #336 (jeffbryner's
solar-livestream branch), which adds the lake flow:
  get-streams(provider="lake") -> device/wakeup -> wcsa/create-connection
"""

import json
from typing import Any, Callable, Dict, List, Optional

from wyzeapy import Wyzeapy
from wyzeapy.exceptions import TwoFactorAuthenticationEnabled
from wyzeapy.services.camera_service import LAKE_API_MODELS
from wyzeapy.wyze_auth_lib import Token

TOKEN_KEY = "wyze_token"


class NeedsTwoFactor(Exception):
    pass


def _redact(value: Any, keep: int = 4) -> str:
    s = str(value)
    if len(s) <= keep * 2:
        return "*" * len(s)
    return "%s…%s (%d chars)" % (s[:keep], s[-keep:], len(s))


def summarize_stream_info(info: Dict[str, Any]) -> Dict[str, str]:
    """Safe-to-log view of get_stream_info output: shapes and lengths, not secrets."""
    secret_keys = {"rtc_token", "token", "encryption_key", "encryption_salt", "app_id"}
    out = {}
    for k, v in sorted(info.items()):
        if k in secret_keys:
            out[k] = _redact(v) if v else "(empty)"
        elif isinstance(v, (dict, list)):
            out[k] = json.dumps(v)[:200]
        else:
            out[k] = str(v)
    return out


class WyzeClient:
    def __init__(self, storage, log: Callable[..., None]):
        self.storage = storage
        self.log = log
        self.api: Optional[Wyzeapy] = None

    def _saved_token(self) -> Optional[Token]:
        raw = self.storage.getItem(TOKEN_KEY)
        if not raw:
            return None
        try:
            d = json.loads(raw)
            return Token(d["access_token"], d["refresh_token"], d.get("refresh_time"))
        except Exception:
            return None

    def _save_token(self, token: Token) -> None:
        self.storage.setItem(
            TOKEN_KEY,
            json.dumps(
                {
                    "access_token": token.access_token,
                    "refresh_token": token.refresh_token,
                    "refresh_time": token.refresh_time,
                }
            ),
        )

    def forget_token(self) -> None:
        self.storage.removeItem(TOKEN_KEY)

    async def login(self, email: str, password: str, key_id: str, api_key: str) -> None:
        """Log in, reusing a saved token when possible (Wyze rate-limits password logins)."""
        self.api = await Wyzeapy.create()
        self.api.register_for_token_callback(self._save_token)
        token = self._saved_token()
        if token:
            try:
                await self.api.login(email, password, key_id, api_key, token)
                self.log("Wyze: logged in with saved token")
                return
            except Exception as e:
                self.log("Wyze: saved token rejected (%s), logging in with password" % e)
                self.forget_token()
                self.api = await Wyzeapy.create()
                self.api.register_for_token_callback(self._save_token)
        try:
            await self.api.login(email, password, key_id, api_key)
        except TwoFactorAuthenticationEnabled:
            raise NeedsTwoFactor()
        # Not every login path fires the token callback, so save explicitly.
        self._save_token(self.api._auth_lib.token)
        self.log("Wyze: logged in with password")

    async def complete_two_factor(self, code: str) -> None:
        token = await self.api.login_with_2fa(code)
        self._save_token(token)
        self.log("Wyze: two-factor login complete")

    async def cameras(self) -> List[Any]:
        """Cameras wyzeapy recognizes, plus any Agora-model device it filed under another type."""
        from wyzeapy.services.camera_service import Camera

        service = await self.api.camera_service
        cameras = await service.get_cameras()
        macs = {c.mac for c in cameras}
        devices = await service.get_object_list()
        self.log("Wyze: all %d devices on the account:" % len(devices))
        for d in devices:
            self.log("  %s  type=%s  model=%s  mac=%s" % (
                getattr(d, "nickname", "?"), getattr(d, "product_type", "?"), d.product_model, d.mac))
            params = getattr(d, "device_params", None) or {}
            self.log("    ip=%r p2p_id=%s p2p_type=%r enr=%s" % (
                params.get("ip"), "set" if params.get("p2p_id") else "none", params.get("p2p_type"),
                "set" if getattr(d, "enr", None) else "none"))
            if d.product_model in LAKE_API_MODELS and d.mac not in macs:
                cameras.append(Camera(d.raw_dict))
        return cameras

    async def stream_info(self, camera, resolution: Optional[int] = None) -> Dict[str, Any]:
        service = await self.api.camera_service
        if resolution is None or not self.is_lake(camera):
            return await service.get_stream_info(camera)
        return await self._lake_stream_info(service, camera, resolution)

    async def request_resolution(self, camera, resolution: int) -> None:
        """Re-send create-connection while the camera is already streaming.

        Experiment: on a fresh wake the camera seems to ignore the resolution and start
        in SD; a create-connection sent once it is live appears to switch it.
        """
        import random

        service = await self.api.camera_service
        await service._create_rtc_connection(camera, random.randint(10000, 999999), resolution=resolution)

    async def _lake_stream_info(self, service, camera, resolution: int) -> Dict[str, Any]:
        """wyzeapy's _get_lake_stream_info (PR #336) with a caller-chosen create-connection resolution.

        Uses wyzeapy's private helpers; the web app always sends resolution=2.
        """
        import random

        from wyzeapy.crypto import xxtea_decrypt_b64
        from wyzeapy.exceptions import UnknownApiError

        uid = random.randint(10000, 999999)
        data = await service._get_camera_stream(camera, provider="lake")
        if "data" not in data or len(data["data"]) != 1:
            raise UnknownApiError("Unexpected get-streams response: " + str(data))
        data = data["data"][0]
        if data.get("property", {}).get("iot-device::iot-state") != 1:
            raise UnknownApiError("Camera is offline according to get-streams: " + str(data))
        await service._wakeup_device(camera, service._user_id_from_token(), uid)
        connection = await service._create_rtc_connection(camera, uid, resolution=resolution)

        params = dict(data["params"])
        params.update(connection["data"])
        params["provider"] = "lake"
        params["requested_resolution"] = resolution
        token = service._auth_lib.token.access_token
        for k in ("encryption_key", "encryption_salt"):
            if params.get(k):
                params[k] = xxtea_decrypt_b64(params[k], token)
        return params

    @staticmethod
    def is_lake(camera) -> bool:
        return camera.product_model in LAKE_API_MODELS
