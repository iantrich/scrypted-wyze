"""Agora SDK load probe (stage 1): does Agora's native SDK load in this Scrypted server?"""

import ctypes
import os
import platform
import ssl
import sys
import time
import traceback
from typing import Any, Dict, List

# The agora package replaces this process-wide with an unverified context on import.
# Keep the original so we can put TLS verification back afterwards.
_ORIGINAL_HTTPS_CONTEXT = ssl._create_default_https_context

# Libraries the SDK loads (or tries to) on Linux, in its own load order.
LINUX_LIBS = [
    "libaosl.so",
    "libagora-fdkaac.so",
    "libagora-ffmpeg.so",
    "libagora-soundtouch.so",
    "libagora_rtc_sdk.so",
    "libagora_ai_noise_suppression_extension.so",
]


def _cpu_features() -> str:
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.lower().startswith("features"):
                    return line.split(":", 1)[1].strip()
    except Exception as e:
        return "unavailable: %s" % e
    return "not found"


def _environment() -> Dict[str, str]:
    libc = platform.libc_ver()
    return {
        "machine": platform.machine(),
        "system": "%s %s" % (platform.system(), platform.release()),
        "libc": "%s %s" % libc if libc[0] else "unknown",
        "python": "%s (%s)" % (platform.python_version(), sys.executable),
        "cpu_features": _cpu_features(),
    }


def run_probe() -> Dict[str, Any]:
    """Blocking: runs in a worker thread so a slow SDK download can't stall the plugin."""
    result: Dict[str, Any] = {"env": _environment(), "steps": [], "ok": False}
    steps: List[str] = result["steps"]

    start = time.time()
    try:
        # The SDK calls sys.exit(1) if the library is missing after download,
        # so catch BaseException rather than letting it take the plugin down.
        import agora  # noqa: F401  (triggers download + libaosl load)

        steps.append("import agora: ok (%.1fs)" % (time.time() - start))
        lib_dir = agora.sdk_library_dir
        result["lib_dir"] = lib_dir
        try:
            with open(os.path.join(agora.sdk_root_dir, "version.txt")) as f:
                result["sdk_url"] = f.read().strip()
        except Exception:
            result["sdk_url"] = "unknown"

        for name in LINUX_LIBS:
            path = os.path.join(lib_dir, name)
            if not os.path.exists(path):
                steps.append("%s: not present" % name)
                continue
            try:
                ctypes.CDLL(path)
                steps.append("%s: loaded" % name)
            except OSError as e:
                steps.append("%s: FAILED %s" % (name, e))

        import agora.rtc  # noqa: F401  (the SDK's own RTC binding load)
        from agora.rtc.agora_service import AgoraService  # noqa: F401

        steps.append("import agora.rtc.agora_service: ok")
        result["ok"] = not any("FAILED" in s for s in steps)
    except BaseException as e:
        steps.append("FAILED: %r" % e)
        result["traceback"] = traceback.format_exc()
    finally:
        ssl._create_default_https_context = _ORIGINAL_HTTPS_CONTEXT

    result["seconds"] = round(time.time() - start, 1)
    return result
