"""Minimal MPEG-TS muxer for one H.264/H.265 video stream.

Agora hands us Annex B access units with no container. Feeding those to ffmpeg
as a raw elementary stream leaves ffmpeg to invent timestamps, which it does
badly while probing (non-monotonic DTS at startup). Wrapping each access unit
in a PES packet with our own PTS gives ffmpeg real timing.

Only what one video stream needs: PAT, PMT, PES with PTS (DTS == PTS, the
camera sends no B-frames), PCR on every frame, random access flag on keyframes.
"""

from typing import List

TS_SIZE = 188
PID_PMT = 0x1000
PID_VIDEO = 0x0100
STREAM_TYPES = {"H264": 0x1B, "H265": 0x24}
CLOCK = 90000


def _crc32_mpeg(data: bytes) -> int:
    crc = 0xFFFFFFFF
    for b in data:
        crc ^= b << 24
        for _ in range(8):
            crc = ((crc << 1) ^ 0x04C11DB7) if crc & 0x80000000 else (crc << 1)
            crc &= 0xFFFFFFFF
    return crc


def _pts_bytes(pts: int, marker: int = 0x2) -> bytes:
    pts &= (1 << 33) - 1
    return bytes([
        (marker << 4) | (((pts >> 30) & 0x07) << 1) | 1,
        (pts >> 22) & 0xFF,
        (((pts >> 15) & 0x7F) << 1) | 1,
        (pts >> 7) & 0xFF,
        ((pts & 0x7F) << 1) | 1,
    ])


def _pcr_bytes(pcr: int) -> bytes:
    base = pcr & ((1 << 33) - 1)
    v = (base << 15) | (0x3F << 9)  # 6 reserved bits, extension 0
    return v.to_bytes(6, "big")


class TsMuxer:
    def __init__(self, codec: str = "H265"):
        if codec not in STREAM_TYPES:
            raise ValueError("unsupported codec for MPEG-TS: %s" % codec)
        self.stream_type = STREAM_TYPES[codec]
        self.cc = {}

    def _next_cc(self, pid: int) -> int:
        c = self.cc.get(pid, -1)
        c = (c + 1) & 0x0F
        self.cc[pid] = c
        return c

    def _packet(self, pid: int, pusi: bool, payload: bytes, pcr: int = None, rai: bool = False) -> bytes:
        """One 188-byte packet; payload must fit after any adaptation field."""
        af_flags = 0
        af_opt = b""
        if pcr is not None:
            af_flags |= 0x10
            af_opt = _pcr_bytes(pcr)
        if rai:
            af_flags |= 0x40
        need_af = bool(af_flags)
        af = b""
        space = 184 - len(payload)
        if space < 0:
            raise ValueError("payload too large")
        if need_af or space > 0:
            if space == 1 and not need_af:
                af = b"\x00"
            else:
                body_len = space - 1  # adaptation_field_length excludes itself
                if body_len < 1 + len(af_opt):
                    raise ValueError("no room for adaptation field")
                af = bytes([body_len, af_flags]) + af_opt + b"\xff" * (body_len - 1 - len(af_opt))
        afc = 0x30 if af else 0x10
        header = bytes([
            0x47,
            (0x40 if pusi else 0) | ((pid >> 8) & 0x1F),
            pid & 0xFF,
            afc | self._next_cc(pid),
        ])
        pkt = header + af + payload
        assert len(pkt) == TS_SIZE, len(pkt)
        return pkt

    def _psi_packet(self, pid: int, payload: bytes) -> bytes:
        # PSI packets are padded with 0xFF in the payload, not with an adaptation field.
        header = bytes([0x47, 0x40 | ((pid >> 8) & 0x1F), pid & 0xFF, 0x10 | self._next_cc(pid)])
        return header + payload + b"\xff" * (184 - len(payload))

    def _pat(self) -> bytes:
        body = bytes([0x00, 0x01, 0xC1, 0x00, 0x00, 0x00, 0x01, 0xE0 | (PID_PMT >> 8), PID_PMT & 0xFF])
        table = bytes([0x00, 0xB0 | ((len(body) + 4) >> 8), (len(body) + 4) & 0xFF]) + body
        return self._psi_packet(0x0000, b"\x00" + table + _crc32_mpeg(table).to_bytes(4, "big"))

    def _pmt(self) -> bytes:
        body = bytes([
            0x00, 0x01, 0xC1, 0x00, 0x00,               # program 1, version 0, current
            0xE0 | (PID_VIDEO >> 8), PID_VIDEO & 0xFF,  # PCR PID
            0xF0, 0x00,                                 # program_info_length 0
            self.stream_type, 0xE0 | (PID_VIDEO >> 8), PID_VIDEO & 0xFF, 0xF0, 0x00,
        ])
        table = bytes([0x02, 0xB0 | ((len(body) + 4) >> 8), (len(body) + 4) & 0xFF]) + body
        return self._psi_packet(PID_PMT, b"\x00" + table + _crc32_mpeg(table).to_bytes(4, "big"))

    def mux(self, frame: bytes, pts: int, is_key: bool) -> bytes:
        """TS packets for one access unit. `pts` is in 90 kHz units and must increase."""
        out: List[bytes] = []
        if is_key:
            out.append(self._pat())
            out.append(self._pmt())
        pes = b"\x00\x00\x01\xE0\x00\x00" + bytes([0x80, 0x80, 0x05]) + _pts_bytes(pts) + frame
        pcr = max(pts - CLOCK // 10, 0)  # PCR 100ms ahead of presentation
        first_room = 184 - 8  # adaptation field: length + flags + 6 PCR bytes
        chunk = pes[:first_room]
        out.append(self._packet(PID_VIDEO, True, chunk, pcr=pcr, rai=is_key))
        pos = len(chunk)
        while pos < len(pes):
            chunk = pes[pos:pos + 184]
            out.append(self._packet(PID_VIDEO, False, chunk))
            pos += len(chunk)
        return b"".join(out)
