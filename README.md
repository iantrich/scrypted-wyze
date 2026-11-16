# scrypted-wyze

Scrypted plugin (prototype) for Wyze cameras that stream over Agora, Wyze's
"lake" provider. Verified with the Wyze Solar Cam Pan (`ME_WCO3`).

Older Wyze cameras that speak TUTK P2P locally (Cam v3, Pan v3, ...) are better
served by [scrypted-wyze-native](https://github.com/apocaliss92/scrypted-wyze-native);
this plugin only handles Agora cameras, which have no local IP or TUTK endpoint.

## How it works

1. **Wyze login** ([src/wyze.py](src/wyze.py)): wyzeapy, pinned to the commit from
   SecKatie/wyzeapy PR #336, which adds the lake flow: `get-streams(provider=lake)`
   → `device/wakeup` → `wcsa/create-connection`. Returns the Agora app id, channel,
   token, and the XXTEA-decrypted encryption key and salt. The token is cached in
   plugin storage so restarts don't re-send the password.
2. **Agora receive** ([src/agora_rx.py](src/agora_rx.py)): Agora's Python server SDK
   joins the channel with AES-128-GCM2 encryption and subscribes to encoded frames
   (H.265 Annex B from the Solar Cam Pan, 2304x1296 @ 20 fps).
3. **Scrypted camera** ([src/camera.py](src/camera.py)): each lake camera is a
   `VideoCamera`. `getVideoStream` returns an FFmpeg input reading MPEG-TS from a
   localhost TCP port; each client gets its own Agora session. Frames are
   timestamped on arrival and muxed by [src/ts.py](src/ts.py).

## Develop

```bash
npm install
npx scrypted login <scrypted-ip>
npm run scrypted-deploy <scrypted-ip>
```

Then set Wyze email, password, API Key ID and API Key (from
developer-api-console.wyze.com) in the plugin's Settings.

## Known quirks

- **Resolution:** the `resolution` value in create-connection doesn't reliably pick
  the stream size; the camera has alternated SD/HD between sessions. The stream
  waits for a keyframe at the requested size, re-sends create-connection, then
  rejoins (up to 3 tries) before accepting whatever arrives.
- **Token lifetime:** Wyze issues the rtc token for 3600 s; sessions end at 55 min
  and Rebroadcast reconnects.
- **TLS:** importing `agora` disables TLS verification process-wide; the plugin
  restores the default right after (see [src/agora_probe.py](src/agora_probe.py)).
- **cffi:** pinned to 1.16.0 to match the `_cffi_backend` Scrypted's runtime loads.
- **Agora's native SDK** is proprietary and is downloaded by the `agora` package on
  first import, never bundled.
- Agora's SDK logs a harmless `TypeError` traceback from its own capabilities
  callback on connect.

## Not done yet

HomeKit, audio, motion events, snapshots, battery/spotlight, and removing the
stage 1–3 test buttons.
