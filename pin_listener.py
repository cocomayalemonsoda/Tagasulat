"""
pin_listener.py — TikTok Live pin listener for Tagasulat.

Uses the TikTokLive library for connection (handles sign server authentication).
Reads pin event data via direct betterproto attributes (v6 API).

Requirements:
    pip install TikTokLive websockets

Usage in Tagasulat:
    from pin_listener import PinListener

    def on_pin(username, nickname, comment):
        api.flash_capture(username=username, nickname=nickname)

    listener = PinListener("xiaoclothingph", callback=on_pin,
                           on_status=lambda s: print(s))
    listener.start()
    listener.stop()
"""

__version__ = "2026-06-02"

import threading
import asyncio
from typing import Callable, Optional



class PinListener:
    """
    TikTok Live pin event listener.

    Connects to the given host's live room and calls `callback` every time
    a comment is pinned. Uses TikTokLive library for the connection layer
    (handles TikTok's sign server requirement) but reads event data directly
    from raw dicts to avoid the nickName/nick_name breakage.

    on_status(status: str) is called with:
        "connecting"   — attempting to connect
        "connected"    — successfully connected to live room
        "disconnected" — connection lost, will retry
        "not_live"     — host is not currently live
        "unconfigured" — no username provided
    """

    RECONNECT_DELAY = 5.0

    def __init__(
        self,
        host_username:  str,
        callback:       Callable,
        on_status:      Optional[Callable] = None,
        session_id:     Optional[str] = None,
        verbose:        bool = True,
    ):
        self.host_username = host_username.lstrip("@")
        self.callback      = callback
        self.on_status     = on_status
        self.session_id    = session_id
        self.verbose       = verbose
        self._running      = False
        self._thread       = None
        self._loop         = None
        self._client       = None

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(target=self._thread_main, daemon=True)
        self._thread.start()
        self._log(f"started for @{self.host_username}")

    def stop(self):
        self._running = False
        if self._client:
            try:
                # Schedule disconnect on the event loop
                if self._loop and self._loop.is_running():
                    asyncio.run_coroutine_threadsafe(
                        self._client.disconnect(), self._loop
                    )
            except Exception:
                pass
        self._log("stopped")

    def _log(self, msg: str):
        if self.verbose:
            print(f"  [pin_listener] {msg}")

    def _push_status(self, status: str):
        if self.on_status:
            try:
                self.on_status(status)
            except Exception:
                pass

    def _thread_main(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._async_main())
        except Exception as e:
            self._log(f"thread error: {e}")

    async def _async_main(self):
        while self._running:
            try:
                await self._connect_and_listen()
            except Exception as e:
                self._log(f"error: {e}")
                self._push_status("disconnected")
            if self._running:
                self._log(f"reconnecting in {self.RECONNECT_DELAY}s...")
                await asyncio.sleep(self.RECONNECT_DELAY)

    async def _connect_and_listen(self):
        try:
            from TikTokLive import TikTokLiveClient
            from TikTokLive.events import ConnectEvent, DisconnectEvent, RoomPinEvent
        except ImportError as e:
            self._log(f"TikTokLive not installed: {e}")
            self._log("Run: pip install TikTokLive")
            self._push_status("unconfigured")
            self._running = False
            return

        self._push_status("connecting")
        self._log(f"connecting to @{self.host_username}...")

        client = TikTokLiveClient(f"@{self.host_username}")
        if self.session_id:
            # v6 API: session cookie is set on the web client, not the constructor
            client.web.set_session(self.session_id, None)
        self._client = client

        @client.on(ConnectEvent)
        async def on_connect(event: ConnectEvent):
            self._log(f"✅ connected to room {event.room_id}")
            self._push_status("connected")

        @client.on(DisconnectEvent)
        async def on_disconnect(event: DisconnectEvent):
            self._log("disconnected")
            self._push_status("disconnected")

        @client.on(RoomPinEvent)
        async def on_pin(event: RoomPinEvent):
            # action=1 → pin a comment, action=2 → unpin/clear (no user data)
            if event.action != 1:
                self._log(f"pin action={event.action} (unpin/clear), skipping")
                return

            self._log("📌 pin event received")
            try:
                # v6: direct attribute access (betterproto, snake_case fields)
                chat     = event.chat_message
                user     = chat.user_info
                username = user.username or ""
                nickname = user.nick_name or ""
                comment  = chat.content or ""

                if not username and not nickname:
                    self._log("pin received but could not extract user data")
                    import json as _dbg_json
                    self._log(f"  FULL RAW: {_dbg_json.dumps(event.to_pydict(), default=str)[:1000]}")
                    return

                self._log(f"  @{username} ({nickname}): {comment}")
                try:
                    self.callback(username, nickname, comment)
                except Exception as cb_err:
                    self._log(f"callback error: {cb_err}")

            except Exception as e:
                self._log(f"pin decode error: {e}")
                try:
                    import json
                    self._log(f"  raw: {json.dumps(event.to_pydict(), default=str)[:300]}")
                except Exception:
                    pass

        try:
            await client.connect()
        except Exception as e:
            err = str(e)
            if "not live" in err.lower() or "404" in err or "not_live" in err.lower():
                self._log(f"host @{self.host_username} is not currently live")
                self._push_status("not_live")
            elif "DEVICE_BLOCKED" in err:
                self._log("blocked — try providing a sessionid in Settings")
                self._push_status("disconnected")
            else:
                self._log(f"connect error: {e}")
                self._push_status("disconnected")
            raise


# ── CLI standalone test ───────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys, time

    if len(sys.argv) < 2:
        print("Usage: python pin_listener.py HOST_USERNAME [sessionid]")
        sys.exit(1)

    host    = sys.argv[1]
    sess_id = sys.argv[2] if len(sys.argv) >= 3 else None

    def on_pin(username, nickname, comment):
        print(f"\n{'='*55}")
        print(f"  📌 PINNED COMMENT CAPTURED")
        print(f"  username : @{username}")
        print(f"  nickname : {nickname}")
        print(f"  comment  : {comment}")
        print(f"{'='*55}\n")

    listener = PinListener(host, callback=on_pin, session_id=sess_id)
    listener.start()
    print(f"\nListening to @{host}'s live. Pin any comment. Ctrl+C to stop.\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        listener.stop()
        print("Stopped.")