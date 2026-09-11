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

__version__ = "2026-09-10"

import re
import threading
import asyncio
from typing import Callable, Optional


def _first(obj, *names):
    """First attribute of `obj` from `names` that exists and is not None/empty.
    TikTokLive renamed its event fields between v6 and v7; this lets one file serve
    both rather than forking the listener per machine."""
    if obj is None:
        return None
    for n in names:
        v = getattr(obj, n, None)
        if v is not None and v != "":
            return v
    return None


def _dump(event) -> str:
    """Best-effort JSON of a protobuf event for the log. v6 exposes to_pydict(),
    v7 (betterproto2) exposes to_dict(). Never raises — this is only for diagnosis."""
    import json
    for meth in ("to_dict", "to_pydict"):
        fn = getattr(event, meth, None)
        if callable(fn):
            try:
                return json.dumps(fn(), default=str)
            except Exception:
                continue
    try:
        return repr(event)
    except Exception:
        return "<unprintable event>"


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

    # ── Retry pacing ─────────────────────────────────────────────────────────
    # This used to be one fixed 5-second delay, retried forever with no ceiling and
    # no distinction between kinds of failure. The result, measured across every log
    # on the dev machine: 2,605 connection attempts, ALL of them to the literal
    # placeholder "@username", and 18,202 of 19,110 log lines — 95% of everything the
    # app has ever recorded. Real diagnostics were unfindable underneath it, and
    # TikTok rate-limits connections per IP per day, so that budget was being spent
    # on a handle that could never work.
    #
    # Two ceilings, because the two failures mean opposite things:
    #   host offline — expected. She has not gone live YET, and may at any moment, so
    #                  staying responsive matters more than saving requests. Backs off
    #                  gently and stops there.
    #   anything else — something is actually wrong. Nothing is gained by asking fast.
    RECONNECT_DELAY     = 5.0
    MAX_OFFLINE_DELAY   = 10.0    # worst case 10s late to a live she just started
    MAX_ERROR_DELAY     = 120.0

    # TikTok's own rule for a username: letters, digits, underscore, period, 2–24.
    _HANDLE_RE = re.compile(r"^[A-Za-z0-9._]{2,24}$")

    # Deliberately SHORT, and only words that are template text rather than plausible
    # accounts. An earlier draft also blocked "user", "handle", "example" and "test" —
    # all four are short, desirable handles that a real person could easily own, and
    # refusing a paying seller because her handle happens to be @test would be a far
    # worse bug than the one being fixed. If in doubt, it is not on this list.
    _PLACEHOLDERS = frozenset({
        "username", "yourhandle", "your_handle", "your_username",
        "tiktokusername", "tiktok_username", "yourtiktok",
    })

    @classmethod
    def is_placeholder(cls, handle) -> bool:
        """Blank, or template text nobody could actually own.

        The ONLY condition that stops the listener outright, because it is the only one
        we can be certain about. "@username" was the value in the field for all 2,605
        attempts on the dev machine.
        """
        h = (handle or "").strip().lstrip("@")
        return (not h) or h.lower() in cls._PLACEHOLDERS

    @classmethod
    def looks_malformed(cls, handle) -> bool:
        """Does not match TikTok's username rule.

        A WARNING, never a refusal. If this check is wrong — TikTok changes its rules,
        or the pattern misses a legitimate form — the cost is one slow retry loop, not
        a seller who can never connect. Being wrong quietly in the safe direction
        matters more here than being right.
        """
        h = (handle or "").strip().lstrip("@")
        return not bool(cls._HANDLE_RE.match(h))

    def _retry_delay(self, fails: int, offline: bool) -> float:
        """Double per consecutive failure, up to the ceiling for this kind."""
        cap = self.MAX_OFFLINE_DELAY if offline else self.MAX_ERROR_DELAY
        if fails <= 1:
            return self.RECONNECT_DELAY
        return min(self.RECONNECT_DELAY * (2 ** min(fails - 1, 8)), cap)

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
        self._wake          = None    # set once the event loop exists
        self._reset_backoff = False

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

    def nudge(self):
        """Try again NOW, and forget the accumulated backoff.

        The listener starts when the app opens and runs all day, so by evening it is
        sitting on the maximum gap between attempts. Without this, a seller going live
        at 8pm could wait that whole gap before the app even tried — the backoff exists
        to stop pointless calls while nothing is happening, not to make the app slow at
        the one moment that matters.

        Called when the app learns she is about to go live. Safe from any thread, safe
        before the loop exists, and safe when the listener is not running.
        """
        self._reset_backoff = True
        try:
            if self._loop and self._loop.is_running() and self._wake is not None:
                self._loop.call_soon_threadsafe(self._wake.set)
        except Exception:
            pass

    def _log(self, msg: str):
        # Route into the SAME rotating file the app uses ("tagasulat" logger, configured
        # by Tagasulat._init_logger). In the packaged .exe there is no console, so the
        # bare print() went nowhere — which is why "the app stopped capturing pins"
        # mid-live (2026-07-16) left ZERO trace in the log and was undiagnosable.
        if self.verbose:
            print(f"  [pin_listener] {msg}")
        try:
            import logging
            logging.getLogger("tagasulat").info(f"[listener] {msg}")
        except Exception:
            pass

    def _push_status(self, status: str):
        # Status TRANSITIONS are the key diagnostic for a silent listener death, so
        # every change is still recorded. What is dropped is the repetition: the same
        # status re-pushed on every retry produced 7,799 identical lines, which is not
        # seven thousand diagnostics but one, restated.
        #
        # The callback still fires every time regardless — the UI needs to know the
        # current state, not only that it changed.
        try:
            if status != getattr(self, "_last_logged_status", None):
                import logging
                logging.getLogger("tagasulat").info(f"[listener] status={status}")
                self._last_logged_status = status
        except Exception:
            pass
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
        # Only template text stops us outright — that is the one case we can be sure
        # about, and it is the one that produced all 2,605 attempts. "unconfigured" is
        # an existing status the UI already understands.
        if self.is_placeholder(self.host_username):
            self._log(f"'@{self.host_username}' is placeholder text, not a TikTok "
                      f"handle — set the seller's handle in Settings. Not connecting.")
            self._push_status("unconfigured")
            self._running = False
            return

        # An odd-looking handle is still TRIED. If this check is wrong, the cost is a
        # slow retry loop rather than a seller who can never connect.
        odd = self.looks_malformed(self.host_username)
        if odd:
            self._log(f"'@{self.host_username}' does not match TikTok's username rule "
                      f"— trying it anyway, but backing off quickly if it fails.")

        # An interruptible sleep, so nudge() can cut a wait short instead of the app
        # sitting out the full gap when it already knows she is going live.
        self._wake = asyncio.Event()

        fails    = 0
        last_err = None
        while self._running:
            if self._reset_backoff:
                fails, last_err, self._reset_backoff = 0, None, False
            try:
                await self._connect_and_listen()
                fails, last_err = 0, None      # a good connection forgives everything
            except Exception as e:
                msg = str(e)
                fails += 1
                # Log the first of a streak, any CHANGE of error, and then every tenth.
                # A thousand identical lines say nothing a count does not.
                if fails == 1 or msg != last_err:
                    self._log(f"error: {msg}")
                elif fails % 10 == 0:
                    self._log(f"error: still failing (x{fails}) — {msg}")
                last_err = msg
                self._push_status("disconnected")

            if not self._running:
                break
            # "is offline" is TikTokLive's wording for a host who simply has not gone
            # live. Matched on the message because the library's exception types have
            # moved between versions; a wrong guess here only costs pacing.
            # An odd-looking handle uses the long ceiling: if it really is wrong, this
            # stops it costing 8,640 requests a day while still leaving it working if
            # the check was mistaken.
            offline = (not odd) and "offline" in (last_err or "").lower()
            delay   = self._retry_delay(fails, offline)
            if fails <= 1 or fails % 10 == 0:
                self._log(f"reconnecting in {delay:.0f}s...")
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=delay)
                self._log("woken early — retrying now")
            except asyncio.TimeoutError:
                pass                     # the ordinary case: the wait simply elapsed
            self._wake.clear()

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
                # The field names differ between library generations, and this file runs
                # on both: the PC is on 6.6.5, the cloud VM on 7.0.1 (since 2026-09-10 —
                # Euler Stream's fallback only works there). Read whichever exists.
                #
                #   v6 (betterproto):   chat.user_info.username / .nick_name
                #   v7 (betterproto2):  chat.user.display_id    / .nickname
                #
                # display_id IS the @handle. Checked against the real v7 classes before
                # shipping, not guessed — so the PC upgrade cannot break pins the way the
                # VM's did on its first live.
                chat     = event.chat_message
                user     = _first(chat, "user", "user_info")
                username = _first(user, "display_id", "unique_id", "username") or ""
                nickname = _first(user, "nickname", "nick_name") or ""
                comment  = getattr(chat, "content", None) or ""

                if not username and not nickname:
                    self._log("pin received but could not extract user data")
                    self._log(f"  FULL RAW: {_dump(event)[:1000]}")
                    return

                self._log(f"  @{username} ({nickname}): {comment}")
                try:
                    self.callback(username, nickname, comment)
                except Exception as cb_err:
                    self._log(f"callback error: {cb_err}")

            except Exception as e:
                self._log(f"pin decode error: {e}")
                self._log(f"  raw: {_dump(event)[:300]}")

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