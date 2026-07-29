#!/usr/bin/env python3
"""
Neon / Integral-style FIX 4.4 quote session (top-of-book), mirroring C++ NeonFixFeed.cpp.

Default: TCP to ``external_feed.fix.host:port`` (stunnel). Optional **direct_tls**: Python opens TLS to
Neon with ``tls_server_name`` (SNI). Use when stunnel logs TLS OK then immediate **close_notify** with
**SNI: sending servername: (IP)** — the venue often expects a hostname SNI (e.g. ``target_comp_id``).

Env: ``MM_DESK_NEON_DIRECT_TLS=1`` forces ``direct_tls`` on. ``MM_DESK_NEON_FIX_DEBUG=1`` logs raw recv hex.
``MM_DESK_NEON_FIX_ERR_DEDUPE_SEC`` (default 600): dedupe identical console errors.
When ``logon_reset_seq`` is false (FIX 141=N), ``MM_DESK_NEON_SEQ_STATE`` (optional path) or
``logs/neon_fix_mm_desk_seq.json`` stores the next outbound MsgSeqNum so the desk can follow a session
after ``trading_client`` (or a prior desk run) advanced the acceptor's expected seq.
"""

from __future__ import annotations

import json
import math
import os
import re
import socket
import ssl
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

SOH = "\x01"

# Venue Logout text when our MsgSeqNum (34) is behind the acceptor's expected next seq.
_SEQ_GAP_LOGOUT = re.compile(
    r"MsgSeqNum\s+too\s+low,\s+expecting\s+(\d+)\s+but\s+received\s+(\d+)", re.IGNORECASE
)


def is_neon_fix_provider(provider: str) -> bool:
    p = str(provider or "").strip().lower()
    return p in ("neon_fix", "fix_neon", "integral_fix", "fix_integral")


def fix_symbol_canonical(sym: str) -> str:
    return re.sub(r"[/\-_\s]+", "", sym.upper())


def utc_timestamp_fix() -> str:
    now = datetime.now(timezone.utc)
    ms = int(now.microsecond / 1000)
    return now.strftime("%Y%m%d-%H:%M:%S.") + f"{ms:03d}"


def fix_checksum_sum(msg: str) -> int:
    return sum(ord(c) & 0xFF for c in msg) % 256


def wrap_fix(body_from_35: str) -> str:
    ver = f"8=FIX.4.4{SOH}"
    len_field = f"9={len(body_from_35)}{SOH}"
    without_10 = ver + len_field + body_from_35
    cs = f"{fix_checksum_sum(without_10):03d}"
    return without_10 + f"10={cs}{SOH}"


def parse_fields(msg: str) -> list[tuple[int, str]]:
    out: list[tuple[int, str]] = []
    i = 0
    while i < len(msg):
        eq = msg.find("=", i)
        if eq < 0:
            break
        soh = msg.find(SOH, eq)
        if soh < 0:
            break
        try:
            tag = int(msg[i:eq])
            out.append((tag, msg[eq + 1 : soh]))
        except ValueError:
            break
        i = soh + 1
    return out


def find_tag(fields: list[tuple[int, str]], tag: int) -> str | None:
    for t, v in fields:
        if t == tag:
            return v
    return None


def extract_tob(fields: list[tuple[int, str]]) -> tuple[float | None, float | None, bool, bool]:
    have_bid = False
    have_ask = False
    best_bid = -math.inf
    best_ask = math.inf
    for i, (ti, _) in enumerate(fields):
        if ti != 269:
            continue
        try:
            entry_type = int(fields[i][1])
        except ValueError:
            continue
        px = None
        for j in range(i + 1, len(fields)):
            tj, vj = fields[j]
            if tj in (269, 279):
                break
            if tj == 270:
                try:
                    px = float(vj)
                except ValueError:
                    pass
                break
        if px is None:
            continue
        if entry_type == 0:
            have_bid = True
            best_bid = max(best_bid, px)
        elif entry_type == 1:
            have_ask = True
            best_ask = min(best_ask, px)
    if not have_bid or not have_ask:
        for t, v in fields:
            if t == 188:
                try:
                    best_bid = max(best_bid, float(v))
                    have_bid = True
                except ValueError:
                    pass
            elif t == 190:
                try:
                    best_ask = min(best_ask, float(v))
                    have_ask = True
                except ValueError:
                    pass
    bid_o = best_bid if have_bid else None
    ask_o = best_ask if have_ask else None
    return bid_o, ask_o, have_bid, have_ask


def extract_one_fix_message(buf: bytearray) -> str | None:
    """Pop one complete FIX message using tag 9 BodyLength (same as C++)."""
    while buf and buf[0] != ord(b"8"):
        sync = buf.find(b"8=FIX")
        if sync < 0:
            buf.clear()
            return None
        del buf[:sync]
    if len(buf) < 16:
        return None
    s = buf.decode("latin-1", errors="replace")
    soh0 = s.find(SOH)
    if soh0 < 0 or soh0 + 4 >= len(s):
        return None
    if s[soh0 + 1 : soh0 + 3] != "9=":
        del buf[0]
        return None
    len_start = soh0 + 3
    soh1 = s.find(SOH, len_start)
    if soh1 < 0:
        return None
    try:
        body_len = int(s[len_start:soh1])
    except ValueError:
        del buf[0]
        return None
    if body_len <= 0 or body_len > 1 << 20:
        del buf[0]
        return None
    body_start = soh1 + 1
    cksum_pos = body_start + body_len
    total = cksum_pos + 7
    if len(buf) < total:
        return None
    tail = s[cksum_pos : cksum_pos + 7]
    if not tail.startswith("10=") or tail[6] != SOH:
        del buf[0]
        return None
    if not tail[3:6].isdigit():
        del buf[0]
        return None
    msg = bytes(buf[:total]).decode("latin-1", errors="replace")
    del buf[:total]
    return msg


def parse_fix_tag_object(val: Any) -> list[tuple[str, str]]:
    if not isinstance(val, dict):
        return []
    out: list[tuple[str, str]] = []
    for k, v in val.items():
        sk = str(k).strip()
        if not sk or sk.startswith("_"):
            continue
        if isinstance(v, str):
            out.append((sk, v))
        elif isinstance(v, bool):
            out.append((sk, "Y" if v else "N"))
        elif isinstance(v, int):
            out.append((sk, str(v)))
        elif isinstance(v, float):
            out.append((sk, str(v)))
    return out


def append_instrument_extras(parts: list[str], extras: list[tuple[str, str]]) -> None:
    m = {k: v for k, v in extras if k}
    for tag in ("460", "167"):
        if tag in m:
            parts.append(f"{tag}={m[tag]}{SOH}")
            del m[tag]
    rest_tags: list[tuple[int, str, str]] = []
    for tag, val in m.items():
        try:
            rest_tags.append((int(tag), tag, val))
        except ValueError:
            rest_tags.append((1_000_000, tag, val))
    rest_tags.sort(key=lambda x: x[0])
    for _, tag, val in rest_tags:
        parts.append(f"{tag}={val}{SOH}")


_md_id_lock = threading.Lock()
_md_counter = 0
_sl_id_lock = threading.Lock()
_sl_counter = 0


def _next_md_req_id() -> str:
    global _md_counter
    with _md_id_lock:
        _md_counter += 1
        return f"MDPY{_md_counter}"


def _next_sl_req_id() -> str:
    global _sl_counter
    with _sl_id_lock:
        _sl_counter += 1
        return f"SLP{_sl_counter}"


@dataclass
class NeonFixSettings:
    host: str = "127.0.0.1"
    port: int = 14507
    sender_comp_id: str = ""
    target_comp_id: str = ""
    deliver_to_comp_id: str = ""
    sender_sub_id: str = ""
    username: str = ""
    password: str = ""
    heart_bt_int: int = 30
    pricing_symbol: str = ""
    md_symbols: list[str] = field(default_factory=list)
    md_update_type: int = 0
    md_instrument_extra: list[tuple[str, str]] = field(default_factory=list)
    logon_extra: list[tuple[str, str]] = field(default_factory=list)
    md_request_root_extra: list[tuple[str, str]] = field(default_factory=list)
    md_snapshot_only: bool = False
    # When True, connect with TLS to tls_remote_host:tls_remote_port (bypass stunnel). SNI = tls_server_name.
    direct_tls: bool = False
    tls_remote_host: str = ""
    tls_remote_port: int = 0
    tls_server_name: str = ""
    tls_insecure: bool = True
    # FIX 141=Y reset seq on Logon (C++ default). Some venues drop the session if Y is wrong — try false → 141=N.
    logon_reset_seq_y: bool = True
    # Optional: after Logon, send Security List Request (35=x) and parse Security List (35=y) for tag 55 symbols.
    security_list_on_logon: bool = False
    security_list_request_type: int = 4
    security_list_req_id: str = "SLQ1"
    security_list_extra: list[tuple[str, str]] = field(default_factory=list)
    security_list_wait_sec: float = 8.0
    # Periodic Security List Request on an active session (0 = disabled). Typical: 30s from mm_desk.neon_security_list_poll_sec.
    security_list_poll_interval_sec: float = 0.0


def settings_from_merged_config(dc: dict) -> NeonFixSettings | None:
    ext = dc.get("external_feed") if isinstance(dc.get("external_feed"), dict) else {}
    if not is_neon_fix_provider(str(ext.get("provider", ""))):
        return None
    fix = ext.get("fix") if isinstance(ext.get("fix"), dict) else {}
    sym = str(ext.get("symbol", "") or "").strip()
    md_syms: list[str] = []
    raw_md = fix.get("md_symbols")
    if isinstance(raw_md, list):
        for x in raw_md:
            if x is not None and str(x).strip():
                md_syms.append(str(x).strip())
    if not md_syms and sym:
        md_syms = [sym]
    if not sym and md_syms:
        sym = md_syms[0]
    found = any(fix_symbol_canonical(s) == fix_symbol_canonical(sym) for s in md_syms)
    if sym and not found:
        md_syms.insert(0, sym)
    env_direct = os.environ.get("MM_DESK_NEON_DIRECT_TLS", "").strip().lower() in ("1", "true", "yes", "on")
    direct_tls = bool(fix.get("direct_tls", False)) or env_direct
    tls_rh = str(fix.get("tls_remote_host", "") or "").strip()
    try:
        tls_rp = int(fix.get("tls_remote_port", 0) or 0)
    except (TypeError, ValueError):
        tls_rp = 0
    tls_sni = str(fix.get("tls_server_name", "") or "").strip()
    tls_insecure = fix.get("tls_insecure", True)
    if isinstance(tls_insecure, str):
        tls_insecure = tls_insecure.strip().lower() not in ("0", "false", "no", "off")
    else:
        tls_insecure = bool(tls_insecure)
    lrs = fix.get("logon_reset_seq", True)
    if isinstance(lrs, str):
        logon_reset_seq_y = lrs.strip().lower() not in ("0", "false", "no", "off")
    else:
        logon_reset_seq_y = bool(lrs)
    sl_on = fix.get("security_list_on_logon", False)
    if isinstance(sl_on, str):
        security_list_on_logon = sl_on.strip().lower() in ("1", "true", "yes", "on")
    else:
        security_list_on_logon = bool(sl_on)
    try:
        sl_rt = int(fix.get("security_list_request_type", 4) or 4)
    except (TypeError, ValueError):
        sl_rt = 4
    sl_rid = str(fix.get("security_list_req_id", "") or "").strip() or "SLQ1"
    try:
        sl_wait = float(fix.get("security_list_wait_sec", 8.0) or 8.0)
    except (TypeError, ValueError):
        sl_wait = 8.0
    sl_wait = max(2.0, min(sl_wait, 120.0))
    desk = dc.get("mm_desk") if isinstance(dc.get("mm_desk"), dict) else {}
    poll_raw = fix.get("security_list_poll_interval_sec")
    if poll_raw is None:
        poll_raw = desk.get("neon_security_list_poll_sec", 0.0)
    try:
        sl_poll = float(poll_raw or 0.0)
    except (TypeError, ValueError):
        sl_poll = 0.0
    sl_poll = max(0.0, min(sl_poll, 3600.0))
    s = NeonFixSettings(
        host=str(fix.get("host", "127.0.0.1") or "127.0.0.1").strip(),
        port=int(fix.get("port", 14507) or 14507),
        sender_comp_id=str(fix.get("sender_comp_id", "") or ""),
        target_comp_id=str(fix.get("target_comp_id", "") or ""),
        deliver_to_comp_id=str(fix.get("deliver_to_comp_id", "") or ""),
        sender_sub_id=str(fix.get("sender_sub_id", "") or ""),
        username=str(fix.get("username", "") or ""),
        password=str(fix.get("password", "") or ""),
        heart_bt_int=int(fix.get("heart_bt_int", 30) or 30),
        pricing_symbol=sym,
        md_symbols=md_syms,
        md_update_type=int(fix.get("md_update_type", 0)),
        md_instrument_extra=parse_fix_tag_object(fix.get("md_instrument_extra")),
        logon_extra=parse_fix_tag_object(fix.get("logon_extra")),
        md_request_root_extra=parse_fix_tag_object(fix.get("md_request_root_extra")),
        md_snapshot_only=bool(fix.get("md_snapshot_only", False)),
        direct_tls=direct_tls,
        tls_remote_host=tls_rh,
        tls_remote_port=tls_rp,
        tls_server_name=tls_sni,
        tls_insecure=tls_insecure,
        logon_reset_seq_y=logon_reset_seq_y,
        security_list_on_logon=security_list_on_logon,
        security_list_request_type=sl_rt,
        security_list_req_id=sl_rid,
        security_list_extra=parse_fix_tag_object(fix.get("security_list_extra")),
        security_list_wait_sec=sl_wait,
        security_list_poll_interval_sec=sl_poll,
    )
    if not s.md_symbols or not s.sender_comp_id or not s.target_comp_id or not s.username:
        return None
    if s.direct_tls:
        if not s.tls_remote_host or s.tls_remote_port <= 0:
            return None
        if not s.tls_server_name.strip():
            s.tls_server_name = s.target_comp_id
    return s


def _neon_seq_session_key(settings: NeonFixSettings) -> str:
    """Stable key so seq state does not bleed across different FIX endpoints or CompIDs."""
    if settings.direct_tls:
        ep = f"tls:{settings.tls_remote_host}:{settings.tls_remote_port}"
    else:
        ep = f"tcp:{settings.host}:{settings.port}"
    return f"{settings.sender_comp_id}|{settings.target_comp_id}|{ep}"


def _neon_seq_state_path() -> Path:
    raw = os.environ.get("MM_DESK_NEON_SEQ_STATE", "").strip()
    if raw:
        return Path(raw).expanduser()
    return Path(__file__).resolve().parent.parent / "logs" / "neon_fix_mm_desk_seq.json"


def _load_next_out_seq(settings: NeonFixSettings) -> int:
    """Next FIX tag 34 value to send when logon_reset_seq is false (141=N)."""
    path = _neon_seq_state_path()
    key = _neon_seq_session_key(settings)
    try:
        if not path.is_file():
            return 1
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or data.get("session_key") != key:
            return 1
        n = int(data.get("next_out_seq", 1))
        return max(1, n)
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return 1


def _save_next_out_seq(settings: NeonFixSettings, next_seq: int) -> None:
    if settings.logon_reset_seq_y:
        return
    path = _neon_seq_state_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "session_key": _neon_seq_session_key(settings),
            "next_out_seq": max(1, int(next_seq)),
            "saved_ms": int(time.time() * 1000),
        }
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    except OSError:
        pass


def _after_fix_send(settings: NeonFixSettings, out_seq_l: list[int]) -> None:
    if not settings.logon_reset_seq_y:
        _save_next_out_seq(settings, out_seq_l[0])


def _build_session_prefix(settings: NeonFixSettings, out_seq: list[int]) -> str:
    o = out_seq[0]
    out_seq[0] += 1
    parts = [
        f"49={settings.sender_comp_id}{SOH}",
        f"56={settings.target_comp_id}{SOH}",
    ]
    if settings.deliver_to_comp_id:
        parts.append(f"128={settings.deliver_to_comp_id}{SOH}")
    if settings.sender_sub_id:
        parts.append(f"50={settings.sender_sub_id}{SOH}")
    parts.append(f"34={o}{SOH}")
    parts.append(f"52={utc_timestamp_fix()}{SOH}")
    return "".join(parts)


def _build_logon(settings: NeonFixSettings, out_seq: list[int]) -> str:
    body = [f"35=A{SOH}", _build_session_prefix(settings, out_seq)]
    body.append(f"98=0{SOH}")
    body.append(f"108={settings.heart_bt_int}{SOH}")
    body.append(f"553={settings.username}{SOH}")
    body.append(f"554={settings.password}{SOH}")
    body.append(f"141={'Y' if settings.logon_reset_seq_y else 'N'}{SOH}")
    for tag, val in settings.logon_extra:
        if tag:
            body.append(f"{tag}={val}{SOH}")
    return wrap_fix("".join(body))


def _build_md_request_one(settings: NeonFixSettings, out_seq: list[int], sym: str) -> str:
    """One Market Data Request (35=V) for a single tag-55 symbol.

    The venue may reject 146>1 in one message (MDReqReject: MoreThanOneGroup);
    C++ sends one 35=V per `md_symbols` entry; mirror that here.
    """
    body: list[str] = [f"35=V{SOH}", _build_session_prefix(settings, out_seq)]
    body.append(f"262={_next_md_req_id()}{SOH}")
    if settings.md_snapshot_only:
        body.append(f"263=0{SOH}")
        body.append(f"264=1{SOH}")
    else:
        body.append(f"263=1{SOH}")
        body.append(f"264=1{SOH}")
        if settings.md_update_type >= 0:
            body.append(f"265={settings.md_update_type}{SOH}")
    for tag, val in settings.md_request_root_extra:
        if tag:
            body.append(f"{tag}={val}{SOH}")
    body.append(f"267=2{SOH}")
    body.append(f"269=0{SOH}")
    body.append(f"269=1{SOH}")
    body.append(f"146=1{SOH}")
    body.append(f"55={sym}{SOH}")
    append_instrument_extras(body, settings.md_instrument_extra)
    return wrap_fix("".join(body))


def _send_md_subscribe_for_symbols(
    settings: NeonFixSettings,
    out_seq_l: list[int],
    sock: socket.socket,
    symbols: list[str],
    log_line_prefix: str,
    *,
    log_md_wire: bool,
) -> None:
    for sym in symbols:
        md_msg = _build_md_request_one(settings, out_seq_l, sym)
        if log_md_wire:
            vis = md_msg.replace(SOH, "|")
            print(f"{log_line_prefix} wire: {vis}", flush=True)
        _send_all(sock, md_msg.encode("latin-1"))
    _after_fix_send(settings, out_seq_l)


def extract_tag55_symbols(raw: str) -> list[str]:
    """Collect unique FIX tag 55 (Symbol) values in wire order (venue Security List / MD bodies)."""
    fields = parse_fields(raw)
    seen: set[str] = set()
    out: list[str] = []
    for t, v in fields:
        if t != 55:
            continue
        sv = (v or "").strip()
        if not sv:
            continue
        k = fix_symbol_canonical(sv)
        if k in seen:
            continue
        seen.add(k)
        out.append(sv)
    return out


_FX_PAIR_IN_TEXT = re.compile(r"\b([A-Z]{3})/([A-Z]{3})\b")


def extract_security_list_symbols(raw: str) -> list[str]:
    """Parse Security List (35=y): tag 55 plus Neon/Integral-style fallbacks (48, 107).

    Some venues omit tag 55 on every RelatedSym row or encode the pair only in SecurityID (48)
    or SecurityDesc (107). MD bodies still use ``extract_tag55_symbols``."""
    fields = parse_fields(raw)
    seen: set[str] = set()
    out: list[str] = []

    def add(val: str) -> None:
        sv = (val or "").strip()
        if not sv:
            return
        k = fix_symbol_canonical(sv)
        if k in seen:
            return
        seen.add(k)
        out.append(sv)

    for t, v in fields:
        if t == 55:
            add(v)
    for t, v in fields:
        if t != 48:
            continue
        sv = (v or "").strip().upper()
        if not sv:
            continue
        if "/" in sv:
            a, b = sv.split("/", 1)
            if len(a) == 3 and len(b) == 3 and a.isalpha() and b.isalpha():
                add(f"{a}/{b}")
        elif len(sv) == 6 and sv.isalpha():
            add(f"{sv[:3]}/{sv[3:]}")
    for t, v in fields:
        if t != 107:
            continue
        u = (v or "").upper()
        for m in _FX_PAIR_IN_TEXT.finditer(u):
            add(f"{m.group(1)}/{m.group(2)}")
    return out


def _build_security_list_request(
    settings: NeonFixSettings, out_seq: list[int], *, req_id: str | None = None
) -> str:
    body: list[str] = [f"35=x{SOH}", _build_session_prefix(settings, out_seq)]
    rid = (req_id or settings.security_list_req_id or "SLQ1").strip() or "SLQ1"
    body.append(f"320={rid}{SOH}")
    body.append(f"559={settings.security_list_request_type}{SOH}")
    for tag, val in settings.security_list_extra:
        if tag:
            body.append(f"{tag}={val}{SOH}")
    return wrap_fix("".join(body))


def _build_heartbeat(settings: NeonFixSettings, out_seq: list[int]) -> str:
    return wrap_fix(f"35=0{SOH}{_build_session_prefix(settings, out_seq)}")


def _build_test_rsp(settings: NeonFixSettings, out_seq: list[int], test_id: str) -> str:
    return wrap_fix(
        f"35=0{SOH}{_build_session_prefix(settings, out_seq)}112={test_id}{SOH}"
    )


def _build_logout(settings: NeonFixSettings, out_seq: list[int], text: str) -> str:
    b = f"35=5{SOH}{_build_session_prefix(settings, out_seq)}"
    if text:
        b += f"58={text}{SOH}"
    return wrap_fix(b)


def _ssl_context_for_neon(settings: NeonFixSettings) -> ssl.SSLContext:
    """TLS client profile aligned with common stunnel / Integral-style quote fronts."""
    ctx = ssl.create_default_context()
    if settings.tls_insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    try:
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    except AttributeError:
        pass
    # Some gateways mishandle TLS session tickets; stunnel logs often show ticket reuse.
    try:
        ctx.options |= ssl.OP_NO_TICKET
    except AttributeError:
        pass
    return ctx


def connect_fix_socket(settings: NeonFixSettings, *, connect_timeout: float = 10.0) -> socket.socket:
    """
    Plain TCP to fix.host:port (stunnel), or TLS to tls_remote_host:port with tls_server_name as SNI.
    """
    if settings.direct_tls:
        host = settings.tls_remote_host.strip()
        port = int(settings.tls_remote_port)
        if not host or port <= 0:
            raise OSError("direct_tls: set external_feed.fix.tls_remote_host and tls_remote_port.")
        raw = socket.create_connection((host, port), timeout=connect_timeout)
        ctx = _ssl_context_for_neon(settings)
        sni = (settings.tls_server_name or settings.target_comp_id or "").strip()
        if not sni:
            sni = host
        try:
            return ctx.wrap_socket(raw, server_hostname=sni)
        except ssl.SSLError:
            raw.close()
            raise
    return socket.create_connection((settings.host, settings.port), timeout=connect_timeout)


def neon_transport_preflight(settings: NeonFixSettings, timeout: float = 4.0) -> tuple[bool, str]:
    """TCP to stunnel, or TLS handshake to Neon when direct_tls."""
    try:
        s = connect_fix_socket(settings, connect_timeout=timeout)
        s.close()
        return True, ""
    except OSError as e:
        return False, str(e)
    except ssl.SSLError as e:
        return False, f"SSL: {e}"


def _send_all(sock: socket.socket, data: bytes) -> None:
    off = 0
    while off < len(data):
        n = sock.send(data[off:])
        if n <= 0:
            raise OSError("send failed")
        off += n


def _neon_fix_debug_raw() -> bool:
    v = os.environ.get("MM_DESK_NEON_FIX_DEBUG", "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _eof_error_message(logged_on: bool, read_buf: bytearray, *, direct_tls: bool) -> str:
    """Human-readable reason when recv returns empty (peer closed TCP/TLS)."""
    if logged_on:
        return "Neon FIX disconnected (TCP closed)."
    transport = (
        "direct TLS handshake succeeded (see PREFLIGHT), but the venue closed without sending FIX."
        if direct_tls
        else "plain TCP to stunnel accepted (PREFLIGHT), but upstream TLS or FIX may still fail."
    )
    msg = (
        "Neon FIX: peer closed before Logon completed (no inbound 35=A parsed). "
        f"{transport} "
        "Verify username/password, sender_comp_id, target_comp_id, deliver_to_comp_id, sender_sub_id; "
        "toggle external_feed.fix.logon_reset_seq (141=Y vs N); add logon_extra tags Neon documents; "
        "confirm tls_server_name (SNI) with Neon if using direct_tls."
    )
    if read_buf:
        n = min(192, len(read_buf))
        msg += f" Unparsed rx ({len(read_buf)} bytes) hex[{n}]: {read_buf[:n].hex()}"
        if _neon_fix_debug_raw():
            vis = bytes(read_buf[:400]).decode("latin-1", errors="replace").replace(SOH, "|")
            msg += f" | preview: {vis!r}"
    else:
        msg += " Zero bytes received after our Logon — venue dropped the session (credentials, 141=Y/N, or entitlement)."
    return msg


def _drain_preface(sock: socket.socket, read_buf: bytearray, total_wait: float = 0.2) -> None:
    """Read any bytes the server sends right after connect (before our Logon)."""
    old = sock.gettimeout()
    deadline = time.monotonic() + max(0.05, total_wait)
    sock.settimeout(0.05)
    try:
        while time.monotonic() < deadline:
            try:
                chunk = sock.recv(65536)
            except socket.timeout:
                break
            except ssl.SSLError:
                break
            if not chunk:
                break
            read_buf.extend(chunk)
    finally:
        sock.settimeout(old)


def _reconnect_delay_sec(fail_streak: int) -> float:
    return min(0.5 * (1.5 ** min(fail_streak, 14)), 20.0)


@dataclass
class NeonQuoteStore:
    """Thread-safe last quote (for control panel / lightweight consumers)."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    bid: float | None = None
    ask: float | None = None
    mid: float | None = None
    symbol: str = ""
    error: str = ""
    updated_ms: int = 0

    def set_quote(self, sym: str, bid: float, ask: float) -> None:
        with self.lock:
            self.symbol = sym
            self.bid = bid
            self.ask = ask
            self.mid = (bid + ask) / 2.0
            self.error = ""
            self.updated_ms = int(time.time() * 1000)

    def set_error(self, msg: str) -> None:
        with self.lock:
            self.error = str(msg)[:500]

    def snapshot(self) -> tuple[float | None, str]:
        with self.lock:
            return self.mid, self.error


def run_neon_fix_loop(
    settings: NeonFixSettings,
    stop: threading.Event,
    *,
    on_quote: Callable[[str, float, float, float], None],
    on_error: Callable[[str], None] | None = None,
    on_security_list: Callable[[list[str]], list[str]] | None = None,
    reload_settings_after_sl: Callable[[], None] | None = None,
    log_md_wire: bool = False,
    register_sock: Callable[[socket.socket | None], None] | None = None,
) -> None:
    """
    Blocking loop: reconnect until stop is set. on_quote(symbol, bid, ask, mid) per valid TOB update.
    """
    per_sym: dict[str, dict[str, Any]] = {}

    def _schedule_sl_poll(state: dict[str, Any]) -> None:
        pol = float(getattr(settings, "security_list_poll_interval_sec", 0) or 0)
        if pol > 0 and state.get("md_ready"):
            state["sl_next_poll_at"] = time.monotonic() + pol
        else:
            state["sl_next_poll_at"] = 0.0

    def ensure_state(sym_key: str) -> dict[str, Any]:
        if sym_key not in per_sym:
            per_sym[sym_key] = {
                "last_bid": 0.0,
                "last_ask": 0.0,
                "have_bid": False,
                "have_ask": False,
            }
        return per_sym[sym_key]

    def process_raw(
        raw: str,
        sock: socket.socket,
        out_seq_l: list[int],
        state: dict[str, Any],
    ) -> None:
        fields = parse_fields(raw)
        mt = find_tag(fields, 35)
        if not mt:
            return
        if mt == "A":
            state["logged_on"] = True
            if settings.security_list_on_logon:
                state["awaiting_sl_md"] = True
                state["sl_deadline_ts"] = time.monotonic() + float(settings.security_list_wait_sec)
                sl_msg = _build_security_list_request(settings, out_seq_l)
                if log_md_wire:
                    print(f"[Neon FIX] SecurityListRequest wire: {sl_msg.replace(SOH, '|')}", flush=True)
                _send_all(sock, sl_msg.encode("latin-1"))
                _after_fix_send(settings, out_seq_l)
            else:
                state["awaiting_sl_md"] = False
                _send_md_subscribe_for_symbols(
                    settings,
                    out_seq_l,
                    sock,
                    list(settings.md_symbols),
                    "[Neon FIX] MD request",
                    log_md_wire=log_md_wire,
                )
                state["md_ready"] = True
                poll_iv = float(getattr(settings, "security_list_poll_interval_sec", 0) or 0)
                if poll_iv > 0:
                    rid = _next_sl_req_id()
                    sl_msg = _build_security_list_request(settings, out_seq_l, req_id=rid)
                    if log_md_wire:
                        print(
                            f"[Neon FIX] SecurityListRequest immediately after first MD "
                            f"(poll every {poll_iv:g}s)",
                            flush=True,
                        )
                    _send_all(sock, sl_msg.encode("latin-1"))
                    _after_fix_send(settings, out_seq_l)
                    state["sl_next_poll_at"] = time.monotonic() + poll_iv
                else:
                    _schedule_sl_poll(state)
            return
        if mt == "y":
            syms = extract_security_list_symbols(raw)
            frag = find_tag(fields, 893)
            if frag:
                frag = frag.strip().upper()
            n55 = sum(1 for t, _ in fields if t == 55)
            n48 = sum(1 for t, _ in fields if t == 48)
            print(
                f"[Neon FIX] SecurityList 35=y: extracted {len(syms)} symbol(s) "
                f"(tag55={n55}, tag48={n48}, LastFragment={frag or '—'})",
                flush=True,
            )
            if not syms:
                vis = raw.replace(SOH, "|")
                print(
                    f"[Neon FIX] SecurityList 35=y: 0 symbols after parse — wire preview: {vis[:520]}",
                    flush=True,
                )
            added: list[str] = []
            if on_security_list:
                try:
                    ret = on_security_list(syms)
                    added = list(ret) if ret else []
                except Exception as ex:
                    if on_error:
                        on_error(f"Neon FIX on_security_list: {ex}")
            if reload_settings_after_sl:
                try:
                    reload_settings_after_sl()
                except Exception as ex:
                    if on_error:
                        on_error(f"Neon FIX reload_settings_after_sl: {ex}")
            if state.get("awaiting_sl_md"):
                _send_md_subscribe_for_symbols(
                    settings,
                    out_seq_l,
                    sock,
                    list(settings.md_symbols),
                    "[Neon FIX] MD request (after SecurityList)",
                    log_md_wire=log_md_wire,
                )
                state["awaiting_sl_md"] = False
                state["md_ready"] = True
                _schedule_sl_poll(state)
            elif added:
                _send_md_subscribe_for_symbols(
                    settings,
                    out_seq_l,
                    sock,
                    added,
                    "[Neon FIX] Incremental MD request",
                    log_md_wire=log_md_wire,
                )
            return
        if mt == "5":
            t58 = find_tag(fields, 58) or ""
            mseq = _SEQ_GAP_LOGOUT.search(t58)
            if mseq and not settings.logon_reset_seq_y:
                exp = int(mseq.group(1))
                _save_next_out_seq(settings, exp)
                print(
                    f"[Neon FIX] MM desk: saved next outbound MsgSeqNum={exp} for 141=N session "
                    f"(file {_neon_seq_state_path()}). Next TCP connect will use 34={exp} on Logon.",
                    flush=True,
                )
            if on_error:
                on_error(f"Neon FIX Logout: {t58}")
            state["drop_session"] = True
            return
        if mt == "3":
            t58 = find_tag(fields, 58) or "(no text)"
            parts = [t58]
            if find_tag(fields, 371):
                parts.append(f"refTag={find_tag(fields, 371)}")
            mseq = _SEQ_GAP_LOGOUT.search(t58)
            if mseq and not settings.logon_reset_seq_y:
                exp = int(mseq.group(1))
                _save_next_out_seq(settings, exp)
                print(
                    f"[Neon FIX] MM desk: saved next outbound MsgSeqNum={exp} from SessionReject.",
                    flush=True,
                )
            if on_error:
                on_error("Neon FIX SessionReject: " + " ".join(parts))
            state["drop_session"] = True
            return
        if mt == "Y":
            parts = [find_tag(fields, 58) or "(no text)"]
            if on_error:
                on_error("Neon FIX MDReqReject: " + " ".join(parts))
            state["drop_session"] = True
            return
        if mt == "1":
            tr = find_tag(fields, 112)
            if tr:
                _send_all(sock, _build_test_rsp(settings, out_seq_l, tr).encode("latin-1"))
                _after_fix_send(settings, out_seq_l)
            return
        if mt == "0":
            return
        if mt not in ("W", "X"):
            return

        sym_raw = find_tag(fields, 55)
        use_sym = sym_raw or settings.pricing_symbol
        if not use_sym:
            return

        b, a, hb, ha = extract_tob(fields)
        st = ensure_state(use_sym)
        if mt == "W":
            if hb and ha and b is not None and a is not None:
                st["last_bid"], st["last_ask"] = b, a
                st["have_bid"] = st["have_ask"] = True
        else:
            if hb and b is not None:
                st["last_bid"] = b
                st["have_bid"] = True
            if ha and a is not None:
                st["last_ask"] = a
                st["have_ask"] = True

        if not st["have_bid"] or not st["have_ask"]:
            return
        lb, la = st["last_bid"], st["last_ask"]
        if la <= lb:
            return
        on_quote(use_sym, lb, la, (lb + la) / 2.0)

    fail_streak = 0
    while not stop.is_set():
        sock: socket.socket | None = None
        try:
            sock = connect_fix_socket(settings, connect_timeout=10.0)
        except (OSError, ssl.SSLError) as e:
            fail_streak += 1
            if on_error:
                loc = (
                    f"TLS {settings.tls_server_name or settings.target_comp_id}→{settings.tls_remote_host}:{settings.tls_remote_port}"
                    if settings.direct_tls
                    else f"{settings.host}:{settings.port}"
                )
                on_error(f"Neon FIX connect {loc} — {e}")
            if stop.wait(_reconnect_delay_sec(fail_streak)):
                return
            continue

        assert sock is not None
        if register_sock:
            register_sock(sock)
        out_seq_l = [1]
        if not settings.logon_reset_seq_y:
            out_seq_l[0] = _load_next_out_seq(settings)
        sess: dict[str, Any] = {
            "logged_on": False,
            "drop_session": False,
            "awaiting_sl_md": False,
            "sl_deadline_ts": 0.0,
            "sl_next_poll_at": 0.0,
            "md_ready": False,
        }
        read_buf = bytearray()
        logon_sent_ok = False
        try:
            sock.settimeout(max(1.0, settings.heart_bt_int / 2))
            _drain_preface(sock, read_buf)
            while True:
                one = extract_one_fix_message(read_buf)
                if not one:
                    break
                try:
                    process_raw(one, sock, out_seq_l, sess)
                except Exception as ex:
                    if on_error:
                        on_error(f"Neon FIX process: {ex}")
                if sess["drop_session"]:
                    break
            if not sess["drop_session"]:
                try:
                    log_msg = _build_logon(settings, out_seq_l)
                    if _neon_fix_debug_raw():
                        print("[Neon FIX] Logon wire:", log_msg.replace(SOH, "|"), flush=True)
                    _send_all(sock, log_msg.encode("latin-1"))
                    _after_fix_send(settings, out_seq_l)
                    logon_sent_ok = True
                except OSError as e:
                    fail_streak += 1
                    if on_error:
                        on_error(str(e))
            if logon_sent_ok:
                try:
                    while not stop.is_set() and not sess["drop_session"]:
                        try:
                            chunk = sock.recv(65536)
                        except socket.timeout:
                            if sess.get("awaiting_sl_md") and time.monotonic() >= float(
                                sess.get("sl_deadline_ts") or 0.0
                            ):
                                print(
                                    f"[Neon FIX] Security List: no 35=y within {settings.security_list_wait_sec:g}s "
                                    "— sending MarketDataRequest.",
                                    flush=True,
                                )
                                try:
                                    _send_md_subscribe_for_symbols(
                                        settings,
                                        out_seq_l,
                                        sock,
                                        list(settings.md_symbols),
                                        "[Neon FIX] MD request (SL timeout)",
                                        log_md_wire=log_md_wire,
                                    )
                                except OSError as e3:
                                    if on_error:
                                        on_error(str(e3))
                                    break
                                sess["awaiting_sl_md"] = False
                                sess["md_ready"] = True
                                poll_iv = float(getattr(settings, "security_list_poll_interval_sec", 0) or 0)
                                if poll_iv > 0:
                                    rid = _next_sl_req_id()
                                    sl_msg = _build_security_list_request(settings, out_seq_l, req_id=rid)
                                    if log_md_wire:
                                        print(
                                            f"[Neon FIX] SecurityListRequest after SL timeout + first MD "
                                            f"(poll every {poll_iv:g}s)",
                                            flush=True,
                                        )
                                    try:
                                        _send_all(sock, sl_msg.encode("latin-1"))
                                        _after_fix_send(settings, out_seq_l)
                                        sess["sl_next_poll_at"] = time.monotonic() + poll_iv
                                    except OSError as e4:
                                        if on_error:
                                            on_error(str(e4))
                                        break
                                else:
                                    _schedule_sl_poll(sess)
                                continue
                            poll_iv = float(getattr(settings, "security_list_poll_interval_sec", 0) or 0)
                            if (
                                sess["logged_on"]
                                and poll_iv > 0
                                and not sess.get("awaiting_sl_md")
                                and sess.get("md_ready")
                                and float(sess.get("sl_next_poll_at") or 0) > 0
                                and time.monotonic() >= float(sess.get("sl_next_poll_at") or 0)
                            ):
                                rid = _next_sl_req_id()
                                sl_msg = _build_security_list_request(settings, out_seq_l, req_id=rid)
                                if log_md_wire:
                                    print(
                                        f"[Neon FIX] periodic SecurityListRequest ({poll_iv:g}s): "
                                        f"{sl_msg.replace(SOH, '|')[:420]}",
                                        flush=True,
                                    )
                                try:
                                    _send_all(sock, sl_msg.encode("latin-1"))
                                    _after_fix_send(settings, out_seq_l)
                                    sess["sl_next_poll_at"] = time.monotonic() + poll_iv
                                except OSError as e_sl:
                                    if on_error:
                                        on_error(str(e_sl))
                                    break
                                continue
                            if sess["logged_on"]:
                                try:
                                    _send_all(sock, _build_heartbeat(settings, out_seq_l).encode("latin-1"))
                                    _after_fix_send(settings, out_seq_l)
                                except OSError as e2:
                                    if on_error:
                                        on_error(str(e2))
                                    break
                            else:
                                time.sleep(0.05)
                            continue
                        except OSError as e:
                            if on_error:
                                on_error(str(e))
                            break
                        if not chunk:
                            if on_error:
                                on_error(
                                    _eof_error_message(
                                        sess["logged_on"], read_buf, direct_tls=settings.direct_tls
                                    )
                                )
                            break
                        if _neon_fix_debug_raw():
                            print(f"[Neon FIX debug] rx {len(chunk)} b, hex[:160]: {chunk[:80].hex()}", flush=True)
                        read_buf.extend(chunk)
                        while True:
                            one = extract_one_fix_message(read_buf)
                            if not one:
                                break
                            try:
                                process_raw(one, sock, out_seq_l, sess)
                            except Exception as ex:
                                if on_error:
                                    on_error(f"Neon FIX process: {ex}")
                            if sess["drop_session"]:
                                break
                finally:
                    try:
                        _send_all(sock, _build_logout(settings, out_seq_l, "shutdown").encode("latin-1"))
                        _after_fix_send(settings, out_seq_l)
                    except OSError:
                        pass
        finally:
            try:
                sock.close()
            except OSError:
                pass
            if register_sock:
                register_sock(None)

        if not logon_sent_ok:
            if stop.wait(_reconnect_delay_sec(fail_streak)):
                return
            continue

        if sess["logged_on"]:
            fail_streak = 0
        else:
            fail_streak += 1

        if stop.is_set():
            return
        if stop.wait(_reconnect_delay_sec(fail_streak)):
            return


def _reference_error_short_for_ui(msg: str) -> str:
    """Stable short text for snapshot / UI when the full console line is long."""
    if "MsgSeqNum too low" in msg or _SEQ_GAP_LOGOUT.search(msg):
        return (
            "Neon FIX: MsgSeqNum mismatch (141=N). Stop trading_client if it uses the same sender_comp_id, "
            "then Restart feeds — desk saves next seq to logs/neon_fix_mm_desk_seq.json. "
            "Or only one FIX client at a time; or try logon_reset_seq true if Neon allows 141=Y."
        )
    if "before Logon completed" in msg or "peer closed before Logon" in msg:
        if "direct TLS handshake succeeded" in msg:
            return (
                "Neon FIX: TLS OK but venue sent no FIX after Logon — check FIX credentials/comp IDs, "
                "logon_reset_seq (141), tls_server_name (SNI), Neon entitlement."
            )
        if "Zero bytes received" in msg:
            return (
                "Neon FIX: no bytes after Logon — check stunnel/TLS upstream, credentials, logon_reset_seq."
            )
        return (
            "Neon FIX: no Logon ack — check stunnel→venue TLS or direct_tls SNI, username/password, comp IDs."
        )
    if len(msg) > 260:
        return msg[:257] + "…"
    return msg


def run_neon_fix_desk_loop(
    st: Any,
    settings: NeonFixSettings,
    stop: threading.Event,
    desk_log: Callable[..., None],
    register_sock: Callable[[socket.socket | None], None] | None = None,
    on_security_list: Callable[[list[str]], list[str]] | None = None,
    reload_settings_after_sl: Callable[[], None] | None = None,
) -> None:
    """Update mm_live_desk DeskSharedState: ref_all_books, last_ref_book, latency text."""

    err_dedupe = {"msg": "", "ts": 0.0, "suppressed": 0}

    def on_quote(sym: str, bid: float, ask: float, mid: float) -> None:
        row_b = [[bid, 1]]
        row_a = [[ask, 1]]
        with st.lock:
            st.ref_all_books[sym] = {"bids": row_b, "asks": row_a}
            ps = str(st.ref_symbol).strip()
            if fix_symbol_canonical(sym) == fix_symbol_canonical(ps):
                st.last_ref_book = (row_b, row_a)
            st.ref_latency_text = (
                "Neon FIX: TOB (direct TLS)" if settings.direct_tls else "Neon FIX: TOB (TCP/stunnel)"
            )
            st.ref_feed_mode = "neon_fix"
            st.reference_error = ""

    def on_err(msg: str) -> None:
        now = time.time()
        short = _reference_error_short_for_ui(msg)
        with st.lock:
            st.reference_error = short[:300]

        try:
            win = float(os.environ.get("MM_DESK_NEON_FIX_ERR_DEDUPE_SEC", "600") or "600")
        except ValueError:
            win = 600.0
        win = max(30.0, min(win, 3600.0))

        if msg == err_dedupe["msg"] and (now - err_dedupe["ts"]) < win:
            err_dedupe["suppressed"] += 1
            vb = os.environ.get("MM_DESK_VERBOSE", "").strip().lower() in ("1", "true", "yes")
            if vb:
                desk_log(
                    st,
                    f"Neon FIX: same error again (×{err_dedupe['suppressed'] + 1}, suppressing full text for {int(win)}s).",
                    verbose_only=True,
                )
            return

        err_dedupe["msg"] = msg
        err_dedupe["ts"] = now
        err_dedupe["suppressed"] = 0
        desk_log(st, msg, verbose_only=False)

    run_neon_fix_loop(
        settings,
        stop,
        on_quote=on_quote,
        on_error=on_err,
        on_security_list=on_security_list,
        reload_settings_after_sl=reload_settings_after_sl,
        log_md_wire=True,
        register_sock=register_sock,
    )


def run_neon_fix_tk_queue_loop(
    settings: NeonFixSettings,
    stop: threading.Event,
    queue: Any,
) -> None:
    """Put ('neon_tob', bids, asks, rtt_ms) on tkinter queue (same shape as REST reference rows)."""

    def on_quote(sym: str, bid: float, ask: float, mid: float) -> None:
        row_b = [(bid, 1)]
        row_a = [(ask, 1)]
        queue.put(("neon_tob", row_b, row_a, 0.0))

    run_neon_fix_loop(settings, stop, on_quote=on_quote, on_error=None, log_md_wire=False)


def run_neon_fix_quote_store_loop(
    settings: NeonFixSettings,
    stop: threading.Event,
    store: NeonQuoteStore,
) -> None:
    def on_quote(sym: str, bid: float, ask: float, mid: float) -> None:
        store.set_quote(sym, bid, ask)

    def on_err(msg: str) -> None:
        store.set_error(msg)

    run_neon_fix_loop(settings, stop, on_quote=on_quote, on_error=on_err, log_md_wire=False)


def tcp_preflight(host: str, port: int, timeout: float = 3.0) -> tuple[bool, str]:
    try:
        s = socket.create_connection((host, port), timeout=timeout)
        s.close()
        return True, ""
    except OSError as e:
        return False, str(e)
