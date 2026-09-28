#!/usr/bin/env python3
"""spark-noabort: a never-abort HTTP POST carrier for slow LLM inference endpoints.

A socket timeout is an abort. When `urlopen(..., timeout=N)` fires, the client closes the
connection — and a server like vLLM cancels any request whose client has disconnected. The
same happens when the calling process exits, is killed by a `timeout` wrapper, or dies: the
kernel closes its sockets, so the request is never actually "held" by the caller in any way
that survives the caller going away.

That's a real problem for slow inference endpoints: a big model behind a deep queue can take
minutes, and a caller that wants to enforce its own deadline (so *it* can move on) ends up
killing the very request it was waiting on, wasting the GPU time and the queue slot.

    post_json(url, body, deadline_s, late=None)

hands the request to a CARRIER: this same file run as `python3 spark_noabort.py --carry`, in
its own session, with no stdout/stderr shared with the caller. The carrier holds the connection
open with no read timeout short of `READ_TIMEOUT_S` and writes the reply back over a pipe. The
caller waits on a daemon reader thread's `Event` for at most `deadline_s`:

  * reply in time      -> the caller acks, gets {"ok": True, "status", "json", "ms"}.
  * deadline reached   -> the caller closes the pipe without an ack and gets
                          {"abandoned": True, "ms"}. The carrier keeps the connection open, and
                          when the late reply lands it appends one row to a caller-supplied
                          ledger file: `late["row"]` plus the real token counts pulled from the
                          reply's `usage` block, "measured": true, "late": true. Whether the
                          original caller is still alive, was killed, or is long gone makes no
                          difference to this — the carrier is a separate process.
  * transport failure  -> {"error": "...", "ms"} (connection refused, HTTP status >= 400).

`late` is an optional dict: {"path": ledger file, "row": template dict, "tsKey": "ts"|"at"|None,
"tokenKeys": [promptKey, completionKey]}. Pass `None` to skip ledgering late replies entirely.

Only the connect step has a short timeout (nothing has been sent yet, so there is nothing to
cancel by giving up on it). The read timeout is a safety net against a genuinely half-open
socket, not a cancellation mechanism — set it comfortably above the slowest reply your server
can produce.

Under systemd or another process supervisor, run the caller inside its own scope
(e.g. `systemd-run --scope`): a oneshot unit's leftover processes are killed along with its
cgroup when the unit's main process exits, and that kill would close the carrier's connection
right along with everything else.
"""
from __future__ import annotations

import http.client
import json
import os
import subprocess
import sys
import threading
import time
import urllib.parse

CONNECT_TIMEOUT_S = 30
READ_TIMEOUT_S = 7200


def _post(url: str, body: dict) -> tuple[int, str]:
    parts = urllib.parse.urlsplit(url)
    conn_cls = http.client.HTTPSConnection if parts.scheme == "https" else http.client.HTTPConnection
    conn = conn_cls(parts.hostname, parts.port, timeout=CONNECT_TIMEOUT_S)
    conn.connect()
    conn.sock.settimeout(READ_TIMEOUT_S)
    data = json.dumps(body).encode("utf-8")
    path = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
    conn.request("POST", path, body=data,
                 headers={"Content-Type": "application/json", "Content-Length": str(len(data))})
    resp = conn.getresponse()
    raw = resp.read()
    conn.close()
    return resp.status, raw.decode("utf-8", "replace")


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ledger_late(late: dict | None, reply_text: str, started: float) -> None:
    """Append the late reply's real usage to the caller's ledger. Never raises."""
    if not late or not late.get("path"):
        return
    try:
        usage = (json.loads(reply_text).get("usage") or {})
    except (ValueError, AttributeError):
        usage = {}
    pk, ck = (late.get("tokenKeys") or ["promptTokens", "completionTokens"])[:2]
    row = dict(late.get("row") or {})
    ts_key = late.get("tsKey", "ts")
    if ts_key:
        row[ts_key] = _now_iso()
    row.update({pk: usage.get("prompt_tokens"), ck: usage.get("completion_tokens"),
                "measured": usage.get("prompt_tokens") is not None,
                "late": True, "outcome": "late-reply", "lateMs": int((time.time() - started) * 1000)})
    try:
        os.makedirs(os.path.dirname(late["path"]) or ".", exist_ok=True)
        with open(late["path"], "a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    except OSError:
        pass


def _carry() -> int:
    spec = json.loads(sys.stdin.readline())
    try:
        status, text = _post(spec["url"], spec["body"])
        out = {"status": status, "text": text}
    except Exception as err:  # noqa: BLE001 - transport truth goes back to the caller
        out = {"error": f"{type(err).__name__}: {err}"[:300]}
    acked = False
    try:
        sys.stdout.write(json.dumps(out) + "\n")
        sys.stdout.flush()
        acked = sys.stdin.readline().strip() == "ack"
    except (BrokenPipeError, OSError, ValueError):
        acked = False
    if not acked and "text" in out and out.get("status", 0) < 400:
        ledger_late(spec.get("late"), out["text"], float(spec.get("t0") or time.time()))
    return 0


def _reap(proc: subprocess.Popen) -> None:
    threading.Thread(target=proc.wait, daemon=True).start()


def post_json(url: str, body: dict, deadline_s: float, late: dict | None = None) -> dict:
    """POST `body` to `url`; never aborts the underlying request. See the module docstring."""
    t0 = time.time()
    proc = subprocess.Popen(
        [sys.executable, os.path.abspath(__file__), "--carry"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        start_new_session=True, close_fds=True)
    spec = {"url": url, "body": body, "late": late, "t0": t0}
    proc.stdin.write((json.dumps(spec) + "\n").encode("utf-8"))
    proc.stdin.flush()
    box: dict = {}
    got = threading.Event()

    def reader() -> None:
        try:
            box["line"] = proc.stdout.readline()
        except Exception as err:  # noqa: BLE001
            box["line"], box["err"] = b"", err
        finally:
            try:
                proc.stdout.close()
            except OSError:
                pass
        got.set()

    threading.Thread(target=reader, daemon=True).start()
    ms = lambda: int((time.time() - t0) * 1000)  # noqa: E731
    if not got.wait(max(0.0, float(deadline_s))):
        # ABANDON the wait. No ack: the carrier keeps the connection and ledgers the reply.
        try:
            proc.stdin.close()
        except OSError:
            pass
        _reap(proc)
        return {"abandoned": True, "ms": ms()}
    try:
        proc.stdin.write(b"ack\n")
        proc.stdin.close()
    except OSError:
        pass
    _reap(proc)
    line = box.get("line") or b""
    if not line:
        return {"error": f"carrier exited without a reply: {box.get('err', 'no output')}", "ms": ms()}
    out = json.loads(line)
    if "error" in out:
        return {"error": out["error"], "ms": ms()}
    if out["status"] >= 400:
        return {"error": f"http_{out['status']}", "status": out["status"], "text": out["text"][:500], "ms": ms()}
    try:
        payload = json.loads(out["text"])
    except ValueError as err:
        return {"error": f"bad json: {err}", "status": out["status"], "ms": ms()}
    return {"ok": True, "status": out["status"], "json": payload, "ms": ms()}


def _cli(argv: list[str]) -> int:
    """`spark_noabort.py --post URL --deadline S [--late-ledger PATH]`, JSON body on stdin.
    Prints the reply body and exits 0; exits 124 with nothing printed when the wait is abandoned;
    exits 1 on a transport failure."""
    import argparse
    ap = argparse.ArgumentParser(prog="spark_noabort.py")
    ap.add_argument("--post", required=True)
    ap.add_argument("--deadline", type=float, required=True)
    ap.add_argument("--late-ledger")
    a = ap.parse_args(argv)
    late = {"path": a.late_ledger, "row": {"consumer": "shell"}} if a.late_ledger else None
    res = post_json(a.post, json.loads(sys.stdin.read() or "{}"), a.deadline, late=late)
    if res.get("abandoned"):
        return 124
    if not res.get("ok"):
        print(res.get("error"), file=sys.stderr)
        return 1
    print(json.dumps(res["json"]))
    return 0


if __name__ == "__main__":
    if sys.argv[1:] == ["--carry"]:
        raise SystemExit(_carry())
    raise SystemExit(_cli(sys.argv[1:]))
