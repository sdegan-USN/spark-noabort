# spark-noabort

A small POST "carrier" for slow HTTP/JSON endpoints (LLM inference servers especially) that
lets a caller enforce its own deadline without ever aborting the underlying request. Ships as
a Python module and a byte-for-byte-equivalent JS/Node module, same protocol, same behavior.

It exists because closing a connection *is* cancelling the request on the other end: a socket
timeout, a `timeout`-wrapped process getting killed, `fetch`'s own default header timeout,
`process.exit()` — anything that closes the TCP connection tells the server "the client is
gone," and a server like vLLM cancels the in-flight generation right then. That's fine for a
quick API call. It's wasteful for a slow inference request behind a deep queue, where the
caller wants to give up *waiting* on its own timetable without also discarding GPU time and a
queue slot that a well-behaved server would otherwise finish and could still be worth recording.
The fix is to separate "I stop waiting" from "the request stops happening": hand the request to
a short-lived detached child process (the carrier) that holds the real connection open, and
have the caller race a deadline against a pipe from that child instead of racing the connection
itself.

## How it works

`post_json(url, body, deadline_s, late=None)` (Python) / `sparkPost(url, body, { deadlineMs,
late })` (JS) spawns itself as a detached carrier process, sends it the request over a pipe, and
waits at most `deadline_s`/`deadlineMs` for a reply on that pipe:

- **Reply lands in time:** the caller acks, gets `{ok: true, status, json, ms}`, and the carrier
  exits.
- **Deadline passes first:** the caller stops waiting and gets `{abandoned: true, ms}`. It does
  **not** ack, and does **not** close the underlying connection — the carrier keeps holding it,
  independent of whether the original caller is still running, has moved on, or has exited.
  When the real reply eventually lands, the carrier appends one row to an optional ledger file
  (`late`) with the real token/usage counts from the response, so slow-but-successful requests
  still get accounted for even though the caller stopped waiting on them.
- **Transport failure or non-2xx status:** `{error: "...", ms}` (or `{error: "http_500", status,
  text}` etc.) — returned promptly, without waiting for any deadline.

The only short timeout in the whole path is the initial TCP connect (nothing has been sent yet,
so there's nothing to cancel by giving up on it). The read timeout the carrier applies
(`READ_TIMEOUT_S` / `READ_TIMEOUT_MS`, default 2 hours) is a safety net against a genuinely
half-open socket, not a cancellation mechanism — set it comfortably above the slowest reply your
endpoint can produce under a full queue.

## Install

No dependencies beyond the Python 3 standard library, or Node's built-ins for the JS version.

```sh
cp python/spark_noabort.py /wherever/you/vendor/scripts/
# or, for the JS side:
cp js/spark-noabort.mjs /wherever/you/vendor/scripts/
```

## Usage

### Python, as a library

```python
from spark_noabort import post_json

result = post_json(
    "http://localhost:8000/v1/chat/completions",
    {"model": "your-model", "messages": [...]},
    deadline_s=30,
    late={"path": "usage-ledger.jsonl", "row": {"caller": "my-job"}},
)
if result.get("abandoned"):
    ...  # move on; the real reply, if it lands, gets appended to usage-ledger.jsonl
elif result.get("ok"):
    ...  # result["json"] is the parsed response body
else:
    ...  # result["error"]
```

### Python, from the shell

```sh
echo '{"model": "your-model", "messages": []}' | \
  python3 spark_noabort.py --post http://localhost:8000/v1/chat/completions \
    --deadline 30 --late-ledger usage-ledger.jsonl
```

Prints the reply body to stdout and exits 0. Exits 124 (nothing printed) if the wait is
abandoned. Exits 1 (message on stderr) on a transport failure.

Under systemd or another process supervisor, run the caller inside its own scope (e.g.
`systemd-run --scope`): a oneshot unit's leftover processes get killed with its cgroup when the
unit's main process exits, and that kill would take the carrier's connection down with it.

### JS/Node, as a library

```js
import { sparkPost } from "./spark-noabort.mjs";

const result = await sparkPost(
  "http://localhost:8000/v1/chat/completions",
  { model: "your-model", messages: [] },
  { deadlineMs: 30_000, late: { path: "usage-ledger.jsonl", row: { caller: "my-job" } } },
);
```

Same three shapes as the Python version: `{ ok: true, status, json, ms }`,
`{ abandoned: true, ms }`, or `{ error, status?, ms }`.

## The `late` ledger

Pass `late` (Python: dict, JS: object) to have the carrier record a late-arriving reply after
the caller has already given up on it:

| field       | meaning |
|-------------|---------|
| `path`      | JSONL file to append to |
| `row`       | a template dict/object merged into the appended row (put your own identifying fields here) |
| `tsKey`     | key name for a timestamp added to the row; default `"ts"`, pass `null`/`None` to skip |
| `tokenKeys` | `[promptTokenKey, completionTokenKey]`; default `["promptTokens", "completionTokens"]` |

The appended row also gets `measured` (true if the reply's `usage` block had real token counts),
`late: true`, `outcome: "late-reply"`, and `lateMs` (time from the original request to the late
reply). Omit `late` entirely to skip ledgering — late replies are then simply discarded once the
carrier's connection closes.

## Exit codes (`--post` CLI mode, Python)

| code | meaning |
|------|---------|
| 0    | reply received before the deadline, printed to stdout |
| 1    | transport failure or non-2xx status (message on stderr) |
| 124  | deadline reached before a reply arrived (matches the conventional shell `timeout` code) |

## Tests

```sh
python3 -m unittest tests.test_spark_noabort -v
```

Spins up a local mock HTTP server and exercises: a reply that lands before the deadline, a
reply that lands after it (checks the call returns almost immediately rather than blocking for
the server's full delay, and that the late reply still gets ledgered once it arrives), a non-2xx
status, and a refused connection.

## Where this came from

Built for a six-seat multi-agent engineering setup where Claude Code, Codex, and Grok edit the
same repositories concurrently; see sean.theinformed.org/method/
