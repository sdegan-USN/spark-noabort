// spark-noabort.mjs: the never-abort transport for a slow LLM inference endpoint (JS twin of
// python/spark_noabort.py in this repo — same protocol, same behavior).
//
// Any closed connection is an abort: a server like vLLM cancels a request whose client has
// disconnected. An AbortSignal closes it; so does Node's built-in fetch on its own after 300s
// with no response headers (undici's headersTimeout, UND_ERR_HEADERS_TIMEOUT — this is a
// default, not a bug, but it is easy to hit by accident); so does the process exiting,
// `process.exit()` included. A non-streaming chat completion sends its headers only when the
// whole reply is ready, so a queued or slow request can easily cross that default.
//
//   sparkPost(url, body, { deadlineMs, late }) ->
//     { ok: true, status, json, ms } | { abandoned: true, ms } | { error, status?, ms }
//
// The request is handed to a CARRIER: this same file run as `node spark-noabort.mjs --carry`,
// detached, sharing no stdout/stderr with the caller. It holds the connection through
// node:http with no timeout short of READ_TIMEOUT_MS and writes the reply back on a pipe.
// The caller races the reply against `deadlineMs`. In time: it acks and gets the reply.
// Deadline: it closes the pipe without an ack and gets { abandoned: true }; the carrier keeps
// the connection and, when the reply lands, appends one row to a caller-supplied ledger file:
// late.row plus the real token counts from the reply's usage block, measured, late. The
// original caller can exit, crash, or be killed in the meantime — the carrier is a separate,
// detached process.
//
// `late` = { path, row, tsKey = "ts", tokenKeys = ["promptTokens", "completionTokens"] }.
// Pass no `late` (or omit it) to skip ledgering late replies entirely.

import { spawn } from "node:child_process";
import { appendFileSync, mkdirSync } from "node:fs";
import http from "node:http";
import https from "node:https";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";

const SELF = fileURLToPath(import.meta.url);
// A safety net against a genuinely half-open socket, not a cancellation mechanism: set this
// comfortably above the slowest reply your endpoint can produce under a full queue.
export const READ_TIMEOUT_MS = 7_200_000;

function post(url, body) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const data = Buffer.from(JSON.stringify(body));
    const lib = u.protocol === "https:" ? https : http;
    const req = lib.request(
      {
        method: "POST",
        hostname: u.hostname,
        port: u.port || (u.protocol === "https:" ? 443 : 80),
        path: `${u.pathname}${u.search}`,
        headers: { "content-type": "application/json", "content-length": data.length },
        agent: false,
      },
      (res) => {
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () =>
          resolve({ status: res.statusCode || 0, text: Buffer.concat(chunks).toString("utf8") }),
        );
        res.on("error", reject);
      },
    );
    req.setTimeout(READ_TIMEOUT_MS, () => req.destroy(new Error("read timeout (safety net)")));
    req.on("error", reject);
    req.end(data);
  });
}

export function ledgerLate(late, replyText, startedMs) {
  if (!late?.path) return;
  try {
    let usage = {};
    try {
      usage = JSON.parse(replyText)?.usage || {};
    } catch {
      usage = {};
    }
    const [pk, ck] = late.tokenKeys || ["promptTokens", "completionTokens"];
    const row = { ...(late.row || {}) };
    const tsKey = late.tsKey === undefined ? "ts" : late.tsKey;
    if (tsKey) row[tsKey] = new Date().toISOString();
    Object.assign(row, {
      [pk]: usage.prompt_tokens ?? null,
      [ck]: usage.completion_tokens ?? null,
      measured: usage.prompt_tokens != null,
      late: true,
      outcome: "late-reply",
      lateMs: Date.now() - startedMs,
    });
    mkdirSync(dirname(late.path), { recursive: true });
    appendFileSync(late.path, `${JSON.stringify(row)}\n`);
  } catch {
    /* a ledger failure never breaks anything */
  }
}

async function carry() {
  let buf = "";
  let ended = false;
  let wake = null;
  process.stdin.setEncoding("utf8");
  process.stdin.on("data", (d) => {
    buf += d;
    wake?.();
  });
  process.stdin.on("end", () => {
    ended = true;
    wake?.();
  });
  const nextLine = async () => {
    for (;;) {
      const i = buf.indexOf("\n");
      if (i >= 0) {
        const line = buf.slice(0, i);
        buf = buf.slice(i + 1);
        return line;
      }
      if (ended) return null;
      await new Promise((r) => (wake = r));
      wake = null;
    }
  };
  const spec = JSON.parse((await nextLine()) || "{}");
  let out;
  try {
    out = await post(spec.url, spec.body);
  } catch (e) {
    out = { error: String(e?.message || e).slice(0, 300) };
  }
  process.stdout.on("error", () => {});
  let acked = false;
  if (!ended) {
    try {
      process.stdout.write(`${JSON.stringify(out)}\n`);
      acked = (await nextLine())?.trim() === "ack";
    } catch {
      acked = false;
    }
  }
  if (!acked && out.text !== undefined && out.status < 400)
    ledgerLate(spec.late, out.text, spec.t0 || Date.now());
  process.exit(0);
}

/** POST `body` to an endpoint; never aborts the underlying request. See the header. */
export function sparkPost(url, body, { deadlineMs, late = null } = {}) {
  const t0 = Date.now();
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [SELF, "--carry"], {
      detached: true,
      stdio: ["pipe", "pipe", "ignore"],
    });
    let settled = false;
    let out = "";
    const finish = (value, ack) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try {
        child.stdin.end(ack ? "ack\n" : undefined);
      } catch {
        /* already closed */
      }
      child.stdout.destroy();
      child.unref();
      resolve({ ...value, ms: Date.now() - t0 });
    };
    const timer = setTimeout(
      () => finish({ abandoned: true }, false),
      Math.max(0, Number(deadlineMs) || 0),
    );
    child.stdin.on("error", () => {});
    child.on("error", (e) =>
      finish({ error: `carrier failed to start: ${e?.message || e}` }, false),
    );
    child.on("exit", () => {
      if (!out.includes("\n")) finish({ error: "carrier exited without a reply" }, false);
    });
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (d) => {
      out += d;
      const i = out.indexOf("\n");
      if (i < 0 || settled) return;
      let reply;
      try {
        reply = JSON.parse(out.slice(0, i));
      } catch (e) {
        finish({ error: `bad carrier reply: ${e.message}` }, true);
        return;
      }
      if (reply.error) return finish({ error: reply.error }, true);
      if (reply.status >= 400) {
        return finish(
          {
            error: `http_${reply.status}`,
            status: reply.status,
            text: String(reply.text).slice(0, 500),
          },
          true,
        );
      }
      try {
        finish({ ok: true, status: reply.status, json: JSON.parse(reply.text) }, true);
      } catch (e) {
        finish({ error: `bad json: ${e.message}`, status: reply.status }, true);
      }
    });
    child.stdin.write(`${JSON.stringify({ url, body, late, t0 })}\n`);
  });
}

if (process.argv[1] && process.argv[1] === SELF && process.argv[2] === "--carry") {
  carry();
}
