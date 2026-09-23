#!/usr/bin/env python3
"""
Pyth Indices live dashboard (PYTHOIL, SILVER, GOLD)
- Auto-refreshes free fallback token from app.pyth.com
- Polls Pyth Pro latest_price for multiple feeds
- Serves a live dashboard at http://127.0.0.1:8050
"""

from __future__ import annotations

import base64
import json
import ssl
import threading
import time
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Deque, Dict, List, Optional
from urllib.error import HTTPError
from urllib.request import Request, urlopen

TOKEN_URL = "https://app.pyth.com/api/lazer/token"
PRICE_URL = "https://pyth-lazer.dourolabs.app/v1/latest_price"

# Feed registry: key used in UI / API -> metadata
FEEDS: Dict[str, Dict[str, Any]] = {
    "PYTHOIL": {
        "feed_id": 3063,
        "symbol": "Commodities.Index.PYTHOIL/USD",
        "label": "PYTHOIL / USD",
        "color": "#3b82f6",
        "decimals": 5,
    },
    "SILVER": {
        "feed_id": 3154,
        "symbol": "Metal.Index.SILVER/USD",
        "label": "SILVER / USD",
        "color": "#c0c7d2",
        "decimals": 5,
    },
    "GOLD": {
        "feed_id": 3153,
        "symbol": "Metal.Index.GOLD/USD",
        "label": "GOLD / USD",
        "color": "#f5c542",
        "decimals": 5,
    },
    "GAS": {
        "feed_id": 3265,
        "symbol": "Commodities.Index.NATGAS/USD",
        "label": "NATGAS / USD",
        "color": "#f5c542",
        "decimals": 5,
    },
}

FEED_IDS: List[int] = [v["feed_id"] for v in FEEDS.values()]
ID_TO_KEY = {v["feed_id"]: k for k, v in FEEDS.items()}

POLL_INTERVAL_SEC = 1
REFRESH_SKEW_SEC = 60
HISTORY_LEN = 300
HOST = "0.0.0.0"
PORT = 8050


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def make_ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    try:
        import certifi

        ctx.load_verify_locations(certifi.where())
    except Exception:
        pass
    return ctx


SSL_CONTEXT = make_ssl_context()


def http_json(
    method: str,
    url: str,
    body: Optional[dict] = None,
    headers: Optional[dict] = None,
) -> Any:
    data = None
    req_headers = {
        "Accept": "application/json",
        "User-Agent": "pyth-indices-dashboard/1.1",
    }
    if headers:
        req_headers.update(headers)
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        req_headers["Content-Type"] = "application/json"
    req = Request(url, data=data, headers=req_headers, method=method)
    with urlopen(req, timeout=15, context=SSL_CONTEXT) as resp:
        return json.loads(resp.read().decode("utf-8"))


class PriceStore:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        # key -> latest sample
        self.latest: Dict[str, Dict[str, Any]] = {}
        # key -> deque of samples
        self.history: Dict[str, Deque[Dict[str, Any]]] = {
            k: deque(maxlen=HISTORY_LEN) for k in FEEDS
        }
        self.last_error: Optional[str] = None

    def update_many(self, samples: List[Dict[str, Any]]) -> None:
        with self.lock:
            for sample in samples:
                key = sample["key"]
                self.latest[key] = sample
                self.history[key].append(sample)
            self.last_error = None

    def set_error(self, msg: str) -> None:
        with self.lock:
            self.last_error = msg

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "feeds": {
                    k: {
                        "meta": {
                            "feed_id": FEEDS[k]["feed_id"],
                            "symbol": FEEDS[k]["symbol"],
                            "label": FEEDS[k]["label"],
                            "color": FEEDS[k]["color"],
                            "decimals": FEEDS[k]["decimals"],
                        },
                        "latest": self.latest.get(k),
                        "history": list(self.history[k]),
                    }
                    for k in FEEDS
                },
                "last_error": self.last_error,
                "server_time_utc": utc_now().isoformat(),
            }


class PythClient:
    def __init__(self) -> None:
        self.token: Optional[str] = None
        self.expires_at = 0.0

    def _decode_exp(self, token: str) -> float:
        try:
            payload = token.split(".")[1]
            payload += "=" * (-len(payload) % 4)
            claims = json.loads(base64.urlsafe_b64decode(payload))
            return float(claims["exp"])
        except Exception:
            return time.time() + 14 * 60

    def refresh_token(self) -> None:
        data = http_json("POST", TOKEN_URL, body={})
        tok = data["fallbackToken"]["accessToken"]
        self.token = tok
        self.expires_at = self._decode_exp(tok)
        exp_iso = datetime.fromtimestamp(self.expires_at, tz=timezone.utc).isoformat()
        print(f"[{utc_now().isoformat()}] token refreshed, expires {exp_iso}")

    def ensure_token(self) -> None:
        if not self.token or time.time() >= (self.expires_at - REFRESH_SKEW_SEC):
            self.refresh_token()

    def fetch_prices(self) -> List[Dict[str, Any]]:
        self.ensure_token()
        body = {
            "priceFeedIds": FEED_IDS,
            "properties": ["price", "exponent", "feedUpdateTimestamp"],
            "formats": [],
            "channel": "fixed_rate@1000ms",
            "deliveryFormat": "json",
        }
        headers = {"Authorization": f"Bearer {self.token}"}

        try:
            data = http_json("POST", PRICE_URL, body=body, headers=headers)
        except HTTPError as e:
            if e.code in (401, 403, 419):
                self.refresh_token()
                headers = {"Authorization": f"Bearer {self.token}"}
                data = http_json("POST", PRICE_URL, body=body, headers=headers)
            else:
                raise

        polled_at = utc_now().isoformat()
        samples: List[Dict[str, Any]] = []
        for feed in data["parsed"]["priceFeeds"]:
            feed_id = int(feed["priceFeedId"])
            key = ID_TO_KEY.get(feed_id)
            if not key:
                continue
            meta = FEEDS[key]
            expo = int(feed["exponent"])
            raw = int(feed["price"])
            price = raw * (10**expo)
            ts_us = int(
                feed.get("feedUpdateTimestamp")
                or data["parsed"].get("timestampUs")
                or 0
            )
            feed_ts = (
                datetime.fromtimestamp(ts_us / 1_000_000, tz=timezone.utc).isoformat()
                if ts_us
                else None
            )
            samples.append(
                {
                    "key": key,
                    "symbol": meta["symbol"],
                    "feed_id": feed_id,
                    "price": price,
                    "raw_price": raw,
                    "exponent": expo,
                    "feed_update_time_utc": feed_ts,
                    "polled_at_utc": polled_at,
                }
            )
        return samples


store = PriceStore()
client = PythClient()


def poll_loop() -> None:
    while True:
        try:
            samples = client.fetch_prices()
            store.update_many(samples)
            parts = [f"{s['key']} {s['price']}" for s in samples]
            print(f"[{utc_now().isoformat()}] " + " | ".join(parts))
        except Exception as exc:
            msg = f"{type(exc).__name__}: {exc}"
            store.set_error(msg)
            print(f"[{utc_now().isoformat()}] error: {msg}")
        time.sleep(POLL_INTERVAL_SEC)


DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Pyth Indices Dashboard</title>
  <style>
    :root { color-scheme: dark; }
    body {
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      background: #0b1220;
      color: #e8eefc;
    }
    .wrap { max-width: 1100px; margin: 0 auto; padding: 24px; }
    h1 { margin: 0 0 4px; font-size: 1.5rem; }
    .subtitle { color: #9db0d0; margin-bottom: 20px; font-size: .92rem; }
    .widgets {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 16px;
      margin-bottom: 16px;
    }
    .card {
      background: #121a2b;
      border: 1px solid #24304a;
      border-radius: 14px;
      padding: 18px;
      box-shadow: 0 8px 30px rgba(0,0,0,.25);
    }
    .card h2 {
      margin: 0 0 4px;
      font-size: 1rem;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .dot {
      width: 10px; height: 10px; border-radius: 50%;
      display: inline-block;
    }
    .muted { color: #9db0d0; font-size: .82rem; }
    .price {
      font-size: 2.2rem;
      font-weight: 700;
      letter-spacing: -0.03em;
      margin: 10px 0 4px;
    }
    .up { color: #3dde8c; }
    .down { color: #ff6b7a; }
    .flat { color: #e8eefc; }
    .meta-row {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
      margin-top: 12px;
    }
    .metric {
      background: #0e1626;
      border: 1px solid #24304a;
      border-radius: 10px;
      padding: 10px;
    }
    .label { color: #9db0d0; font-size: .72rem; margin-bottom: 3px; }
    .value { font-weight: 600; font-size: .85rem; word-break: break-all; }
    canvas {
      width: 100%;
      height: 120px;
      background: #0e1626;
      border-radius: 10px;
      border: 1px solid #24304a;
      margin-top: 12px;
    }
    .status-card { margin-top: 0; }
    .err { color: #ff6b7a; white-space: pre-wrap; margin-top: 6px; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Pyth Indices Dashboard</h1>
    <div class="subtitle">Live PYTHOIL · SILVER · GOLD · auto token refresh · poll every 10s</div>

    <div class="widgets" id="widgets"></div>

    <div class="card status-card">
      <div class="muted">Status</div>
      <div id="status" class="muted">connecting…</div>
      <div id="server" class="muted"></div>
      <div id="error" class="err"></div>
    </div>
  </div>

<script>
const ORDER = ['PYTHOIL', 'SILVER', 'GOLD','GAS'];
const widgetsEl = document.getElementById('widgets');
const statusEl = document.getElementById('status');
const serverEl = document.getElementById('server');
const errorEl = document.getElementById('error');

function fmtPrice(p, decimals) {
  if (p == null || Number.isNaN(p)) return '—';
  return Number(p).toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

function drawChart(canvas, history, color) {
  const ctx = canvas.getContext('2d');
  const w = canvas.width, h = canvas.height;
  ctx.clearRect(0, 0, w, h);
  if (!history || !history.length) return;
  const values = history.map(x => x.price);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const pad = (max - min) * 0.08 || 0.01;
  const lo = min - pad, hi = max + pad;
  ctx.strokeStyle = color || '#3b82f6';
  ctx.lineWidth = 2;
  ctx.beginPath();
  history.forEach((pt, i) => {
    const x = history.length === 1 ? w / 2 : (i / (history.length - 1)) * (w - 16) + 8;
    const y = h - 8 - ((pt.price - lo) / (hi - lo)) * (h - 16);
    if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  });
  ctx.stroke();
}

function ensureWidgets(feeds) {
  if (widgetsEl.children.length) return;
  for (const key of ORDER) {
    const f = feeds[key];
    if (!f) continue;
    const card = document.createElement('div');
    card.className = 'card';
    card.id = 'card-' + key;
    card.innerHTML = `
      <h2><span class="dot" style="background:${f.meta.color}"></span>${f.meta.label}</h2>
      <div class="muted">${f.meta.symbol} · feed ${f.meta.feed_id}</div>
      <div class="price flat" id="price-${key}">—</div>
      <div class="muted" id="delta-${key}">waiting…</div>
      <div class="meta-row">
        <div class="metric">
          <div class="label">Polled (UTC)</div>
          <div class="value" id="polled-${key}">—</div>
        </div>
        <div class="metric">
          <div class="label">Feed update (UTC)</div>
          <div class="value" id="feedts-${key}">—</div>
        </div>
      </div>
      <canvas id="chart-${key}" width="400" height="120"></canvas>
    `;
    widgetsEl.appendChild(card);
  }
}

function updateFeed(key, feed) {
  const decimals = feed.meta.decimals ?? 5;
  const priceEl = document.getElementById('price-' + key);
  const deltaEl = document.getElementById('delta-' + key);
  const polledEl = document.getElementById('polled-' + key);
  const feedtsEl = document.getElementById('feedts-' + key);
  const canvas = document.getElementById('chart-' + key);
  if (!priceEl) return;

  const hist = feed.history || [];
  drawChart(canvas, hist, feed.meta.color);

  if (!feed.latest) {
    priceEl.textContent = '—';
    deltaEl.textContent = 'waiting for first sample…';
    return;
  }

  const p = feed.latest.price;
  priceEl.textContent = fmtPrice(p, decimals);
  polledEl.textContent = feed.latest.polled_at_utc || '—';
  feedtsEl.textContent = feed.latest.feed_update_time_utc || '—';

  if (hist.length >= 2) {
    const prev = hist[hist.length - 2].price;
    const diff = p - prev;
    const pct = prev ? (diff / prev) * 100 : 0;
    const cls = diff > 0 ? 'up' : (diff < 0 ? 'down' : 'flat');
    priceEl.className = 'price ' + cls;
    const sign = diff >= 0 ? '+' : '';
    deltaEl.textContent =
      `${sign}${diff.toFixed(decimals)} (${sign}${pct.toFixed(3)}%) vs previous sample`;
  } else {
    priceEl.className = 'price flat';
    deltaEl.textContent = 'first sample';
  }
}

async function refresh() {
  try {
    const res = await fetch('/api/data', { cache: 'no-store' });
    const data = await res.json();
    serverEl.textContent = 'Server time (UTC): ' + (data.server_time_utc || '—');
    errorEl.textContent = data.last_error || '';
    const feeds = data.feeds || {};
    ensureWidgets(feeds);
    for (const key of ORDER) {
      if (feeds[key]) updateFeed(key, feeds[key]);
    }
    const ready = ORDER.every(k => feeds[k] && feeds[k].latest);
    statusEl.textContent = ready
      ? 'live · polling every few seconds'
      : 'waiting for first successful poll…';
  } catch (e) {
    statusEl.textContent = 'dashboard fetch failed';
    errorEl.textContent = String(e);
  }
}
refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path in ("/", "/index.html"):
            self._send(200, DASHBOARD_HTML.encode("utf-8"), "text/html; charset=utf-8")
            return
        if self.path.startswith("/api/data"):
            body = json.dumps(store.snapshot()).encode("utf-8")
            self._send(200, body, "application/json")
            return
        self._send(404, b"not found", "text/plain")


def main() -> None:
    t = threading.Thread(target=poll_loop, daemon=True)
    t.start()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"Dashboard: http://127.0.0.1:{PORT}")
    print("Feeds: PYTHOIL (3063), SILVER (3154), GOLD (3153)")
    print("Polling with auto token refresh…")
    server.serve_forever()


if __name__ == "__main__":
    main()
