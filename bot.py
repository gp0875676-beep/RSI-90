"""
RSI Tracking Bot - Production Grade v2
- Multi-Exchange Fallback: Binance → Bybit → Gate.io → KuCoin
- Scans 1000+ USDT coins
- Timeframes: 1h and 4h
- Alerts: RSI >= 90 (overbought) or RSI <= 10 (oversold)
- Telegram alerts
- Railway deployable
"""

import asyncio
import aiohttp
import os
import logging
import time
from datetime import datetime

# ─── Logging Setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
RSI_OVERBOUGHT     = float(os.getenv("RSI_OVERBOUGHT", "90"))
RSI_OVERSOLD       = float(os.getenv("RSI_OVERSOLD", "10"))
RSI_PERIOD         = int(os.getenv("RSI_PERIOD", "14"))
SCAN_INTERVAL_MIN  = int(os.getenv("SCAN_INTERVAL_MIN", "30"))
MAX_CONCURRENT     = int(os.getenv("MAX_CONCURRENT", "50"))
TIMEFRAMES         = ["1h", "4h"]

# ─── Exchange Configs (fallback order) ───────────────────────────────────────
EXCHANGES = [
    {
        "name":        "Binance",
        "type":        "binance",
        "symbols_url": "https://api.binance.com/api/v3/exchangeInfo",
        "klines_url":  "https://api.binance.com/api/v3/klines",
    },
    {
        "name":        "Binance Futures",
        "type":        "binance",
        "symbols_url": "https://fapi.binance.com/fapi/v1/exchangeInfo",
        "klines_url":  "https://fapi.binance.com/fapi/v1/klines",
    },
    {
        "name":        "Bybit",
        "type":        "bybit",
        "symbols_url": "https://api.bybit.com/v5/market/instruments-info?category=spot&limit=1000",
        "klines_url":  "https://api.bybit.com/v5/market/kline",
    },
    {
        "name":        "Gate.io",
        "type":        "gate",
        "symbols_url": "https://api.gateio.ws/api/v4/spot/currency_pairs",
        "klines_url":  "https://api.gateio.ws/api/v4/spot/candlesticks",
    },
    {
        "name":        "KuCoin",
        "type":        "kucoin",
        "symbols_url": "https://api.kucoin.com/api/v2/symbols",
        "klines_url":  "https://api.kucoin.com/api/v1/market/candles",
    },
]

_active_exchange: dict | None = None

# ─── Rate Limiter ─────────────────────────────────────────────────────────────
class RateLimiter:
    def __init__(self, rate: int = 800, per: float = 60.0):
        self.rate       = rate
        self.per        = per
        self.allowance  = float(rate)
        self.last_check = time.monotonic()
        self._lock      = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now             = time.monotonic()
            elapsed         = now - self.last_check
            self.last_check = now
            self.allowance  = min(self.rate, self.allowance + elapsed * (self.rate / self.per))
            if self.allowance < 1:
                wait = (1 - self.allowance) / (self.rate / self.per)
                await asyncio.sleep(wait)
                self.allowance = 0.0
            else:
                self.allowance -= 1.0

rate_limiter = RateLimiter()

# ─── Alert Deduplication ──────────────────────────────────────────────────────
class AlertTracker:
    def __init__(self, cooldown_hours: int = 4):
        self.cooldown = cooldown_hours * 3600
        self.alerts   = {}

    def should_alert(self, symbol: str, tf: str, condition: str) -> bool:
        key  = (symbol, tf, condition)
        now  = time.time()
        if now - self.alerts.get(key, 0) >= self.cooldown:
            self.alerts[key] = now
            return True
        return False

    def cleanup(self):
        now     = time.time()
        expired = [k for k, v in self.alerts.items() if now - v > self.cooldown * 2]
        for k in expired:
            del self.alerts[k]

alert_tracker = AlertTracker(cooldown_hours=4)

# ─── RSI Calculator ───────────────────────────────────────────────────────────
def calculate_rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    deltas   = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains    = [max(d, 0.0) for d in deltas]
    losses   = [abs(min(d, 0.0)) for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    return round(100 - 100 / (1 + avg_gain / avg_loss), 2)

# ─── Exchange: Auto-Probe ─────────────────────────────────────────────────────
async def probe_exchanges(session: aiohttp.ClientSession) -> dict | None:
    global _active_exchange
    for ex in EXCHANGES:
        try:
            await rate_limiter.acquire()
            async with session.get(
                ex["symbols_url"],
                timeout=aiohttp.ClientTimeout(total=10),
                headers={"User-Agent": "Mozilla/5.0"},
            ) as resp:
                if resp.status == 200:
                    log.info(f"✅ Exchange selected: {ex['name']}")
                    _active_exchange = ex
                    return ex
                log.warning(f"⚠️  {ex['name']}: HTTP {resp.status} — trying next...")
        except Exception as e:
            log.warning(f"⚠️  {ex['name']}: {str(e)[:60]} — trying next...")
        await asyncio.sleep(0.5)
    log.error("❌ All exchanges failed!")
    return None

# ─── Fetch Symbols ────────────────────────────────────────────────────────────
async def fetch_symbols(session: aiohttp.ClientSession, ex: dict) -> list[str]:
    try:
        await rate_limiter.acquire()
        async with session.get(
            ex["symbols_url"],
            timeout=aiohttp.ClientTimeout(total=20),
            headers={"User-Agent": "Mozilla/5.0"},
        ) as resp:
            if resp.status != 200:
                log.error(f"fetch_symbols HTTP {resp.status}")
                return []
            data = await resp.json(content_type=None)
            t    = ex["type"]

            if t == "binance":
                syms = [
                    s["symbol"] for s in data.get("symbols", [])
                    if s.get("quoteAsset") == "USDT"
                    and s.get("status") == "TRADING"
                ]
            elif t == "bybit":
                syms = [
                    s["symbol"] for s in data.get("result", {}).get("list", [])
                    if s["symbol"].endswith("USDT") and s.get("status") == "Trading"
                ]
            elif t == "gate":
                syms = [
                    s["id"].replace("_", "") for s in data
                    if s.get("quote") == "USDT" and s.get("trade_status") == "tradable"
                ]
            elif t == "kucoin":
                syms = [
                    s["symbol"].replace("-", "") for s in data.get("data", [])
                    if s.get("quoteCurrency") == "USDT" and s.get("enableTrading")
                ]
            else:
                syms = []

            log.info(f"{ex['name']}: {len(syms)} USDT symbols found")
            return syms
    except Exception as e:
        log.error(f"fetch_symbols error: {e}")
        return []

# ─── Fetch Klines ─────────────────────────────────────────────────────────────
async def fetch_klines(
    session:  aiohttp.ClientSession,
    ex:       dict,
    symbol:   str,
    interval: str,
    limit:    int = 50,
) -> list[float] | None:
    t = ex["type"]

    # Build params per exchange
    if t == "binance":
        params = {"symbol": symbol, "interval": interval, "limit": limit}

    elif t == "bybit":
        tf_map = {"1h": "60", "4h": "240", "15m": "15", "1d": "D"}
        params = {
            "category": "spot",
            "symbol":   symbol,
            "interval": tf_map.get(interval, "60"),
            "limit":    limit,
        }

    elif t == "gate":
        base   = symbol.replace("USDT", "")
        tf_map = {"1h": "1h", "4h": "4h", "15m": "15m", "1d": "1d"}
        params = {
            "currency_pair": f"{base}_USDT",
            "interval":      tf_map.get(interval, "1h"),
            "limit":         limit,
        }

    elif t == "kucoin":
        base   = symbol.replace("USDT", "")
        tf_map = {"1h": "1hour", "4h": "4hour", "15m": "15min", "1d": "1day"}
        params = {"symbol": f"{base}-USDT", "type": tf_map.get(interval, "1hour")}

    else:
        return None

    try:
        await rate_limiter.acquire()
        async with session.get(
            ex["klines_url"],
            params=params,
            timeout=aiohttp.ClientTimeout(total=10),
            headers={"User-Agent": "Mozilla/5.0"},
        ) as resp:
            if resp.status in (400, 404):
                return None
            if resp.status != 200:
                return None
            raw    = await resp.json(content_type=None)
            closes = []

            if t == "binance":
                closes = [float(c[4]) for c in raw]

            elif t == "bybit":
                # newest first → reverse
                rows   = list(reversed(raw.get("result", {}).get("list", [])))
                closes = [float(r[4]) for r in rows]

            elif t == "gate":
                closes = [float(c["c"]) for c in raw]

            elif t == "kucoin":
                # newest first → reverse
                rows   = list(reversed(raw.get("data", [])))
                closes = [float(r[2]) for r in rows]

            return closes if len(closes) >= RSI_PERIOD + 1 else None

    except asyncio.TimeoutError:
        return None
    except Exception:
        return None

# ─── Telegram ─────────────────────────────────────────────────────────────────
async def send_telegram(session: aiohttp.ClientSession, message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.info(f"[NO TG] {message[:100]}")
        return False
    try:
        async with session.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            return resp.status == 200
    except Exception as e:
        log.error(f"Telegram: {e}")
        return False

def build_alert_message(symbol: str, tf: str, rsi: float, condition: str, exchange_name: str) -> str:
    emoji   = "🔴" if condition == "OVERBOUGHT" else "🟢"
    label   = "OVERBOUGHT (RSI ≥ 90)" if condition == "OVERBOUGHT" else "OVERSOLD (RSI ≤ 10)"
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    return (
        f"{emoji} <b>RSI ALERT — {label}</b>\n\n"
        f"🪙 <b>Coin:</b> <code>{symbol}</code>\n"
        f"⏱ <b>Timeframe:</b> {tf.upper()}\n"
        f"📊 <b>RSI:</b> {rsi}\n"
        f"🏦 <b>Exchange:</b> {exchange_name}\n"
        f"🕐 <b>Time:</b> {now_str}\n\n"
        f"#RSI #{symbol} #{tf.upper()} #{condition}"
    )

# ─── Worker ───────────────────────────────────────────────────────────────────
async def process_symbol(
    session:   aiohttp.ClientSession,
    ex:        dict,
    symbol:    str,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    alerts = []
    async with semaphore:
        for tf in TIMEFRAMES:
            closes = await fetch_klines(session, ex, symbol, tf, limit=RSI_PERIOD + 20)
            if not closes:
                continue
            rsi = calculate_rsi(closes, RSI_PERIOD)
            if rsi is None:
                continue
            condition = None
            if rsi >= RSI_OVERBOUGHT:
                condition = "OVERBOUGHT"
            elif rsi <= RSI_OVERSOLD:
                condition = "OVERSOLD"
            if condition and alert_tracker.should_alert(symbol, tf, condition):
                alerts.append({"symbol": symbol, "tf": tf, "rsi": rsi, "condition": condition})
                log.info(f"🔔 {symbol} [{tf}] RSI={rsi} ({condition})")
    return alerts

# ─── Full Scan ────────────────────────────────────────────────────────────────
async def run_scan():
    global _active_exchange
    scan_start = time.monotonic()
    log.info("═" * 60)
    log.info("🚀 Starting RSI scan...")

    connector = aiohttp.TCPConnector(limit=200, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:

        # Probe if no active exchange
        ex = _active_exchange
        if ex is None:
            ex = await probe_exchanges(session)
        if ex is None:
            log.error("No working exchange found — retrying next scan")
            return

        symbols = await fetch_symbols(session, ex)
        if not symbols:
            log.warning(f"{ex['name']} returned 0 symbols — re-probing next scan")
            _active_exchange = None
            return

        log.info(f"Scanning {len(symbols)} symbols × {len(TIMEFRAMES)} timeframes...")

        semaphore  = asyncio.Semaphore(MAX_CONCURRENT)
        all_alerts = []
        BATCH      = 500

        for i in range(0, len(symbols), BATCH):
            chunk   = symbols[i: i + BATCH]
            tasks   = [process_symbol(session, ex, s, semaphore) for s in chunk]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, list):
                    all_alerts.extend(r)
            log.info(f"Progress: {min(i+BATCH, len(symbols))}/{len(symbols)} | Signals: {len(all_alerts)}")

        if all_alerts:
            log.info(f"🔔 Sending {len(all_alerts)} alerts...")
            for a in all_alerts:
                msg = build_alert_message(a["symbol"], a["tf"], a["rsi"], a["condition"], ex["name"])
                await send_telegram(session, msg)
                await asyncio.sleep(0.3)
        else:
            log.info("✅ No RSI alerts this scan")

        alert_tracker.cleanup()
        elapsed = time.monotonic() - scan_start
        log.info(f"✅ Scan complete in {elapsed:.1f}s — {len(all_alerts)} alerts sent")
        log.info("═" * 60)

# ─── Startup ──────────────────────────────────────────────────────────────────
async def send_startup_message(session: aiohttp.ClientSession, ex_name: str):
    msg = (
        f"✅ <b>RSI Bot v2 Started!</b>\n\n"
        f"🏦 <b>Exchange:</b> {ex_name}\n\n"
        f"📊 RSI Period: {RSI_PERIOD}\n"
        f"🔴 Overbought: ≥ {RSI_OVERBOUGHT}\n"
        f"🟢 Oversold:   ≤ {RSI_OVERSOLD}\n"
        f"⏱ Timeframes: {', '.join(t.upper() for t in TIMEFRAMES)}\n"
        f"🔄 Scan every: {SCAN_INTERVAL_MIN} min\n\n"
        f"Bot is live! 🚀"
    )
    await send_telegram(session, msg)

# ─── Main ─────────────────────────────────────────────────────────────────────
async def main():
    global _active_exchange
    log.info("RSI Bot v2 — Initializing with multi-exchange fallback")

    async with aiohttp.ClientSession() as session:
        ex = await probe_exchanges(session)
        if ex:
            await send_startup_message(session, ex["name"])
        else:
            log.error("No exchange reachable on startup — will retry each scan")

    while True:
        try:
            await run_scan()
        except Exception as e:
            log.error(f"Scan crashed (auto-retry): {e}", exc_info=True)
            _active_exchange = None  # force re-probe
        log.info(f"💤 Sleeping {SCAN_INTERVAL_MIN} minutes until next scan...")
        await asyncio.sleep(SCAN_INTERVAL_MIN * 60)

if __name__ == "__main__":
    asyncio.run(main())
