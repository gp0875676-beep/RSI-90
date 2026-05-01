"""
RSI Tracking Bot - Production Grade
- Scans 5000+ coins on Binance (Free API)
- Timeframes: 1h and 4h
- Alerts: RSI >= 90 (overbought) or RSI <= 10 (oversold)
- Telegram alerts (Free)
- Railway deployable
"""

import asyncio
import aiohttp
import os
import logging
import time
from datetime import datetime
from collections import deque

# ─── Logging Setup ───────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Config from Environment Variables ───────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
RSI_OVERBOUGHT     = float(os.getenv("RSI_OVERBOUGHT", "90"))
RSI_OVERSOLD       = float(os.getenv("RSI_OVERSOLD", "10"))
RSI_PERIOD         = int(os.getenv("RSI_PERIOD", "14"))
SCAN_INTERVAL_MIN  = int(os.getenv("SCAN_INTERVAL_MIN", "30"))   # minutes between full scans
MAX_CONCURRENT     = int(os.getenv("MAX_CONCURRENT", "50"))       # parallel API calls

BINANCE_BASE_URL   = "https://api.binance.com"
TIMEFRAMES         = ["1h", "4h"]

# ─── Rate Limiter ─────────────────────────────────────────────────────────────
class RateLimiter:
    """Token bucket rate limiter for Binance: 1200 req/min"""
    def __init__(self, rate: int = 1000, per: float = 60.0):
        self.rate      = rate
        self.per       = per
        self.allowance = rate
        self.last_check = time.monotonic()
        self._lock     = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now     = time.monotonic()
            elapsed = now - self.last_check
            self.last_check = now
            self.allowance  = min(self.rate, self.allowance + elapsed * (self.rate / self.per))
            if self.allowance < 1:
                sleep_time = (1 - self.allowance) / (self.rate / self.per)
                await asyncio.sleep(sleep_time)
                self.allowance = 0
            else:
                self.allowance -= 1

rate_limiter = RateLimiter(rate=1000, per=60.0)

# ─── Alert Deduplication ──────────────────────────────────────────────────────
class AlertTracker:
    """Prevents duplicate alerts for same coin+timeframe within cooldown period"""
    def __init__(self, cooldown_hours: int = 4):
        self.cooldown = cooldown_hours * 3600
        self.alerts   = {}  # key: (symbol, tf, condition) -> timestamp

    def should_alert(self, symbol: str, tf: str, condition: str) -> bool:
        key  = (symbol, tf, condition)
        now  = time.time()
        last = self.alerts.get(key, 0)
        if now - last >= self.cooldown:
            self.alerts[key] = now
            return True
        return False

    def cleanup(self):
        now     = time.time()
        expired = [k for k, v in self.alerts.items() if now - v > self.cooldown * 2]
        for k in expired:
            del self.alerts[k]

alert_tracker = AlertTracker(cooldown_hours=4)

# ─── RSI Calculator ──────────────────────────────────────────────────────────
def calculate_rsi(closes: list[float], period: int = 14) -> float | None:
    """
    Wilder's RSI — exact same formula used by TradingView
    Returns None if not enough data
    """
    if len(closes) < period + 1:
        return None

    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains  = [max(d, 0.0) for d in deltas]
    losses = [abs(min(d, 0.0)) for d in deltas]

    # Initial average (simple mean for first period)
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    # Wilder smoothing for remaining
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs  = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return round(rsi, 2)

# ─── Binance API Calls ────────────────────────────────────────────────────────
async def fetch_all_symbols(session: aiohttp.ClientSession) -> list[str]:
    """Fetch all USDT perpetual futures symbols from Binance"""
    try:
        await rate_limiter.acquire()
        async with session.get(f"{BINANCE_BASE_URL}/api/v3/exchangeInfo", timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                log.error(f"exchangeInfo failed: HTTP {resp.status}")
                return []
            data    = await resp.json()
            symbols = [
                s["symbol"]
                for s in data.get("symbols", [])
                if s.get("quoteAsset") == "USDT"
                and s.get("status") == "TRADING"
                and s.get("isSpotTradingAllowed", False)
            ]
            log.info(f"Found {len(symbols)} USDT spot symbols")
            return symbols
    except Exception as e:
        log.error(f"fetch_all_symbols error: {e}")
        return []

async def fetch_klines(session: aiohttp.ClientSession, symbol: str, interval: str, limit: int = 100) -> list[float] | None:
    """Fetch closing prices for a symbol+timeframe"""
    url    = f"{BINANCE_BASE_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        await rate_limiter.acquire()
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 400:
                return None  # Invalid symbol — skip silently
            if resp.status != 200:
                log.debug(f"klines {symbol} {interval}: HTTP {resp.status}")
                return None
            data   = await resp.json()
            closes = [float(candle[4]) for candle in data]
            return closes if len(closes) >= RSI_PERIOD + 1 else None
    except asyncio.TimeoutError:
        log.debug(f"Timeout: {symbol} {interval}")
        return None
    except Exception as e:
        log.debug(f"klines error {symbol} {interval}: {e}")
        return None

# ─── Telegram Alerts ─────────────────────────────────────────────────────────
async def send_telegram(session: aiohttp.ClientSession, message: str) -> bool:
    """Send a Telegram message — returns True on success"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured — alert skipped")
        log.info(f"ALERT (no TG): {message}")
        return False
    url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       message,
        "parse_mode": "HTML",
    }
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                return True
            body = await resp.text()
            log.error(f"Telegram error {resp.status}: {body}")
            return False
    except Exception as e:
        log.error(f"send_telegram exception: {e}")
        return False

def build_alert_message(symbol: str, tf: str, rsi: float, condition: str) -> str:
    emoji    = "🔴" if condition == "OVERBOUGHT" else "🟢"
    label    = "OVERBOUGHT (RSI ≥ 90)" if condition == "OVERBOUGHT" else "OVERSOLD (RSI ≤ 10)"
    now_str  = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    return (
        f"{emoji} <b>RSI ALERT — {label}</b>\n\n"
        f"🪙 <b>Coin:</b> {symbol}\n"
        f"⏱ <b>Timeframe:</b> {tf.upper()}\n"
        f"📊 <b>RSI:</b> {rsi}\n"
        f"🕐 <b>Time:</b> {now_str}\n\n"
        f"#RSI #{symbol} #{tf.upper()}"
    )

# ─── Worker: Process one symbol across timeframes ─────────────────────────────
async def process_symbol(
    session: aiohttp.ClientSession,
    symbol: str,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """Returns list of alert dicts if any RSI condition triggered"""
    alerts = []
    async with semaphore:
        for tf in TIMEFRAMES:
            closes = await fetch_klines(session, symbol, tf, limit=RSI_PERIOD + 20)
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
                log.info(f"ALERT → {symbol} [{tf}] RSI={rsi} ({condition})")

    return alerts

# ─── Full Scan ────────────────────────────────────────────────────────────────
async def run_scan():
    """One complete scan of all symbols"""
    scan_start = time.monotonic()
    log.info("═" * 60)
    log.info("🚀 Starting RSI scan...")

    connector = aiohttp.TCPConnector(limit=200, ttl_dns_cache=300)
    timeout   = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        symbols = await fetch_all_symbols(session)
        if not symbols:
            log.error("No symbols fetched — skipping scan")
            return

        total     = len(symbols)
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)

        log.info(f"Scanning {total} symbols × {len(TIMEFRAMES)} timeframes...")

        # Chunk into batches to avoid memory overload
        BATCH = 500
        all_alerts = []

        for i in range(0, total, BATCH):
            chunk  = symbols[i : i + BATCH]
            tasks  = [process_symbol(session, sym, semaphore) for sym in chunk]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for r in results:
                if isinstance(r, list):
                    all_alerts.extend(r)
                elif isinstance(r, Exception):
                    log.debug(f"Task exception: {r}")

            done = min(i + BATCH, total)
            log.info(f"Progress: {done}/{total} symbols scanned")

        # Send alerts
        if all_alerts:
            log.info(f"🔔 Sending {len(all_alerts)} alerts...")
            for alert in all_alerts:
                msg = build_alert_message(
                    alert["symbol"], alert["tf"], alert["rsi"], alert["condition"]
                )
                await send_telegram(session, msg)
                await asyncio.sleep(0.3)  # avoid Telegram flood limit
        else:
            log.info("✅ No RSI alerts this scan")

        # Cleanup old alert records
        alert_tracker.cleanup()

        elapsed = time.monotonic() - scan_start
        log.info(f"✅ Scan complete in {elapsed:.1f}s — {len(all_alerts)} alerts sent")
        log.info("═" * 60)

# ─── Startup Test ─────────────────────────────────────────────────────────────
async def send_startup_message(session: aiohttp.ClientSession):
    msg = (
        "✅ <b>RSI Bot Started!</b>\n\n"
        f"📊 RSI Period: {RSI_PERIOD}\n"
        f"🔴 Overbought trigger: ≥ {RSI_OVERBOUGHT}\n"
        f"🟢 Oversold trigger: ≤ {RSI_OVERSOLD}\n"
        f"⏱ Timeframes: {', '.join(t.upper() for t in TIMEFRAMES)}\n"
        f"🔄 Scan interval: every {SCAN_INTERVAL_MIN} minutes\n"
        f"⚡ Concurrency: {MAX_CONCURRENT} parallel requests\n\n"
        "Bot is live and scanning Binance USDT pairs!"
    )
    await send_telegram(session, msg)

# ─── Main Loop ────────────────────────────────────────────────────────────────
async def main():
    log.info("RSI Tracking Bot — Starting up")
    log.info(f"Config: RSI{RSI_PERIOD} | OB≥{RSI_OVERBOUGHT} | OS≤{RSI_OVERSOLD}")
    log.info(f"Timeframes: {TIMEFRAMES} | Concurrency: {MAX_CONCURRENT}")

    # Send startup notification
    async with aiohttp.ClientSession() as session:
        await send_startup_message(session)

    while True:
        try:
            await run_scan()
        except Exception as e:
            log.error(f"Scan crashed (will retry): {e}", exc_info=True)

        log.info(f"💤 Sleeping {SCAN_INTERVAL_MIN} minutes until next scan...")
        await asyncio.sleep(SCAN_INTERVAL_MIN * 60)

if __name__ == "__main__":
    asyncio.run(main())
