# -*- coding: utf-8 -*-
"""
RF Futures Bot — PROFESSIONAL SNIPER EDITION v17.3 (Institutional Hunter)
+ Sniper Engine + Monitoring + Auto-Recovery (FULLY INTEGRATED)
FIX: _ema defined globally for radar scanner
+ Sniper Queue (Non‑Blocking Priority System)
"""

import os, time, math, random, signal, sys, traceback, logging, json, datetime as dt, gc
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
import ccxt
from flask import Flask, jsonify, request
from decimal import Decimal, ROUND_DOWN, InvalidOperation

try:
    from termcolor import colored
except Exception:
    def colored(t,*a,**k): return t

try:
    from zoneinfo import ZoneInfo
    BERLIN_TZ = ZoneInfo("Europe/Berlin")
    UTC = ZoneInfo("UTC")
except ImportError:
    BERLIN_TZ = timezone(timedelta(hours=2))
    UTC = timezone.utc

# =================== CONFIG SYSTEM ===================
import os

class Config:
    def __init__(self):
        self.BINGX_API_KEY     = os.getenv("BINGX_API_KEY")
        self.BINGX_API_SECRET  = os.getenv("BINGX_API_SECRET")
        self.TG_TOKEN          = os.getenv("TELEGRAM_BOT_TOKEN")
        self.TG_CHAT_ID        = os.getenv("TELEGRAM_CHAT_ID")
        self.PORT              = int(os.getenv("PORT", 5000))
        self.SELF_URL          = os.getenv("RENDER_EXTERNAL_URL", "")

CONFIG = Config()

# =================== VALIDATION ===================
def validate_config(cfg):
    errors = []

    if not cfg.BINGX_API_KEY:
        errors.append("Missing BINGX_API_KEY")
    if not cfg.BINGX_API_SECRET:
        errors.append("Missing BINGX_API_SECRET")
    if not cfg.TG_TOKEN:
        errors.append("Missing TELEGRAM_BOT_TOKEN")
    if not cfg.TG_CHAT_ID:
        errors.append("Missing TELEGRAM_CHAT_ID")

    if errors:
        print("CONFIG ERRORS:")
        for e in errors:
            print(" -", e)
        raise SystemExit("❌ CONFIG INVALID")

validate_config(CONFIG)

def now_utc():
    return datetime.now(UTC)

def now_berlin():
    return datetime.now(BERLIN_TZ)

def format_time(dt_obj=None):
    if dt_obj is None:
        dt_obj = now_utc()
    return dt_obj.astimezone(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S")

# =================== STATS PERSISTENCE ===================
STATS = {
    "trades": 0,
    "wins": 0,
    "losses": 0,
    "total_pnl": 0.0
}

def load_stats():
    global STATS
    if os.path.exists("stats.json"):
        try:
            with open("stats.json", "r") as f:
                loaded = json.load(f)
                STATS.update(loaded)
        except Exception:
            pass

def save_stats():
    try:
        with open("stats.json", "w") as f:
            json.dump(STATS, f)
    except Exception:
        pass

# =================== COLOR CLASS ===================
class C:
    GREEN = "\033[92m"
    RED = "\033[91m"
    ORANGE = "\033[38;5;208m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    MAGENTA = "\033[95m"
    WHITE = "\033[97m"
    RESET = "\033[0m"

# =================== DASHBOARD STATE ===================
DASHBOARD_STATE = {
    "account": {
        "balance": 0.0,
        "free": 0.0,
        "used": 0.0,
        "mode": "LIVE"
    },
    "stats": {
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "profit_total": 0.0
    },
    "position": None,
    "scanner": {
        "running": False,
        "last_update": "",
        "top5": []
    },
    "logs": [],
    "errors": []
}

# =================== MONITORING SYSTEM ===================
MONITOR_ERRORS = []
MONITOR_WARNINGS = []
MAX_MONITOR_LOGS = 50

def monitor_log_error(msg):
    entry = f"[{format_time()}] 🔴 {msg}"
    print(f"{C.RED}{entry}{C.RESET}", flush=True)
    MONITOR_ERRORS.append(entry)
    if len(MONITOR_ERRORS) > MAX_MONITOR_LOGS:
        MONITOR_ERRORS.pop(0)
    if TG_TOKEN and TG_CHAT:
        try:
            import requests
            requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                          json={"chat_id": TG_CHAT, "text": f"🔴 ERROR\n{format_time()}\n{msg}"}, timeout=5)
        except:
            pass

def monitor_log_warning(msg):
    entry = f"[{format_time()}] 🟡 {msg}"
    print(f"{C.MAGENTA}{entry}{C.RESET}", flush=True)
    MONITOR_WARNINGS.append(entry)
    if len(MONITOR_WARNINGS) > MAX_MONITOR_LOGS:
        MONITOR_WARNINGS.pop(0)
    # Telegram only for errors, not warnings

def get_monitoring_data():
    return {
        "errors": MONITOR_ERRORS[-10:],
        "warnings": MONITOR_WARNINGS[-10:]
    }

# =================== AUTO-RECOVERY SYSTEM ===================
def retry(func, retries=3, delay=2, name="task"):
    for i in range(retries):
        try:
            return func()
        except Exception as e:
            monitor_log_warning(f"{name} retry {i+1}/{retries} failed: {e}")
            time.sleep(delay)
    monitor_log_error(f"{name} failed after {retries} retries")
    return None

def safe_api_call(func, name="api_call"):
    try:
        return func()
    except Exception as e:
        monitor_log_error(f"{name} error: {e}")
        result = retry(func, retries=3, delay=2, name=name)
        if result is None:
            monitor_log_warning(f"{name} switching to safe mode")
        return result

SAFE_MODE = False
SAFE_MODE_TIMER = 0

def activate_safe_mode(duration=60):
    global SAFE_MODE, SAFE_MODE_TIMER
    SAFE_MODE = True
    SAFE_MODE_TIMER = time.time() + duration
    monitor_log_warning("⚠️ SAFE MODE ACTIVATED")

def check_safe_mode():
    global SAFE_MODE
    if SAFE_MODE and time.time() > SAFE_MODE_TIMER:
        SAFE_MODE = False
        monitor_log_warning("✅ SAFE MODE DEACTIVATED")
    return SAFE_MODE

def safe_execute(execute_func, side, symbol):
    if check_safe_mode():
        monitor_log_warning("Trade blocked (SAFE MODE)")
        return
    try:
        execute_func(side, symbol)
    except Exception as e:
        monitor_log_error(f"Trade execution failed: {e}")
        activate_safe_mode()

# =================== DASHBOARD HELPER FUNCTIONS ===================
def log_event_dashboard(msg):
    timestamp = format_time()
    full_msg = f"[{timestamp}] {msg}"
    DASHBOARD_STATE["logs"].append(full_msg)
    if len(DASHBOARD_STATE["logs"]) > 200:
        DASHBOARD_STATE["logs"].pop(0)
    if "execution_events" not in bot_state:
        bot_state["execution_events"] = []
    bot_state["execution_events"].append({
        "time": timestamp,
        "level": "INFO",
        "message": msg
    })
    bot_state["execution_events"] = bot_state["execution_events"][-20:]
    if "events" not in STATE:
        STATE["events"] = []
    STATE["events"].append(f"{timestamp} {msg}")
    STATE["events"] = STATE["events"][-20:]

def log_error_dashboard(msg, etype="GENERAL"):
    timestamp = format_time()
    full_msg = f"[{timestamp}] [{etype}] {msg}"
    DASHBOARD_STATE["errors"].append(full_msg)
    if len(DASHBOARD_STATE["errors"]) > 100:
        DASHBOARD_STATE["errors"].pop(0)
    if "errors" not in bot_state:
        bot_state["errors"] = []
    bot_state["errors"].append({
        "time": timestamp,
        "message": msg
    })
    bot_state["errors"] = bot_state["errors"][-10:]
    if "errors" not in STATE:
        STATE["errors"] = []
    STATE["errors"].append(f"{timestamp} {msg}")
    STATE["errors"] = STATE["errors"][-10:]
    print(f"{C.RED}🔴 ERROR [{etype}]: {msg}{C.RESET}", flush=True)
    monitor_log_error(msg)

def update_account_dashboard(balance, free, used, mode="LIVE"):
    DASHBOARD_STATE["account"].update({
        "balance": float(balance),
        "free": float(free),
        "used": float(used),
        "mode": mode
    })

def update_stats_dashboard(pnl_usdt):
    global STATS
    DASHBOARD_STATE["stats"]["trades"] += 1
    DASHBOARD_STATE["stats"]["profit_total"] += pnl_usdt
    if pnl_usdt >= 0:
        DASHBOARD_STATE["stats"]["wins"] += 1
    else:
        DASHBOARD_STATE["stats"]["losses"] += 1
    STATS["trades"] = DASHBOARD_STATE["stats"]["trades"]
    STATS["wins"] = DASHBOARD_STATE["stats"]["wins"]
    STATS["losses"] = DASHBOARD_STATE["stats"]["losses"]
    STATS["total_pnl"] = DASHBOARD_STATE["stats"]["profit_total"]
    save_stats()

def update_position_dashboard(symbol, side, entry, price, qty, leverage=1):
    side_display = normalize_side(side)
    if side_display == "LONG":
        pnl_pct = ((price - entry) / entry) * 100 * leverage if entry != 0 else 0
    else:
        pnl_pct = ((entry - price) / entry) * 100 * leverage if entry != 0 else 0
    profit = (pnl_pct / 100) * (entry * qty) if entry != 0 else 0
    DASHBOARD_STATE["position"] = {
        "symbol": symbol,
        "side": side_display,
        "entry": round(entry, 6),
        "price": round(price, 6),
        "pnl_pct": round(pnl_pct, 2),
        "profit": round(profit, 4)
    }

def clear_position_dashboard():
    DASHBOARD_STATE["position"] = None

def update_top5_dashboard(opps):
    def zone_label(z):
        if z >= 8:
            return "STRONG"
        if z >= 5:
            return "MEDIUM"
        return "WEAK"
    ranked = sorted(opps, key=lambda x: x.get("score", 0), reverse=True)[:5]
    out = []
    for o in ranked:
        z = zone_label(o.get("zone_score", 0))
        suggest = "WAIT"
        if o.get("score", 0) >= 8 and z == "STRONG":
            suggest = "READY"
        elif o.get("score", 0) >= 6:
            suggest = "WATCH"
        out.append({
            "symbol": o.get("symbol", "?"),
            "score": round(o.get("score", 0), 2),
            "zone": z,
            "reason": o.get("reason", ""),
            "suggest": suggest
        })
    DASHBOARD_STATE["scanner"]["top5"] = out
    DASHBOARD_STATE["scanner"]["last_update"] = format_time()

# =================== HELPER: JSON SERIALIZATION ===================
def make_serializable(obj):
    if isinstance(obj, (np.int64, np.int32, np.int16, np.int8)):
        return int(obj)
    elif isinstance(obj, (np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: make_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [make_serializable(i) for i in obj]
    else:
        return obj

# =================== LOGGING ===================
SNAPSHOT_INTERVAL = 30
LAST_LOG = 0
LOG_INTERVAL = 30

def log(msg):
    t = format_time()
    print(f"[{t}] {msg}", flush=True)
    log_event_dashboard(msg)

def log_buy(msg):
    print(f"{C.GREEN}🟢 BUY → {msg}{C.RESET}", flush=True)
    log_event_dashboard(f"BUY → {msg}")

def log_sell(msg):
    print(f"{C.RED}🔴 SELL → {msg}{C.RESET}", flush=True)
    log_event_dashboard(f"SELL → {msg}")

def log_scan(msg):
    print(f"{C.ORANGE}🟠 SCAN → {msg}{C.RESET}", flush=True)
    log_event_dashboard(f"SCAN → {msg}")

def log_market(msg):
    print(f"{C.BLUE}🔵 MARKET → {msg}{C.RESET}", flush=True)
    log_event_dashboard(f"MARKET → {msg}")

def log_warn(msg):
    print(f"{C.MAGENTA}⚠️ WARN → {msg}{C.RESET}", flush=True)
    log_event_dashboard(f"WARN → {msg}")
    monitor_log_warning(msg)

def log_event(level, message):
    global bot_state
    timestamp = format_time()
    full_msg = f"[{level}] {message}"
    if level == "ERROR":
        log_error_dashboard(message, etype=level)
    else:
        log_event_dashboard(message)
    if "events" not in STATE:
        STATE["events"] = []
    STATE["events"].append(f"{timestamp} {full_msg}")
    STATE["events"] = STATE["events"][-20:]

def log_error(message):
    log_error_dashboard(message, etype="GENERAL")
    if "STATE" in globals():
        STATE["last_error"] = message

# =================== SNAPSHOT ===================
def snapshot():
    try:
        now = format_time()
        if PAPER_MODE:
            balance = paper.get("balance", 0)
            wins = paper.get("wins", 0)
            losses = paper.get("losses", 0)
            trades = len(paper.get("trades", []))
        else:
            balance = get_balance(ex) if MODE_LIVE else 0
            wins = 0
            losses = 0
            trades = 0
        winrate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0
        mode = "LIVE" if (MODE_LIVE and not PAPER_MODE) else "PAPER"
        in_trade = has_open_position()
        if in_trade:
            side = STATE.get("side", "").upper()
            entry = STATE.get("entry", 0)
            current_price = price_now() or entry
            if side == "LONG":
                pnl_pct = (current_price - entry) / entry * 100 if entry else 0
            else:
                pnl_pct = (entry - current_price) / entry * 100 if entry else 0
            trade_info = f"📈 {side} | Entry: {entry:.6f} | Now: {current_price:.6f} | PnL: {pnl_pct:.2f}%"
        else:
            trade_info = "❌ No Trade"
        scanned = bot_state.get("coins_scanned", 0)
        best_symbol = bot_state.get("best_symbol", "NONE")
        best_score = bot_state.get("best_score", 0)
        current_symbol = bot_state.get("current_symbol", "NONE")
        cpu = "-"
        mem = "-"
        try:
            import psutil
            cpu = f"{psutil.cpu_percent()}%"
            mem = f"{psutil.virtual_memory().percent}%"
        except ImportError:
            pass
        last_error = STATE.get("last_error", "None")
        daily_trades = STATE.get("daily_trades", 0)
        daily_loss_limit_hit = STATE.get("daily_loss_limit_hit", False)

        zone = bot_state.get("last_zone")
        if zone:
            age = time.time() - zone["time"]
            if age < 60:
                zone_line = f"📊 ZONE STATUS → {zone['symbol']} | {zone['side'].upper()} | score={zone['score']:.1f} | reasons={','.join(zone['reasons'])}"
            else:
                zone_line = "📊 ZONE STATUS → no active zone (expired)"
        else:
            zone_line = "📊 ZONE STATUS → scanning..."

        log("=" * 50)
        log(f"📸 SNAPSHOT | {now} | MODE: {mode}")
        log(f"💰 Balance: {balance:.2f} | Trades: {trades} | WinRate: {winrate:.1f}%")
        log(f"📊 Scanner: {scanned} coins | Best: {best_symbol} ({best_score:.1f})")
        log(f"🎯 Current: {current_symbol}")
        log(f"{trade_info}")
        log(zone_line)
        log(f"⚙ CPU: {cpu} | MEM: {mem}")
        log(f"⚠ Last Error: {last_error}")
        if daily_loss_limit_hit:
            log(f"🚨 Daily loss limit hit! Trading paused.")
        log("=" * 50)
    except Exception as e:
        log(f"⚠ snapshot error: {e}")

# =================== PAPER TRADING ===================
PAPER_MODE = os.getenv("PAPER_MODE", "True") == "False"

paper = {
    "balance": 1000.0,
    "start_balance": 1000.0,
    "position": None,
    "trades": [],
    "wins": 0,
    "losses": 0
}

trade_history = []

def paper_open(symbol, side, price, qty):
    global paper
    if paper["position"] is not None:
        return
    paper["position"] = {
        "symbol": symbol,
        "side": side,
        "entry": price,
        "qty": qty,
        "time": time.time(),
        "remaining_qty": qty,
        "tp1_done": False,
        "trail": None
    }
    log(f"🟢 PAPER OPEN {side} {symbol} @ {price} qty={qty}")

def paper_close(price, qty=None):
    global paper, trade_history
    pos = paper["position"]
    if pos is None:
        return
    if qty is None:
        qty = pos["qty"]
    if qty < pos["remaining_qty"]:
        pos["remaining_qty"] -= qty
        log(f"🔴 PAPER PARTIAL CLOSE {pos['side']} {pos['symbol']} @ {price} qty={qty}")
        return
    entry = pos["entry"]
    side = pos["side"]
    if side.upper() in ("BUY", "LONG"):
        pnl = (price - entry) * pos["qty"]
    else:
        pnl = (entry - price) * pos["qty"]
    paper["balance"] += pnl
    paper["trades"].append(pnl)
    trade_history.append({
        "symbol": pos["symbol"],
        "side": pos["side"],
        "entry": entry,
        "exit": price,
        "pnl": pnl
    })
    if pnl > 0:
        paper["wins"] += 1
    else:
        paper["losses"] += 1
    log(f"🔴 PAPER CLOSE | PnL={pnl:.2f} | Balance={paper['balance']:.2f}")
    paper["position"] = None

# =================== REAL POSITION HANDLER (FIXED) ===================
MIN_NOTIONAL = 5.0

def is_dust(notional):
    return notional < MIN_NOTIONAL

def normalize_side(side_raw):
    s = str(side_raw).lower().strip()
    if s in ["buy", "long", "1"]:
        return "LONG"
    if s in ["sell", "short", "-1"]:
        return "SHORT"
    return "UNKNOWN"

# ✅ NEW: Reliable position sync (FIXED)
def get_real_position(symbol):
    """Fetch actual exchange position safely. Returns None or dict with side/amount."""
    try:
        positions = ex.fetch_positions([symbol])
        for p in positions:
            amt = float(p.get("contracts", 0) or p.get("positionAmt", 0))
            if abs(amt) > 0:
                side = "LONG" if amt > 0 else "SHORT"
                return {
                    "side": side,
                    "amount": abs(amt),
                    "raw": p
                }
        return None
    except Exception as e:
        log_error(f"❌ get_real_position error: {e}")
        return None

# Keep old function for compatibility but delegate to new one
def get_real_position_safe(exchange, symbol):
    return get_real_position(symbol)

# =================== FIXED SAFE_CLOSE ===================
def detect_position_mode():
    try:
        positions = ex.fetch_positions()
        if positions and "positionSide" in positions[0]:
            return "hedge"
    except Exception:
        pass
    return "oneway"

def safe_close(exchange, symbol):
    for attempt in range(3):
        try:
            pos = get_real_position(symbol)
            if pos is None:
                log("✅ No position → already closed")
                return True

            size = pos["amount"]
            if size < 1e-6:
                log("⚠️ Dust position → ignore")
                return True

            side = "sell" if pos["side"] == "LONG" else "buy"
            position_side = pos["side"]

            amount = float(exchange.amount_to_precision(symbol, size))
            params = {"reduceOnly": True, "positionSide": position_side}   # ✅ FIX: include positionSide

            order = exchange.create_order(
                symbol,
                "market",
                side,
                amount,
                params=params
            )

            log(f"✅ CLOSE SUCCESS → {symbol}")
            return True

        except Exception as e:
            log(f"❌ CLOSE ATTEMPT {attempt+1} FAILED: {e}")
            time.sleep(1)

    return False

def force_close(symbol):
    try:
        pos = get_real_position(symbol)
        if pos:
            side = "sell" if pos["side"] == "LONG" else "buy"
            amount = pos["amount"]
            log(f"⚠️ FORCE CLOSE TRIGGERED for {symbol} {pos['side']} {amount}")
            return safe_close(ex, symbol)
    except Exception as e:
        log(f"❌ FORCE CLOSE FAILED: {e}")
    return False

def close_position_full():
    if PAPER_MODE:
        price = price_now()
        paper_close(price)
        return True
    else:
        result = safe_close(ex, SYMBOL)
        if not result:
            log_warn("safe_close failed, attempting force_close")
            result = force_close(SYMBOL)
        return result

def has_open_position():
    if PAPER_MODE:
        return paper["position"] is not None
    if MODE_LIVE:
        pos = get_real_position(SYMBOL)
        return pos is not None
    return False

# =================== BOT STATE ===================
bot_state = {
    "scanner_status": "idle",
    "current_symbol": None,
    "best_symbol": None,
    "best_score": 0,
    "threshold": 12,
    "coins_scanned": 0,
    "last_reject_reason": None,
    "top_opportunities": [],
    "btc_trend": "neutral",
    "btc_1h_change": 0.0,
    "errors": [],
    "execution_events": [],
    "watchlist": [],
    "decision": {
        "action": "none",
        "score": 0,
        "needed": 12,
        "reason": "initial"
    },
    "sr": {
        "support": None,
        "resistance": None,
        "dist_support": None,
        "dist_resistance": None
    },
    "zone": {
        "type": "NONE",
        "low": None,
        "high": None,
        "distance": None
    },
    "indicators": {
        "adx": None,
        "di_plus": None,
        "di_minus": None,
        "trend": None
    },
    "live": {
        "symbol": None,
        "momentum": None,
        "zone": None,
        "behavior": None,
        "score": None
    },
    "pump": {
        "status": "N/A",
        "reason": ""
    },
    "retest": {
        "level": None,
        "direction": None,
        "confirmed": False
    },
    "runner": {
        "active": False,
        "trail_price": None,
        "peak_profit": 0.0
    },
    "zone_watchlist": [],
    "last_zone": None,
    "memory": {
        "last_sweep": None,
        "last_zone": None,
        "last_structure_shift": None,
    }
}

# =================== ENV / MODE ===================
API_KEY = CONFIG.BINGX_API_KEY
API_SECRET = CONFIG.BINGX_API_SECRET
MODE_LIVE = bool(API_KEY and API_SECRET) and not PAPER_MODE

SELF_URL = CONFIG.SELF_URL
PORT = CONFIG.PORT

LOG_LEGACY = False
LOG_ADDONS = True

EXECUTE_ORDERS = True
SHADOW_MODE_DASHBOARD = False
DRY_RUN = False

BOT_VERSION = "PROFESSIONAL SNIPER v17.3 (Institutional Hunter) + Sniper Engine + Recovery"
print("🔁 Booting:", BOT_VERSION, flush=True)

STATE_PATH = "./bot_state.json"
RESUME_ON_RESTART = True
RESUME_LOOKBACK_SECS = 60 * 60

BOOKMAP_DEPTH = 20
BOOKMAP_TOPWALLS = 3
IMBALANCE_ALERT = 1.30

FLOW_WINDOW = 20
FLOW_SPIKE_Z = 1.60
CVD_SMOOTH = 8

OBI_EDGE = 0.18
DELTA_EDGE = 1.5
WALL_PROX_BPS = 8.0
FLOW_VOTE  = 2
FLOW_SCORE = 1.2

SYMBOL     = os.getenv("SYMBOL", "DOGE/USDT:USDT")
INTERVAL   = os.getenv("INTERVAL", "15m")
LEVERAGE   = int(os.getenv("LEVERAGE", 5))
RISK_ALLOC = float(os.getenv("RISK_ALLOC", 0.60))
POSITION_MODE = os.getenv("BINGX_POSITION_MODE", "oneway")

MIN_ENTRY_SCORE = 12
STRONG_ENTRY_SCORE = 15
ULTRA_ENTRY_SCORE = 18
bot_state["threshold"] = MIN_ENTRY_SCORE

MAX_TRADES_PER_DAY = 999999

MAX_SCAN_COINS = 50
SCAN_INTERVAL = 20
SCAN_BATCH = 10
_scan_idx = 0

RF_SOURCE = "close"
RF_PERIOD = int(os.getenv("RF_PERIOD", 20))
RF_MULT   = float(os.getenv("RF_MULT", 3.5))
RF_LIVE_ONLY = True
RF_HYST_BPS  = 6.0

RSI_LEN = 14
ADX_LEN = 14
ATR_LEN = 14

ENTRY_RF_ONLY = True
MAX_SPREAD_BPS = float(os.getenv("MAX_SPREAD_BPS", 6.0))

TP1_PERCENT = 0.5
TP1_PROFIT_PCT = 0.5
TRAIL_ATR_MULT = 1.5
TRAIL_ACTIVATE_PCT = 0.50
ATR_TRAIL_MULT = 1.6
BREAKEVEN_OFFSET = 0.0005
LIQUIDITY_TARGET_DIST = 0.002

RUNNER_ACTIVATE_AFTER_TP2 = False
RUNNER_DRAWDOWN_PCT = 1.0
RUNNER_ATR_MULT = 2.5
RUNNER_TREND_WEAK_ADX = 20
RUNNER_DI_CROSS_CLOSE = False

FINAL_CHUNK_QTY = 0.0
RESIDUAL_MIN_QTY = float(os.getenv("RESIDUAL_MIN_QTY", 9.0))

CLOSE_RETRY_ATTEMPTS = 6
CLOSE_VERIFY_WAIT_S  = 2.0

BASE_SLEEP   = 5
NEAR_CLOSE_S = 1

SMART_MODE = os.getenv("SMART_MODE", "pro")

ADX_TREND_MIN = 20
DI_SPREAD_TREND = 6
RSI_MA_LEN = 9
RSI_NEUTRAL_BAND = (45, 55)
RSI_TREND_PERSIST = 3

GZ_MIN_SCORE = 6.0
GZ_REQ_ADX = 20
GZ_REQ_VOL_MA = 20
ALLOW_GZ_ENTRY = True

SCALP_TP1 = 0.40
SCALP_BE_AFTER = 0.30
SCALP_ATR_MULT = 1.6
TREND_TP1 = 1.20
TREND_BE_AFTER = 0.80
TREND_ATR_MULT = 1.8

COOLDOWN_SECS_AFTER_CLOSE = 60
COOLDOWN_MINUTES_LOSS = 10
ADX_GATE = 22
MAX_VWAP_DISTANCE_PCT = 0.008

TP1_SCALP_PCT      = 0.35/100
TP1_TREND_PCT      = 0.60/100
HARD_CLOSE_PNL_PCT = 1.10/100
WICK_ATR_MULT      = 1.5
EVX_SPIKE          = 1.8
BM_WALL_PROX_BPS   = 5
TIME_IN_TRADE_MIN  = 8
TRAIL_TIGHT_MULT   = 1.20

ENABLE_LIQUIDITY_POOLS = True
ENABLE_STRUCTURE = True
ENABLE_DISPLACEMENT = True
ENABLE_VOLATILITY_FILTER = True
MIN_ATR_PCT = 0.001
ENABLE_WHALE_TRAP = True
ENABLE_LIQUIDITY_HEATMAP = True
ENABLE_STOP_HUNT = True
ENABLE_LIQUIDITY_VOID = True
ENABLE_LIQUIDITY_REVERSAL = True

ENABLE_SUPPLY_DEMAND = True
SD_MIN_ZONE_STRENGTH = 3
SD_MAX_ZONE_DISTANCE_PCT = 0.003
SD_REQUIRE_REJECTION = True
SD_REQUIRE_ORDERFLOW = True
SD_ENTRY_SCORE_WEIGHT = 2

MAX_CONSECUTIVE_LOSSES = 3
COOLDOWN_MINUTES_DRAWDOWN = 20
MAX_DAILY_LOSS_PCT = 5.0

BTC_CRASH_THRESHOLD = 3.0
PUMP_MIN_PRICE_MOVE = 0.5
PUMP_MIN_ADX = 15

CACHE = {"ohlcv": {}, "orderbook": {}, "trades": {}}
CACHE_TTL = {"ohlcv": 15, "orderbook": 5, "trades": 5}

GLOBAL_SCAN_INTERVAL = 300
TOP_SYMBOLS = []
SCAN_LIST = []
LAST_FULL_SCAN = 0

SM_WEIGHT = 2
PRE_WEIGHT = 2
TRAP_PENALTY = 1.5
INSTITUTIONAL_MIN_SCORE = 5

# ========== SMART PIPELINE CONFIG ==========
SMART_PIPELINE_ENABLED = True
SYMBOL_MEMORY = {}

# ========== NEW INSTITUTIONAL HUNTER MODULES ==========
WATCHLIST = []  # Changed from dict to list
WATCHLIST_META = {}  # Metadata for watchlist items
MAX_WATCHLIST = 10
LIQUIDITY_ZONES = []
TRADE_LOG = []
MIN_SCORE_THRESHOLD = 6
LAST_TRADE_TIME = 0
PEAK_PNL = 0

# =================== GLOBAL STATE ===================
STATE = {
    "open": False,
    "side": None,
    "entry": 0.0,
    "qty": 0.0,
    "remaining_qty": 0.0,
    "tp1_done": False,
    "pnl": 0.0,
    "bars": 0,
    "trail": None,
    "breakeven": None,
    "highest_profit_pct": 0.0,
    "trail_activated": False,
    "trail_stop": None,
    "trail_multiplier": TRAIL_ATR_MULT,
    "prev_adx": 0,
    "trend_strength": "weak",
    "regime_at_entry": "range",
    "signal_strength": "MEDIUM",
    "trend_strength_entry": "weak",
    "entry_score": 0,
    "tp_config": None,
    "cooldown_until": None,
    "daily_trades": 0,
    "last_trade_day": None,
    "consecutive_losses": 0,
    "daily_peak_balance": None,
    "daily_loss_limit_hit": False,
    "opened_at": None,
    "leverage": LEVERAGE,
    "heat_score": 0,
    "heat_breakdown": {},
    "current_market_regime": "range",
    "supply_demand_trigger": False,
    "zone_state": {},
    "trend": None,
    "last_error": None,
    "balance": 0.0,
    "current_symbol": None,
    "protected": False,
    "tp1": False,
    "tp1_wait": False,
    "peak": 0.0,
    "target_price": None,
    "pme_state": {},
    "balance_free": 0.0,
    "balance_used": 0.0,
    "balance_total": 0.0,
    "events": [],
    "errors": [],
    "price": 0.0,
    "trade": None,
    "signal": None,
    "tp1_hit": False,
    "tp2_hit": False,
}

compound_pnl = 0.0
wait_for_next_signal_side = None

# =================== DATA ENGINE ===================
DATA_CACHE = {
    "ticker": {},
    "ohlcv": {},
    "orderbook": {},
    "trades": {}
}

DATA_TTL = {
    "ticker": 5,
    "ohlcv": 15,
    "orderbook": 5,
    "trades": 5
}

LAST_API_CALL = 0
MIN_API_DELAY = 0.2

def rate_limit():
    global LAST_API_CALL
    now = time.time()
    diff = now - LAST_API_CALL
    if diff < MIN_API_DELAY:
        time.sleep(MIN_API_DELAY - diff)
    LAST_API_CALL = time.time()

def data_get(kind, key):
    item = DATA_CACHE[kind].get(key)
    if not item:
        return None
    ts, val = item
    if time.time() - ts > DATA_TTL[kind]:
        return None
    return val

def data_set(kind, key, value):
    DATA_CACHE[kind][key] = (time.time(), value)

def get_ticker_safe(symbol):
    cached = data_get("ticker", symbol)
    if cached is not None:
        return cached
    try:
        rate_limit()
        ticker = ex.fetch_ticker(symbol)
        price = ticker.get("last", 0)
        data_set("ticker", symbol, price)
        return price
    except Exception as e:
        log_event("WARN", f"Ticker failed for {symbol}: {e}")
        return cached if cached is not None else 0

def get_ohlcv_safe(symbol, timeframe=INTERVAL, limit=120):
    key = f"{symbol}_{timeframe}_{limit}"
    cached = data_get("ohlcv", key)
    if cached is not None:
        return cached
    try:
        rate_limit()
        data = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        data_set("ohlcv", key, df)
        return df
    except Exception as e:
        log_event("WARN", f"OHLCV failed for {symbol}: {e}")
        if cached is not None:
            return cached
        return pd.DataFrame()

def get_orderbook_safe(symbol, limit=BOOKMAP_DEPTH):
    key = f"{symbol}_{limit}"
    cached = data_get("orderbook", key)
    if cached is not None:
        return cached
    try:
        rate_limit()
        ob = ex.fetch_order_book(symbol, limit=limit)
        data_set("orderbook", key, ob)
        return ob
    except Exception as e:
        log_event("WARN", f"Orderbook failed for {symbol}: {e}")
        if cached is not None:
            return cached
        return {"bids": [], "asks": []}

def fetch_trades_cached(symbol, limit=200):
    if symbol is None:
        symbol = SYMBOL
    cached = cache_get("trades", symbol)
    if cached is not None:
        return cached
    try:
        rate_limit()
        data = ex.fetch_trades(symbol, limit=limit)
        cache_set("trades", symbol, data)
        return data
    except Exception as e:
        log_event("WARN", f"Trades fetch failed: {e}")
        return []

def fetch_ohlcv_cached(symbol=None, interval=None, limit=120):
    if symbol is None:
        symbol = SYMBOL
    if interval is None:
        interval = INTERVAL
    return get_ohlcv_safe(symbol, interval, limit)

def fetch_orderbook_cached(symbol=None, limit=BOOKMAP_DEPTH):
    if symbol is None:
        symbol = SYMBOL
    allowed_limits = [5, 10, 20]
    if limit not in allowed_limits:
        limit = 20
    return get_orderbook_safe(symbol, limit)

# =================== BALANCE CACHING ===================
_balance_cache = {"value": 0.0, "timestamp": 0}
BALANCE_CACHE_TTL = 30

def get_real_balance(exchange):
    now = time.time()
    if now - _balance_cache["timestamp"] < BALANCE_CACHE_TTL:
        return _balance_cache["value"]
    try:
        rate_limit()
        balance = exchange.fetch_balance()
        usdt_balance = balance.get('total', {}).get('USDT', 0)
        if usdt_balance is None:
            usdt_balance = 0
        _balance_cache["value"] = float(usdt_balance)
        _balance_cache["timestamp"] = now
        return _balance_cache["value"]
    except Exception as e:
        log_event("WARN", f"Balance fetch failed: {e}")
        return _balance_cache["value"]

def get_balance(exchange=None):
    if PAPER_MODE:
        return float(paper.get("balance", 0))
    try:
        if exchange is None:
            exchange = globals().get("ex", None)
        if exchange is None:
            return 0.0
        return get_real_balance(exchange)
    except Exception:
        return 0.0

# =================== NEW ACCOUNT SYNC ENGINE ===================
def calculate_pnl(entry, current, side, size):
    if side == "LONG":
        pnl_pct = ((current - entry) / entry) * 100 if entry else 0
        pnl_usdt = pnl_pct * size / 100
    else:
        pnl_pct = ((entry - current) / entry) * 100 if entry else 0
        pnl_usdt = pnl_pct * size / 100
    return pnl_pct, pnl_usdt

def sync_account_state():
    global STATE

    if PAPER_MODE:
        STATE["balance_free"] = paper.get("balance", 0.0)
        STATE["balance_used"] = 0.0
        STATE["balance_total"] = paper.get("balance", 0.0)
        STATE["balance"] = STATE["balance_free"]
        STATE["open"] = paper["position"] is not None
        if STATE["open"]:
            STATE["side"] = paper["position"]["side"].lower()
            STATE["entry"] = paper["position"]["entry"]
            STATE["qty"] = paper["position"]["qty"]
            STATE["remaining_qty"] = paper["position"]["remaining_qty"]
        else:
            STATE["side"] = None
            STATE["entry"] = 0.0
            STATE["qty"] = 0.0
            STATE["remaining_qty"] = 0.0

        STATE["price"] = price_now() or STATE["entry"]
        if STATE["open"]:
            pnl_pct, pnl_usdt = calculate_pnl(STATE["entry"], STATE["price"], STATE["side"].upper(), STATE["qty"])
            STATE["trade"] = {
                "side": STATE["side"].upper(),
                "entry": STATE["entry"],
                "price": STATE["price"],
                "pnl": round(pnl_pct, 2),
                "profit": round(pnl_usdt, 2)
            }
            STATE["pnl"] = pnl_pct
            update_position_dashboard(SYMBOL, STATE["side"].upper(), STATE["entry"], STATE["price"], STATE["qty"], LEVERAGE)
        else:
            STATE["trade"] = None
            STATE["pnl"] = 0.0
            clear_position_dashboard()

        update_account_dashboard(STATE["balance_free"], STATE["balance_free"], STATE["balance_used"], mode="PAPER")
        return

    if not MODE_LIVE:
        STATE["balance_free"] = 100.0
        STATE["balance_used"] = 0.0
        STATE["balance_total"] = 100.0
        STATE["balance"] = 100.0
        STATE["open"] = False
        STATE["side"] = None
        STATE["entry"] = 0.0
        STATE["qty"] = 0.0
        STATE["price"] = price_now() or 0
        STATE["trade"] = None
        STATE["pnl"] = 0.0
        clear_position_dashboard()
        update_account_dashboard(100.0, 100.0, 0.0, mode="SIM")
        return

    try:
        balance = ex.fetch_balance()
        usdt = balance.get('USDT', {})
        free = float(usdt.get('free', 0))
        used = float(usdt.get('used', 0))
        total = float(usdt.get('total', 0))

        pos = get_real_position(SYMBOL)
        if pos and pos.get("amount", 0) > 0:
            STATE["open"] = True
            STATE["side"] = pos["side"].lower()
            # Fetch entry price from position raw data
            entry_price = float(pos["raw"].get("entryPrice", 0))
            STATE["entry"] = entry_price
            STATE["qty"] = pos["amount"]
            STATE["remaining_qty"] = STATE["qty"]
            price = price_now() or STATE["entry"]
            update_position_dashboard(SYMBOL, STATE["side"].upper(), STATE["entry"], price, STATE["qty"], LEVERAGE)
        else:
            STATE["open"] = False
            STATE["side"] = None
            STATE["entry"] = 0.0
            STATE["qty"] = 0.0
            STATE["remaining_qty"] = 0.0
            clear_position_dashboard()

        STATE["balance_free"] = free
        STATE["balance_used"] = used
        STATE["balance_total"] = total
        STATE["balance"] = free

        STATE["price"] = price_now() or STATE["entry"]
        if STATE["open"]:
            pnl_pct, pnl_usdt = calculate_pnl(STATE["entry"], STATE["price"], STATE["side"].upper(), STATE["qty"])
            STATE["trade"] = {
                "side": STATE["side"].upper(),
                "entry": STATE["entry"],
                "price": STATE["price"],
                "pnl": round(pnl_pct, 2),
                "profit": round(pnl_usdt, 2)
            }
            STATE["pnl"] = pnl_pct
        else:
            STATE["trade"] = None
            STATE["pnl"] = 0.0

        update_account_dashboard(free, free, used, mode="LIVE")

        log_g(f"SYNC → Free:{free:.2f} | Used:{used:.2f} | Open:{STATE['open']} | Side:{STATE['side']} | PnL:{STATE['pnl']:.2f}%")

    except Exception as e:
        log_e(f"SYNC ERROR: {e}")

# =================== NEW TRADE GUARDS ===================
def can_open_trade():
    if STATE.get("open"):
        log_warn("Skip: already in position (exchange)")
        return False

    if STATE.get("balance_free", 0) < 2:
        log_warn("Skip: low free balance")
        return False

    return True

def get_trade_budget():
    free = STATE.get("balance_free", 0)
    if free < 2:
        return 0
    return free * RISK_ALLOC

def has_sufficient_margin(qty, price):
    required_margin = (qty * price) / LEVERAGE
    free = STATE.get("balance_free", 0)
    if required_margin > free:
        log_warn(f"Insufficient REAL margin: required {required_margin:.2f} > free {free:.2f}")
        return False
    return True

def calculate_position_size_real(symbol, price, score):
    budget = get_trade_budget()
    if budget <= 0:
        return 0.0

    notional = budget * LEVERAGE
    raw_qty = notional / price

    try:
        market = ex.market(symbol)
        step = market['precision']['amount']
        raw_qty = math.floor(raw_qty / step) * step
        min_qty = market['limits']['amount']['min']
        if raw_qty < min_qty:
            raw_qty = min_qty
    except Exception:
        pass

    return raw_qty

# =================== FIXED BINGX LEVERAGE ===================
def set_leverage_safe(symbol, leverage, side):
    try:
        params = {"side": "LONG" if side == "buy" else "SHORT"}   # ✅ FIX: include positionSide
        ex.set_leverage(leverage, symbol, params=params)
        log_i(f"⚙️ leverage {leverage}x for {side} on {symbol}")
    except Exception as e:
        log_warn(f"leverage error: {e}")

def set_margin_mode(exchange, symbol, mode="ISOLATED"):
    try:
        exchange.set_margin_mode(mode, symbol)
        log_event("INFO", f"Margin mode set to {mode} for {symbol}")
    except Exception as e:
        log_event("WARN", f"Margin mode setting failed: {e}")

# =================== SMART EXECUTION ===================
def execute_trade_smart(symbol, side, qty):
    try:
        ob = get_orderbook_safe(symbol, limit=5)
        if not ob["bids"] or not ob["asks"]:
            log_warn("orderbook empty, using ticker price")
            price = get_ticker_safe(symbol)
        else:
            if side == "buy":
                price = ob['asks'][0][0]
            else:
                price = ob['bids'][0][0]
        params = {"positionSide": "LONG" if side == "buy" else "SHORT"}   # ✅ FIX
        order = ex.create_order(
            symbol,
            "market",
            side,
            qty,
            params=params
        )
        log_g(f"✅ executed {side} {qty:.6f} {symbol} @ approx {price:.6f}")
        return order
    except Exception as e:
        log_error(f"execution fail: {e}")
        return None

# =================== LIVE EXECUTION ENGINE ===================
def get_position_side(side):
    return "LONG" if side.lower() == "buy" else "SHORT"

def build_order_params(position_side):
    return {"positionSide": position_side}

def execute_market(symbol, side, amount, position_side, exchange=None):
    if exchange is None:
        exchange = globals().get("ex", None)
    if exchange is None:
        log_event("ERROR", "No exchange object available")
        return None
    try:
        rate_limit()
        params = build_order_params(position_side)
        order = exchange.create_order(
            symbol=symbol,
            type="market",
            side=side,
            amount=amount,
            params=params
        )
        return order
    except Exception as e:
        log_event("ERROR", f"Market order failed: {e}")
        return None

def execute_limit(symbol, side, amount, price, position_side, exchange=None):
    if exchange is None:
        exchange = globals().get("ex", None)
    if exchange is None:
        log_event("ERROR", "No exchange object available")
        return None
    try:
        rate_limit()
        params = build_order_params(position_side)
        order = exchange.create_order(
            symbol=symbol,
            type="limit",
            side=side,
            amount=amount,
            price=price,
            params=params
        )
        return order
    except Exception as e:
        log_event("ERROR", f"Limit order failed: {e}")
        return None

def execute_stop_market(symbol, side, amount, stop_price, position_side, exchange=None):
    if exchange is None:
        exchange = globals().get("ex", None)
    if exchange is None:
        log_event("ERROR", "No exchange object available")
        return None
    try:
        rate_limit()
        params = build_order_params(position_side)
        params["stopPrice"] = stop_price
        order = exchange.create_order(
            symbol=symbol,
            type="stop_market",
            side=side,
            amount=amount,
            params=params
        )
        return order
    except Exception as e:
        log_event("ERROR", f"Stop market order failed: {e}")
        return None

def close_with_retry(symbol, position, qty, max_retries=3):
    close_side = "sell" if position["side"].lower() == "buy" else "buy"
    pos_side = get_position_side(position["side"])
    for attempt in range(max_retries):
        order = execute_market(symbol, close_side, qty, pos_side)
        if order:
            log_event("INFO", f"Closed {position['side']} position on attempt {attempt+1}")
            return True
        log_event("WARN", f"Close attempt {attempt+1} failed")
        time.sleep(1)
    log_event("ERROR", "Failed to close position after retries")
    return False

def execute_live_trade(exchange, symbol, side, balance):
    try:
        log_event("INFO", f"Start trade {symbol} {side}")
        set_margin_mode(exchange, symbol, "ISOLATED")
        set_leverage_safe(symbol, LEVERAGE, side)
        price = get_ticker_safe(symbol)
        if not price:
            log_event("ERROR", f"Failed to fetch price for {symbol}")
            return None
        real_balance = get_balance(exchange)
        if balance != real_balance:
            balance = real_balance
        if balance < 25:
            risk_pct = 0.3
        elif balance < 15:
            risk_pct = 0.2
        else:
            risk_pct = 0.6
        position_value = balance * risk_pct * LEVERAGE
        qty = position_value / price
        market = exchange.market(symbol)
        step = market.get("precision", {}).get("amount", 0.0001)
        if step > 0:
            qty = math.floor(qty / step) * step
        min_qty = market.get('limits', {}).get('amount', {}).get('min', 0)
        if min_qty > 0 and qty < min_qty:
            log_event("WARN", f"Calculated qty {qty:.8f} below min {min_qty}, using min")
            qty = min_qty
        notional = qty * price
        min_cost = market.get('limits', {}).get('cost', {}).get('min', 5)
        if min_cost > 0 and notional < min_cost:
            log_event("ERROR", f"Notional {notional:.2f} below min cost {min_cost}")
            return None
        if not validate_order_value(exchange, symbol, qty):
            log_event("ERROR", "Order value validation failed")
            return None
        pos_side = get_position_side(side)
        order = execute_market(symbol, side.lower(), qty, pos_side, exchange)
        if order is None:
            return None
        log_event("INFO", f"Order sent: {order['id']}")
        time.sleep(2)
        info = exchange.fetch_order(order["id"], symbol)
        if info["status"] != "closed":
            log_event("WARN", f"Order not filled: {info['status']}")
        else:
            log_event("INFO", f"Order filled: {info['filled']} {symbol} @ avg price {info['average']}")
        return info
    except Exception as e:
        log_event("ERROR", f"{symbol} trade failed: {e}")
        return None

def validate_order_value(exchange, symbol, qty):
    try:
        price = get_ticker_safe(symbol)
        notional = qty * price
        market = exchange.market(symbol)
        min_cost = market.get('limits', {}).get('cost', {}).get('min', 5)
        if notional < min_cost:
            log_event("WARN", f"Order too small: {notional:.2f} USDT < {min_cost} USDT min cost")
            return False
        return True
    except Exception as e:
        log_event("ERROR", f"Order validation error: {e}")
        return False

def execute_trade_decision(side, price, qty, mode, council_data, gz_data, source="RF"):
    if not EXECUTE_ORDERS or DRY_RUN:
        log(f"DRY_RUN: {side} {qty:.4f} @ {price:.6f} | mode={mode} | source={source}")
        return True
    if PAPER_MODE:
        paper_open(SYMBOL, side, price, qty)
        return True
    if MODE_LIVE:
        bal = get_balance(ex)
        log(f"💰 BALANCE USED: {bal:.2f} USDT")
        if bal is None or bal <= 0:
            log_event("ERROR", "Cannot execute trade: invalid balance")
            return False
        result = execute_live_trade(ex, SYMBOL, side, bal)
        if result:
            log_event("INFO", f"Trade executed: {side} {qty:.4f} @ ~{price:.6f} | id={result['id']}")
            return True
        else:
            log_event("ERROR", "Trade execution failed")
            return False
    else:
        log_warn("No live mode and not paper mode?")
        return False

# =================== CACHE HELPERS ===================
def cache_get(store, key):
    item = CACHE[store].get(key)
    if not item:
        return None
    ts, value = item
    if time.time() - ts > CACHE_TTL[store]:
        return None
    return value

def cache_set(store, key, value):
    CACHE[store][key] = (time.time(), value)

def log_i(msg): log(f"ℹ️ {msg}")
def log_g(msg): log(f"✅ {msg}")
def log_e(msg): log(f"❌ {msg}")
def log_banner(text): log(f"\n{'—'*12} {text} {'—'*12}\n")

def save_state(state: dict):
    try:
        def convert(o):
            import numpy as np
            if isinstance(o, (np.bool_, bool)):
                return bool(o)
            if isinstance(o, np.integer):
                return int(o)
            if isinstance(o, np.floating):
                return float(o)
            if isinstance(o, np.ndarray):
                return o.tolist()
            if isinstance(o, datetime):
                return o.isoformat()
            return str(o)
        safe_state = state.copy()
        if "cooldown_until" in safe_state and isinstance(safe_state["cooldown_until"], datetime):
            safe_state["cooldown_until"] = safe_state["cooldown_until"].isoformat()
        safe_state["ts"] = int(time.time())
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(safe_state, f, default=convert, ensure_ascii=False, indent=2)
        log_i(f"state saved → {STATE_PATH}")
    except Exception as e:
        log_warn(f"state save failed: {e}")

def load_state() -> dict:
    try:
        if not os.path.exists(STATE_PATH): return {}
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log_warn(f"state load failed: {e}")
    return {}

def verify_execution_environment():
    log("⚙️ EXECUTION ENVIRONMENT")
    log(f"🔧 EXECUTE_ORDERS: {EXECUTE_ORDERS} | SHADOW_MODE: {SHADOW_MODE_DASHBOARD} | DRY_RUN: {DRY_RUN}")
    log(f"📄 PAPER_MODE: {PAPER_MODE} | BALANCE: {paper['balance']} USDT")
    log(f"🎯 ENTRY THRESHOLD: MIN_SCORE={MIN_ENTRY_SCORE} | ADX_GATE={ADX_GATE} | MAX_SPREAD={MAX_SPREAD_BPS}bps | MAX_VWAP_DIST={MAX_VWAP_DISTANCE_PCT*100}%")
    log(f"📈 PROFIT TAKING: TP1 at {TP1_PROFIT_PCT}% (close 50%), then breakeven + trailing (ATR×{TRAIL_ATR_MULT}) activate at +{TRAIL_ACTIVATE_PCT}%")
    log(f"🧩 ENGINES: LIQ_POOLS={ENABLE_LIQUIDITY_POOLS} | STRUCTURE={ENABLE_STRUCTURE} | DISPLACEMENT={ENABLE_DISPLACEMENT}")
    log(f"🐋 WHALE_TRAP={ENABLE_WHALE_TRAP} | 🔥 HEATMAP={ENABLE_LIQUIDITY_HEATMAP} | 🎯 STOP_HUNT={ENABLE_STOP_HUNT} | 🌀 LIQUIDITY_VOID={ENABLE_LIQUIDITY_VOID} | 🔄 REVERSAL={ENABLE_LIQUIDITY_REVERSAL}")
    log(f"📊 SUPPLY/DEMAND ENGINE: {ENABLE_SUPPLY_DEMAND} (strength≥{SD_MIN_ZONE_STRENGTH}, dist≤{SD_MAX_ZONE_DISTANCE_PCT*100}%, rejection={SD_REQUIRE_REJECTION}, orderflow={SD_REQUIRE_ORDERFLOW})")
    log(f"🧠 MARKET REGIME ENGINE: ADVANCED | OPPORTUNITY RANKING v3 (SMC): ACTIVE | DRAWDOWN PROTECTION: ACTIVE")
    log(f"📦 BATCH SCAN: {SCAN_BATCH} coins/cycle | SINGLE TRADE MODE")
    log(f"🛡️ BTC MARKET CONTROL: LIGHT (crash protection only, {BTC_CRASH_THRESHOLD}%) | EMERGENCY KILL SWITCH: {MAX_DAILY_LOSS_PCT}%")
    log(f"📊 TREND STRENGTH ENGINE: ACTIVE | EXECUTION GUARD: ACTIVE")
    log(f"💹 POSITION SIZING: dynamic based on entry score (40/50/60% of balance × 5x leverage)")
    log(f"🧠 SMC ENHANCEMENTS: ENABLED (Order Blocks, FVG, Liquidity Sweep, Structure, Supply/Demand)")
    log(f"📉 ADX FALLING DETECTION: ENABLED (tightens trail for strong signals)")
    log(f"🕯️ CANDLE PATTERN ANALYSIS: ENABLED (Pin Bar, Engulfing, Inside Bar)")
    log(f"🌊 VOLATILITY EXPANSION INDICATOR: ENABLED")
    log(f"🗺️ LIQUIDITY MAP ENGINE: ENABLED (equal highs/lows clusters with ATR‑based tolerance, sweeps require displacement)")
    log(f"🎯 HUNTER MODE: ACTIVE (requires liquidity sweep and VWAP distance ≥0.2%)")
    log(f"📈 HIGHER TIMEFRAME TREND ALIGNMENT: ENABLED (1h EMA200)")
    log(f"🔥 ADVANCED ZONE DETECTION: Order Blocks, FVGs, Wyckoff Spring/Upthrust")
    log(f"🧲 LIQUIDITY MAGNET ENGINE: ENHANCED (cluster‑based detection)")
    log(f"🌌 LIQUIDITY GRAVITY ENGINE: NEW (measures liquidity attraction)")
    log(f"📏 MID‑RANGE FILTER: STRICT (avoids entries within 35% of range)")
    log(f"🔥 LIQUIDITY HEATMAP ENGINE: NEW (analyzes order book walls)")
    log(f"🔄 LIQUIDITY REVERSAL ENGINE: NEW (detects Spring/Upthrust for early entries)")
    log(f"🛡️ VETO LAYER: ACTIVE (whale trap, score gap, liquidity position filters)")
    log(f"📊 MACRO/MICRO SCAN: ENABLED (full scan every {GLOBAL_SCAN_INTERVAL//60} min, micro on top 30)")
    log(f"✅ ENTRY QUALITY CHECK: Zone + Rejection + ADX≥{PUMP_MIN_ADX} required before entry")
    log(f"🔥 HYBRID ENTRY SYSTEM: Momentum + Zone + Behavior + Pump Validation + Retest Entry")
    log(f"🏆 PROFIT MAXIMIZER: Partial TP1, breakeven, liquidity targets, ATR trailing (activate at {TRAIL_ACTIVATE_PCT}%)")
    log(f"✨ EARLY SETUP FILTER: ENABLED (score threshold = {EARLY_SETUP_MIN_SCORE})")
    log(f"🛡️ ANTI‑TRAP ENGINE: ENABLED (rejects fake accumulations)")
    log(f"🧠 SMART MONEY SCORE (SM): WEIGHT={SM_WEIGHT}, PRE‑MOVE WEIGHT={PRE_WEIGHT}, TRAP PENALTY={TRAP_PENALTY}")
    log(f"🏦 INSTITUTIONAL ENTRY: MIN SCORE={INSTITUTIONAL_MIN_SCORE} (requires Sweep + Structure Shift + Displacement)")
    log(f"🧪 PHASE + EXHAUSTION ENGINE: ACTIVE (prevents early reversals)")
    log(f"🚀 v17.3: +Safe Position, +Watchlist, +Liquidity Sweep, +HTF, +Fakeout Filter, +Smart Exit, +Trade Memory, +Adaptive Threshold, +Berlin TZ")
    log(f"🔥 INTEGRATED: Sniper Engine (Radar/Watchlist) + Monitoring + Auto-Recovery")
    if not EXECUTE_ORDERS:
        log("🟡 WARNING: EXECUTE_ORDERS=False - analysis only!")
    if DRY_RUN:
        log("🟡 WARNING: DRY_RUN=True - simulation!")

# =================== DYNAMIC SYMBOLS ===================
def build_symbols():
    try:
        ex.load_markets()
        symbols = []
        for s, m in ex.markets.items():
            if m.get("swap") and "USDT" in s and ":USDT" in s and m.get("active", False):
                symbols.append(s)
        return sorted(symbols)
    except Exception as e:
        log_warn(f"build_symbols error: {e}")
        return []

def filter_liquid_symbols_fast(symbols, min_volume=5_000_000, top_n=MAX_SCAN_COINS):
    try:
        rate_limit()
        tickers = ex.fetch_tickers()
        volumes = []
        for s in symbols:
            t = tickers.get(s, {})
            vol = t.get("quoteVolume", 0)
            if vol >= min_volume:
                volumes.append((s, vol))
        volumes.sort(key=lambda x: x[1], reverse=True)
        return [s for s, _ in volumes[:top_n]]
    except Exception as e:
        log_warn(f"fast filter error: {e}")
        return symbols[:top_n]

def macro_scan_all_symbols(symbols):
    try:
        if not symbols:
            return []
        all_symbols = build_symbols()
        if all_symbols:
            filtered = filter_liquid_symbols_fast(all_symbols, top_n=MAX_SCAN_COINS)
            log_i(f"Macro scan: updated symbols to {len(filtered)} coins")
            return filtered
        else:
            return symbols[:MAX_SCAN_COINS]
    except Exception as e:
        log_error(f"macro_scan_all_symbols error: {e}")
        return symbols[:MAX_SCAN_COINS] if symbols else []

def update_scan_list(symbols, step):
    window = 30
    if not symbols:
        return []
    start = (step * 10) % len(symbols)
    return symbols[start:start + window]

# =================== ENHANCED INDICATORS ===================
def rma(series, length):
    alpha = 1.0 / length
    return series.ewm(alpha=alpha, adjust=False).mean()

def sma(series, n: int):
    return series.rolling(n, min_periods=1).mean()

def compute_rsi(close, n: int = 14):
    delta = close.diff()
    up = delta.clip(lower=0)
    down = (-delta).clip(lower=0)
    roll_up = rma(up, n)
    roll_down = rma(down, n)
    rs = roll_up / roll_down.replace(0, 1e-12)
    rsi = 100 - (100/(1+rs))
    return rsi.fillna(50)

def rsi_ma_context(df):
    if len(df) < max(RSI_MA_LEN, 14):
        return {"rsi": 50, "rsi_ma": 50, "cross": "none", "trendZ": "none", "in_chop": True}
    rsi = compute_rsi(df['close'].astype(float), 14)
    rsi_ma = sma(rsi, RSI_MA_LEN)
    cross = "none"
    if len(rsi) >= 2:
        if (rsi.iloc[-2] <= rsi_ma.iloc[-2]) and (rsi.iloc[-1] > rsi_ma.iloc[-1]):
            cross = "bull"
        elif (rsi.iloc[-2] >= rsi_ma.iloc[-2]) and (rsi.iloc[-1] < rsi_ma.iloc[-1]):
            cross = "bear"
    above = (rsi > rsi_ma)
    below = (rsi < rsi_ma)
    persist_bull = above.tail(RSI_TREND_PERSIST).all() if len(above) >= RSI_TREND_PERSIST else False
    persist_bear = below.tail(RSI_TREND_PERSIST).all() if len(below) >= RSI_TREND_PERSIST else False
    current_rsi = float(rsi.iloc[-1])
    in_chop = RSI_NEUTRAL_BAND[0] <= current_rsi <= RSI_NEUTRAL_BAND[1]
    return {
        "rsi": current_rsi,
        "rsi_ma": float(rsi_ma.iloc[-1]),
        "cross": cross,
        "trendZ": "bull" if persist_bull else ("bear" if persist_bear else "none"),
        "in_chop": in_chop
    }

def detect_liquidity_sweep_advanced(df):
    try:
        highs = df["high"].astype(float)
        lows = df["low"].astype(float)
        close = df["close"].astype(float)
        open_ = df["open"].astype(float)
        recent_high = highs.tail(20).iloc[:-1].max() if len(highs) > 1 else highs.max()
        recent_low = lows.tail(20).iloc[:-1].min() if len(lows) > 1 else lows.min()
        current_high = highs.iloc[-1]
        current_low = lows.iloc[-1]
        body = abs(close.iloc[-1] - open_.iloc[-1])
        wick_up = current_high - max(close.iloc[-1], open_.iloc[-1])
        wick_down = min(close.iloc[-1], open_.iloc[-1]) - current_low
        if current_high > recent_high and wick_up > body * 1.5:
            return "sell_sweep"
        if current_low < recent_low and wick_down > body * 1.5:
            return "buy_sweep"
        return None
    except Exception as e:
        log_warn(f"detect_liquidity_sweep_advanced error: {e}")
        return None

def detect_order_block_pro(df):
    if len(df) < 5:
        return None
    try:
        atr = compute_indicators(df).get('atr', 0)
        if atr == 0:
            return None
        for i in range(-4, -1):
            candle = df.iloc[i]
            prev_candle = df.iloc[i-1]
            next_candle = df.iloc[i+1]
            body = abs(candle['close'] - candle['open'])
            avg_body = (df['high'] - df['low']).rolling(14).mean().iloc[i]
            if body < avg_body * 0.6:
                if next_candle['close'] > candle['high'] and candle['close'] < candle['open']:
                    swing_low = df['low'].iloc[max(0, i-10):i+1].min()
                    swing_high = df['high'].iloc[max(0, i-10):i+1].max()
                    fib_levels = fibonacci_levels(swing_low, swing_high)
                    is_at_fib = any(abs(candle['low'] - level) / level < 0.001 for level in fib_levels.values())
                    return {"type": "bullish", "low": float(candle['low']), "high": float(candle['high']), "index": i, "fibonacci": is_at_fib}
                if next_candle['close'] < candle['low'] and candle['close'] > candle['open']:
                    swing_low = df['low'].iloc[max(0, i-10):i+1].min()
                    swing_high = df['high'].iloc[max(0, i-10):i+1].max()
                    fib_levels = fibonacci_levels(swing_low, swing_high)
                    is_at_fib = any(abs(candle['high'] - level) / level < 0.001 for level in fib_levels.values())
                    return {"type": "bearish", "low": float(candle['low']), "high": float(candle['high']), "index": i, "fibonacci": is_at_fib}
    except Exception as e:
        log_warn(f"detect_order_block_pro error: {e}")
    return None

def detect_fvg_pro(df):
    if len(df) < 3:
        return None
    try:
        atr = compute_indicators(df).get('atr', 0)
        if atr == 0:
            return None
        c1 = df.iloc[-3]
        c2 = df.iloc[-2]
        c3 = df.iloc[-1]
        if c1['high'] < c3['low']:
            gap_size = c3['low'] - c1['high']
            if gap_size > atr * 0.25:
                swing_low = df['low'].iloc[-20:].min()
                swing_high = df['high'].iloc[-20:].max()
                fib_levels = fibonacci_levels(swing_low, swing_high)
                mid = (c1['high'] + c3['low']) / 2
                is_at_fib = any(abs(mid - level) / level < 0.001 for level in fib_levels.values())
                return {"type": "bullish", "low": float(c1['high']), "high": float(c3['low']), "mid": float(mid), "fibonacci": is_at_fib}
        if c1['low'] > c3['high']:
            gap_size = c1['low'] - c3['high']
            if gap_size > atr * 0.25:
                swing_low = df['low'].iloc[-20:].min()
                swing_high = df['high'].iloc[-20:].max()
                fib_levels = fibonacci_levels(swing_low, swing_high)
                mid = (c3['high'] + c1['low']) / 2
                is_at_fib = any(abs(mid - level) / level < 0.001 for level in fib_levels.values())
                return {"type": "bearish", "low": float(c3['high']), "high": float(c1['low']), "mid": float(mid), "fibonacci": is_at_fib}
    except Exception as e:
        log_warn(f"detect_fvg_pro error: {e}")
    return None

def detect_structure(df):
    try:
        if len(df) < 2:
            return {"choch_up": False, "choch_down": False}
        highs = df["high"].astype(float)
        lows = df["low"].astype(float)
        choch_up = False
        choch_down = False
        if highs.iloc[-1] > highs.iloc[-2] and lows.iloc[-1] > lows.iloc[-2]:
            choch_up = True
        elif highs.iloc[-1] < highs.iloc[-2] and lows.iloc[-1] < lows.iloc[-2]:
            choch_down = True
        return {"choch_up": choch_up, "choch_down": choch_down, "ok": True}
    except Exception as e:
        log_warn(f"detect_structure error: {e}")
        return {"ok": False, "choch_up": False, "choch_down": False}

def detect_displacement(df, mult=2.2):
    try:
        if len(df) < 20:
            return {"ok": False}
        body = abs(df['close'].astype(float) - df['open'].astype(float))
        avg_body = body.rolling(20).mean().iloc[-1]
        current_body = body.iloc[-1]
        displaced = current_body > avg_body * mult
        direction = "up" if df['close'].iloc[-1] > df['open'].iloc[-1] else "down"
        return {
            "ok": True,
            "displaced": displaced,
            "direction": direction,
            "current_body": current_body,
            "avg_body": avg_body,
            "ratio": current_body / avg_body if avg_body != 0 else 0
        }
    except Exception as e:
        log_warn(f"detect_displacement error: {e}")
        return {"ok": False}

def compute_vwap(df):
    typical = (df['high'] + df['low'] + df['close']) / 3
    vwap = (typical * df['volume']).cumsum() / df['volume'].cumsum()
    return vwap

def vwap_context(df):
    vwap = compute_vwap(df)
    price = float(df['close'].iloc[-1])
    vwap_now = float(vwap.iloc[-1])
    vwap_5ago = float(vwap.iloc[-6]) if len(vwap) >= 6 else vwap_now
    vwap_slope_up = vwap_now > vwap_5ago
    vwap_slope_down = vwap_now < vwap_5ago
    buy_context = (price > vwap_now) and vwap_slope_up
    sell_context = (price < vwap_now) and vwap_slope_down
    return {
        "price": price,
        "vwap": vwap_now,
        "buy_context": buy_context,
        "sell_context": sell_context,
        "distance_pct": abs(price - vwap_now) / price if price != 0 else 0
    }

def compute_indicators(df: pd.DataFrame):
    if len(df) < max(ATR_LEN, RSI_LEN, ADX_LEN) + 2:
        return {"rsi":50.0,"plus_di":0.0,"minus_di":0.0,"dx":0.0,"adx":0.0,"atr":0.0}
    c,h,l = df["close"].astype(float), df["high"].astype(float), df["low"].astype(float)
    tr = pd.concat([(h-l).abs(), (h-c.shift(1)).abs(), (l-c.shift(1)).abs()], axis=1).max(axis=1)
    atr = rma(tr, ATR_LEN)
    rsi = compute_rsi(c, RSI_LEN)
    up_move = h.diff()
    down_move = l.shift(1) - l
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
    plus_di = 100 * rma(plus_dm, ADX_LEN) / atr.replace(0, 1e-12)
    minus_di = 100 * rma(minus_dm, ADX_LEN) / atr.replace(0, 1e-12)
    dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, 1e-12)).fillna(0.0)
    adx = rma(dx, ADX_LEN)
    i = len(df)-1
    return {
        "rsi": float(rsi.iloc[i]), "plus_di": float(plus_di.iloc[i]),
        "minus_di": float(minus_di.iloc[i]), "dx": float(dx.iloc[i]),
        "adx": float(adx.iloc[i]), "atr": float(atr.iloc[i])
    }

def compute_flow_pressure(exchange, symbol, now_px, lookback_sec=120):
    try:
        ob = fetch_orderbook_cached(symbol)
        bids = ob.get("bids", [])[:30]; asks = ob.get("asks", [])[:30]
        w = lambda p: 1.0/max(1e-9, abs(p-now_px))
        bpow = sum(q*w(p) for p,q in bids); apow = sum(q*w(p) for p,q in asks)
        obi = (apow - bpow) / max(1e-9,(apow+bpow))
        since = int((time.time()-lookback_sec)*1000)
        trades = fetch_trades_cached(symbol)
        recent_trades = [t for t in trades if t["timestamp"] >= since]
        buyv = sum(t["amount"] for t in recent_trades if str(t.get("side","")).lower()=="buy")
        sellv= sum(t["amount"] for t in recent_trades if str(t.get("side","")).lower()=="sell")
        delta = (buyv - sellv)
        ask_p = min(asks, key=lambda x:x[0])[0] if asks else None
        bid_p = max(bids, key=lambda x:x[0])[0] if bids else None
        ask_bps = abs((ask_p-now_px)/now_px)*10000.0 if ask_p else None
        bid_bps = abs((bid_p-now_px)/now_px)*10000.0 if bid_p else None
        wall_close = (ask_bps and ask_bps<=WALL_PROX_BPS) or (bid_bps and bid_bps<=WALL_PROX_BPS)
        return {"ok":True,"obi":obi,"delta":delta,
                "big_sellers": (obi>=OBI_EDGE) or (delta<-DELTA_EDGE),
                "big_buyers":  (obi<=-OBI_EDGE) or (delta> DELTA_EDGE),
                "wall_close": bool(wall_close)}
    except Exception as e:
        log_warn(f"flowx err: {e}")
        return {"ok":False}

def detect_volume_explosion(df):
    vol = df["volume"].astype(float)
    avg = vol.rolling(20).mean()
    if len(avg) == 0:
        return False
    return vol.iloc[-1] > avg.iloc[-1] * 2

def volume_above_ma(df, lookback=20, mult=1.0):
    vol = df['volume'].astype(float)
    if len(vol) < lookback+1:
        return False
    avg_vol = vol.iloc[-lookback-1:-1].mean()
    return vol.iloc[-1] > avg_vol * mult

def detect_engulfing(df):
    patterns = detect_candle_patterns(df)
    return patterns.get("bullish_engulfing") or patterns.get("bearish_engulfing")

def has_long_wick(df):
    last = df.iloc[-1]
    body = abs(last['close'] - last['open'])
    upper_wick = last['high'] - max(last['close'], last['open'])
    lower_wick = min(last['close'], last['open']) - last['low']
    if body == 0:
        return False
    return (upper_wick > body * 1.2) or (lower_wick > body * 1.2)

def detect_choch_bos(df):
    bos = detect_bos_choch(df)
    return bos.get("bos_up") or bos.get("bos_down") or bos.get("choch_up") or bos.get("choch_down")

def micro_structure_shift(df):
    try:
        if len(df) < 10:
            return False
        highs = df['high'].astype(float)
        lows = df['low'].astype(float)
        if highs.iloc[-1] > highs.iloc[-2] and lows.iloc[-1] > lows.iloc[-2]:
            return True
        if highs.iloc[-1] < highs.iloc[-2] and lows.iloc[-1] < lows.iloc[-2]:
            return True
        return False
    except:
        return False

def near_equal_highs_lows(df):
    try:
        clusters = detect_liquidity_clusters_advanced(df, window=20, atr_mult=1.0)
        if clusters.get("ok") and (clusters.get("near_high") or clusters.get("near_low")):
            return True
        return False
    except:
        return False

def volatility_filter(df):
    try:
        atr = compute_indicators(df).get('atr', 0)
        price = float(df['close'].iloc[-1])
        atr_pct = atr / price
        if atr_pct < MIN_ATR_PCT:
            return False, f"Low volatility (ATR%={atr_pct*100:.3f}%)"
        return True, "OK"
    except:
        return False, "Volatility filter error"

def detect_btc_trend():
    try:
        df = get_ohlcv_safe("BTC/USDT:USDT", interval="15m", limit=100)
        if len(df) < 50:
            return "neutral", 0.0
        ema50 = df["close"].ewm(span=50).mean().iloc[-1]
        ema200 = df["close"].ewm(span=200).mean().iloc[-1]
        price = df["close"].iloc[-1]
        if price > ema50 and ema50 > ema200:
            trend = "bullish"
        elif price < ema50 and ema50 < ema200:
            trend = "bearish"
        else:
            trend = "neutral"
        price_1h_ago = df["close"].iloc[-5] if len(df) >= 5 else df["close"].iloc[0]
        change_1h = (price - price_1h_ago) / price_1h_ago * 100
        return trend, change_1h
    except Exception:
        return "neutral", 0.0

def btc_crash_protection(btc_change_1h):
    return btc_change_1h < -BTC_CRASH_THRESHOLD

def detect_liquidity_pools(df, window=20):
    try:
        highs = df['high'].astype(float)
        lows = df['low'].astype(float)
        recent_highs = highs.tail(window)
        recent_lows = lows.tail(window)
        high_mean = recent_highs.mean()
        eq_highs = recent_highs[abs(recent_highs - high_mean) / high_mean < 0.001]
        low_mean = recent_lows.mean()
        eq_lows = recent_lows[abs(recent_lows - low_mean) / low_mean < 0.001]
        liquidity_high = recent_highs.max()
        liquidity_low = recent_lows.min()
        current_price = float(df['close'].iloc[-1])
        near_liquidity_high = abs(current_price - liquidity_high) / liquidity_high < 0.002
        near_liquidity_low = abs(current_price - liquidity_low) / liquidity_low < 0.002
        sweep_advanced = detect_liquidity_sweep_advanced(df)
        swept_low_advanced = (sweep_advanced == "buy_sweep")
        swept_high_advanced = (sweep_advanced == "sell_sweep")
        return {
            "ok": True,
            "equal_highs": eq_highs.tolist() if len(eq_highs) > 0 else [],
            "equal_lows": eq_lows.tolist() if len(eq_lows) > 0 else [],
            "liquidity_high": liquidity_high,
            "liquidity_low": liquidity_low,
            "near_high": near_liquidity_high,
            "near_low": near_liquidity_low,
            "swept_high": swept_high_advanced,
            "swept_low": swept_low_advanced,
            "sweep_advanced": sweep_advanced
        }
    except Exception as e:
        log_warn(f"detect_liquidity_pools error: {e}")
        return {"ok": False}

def detect_whale_trap(df):
    try:
        highs = df["high"].astype(float)
        lows = df["low"].astype(float)
        close = df["close"].astype(float)
        open_ = df["open"].astype(float)
        recent_high = highs.tail(20).max()
        recent_low = lows.tail(20).min()
        current_high = highs.iloc[-1]
        current_low = lows.iloc[-1]
        body = abs(close.iloc[-1] - open_.iloc[-1])
        wick_up = current_high - max(close.iloc[-1], open_.iloc[-1])
        wick_down = min(close.iloc[-1], open_.iloc[-1]) - current_low
        trap = None
        if current_high > recent_high and wick_up > body * 1.5:
            trap = "bull_trap"
        if current_low < recent_low and wick_down > body * 1.5:
            trap = "bear_trap"
        return trap
    except Exception as e:
        log_warn(f"whale_trap error: {e}")
        return None

def detect_stop_hunt(df):
    """Liquidity sweep detection (stop hunt). Returns 'LONG' for bullish sweep, 'SHORT' for bearish, else None."""
    try:
        high = df["high"]
        low = df["low"]
        prev_high = high.iloc[-20:-1].max()
        prev_low = low.iloc[-20:-1].min()
        last = df.iloc[-1]
        if last["high"] > prev_high and last["close"] < prev_high:
            return "SHORT"
        if last["low"] < prev_low and last["close"] > prev_low:
            return "LONG"
        return None
    except Exception as e:
        log_warn(f"detect_stop_hunt error: {e}")
        return None

def liquidity_heatmap_engine(symbol):
    if not ENABLE_LIQUIDITY_HEATMAP:
        return {"side": None, "score": 0, "bid_wall": None, "ask_wall": None}
    try:
        ob = get_orderbook_safe(symbol, limit=BOOKMAP_DEPTH)
        bids = ob.get("bids", [])[:BOOKMAP_DEPTH]
        asks = ob.get("asks", [])[:BOOKMAP_DEPTH]
        bid_walls = sorted(bids, key=lambda x: x[1], reverse=True)[:3]
        ask_walls = sorted(asks, key=lambda x: x[1], reverse=True)[:3]
        largest_bid = bid_walls[0] if bid_walls else None
        largest_ask = ask_walls[0] if ask_walls else None
        side = None
        score = 0
        if largest_bid and largest_ask:
            if largest_bid[1] > largest_ask[1] * 1.5:
                side = "BUY"
                score = 2
            elif largest_ask[1] > largest_bid[1] * 1.5:
                side = "SELL"
                score = 2
        elif largest_bid and not largest_ask:
            side = "BUY"
            score = 1
        elif largest_ask and not largest_bid:
            side = "SELL"
            score = 1
        return {
            "side": side,
            "score": score,
            "bid_wall": largest_bid,
            "ask_wall": largest_ask
        }
    except Exception as e:
        log_warn(f"Heatmap error: {e}")
        return {"side": None, "score": 0, "bid_wall": None, "ask_wall": None}

def liquidity_reversal_engine(df):
    if not ENABLE_LIQUIDITY_REVERSAL:
        return {"side": None, "score": 0}
    try:
        highs = df["high"].astype(float)
        lows = df["low"].astype(float)
        close = df["close"].astype(float)
        open_ = df["open"].astype(float)
        recent_high = highs.tail(20).iloc[:-1].max() if len(highs) > 1 else highs.max()
        recent_low = lows.tail(20).iloc[:-1].min() if len(lows) > 1 else lows.min()
        current_high = highs.iloc[-1]
        current_low = lows.iloc[-1]
        body = abs(close.iloc[-1] - open_.iloc[-1])
        wick_up = current_high - max(close.iloc[-1], open_.iloc[-1])
        wick_down = min(close.iloc[-1], open_.iloc[-1]) - current_low
        signal = None
        score = 0
        if current_low < recent_low and wick_down > body * 1.5:
            signal = "BUY"
            score = 4
        if current_high > recent_high and wick_up > body * 1.5:
            signal = "SELL"
            score = 4
        return {
            "side": signal,
            "score": score
        }
    except Exception as e:
        log_warn(f"liquidity_reversal_engine error: {e}")
        return {"side": None, "score": 0}

def detect_supply_demand_zones(df, lookback=50, min_touches=2):
    if len(df) < lookback:
        return {"supply_zones": [], "demand_zones": []}
    highs = df['high'].values[-lookback:]
    lows = df['low'].values[-lookback:]
    closes = df['close'].values[-lookback:]
    volumes = df['volume'].values[-lookback:]
    supply_zones = []
    demand_zones = []
    swing_highs = []
    swing_lows = []
    for i in range(2, len(highs)-2):
        if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]:
            swing_highs.append((i, highs[i], volumes[i], closes[i]))
        if lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]:
            swing_lows.append((i, lows[i], volumes[i], closes[i]))
    tolerance = np.mean(highs) * 0.002
    used_high = [False] * len(swing_highs)
    for i, (idx_i, price_i, vol_i, close_i) in enumerate(swing_highs):
        if used_high[i]:
            continue
        cluster = [(idx_i, price_i, vol_i, close_i)]
        used_high[i] = True
        for j, (idx_j, price_j, vol_j, close_j) in enumerate(swing_highs[i+1:], i+1):
            if not used_high[j] and abs(price_i - price_j) <= tolerance:
                cluster.append((idx_j, price_j, vol_j, close_j))
                used_high[j] = True
        if len(cluster) >= min_touches:
            avg_price = sum(p for _, p, _, _ in cluster) / len(cluster)
            total_vol = sum(v for _, _, v, _ in cluster)
            strength = len(cluster) + (total_vol / np.mean(volumes) if np.mean(volumes) > 0 else 0)
            rejection = False
            for idx, _, _, _ in cluster:
                open_price = df['open'].iloc[idx - lookback]
                body = abs(closes[idx] - open_price)
                upper_wick = highs[idx] - max(closes[idx], open_price)
                if upper_wick > body * 1.5:
                    rejection = True
                    break
            supply_zones.append({
                'price': avg_price,
                'strength': strength,
                'touches': len(cluster),
                'rejection': rejection
            })
    used_low = [False] * len(swing_lows)
    for i, (idx_i, price_i, vol_i, close_i) in enumerate(swing_lows):
        if used_low[i]:
            continue
        cluster = [(idx_i, price_i, vol_i, close_i)]
        used_low[i] = True
        for j, (idx_j, price_j, vol_j, close_j) in enumerate(swing_lows[i+1:], i+1):
            if not used_low[j] and abs(price_i - price_j) <= tolerance:
                cluster.append((idx_j, price_j, vol_j, close_j))
                used_low[j] = True
        if len(cluster) >= min_touches:
            avg_price = sum(p for _, p, _, _ in cluster) / len(cluster)
            total_vol = sum(v for _, _, v, _ in cluster)
            strength = len(cluster) + (total_vol / np.mean(volumes) if np.mean(volumes) > 0 else 0)
            rejection = False
            for idx, _, _, _ in cluster:
                open_price = df['open'].iloc[idx - lookback]
                body = abs(closes[idx] - open_price)
                lower_wick = min(closes[idx], open_price) - lows[idx]
                if lower_wick > body * 1.5:
                    rejection = True
                    break
            demand_zones.append({
                'price': avg_price,
                'strength': strength,
                'touches': len(cluster),
                'rejection': rejection
            })
    supply_zones.sort(key=lambda x: x['strength'], reverse=True)
    demand_zones.sort(key=lambda x: x['strength'], reverse=True)
    return {
        'supply_zones': supply_zones,
        'demand_zones': demand_zones
    }

def supply_demand_engine(df, flowx=None):
    if not ENABLE_SUPPLY_DEMAND:
        return {"side": None, "score": 0, "zone": None}
    try:
        zones = detect_supply_demand_zones(df, min_touches=SD_MIN_ZONE_STRENGTH)
        price = float(df['close'].iloc[-1])
        atr = compute_indicators(df).get('atr', 0)
        nearest_supply = None
        nearest_demand = None
        min_dist_supply = float('inf')
        min_dist_demand = float('inf')
        for z in zones['supply_zones']:
            dist = abs(price - z['price']) / price
            if dist < min_dist_supply:
                min_dist_supply = dist
                nearest_supply = z
        for z in zones['demand_zones']:
            dist = abs(price - z['price']) / price
            if dist < min_dist_demand:
                min_dist_demand = dist
                nearest_demand = z
        side = None
        score = 0
        zone_used = None
        if nearest_supply and min_dist_supply <= SD_MAX_ZONE_DISTANCE_PCT:
            condition_met = True
            if SD_REQUIRE_REJECTION and not nearest_supply['rejection']:
                condition_met = False
            if SD_REQUIRE_ORDERFLOW and flowx and not flowx.get('big_sellers', False):
                condition_met = False
            if condition_met and price <= nearest_supply['price']:
                side = "SELL"
                score = nearest_supply['strength'] * 2
                zone_used = nearest_supply
        if nearest_demand and min_dist_demand <= SD_MAX_ZONE_DISTANCE_PCT:
            condition_met = True
            if SD_REQUIRE_REJECTION and not nearest_demand['rejection']:
                condition_met = False
            if SD_REQUIRE_ORDERFLOW and flowx and not flowx.get('big_buyers', False):
                condition_met = False
            if condition_met and price >= nearest_demand['price']:
                side = "BUY"
                score = nearest_demand['strength'] * 2
                zone_used = nearest_demand
        if side:
            score = min(score, 10)
            score = score * SD_ENTRY_SCORE_WEIGHT / 2
        return {
            "side": side,
            "score": score,
            "zone": zone_used
        }
    except Exception as e:
        log_warn(f"SupplyDemandEngine error: {e}")
        return {"side": None, "score": 0, "zone": None}

def find_all_order_blocks(df, lookback=50, min_touches=1):
    if len(df) < lookback:
        lookback = len(df)
    order_blocks = []
    atr = compute_indicators(df).get('atr', 0)
    if atr == 0:
        return []
    for i in range(-lookback, -2):
        candle = df.iloc[i]
        prev_candle = df.iloc[i-1]
        next_candle = df.iloc[i+1]
        body = abs(candle['close'] - candle['open'])
        avg_body = (df['high'] - df['low']).rolling(14).mean().iloc[i]
        if body < avg_body * 0.6:
            if next_candle['close'] > candle['high'] and candle['close'] < candle['open']:
                zone_price = candle['low']
                touches = 0
                for j in range(i+2, min(i+20, len(df))):
                    if df['low'].iloc[j] <= zone_price * 1.002 and df['low'].iloc[j] >= zone_price * 0.998:
                        touches += 1
                strength = touches + (1 if body < avg_body * 0.3 else 0)
                order_blocks.append({
                    'price': zone_price,
                    'type': 'bullish',
                    'strength': strength,
                    'touches': touches
                })
            if next_candle['close'] < candle['low'] and candle['close'] > candle['open']:
                zone_price = candle['high']
                touches = 0
                for j in range(i+2, min(i+20, len(df))):
                    if df['high'].iloc[j] <= zone_price * 1.002 and df['high'].iloc[j] >= zone_price * 0.998:
                        touches += 1
                strength = touches + (1 if body < avg_body * 0.3 else 0)
                order_blocks.append({
                    'price': zone_price,
                    'type': 'bearish',
                    'strength': strength,
                    'touches': touches
                })
    unique_blocks = []
    tolerance = atr * 0.5
    for ob in order_blocks:
        found = False
        for u in unique_blocks:
            if abs(ob['price'] - u['price']) < tolerance and ob['type'] == u['type']:
                u['strength'] += ob['strength']
                u['touches'] += ob['touches']
                found = True
                break
        if not found:
            unique_blocks.append(ob)
    return unique_blocks

def find_all_fvgs(df, lookback=50):
    if len(df) < 3:
        return []
    fvgs = []
    atr = compute_indicators(df).get('atr', 0)
    if atr == 0:
        return []
    for i in range(-lookback, -1):
        if i+1 >= len(df) or i+2 >= len(df):
            continue
        c1 = df.iloc[i]
        c2 = df.iloc[i+1]
        c3 = df.iloc[i+2]
        if c1['high'] < c3['low']:
            gap_size = c3['low'] - c1['high']
            if gap_size > atr * 0.25:
                fvgs.append({
                    'price': (c1['high'] + c3['low']) / 2,
                    'type': 'bullish',
                    'strength': gap_size / atr
                })
        if c1['low'] > c3['high']:
            gap_size = c1['low'] - c3['high']
            if gap_size > atr * 0.25:
                fvgs.append({
                    'price': (c3['high'] + c1['low']) / 2,
                    'type': 'bearish',
                    'strength': gap_size / atr
                })
    return fvgs

def detect_liquidity_clusters_advanced(df, window=30, atr_mult=1.0):
    if len(df) < window:
        return {"ok": False}
    highs = df['high'].astype(float).tail(window).values
    lows = df['low'].astype(float).tail(window).values
    current_price = float(df['close'].iloc[-1])
    atr = compute_indicators(df).get('atr', 0)
    tolerance = atr * atr_mult
    high_clusters = []
    used = [False] * len(highs)
    for i in range(len(highs)):
        if used[i]:
            continue
        cluster = [highs[i]]
        used[i] = True
        for j in range(i+1, len(highs)):
            if not used[j] and abs(highs[i] - highs[j]) < tolerance:
                cluster.append(highs[j])
                used[j] = True
        if len(cluster) > 1:
            high_clusters.append(cluster)
    low_clusters = []
    used = [False] * len(lows)
    for i in range(len(lows)):
        if used[i]:
            continue
        cluster = [lows[i]]
        used[i] = True
        for j in range(i+1, len(lows)):
            if not used[j] and abs(lows[i] - lows[j]) < tolerance:
                cluster.append(lows[j])
                used[j] = True
        if len(cluster) > 1:
            low_clusters.append(cluster)
    clusters_high = []
    for c in high_clusters:
        clusters_high.append({
            'price': sum(c) / len(c),
            'touches': len(c),
            'strength': len(c)
        })
    clusters_low = []
    for c in low_clusters:
        clusters_low.append({
            'price': sum(c) / len(c),
            'touches': len(c),
            'strength': len(c)
        })
    nearest_high = None
    min_dist = float('inf')
    for c in clusters_high:
        dist = abs(current_price - c['price']) / current_price
        if dist < min_dist:
            min_dist = dist
            nearest_high = c
    nearest_low = None
    min_dist = float('inf')
    for c in clusters_low:
        dist = abs(current_price - c['price']) / current_price
        if dist < min_dist:
            min_dist = dist
            nearest_low = c
    near_high = nearest_high and (abs(current_price - nearest_high['price']) / current_price < 0.002)
    near_low = nearest_low and (abs(current_price - nearest_low['price']) / current_price < 0.002)
    return {
        "ok": True,
        "high_clusters": clusters_high,
        "low_clusters": clusters_low,
        "nearest_high": nearest_high,
        "nearest_low": nearest_low,
        "near_high": near_high,
        "near_low": near_low
    }

def detect_liquidity_map(df, window=30, tolerance_ratio=0.0005):
    clusters = detect_liquidity_clusters_advanced(df, window, atr_mult=1.0)
    if not clusters.get("ok"):
        return {"ok": False}
    current_high = df['high'].iloc[-1]
    current_low = df['low'].iloc[-1]
    current_close = df['close'].iloc[-1]
    ind = compute_indicators(df)
    atr = ind.get('atr', 0)
    body = abs(df['close'].iloc[-1] - df['open'].iloc[-1])
    has_displacement = body >= atr * 0.8
    swept_high = False
    bearish_sweep = False
    for cluster in clusters.get("high_clusters", []):
        if current_high > cluster['price'] and current_close < cluster['price']:
            swept_high = True
            bearish_sweep = bearish_sweep or has_displacement
        if len(df) > 2:
            if df['high'].iloc[-2] > cluster['price'] and df['close'].iloc[-2] < cluster['price']:
                swept_high = True
                bearish_sweep = bearish_sweep or has_displacement
    swept_low = False
    bullish_sweep = False
    for cluster in clusters.get("low_clusters", []):
        if current_low < cluster['price'] and current_close > cluster['price']:
            swept_low = True
            bullish_sweep = bullish_sweep or has_displacement
        if len(df) > 2:
            if df['low'].iloc[-2] < cluster['price'] and df['close'].iloc[-2] > cluster['price']:
                swept_low = True
                bullish_sweep = bullish_sweep or has_displacement
    return {
        "ok": True,
        "equal_highs": [c['price'] for c in clusters.get("high_clusters", [])],
        "equal_lows": [c['price'] for c in clusters.get("low_clusters", [])],
        "nearest_high": clusters.get("nearest_high", {}).get('price') if clusters.get("nearest_high") else None,
        "nearest_low": clusters.get("nearest_low", {}).get('price') if clusters.get("nearest_low") else None,
        "near_high": clusters.get("near_high", False),
        "near_low": clusters.get("near_low", False),
        "swept_high": swept_high,
        "swept_low": swept_low,
        "bullish_sweep": bullish_sweep,
        "bearish_sweep": bearish_sweep,
        "clusters": clusters
    }

def detect_spring(df, lookback=30):
    if len(df) < lookback:
        return False
    lows = df['low'].tail(lookback).values
    support_level = min(lows[:-1])
    current_low = lows[-1]
    current_close = df['close'].iloc[-1]
    vol = df['volume'].astype(float).tail(lookback)
    avg_vol = vol[:-1].mean()
    current_vol = vol.iloc[-1]
    volume_ok = current_vol > avg_vol * 1.5
    if current_low < support_level and current_close > support_level and volume_ok:
        return True
    return False

def detect_upthrust(df, lookback=30):
    if len(df) < lookback:
        return False
    highs = df['high'].tail(lookback).values
    resistance_level = max(highs[:-1])
    current_high = highs[-1]
    current_close = df['close'].iloc[-1]
    vol = df['volume'].astype(float).tail(lookback)
    avg_vol = vol[:-1].mean()
    current_vol = vol.iloc[-1]
    volume_ok = current_vol > avg_vol * 1.5
    if current_high > resistance_level and current_close < resistance_level and volume_ok:
        return True
    return False

def detect_liquidity_void(df, threshold=2.5):
    try:
        if len(df) < 20:
            return {"ok": False}
        body = abs(df["close"].astype(float) - df["open"].astype(float))
        avg_body = body.rolling(20).mean().iloc[-1]
        current_body = body.iloc[-1]
        if avg_body == 0:
            return {"ok": False}
        ratio = current_body / avg_body
        if ratio > threshold:
            high = float(df["high"].iloc[-1])
            low = float(df["low"].iloc[-1])
            direction = "up" if df["close"].iloc[-1] > df["open"].iloc[-1] else "down"
            return {
                "ok": True,
                "direction": direction,
                "high": high,
                "low": low,
                "ratio": ratio
            }
        return {"ok": False}
    except Exception as e:
        log_warn(f"liquidity_void error: {e}")
        return {"ok": False}

def detect_bos_choch(df, window=5):
    try:
        if len(df) < window*2 + 2:
            return {"bos_up": False, "bos_down": False, "choch_up": False, "choch_down": False}
        highs = df['high'].values
        lows = df['low'].values
        last_idx = len(df) - 1
        recent_high_idx = max(range(max(0, last_idx-window*2), last_idx), key=lambda i: highs[i])
        recent_low_idx = min(range(max(0, last_idx-window*2), last_idx), key=lambda i: lows[i])
        recent_high = highs[recent_high_idx]
        recent_low = lows[recent_low_idx]
        current_high = highs[last_idx]
        current_low = lows[last_idx]
        bos_up = current_high > recent_high
        bos_down = current_low < recent_low
        choch_up = False
        choch_down = False
        return {
            "bos_up": bos_up,
            "bos_down": bos_down,
            "choch_up": choch_up,
            "choch_down": choch_down,
            "recent_high": recent_high,
            "recent_low": recent_low
        }
    except Exception as e:
        log_warn(f"detect_bos_choch error: {e}")
        return {"bos_up": False, "bos_down": False, "choch_up": False, "choch_down": False}

def detect_candle_patterns(df):
    try:
        if len(df) < 3:
            return {}
        patterns = {}
        last = df.iloc[-1]
        prev = df.iloc[-2]
        open_, high, low, close = last['open'], last['high'], last['low'], last['close']
        body = abs(close - open_)
        upper_wick = high - max(close, open_)
        lower_wick = min(close, open_) - low
        if body > 0 and (upper_wick > body * 2 or lower_wick > body * 2):
            if upper_wick > body * 2 and close < open_:
                patterns['pin_bar_top'] = True
            elif lower_wick > body * 2 and close > open_:
                patterns['pin_bar_bottom'] = True
        prev_body = abs(prev['close'] - prev['open'])
        if close > open_ and prev['close'] < prev['open'] and close > prev['high'] and open_ < prev['low']:
            patterns['bullish_engulfing'] = True
        if close < open_ and prev['close'] > prev['open'] and close < prev['low'] and open_ > prev['high']:
            patterns['bearish_engulfing'] = True
        if high <= prev['high'] and low >= prev['low']:
            patterns['inside_bar'] = True
        return patterns
    except Exception as e:
        log_warn(f"detect_candle_patterns error: {e}")
        return {}

def detect_market_regime_advanced(df):
    try:
        if len(df) < 20:
            return "range"
        ind = compute_indicators(df)
        adx = ind.get('adx', 0)
        atr = ind.get('atr', 0)
        close = df['close'].iloc[-1]
        atr_pct = atr / close if close != 0 else 0
        sma20 = df['close'].rolling(20).mean().iloc[-1]
        std20 = df['close'].rolling(20).std().iloc[-1]
        bb_width = (std20 * 2) / sma20 if sma20 != 0 else 0
        if adx >= 25 and atr_pct > 0.004:
            return "trend"
        if atr_pct > 0.01 or bb_width > 0.1:
            return "volatile"
        return "range"
    except Exception as e:
        log_warn(f"detect_market_regime_advanced error: {e}")
        return "range"

def classify_trend_strength(adx, plus_di, minus_di):
    spread = abs(plus_di - minus_di)
    if adx >= 35 and spread > 10:
        return "strong"
    elif adx >= 22 and spread > 6:
        return "trend"
    elif adx >= 18:
        return "emerging"
    else:
        return "weak"

def fibonacci_levels(swing_low, swing_high):
    diff = swing_high - swing_low
    return {
        0.236: swing_low + 0.236 * diff,
        0.382: swing_low + 0.382 * diff,
        0.5: swing_low + 0.5 * diff,
        0.618: swing_low + 0.618 * diff,
        0.786: swing_low + 0.786 * diff
    }

def fibonacci_extensions(swing_low, swing_high, pullback_low=None):
    if pullback_low is None:
        pullback_low = swing_low
    diff = swing_high - swing_low
    return {
        1.272: swing_high + 0.272 * diff,
        1.414: swing_high + 0.414 * diff,
        1.618: swing_high + 0.618 * diff,
        2.0: swing_high + 1.0 * diff,
        2.618: swing_high + 1.618 * diff
    }

def golden_zone_check_pro(df, ind=None, side_hint=None):
    if len(df) < 30:
        return {"ok": False, "score": 0.0, "zone": None, "reasons": ["short_df"]}
    try:
        h = df['high'].astype(float)
        l = df['low'].astype(float)
        c = df['close'].astype(float)
        v = df['volume'].astype(float)
        swing_hi = h.rolling(10).max().iloc[-1]
        swing_lo = l.rolling(10).min().iloc[-1]
        if swing_hi <= swing_lo:
            return {"ok": False, "score": 0.0, "zone": None, "reasons": ["flat_market"]}
        fibs = fibonacci_levels(swing_lo, swing_hi)
        last_close = float(c.iloc[-1])
        current_open = float(df['open'].iloc[-1])
        current_high = float(h.iloc[-1])
        current_low = float(l.iloc[-1])
        body = abs(last_close - current_open)
        wick_up = current_high - max(last_close, current_open)
        wick_down = min(last_close, current_open) - current_low
        bull_candle = wick_down > (body * 1.2) and last_close > current_open
        bear_candle = wick_up > (body * 1.2) and last_close < current_open
        vol_ma20 = v.rolling(20).mean().iloc[-1]
        vol_ok = float(v.iloc[-1]) >= vol_ma20 * 0.8
        adx = ind.get('adx', 0) if ind else 0
        rsi_ctx = rsi_ma_context(df)
        score = 0.0
        zone_type = None
        reasons = []
        level_used = None
        for level_name, level_price in fibs.items():
            if abs(last_close - level_price) / level_price < 0.002:
                if bull_candle and level_price < swing_hi * 0.8:
                    score += 4.0
                    reasons.append(f"فيبو_{level_name}+شمعة_صاعدة")
                    level_used = level_name
                    if adx >= GZ_REQ_ADX:
                        score += 2.0
                        reasons.append("ADX_قوي")
                    if rsi_ctx["cross"] == "bull" or rsi_ctx["trendZ"] == "bull":
                        score += 1.5
                        reasons.append("RSI_إيجابي")
                    if vol_ok:
                        score += 0.5
                        reasons.append("حجم_مرتفع")
                    zone_type = f"golden_bottom_{level_name}"
                    break
                if bear_candle and level_price > swing_lo * 1.2:
                    score += 4.0
                    reasons.append(f"فيبو_{level_name}+شمعة_هابطة")
                    level_used = level_name
                    if adx >= GZ_REQ_ADX:
                        score += 2.0
                        reasons.append("ADX_قوي")
                    if rsi_ctx["cross"] == "bear" or rsi_ctx["trendZ"] == "bear":
                        score += 1.5
                        reasons.append("RSI_سلبي")
                    if vol_ok:
                        score += 0.5
                        reasons.append("حجم_مرتفع")
                    zone_type = f"golden_top_{level_name}"
                    break
        ok = zone_type is not None and ALLOW_GZ_ENTRY
        return {
            "ok": ok,
            "score": score,
            "zone": {"type": zone_type, "fib": level_used, "price": level_price} if zone_type else None,
            "reasons": reasons
        }
    except Exception as e:
        return {"ok": False, "score": 0.0, "zone": None, "reasons": [f"error: {e}"]}

def detect_mss(df, lookback=10):
    try:
        if len(df) < lookback*2:
            return None
        highs = df['high'].values
        lows = df['low'].values
        last = len(df)-1
        swing_highs = []
        swing_lows = []
        for i in range(lookback, last-lookback):
            if highs[i] > highs[i-1] and highs[i] > highs[i+1]:
                swing_highs.append((i, highs[i]))
            if lows[i] < lows[i-1] and lows[i] < lows[i+1]:
                swing_lows.append((i, lows[i]))
        if len(swing_highs) < 2 or len(swing_lows) < 2:
            return None
        last_high_idx, last_high = swing_highs[-1]
        prev_high_idx, prev_high = swing_highs[-2]
        last_low_idx, last_low = swing_lows[-1]
        prev_low_idx, prev_low = swing_lows[-2]
        current_price = df['close'].iloc[-1]
        if last_low > prev_low and current_price > last_high:
            return "bullish"
        if last_high < prev_high and current_price < last_low:
            return "bearish"
        return None
    except Exception as e:
        log_warn(f"detect_mss error: {e}")
        return None

def _find_clusters(values, tolerance):
    used = [False] * len(values)
    clusters = []
    for i in range(len(values)):
        if used[i]:
            continue
        cluster = [values[i]]
        used[i] = True
        for j in range(i+1, len(values)):
            if not used[j] and abs(values[i] - values[j]) < tolerance:
                cluster.append(values[j])
                used[j] = True
        if len(cluster) > 1:
            clusters.append(cluster)
    return clusters

def liquidity_magnet_engine_enhanced(df, lookback=40):
    try:
        highs = df["high"].tail(lookback).astype(float).values
        lows = df["low"].tail(lookback).astype(float).values
        price = float(df["close"].iloc[-1])
        tolerance_high = np.mean(highs) * 0.0015
        tolerance_low = np.mean(lows) * 0.0015
        high_clusters = _find_clusters(highs, tolerance_high)
        low_clusters = _find_clusters(lows, tolerance_low)
        score = 0
        side = None
        if len(low_clusters) > 0:
            score += 3
            side = "BUY"
        if len(high_clusters) > 0:
            score += 3
            side = "SELL"
        return {
            "score": score,
            "side": side,
            "high_clusters": high_clusters,
            "low_clusters": low_clusters
        }
    except Exception as e:
        log_warn(f"liquidity_magnet_engine_enhanced error: {e}")
        return {"score": 0, "side": None, "high_clusters": [], "low_clusters": []}

def liquidity_gravity_engine(df, lookback=40):
    try:
        highs = df["high"].tail(lookback).astype(float).values
        lows = df["low"].tail(lookback).astype(float).values
        price = float(df["close"].iloc[-1])
        tol_high = np.mean(highs) * 0.0015
        tol_low  = np.mean(lows)  * 0.0015
        high_clusters = _find_clusters(highs, tol_high)
        low_clusters  = _find_clusters(lows, tol_low)
        gravity_buy = 0
        gravity_sell = 0
        for c in low_clusters:
            lvl = np.mean(c)
            dist = abs(price - lvl) / price
            strength = len(c)
            gravity_buy += strength / (dist + 1e-6)
        for c in high_clusters:
            lvl = np.mean(c)
            dist = abs(price - lvl) / price
            strength = len(c)
            gravity_sell += strength / (dist + 1e-6)
        if gravity_buy > gravity_sell:
            side = "BUY"
            score = 3
        elif gravity_sell > gravity_buy:
            side = "SELL"
            score = 3
        else:
            side = None
            score = 0
        return {
            "side": side,
            "score": score,
            "buy_gravity": gravity_buy,
            "sell_gravity": gravity_sell,
            "high_clusters": len(high_clusters),
            "low_clusters": len(low_clusters)
        }
    except Exception as e:
        log_warn(f"gravity_engine error: {e}")
        return {"side": None, "score": 0}

def liquidity_sweep(df):
    try:
        prev_high = df.high.iloc[-2]
        prev_low = df.low.iloc[-2]
        last_high = df.high.iloc[-1]
        last_low = df.low.iloc[-1]
        close = df.close.iloc[-1]
        if last_low < prev_low and close > prev_low:
            return "BUY"
        if last_high > prev_high and close < prev_high:
            return "SELL"
        return None
    except Exception as e:
        log_warn(f"liquidity_sweep error: {e}")
        return None

def volatility_expansion_engine(df, lookback=20):
    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    volume = df["volume"].astype(float)
    tr = (high - low).abs()
    atr = tr.rolling(14).mean()
    current_atr = atr.iloc[-1]
    avg_atr = atr.tail(lookback).mean()
    avg_vol = volume.tail(lookback).mean()
    current_vol = volume.iloc[-1]
    body = abs(close.iloc[-1] - df["open"].iloc[-1])
    avg_body = abs(close - df["open"]).rolling(lookback).mean().iloc[-1]
    expansion = False
    side = None
    score = 0
    if current_atr > avg_atr * 1.4 and current_vol > avg_vol * 1.5 and body > avg_body * 1.8:
        expansion = True
        if close.iloc[-1] > df["open"].iloc[-1]:
            side = "BUY"
        else:
            side = "SELL"
        score = 3
    return {
        "expansion": expansion,
        "side": side,
        "score": score
    }

def detect_smart_money_intent(df, ind):
    try:
        high = df['high'].iloc[-1]
        low = df['low'].iloc[-1]
        close = df['close'].iloc[-1]

        prev_high = df['high'].rolling(10).max().iloc[-2]
        prev_low = df['low'].rolling(10).min().iloc[-2]

        vol = df['volume'].iloc[-1]
        vol_ma = df['volume'].rolling(20).mean().iloc[-1]

        adx = ind.get("adx", 0)

        signals = []
        score = 0

        if low < prev_low:
            score += 2
            signals.append("sweep lows")

        if high > prev_high:
            score += 2
            signals.append("sweep highs")

        body = abs(close - df['open'].iloc[-1])
        candle_range = high - low

        if candle_range > 0:
            rejection_ratio = body / candle_range
            if rejection_ratio < 0.4:
                score += 2
                signals.append("absorption")

        if vol > vol_ma * 1.3:
            score += 2
            signals.append("volume confirm")

        if adx > 20:
            score += 1
            signals.append("trend context")

        if score >= 5:
            return score, signals

        return None

    except Exception as e:
        log_warn(f"intent detection error: {e}")
        return None

def detect_accumulation(df, ind):
    try:
        body = abs(df['close'] - df['open'])
        atr = ind.get('atr', 0)
        if atr == 0:
            return False
        avg_body = body.rolling(14).mean().iloc[-1]
        small_candles = avg_body < 0.3 * atr

        atr_series = df['high'] - df['low']
        atr_14 = atr_series.rolling(14).mean()
        if len(atr_14) > 20:
            atr_trend = atr_14.iloc[-1] < atr_14.iloc[-10]
        else:
            atr_trend = False

        high_20 = df['high'].iloc[-20:].max()
        low_20 = df['low'].iloc[-20:].min()
        range_width = (high_20 - low_20) / high_20
        is_range = range_width < 0.03

        adx = ind.get('adx', 0)
        low_adx = adx < 20

        wick_ratio = (df['high'] - df['low'] - body) / (df['high'] - df['low'] + 1e-12)
        high_wick_count = (wick_ratio > 0.6).tail(10).sum()

        accumulation = (small_candles or low_adx) and is_range and (atr_trend or high_wick_count >= 2)
        return accumulation
    except Exception as e:
        log_warn(f"detect_accumulation error: {e}")
        return False

def detect_fake_breakout(df, lookback=10, volume_mult=1.5):
    try:
        if len(df) < lookback + 3:
            return False

        highs = df['high'].astype(float)
        lows = df['low'].astype(float)
        close = df['close'].astype(float)
        volume = df['volume'].astype(float)

        recent_high = highs.iloc[-lookback-1:-1].max()
        recent_low = lows.iloc[-lookback-1:-1].min()
        current_high = highs.iloc[-1]
        current_low = lows.iloc[-1]
        current_close = close.iloc[-1]

        avg_volume = volume.iloc[-lookback-1:-1].mean()
        current_volume = volume.iloc[-1]

        if current_high > recent_high:
            if current_close < recent_high:
                return True
            if current_volume < avg_volume * volume_mult:
                return True
        if current_low < recent_low:
            if current_close > recent_low:
                return True
            if current_volume < avg_volume * volume_mult:
                return True
        return False
    except Exception as e:
        log_warn(f"detect_fake_breakout error: {e}")
        return False

def identify_stop_clusters(df, atr_mult=1.0):
    try:
        if len(df) < 30:
            return [], []
        atr = compute_indicators(df).get('atr', 0)
        tolerance = atr * atr_mult

        highs = df['high'].astype(float).values
        lows = df['low'].astype(float).values

        swing_highs = []
        swing_lows = []
        for i in range(2, len(highs)-2):
            if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]:
                swing_highs.append(highs[i])
            if lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]:
                swing_lows.append(lows[i])

        high_clusters = []
        used = [False] * len(swing_highs)
        for i in range(len(swing_highs)):
            if used[i]:
                continue
            cluster = [swing_highs[i]]
            used[i] = True
            for j in range(i+1, len(swing_highs)):
                if not used[j] and abs(swing_highs[i] - swing_highs[j]) < tolerance:
                    cluster.append(swing_highs[j])
                    used[j] = True
            if len(cluster) >= 2:
                high_clusters.append(np.mean(cluster))

        low_clusters = []
        used = [False] * len(swing_lows)
        for i in range(len(swing_lows)):
            if used[i]:
                continue
            cluster = [swing_lows[i]]
            used[i] = True
            for j in range(i+1, len(swing_lows)):
                if not used[j] and abs(swing_lows[i] - swing_lows[j]) < tolerance:
                    cluster.append(swing_lows[j])
                    used[j] = True
            if len(cluster) >= 2:
                low_clusters.append(np.mean(cluster))

        if len(swing_highs) > 0:
            high_clusters.append(swing_highs[-1])
        if len(swing_lows) > 0:
            low_clusters.append(swing_lows[-1])

        return high_clusters, low_clusters
    except Exception as e:
        log_warn(f"identify_stop_clusters error: {e}")
        return [], []

def detect_support_zone(df, zone_type="demand"):
    try:
        zones = detect_supply_demand_zones(df, min_touches=2)
        price = float(df['close'].iloc[-1])
        if zone_type == "demand":
            zones_list = zones['demand_zones']
        else:
            zones_list = zones['supply_zones']

        for z in zones_list:
            dist = abs(price - z['price']) / price
            if dist <= 0.005:
                return z['price'], dist
        return None, None
    except Exception as e:
        log_warn(f"detect_support_zone error: {e}")
        return None, None

def detect_zone_hunter(df, ind):
    try:
        price = float(df['close'].iloc[-1])
        high = float(df['high'].iloc[-1])
        low = float(df['low'].iloc[-1])
        prev_close = float(df['close'].iloc[-2])

        demand_zone, dist_demand = detect_support_zone(df, "demand")
        supply_zone, dist_supply = detect_support_zone(df, "supply")

        sweep_low = low < df['low'].rolling(15).min().iloc[-2]
        sweep_high = high > df['high'].rolling(15).max().iloc[-2]

        body = abs(df['close'].iloc[-1] - df['open'].iloc[-1])
        rng = high - low
        if rng > 0:
            wick_up = high - max(df['close'].iloc[-1], df['open'].iloc[-1])
            wick_down = min(df['close'].iloc[-1], df['open'].iloc[-1]) - low
            strong_rejection_buy = wick_down > body * 1.5 and df['close'].iloc[-1] > df['open'].iloc[-1]
            strong_rejection_sell = wick_up > body * 1.5 and df['close'].iloc[-1] < df['open'].iloc[-1]
        else:
            strong_rejection_buy = strong_rejection_sell = False

        structure = detect_structure(df)
        bos = detect_bos_choch(df)
        structure_buy = structure.get("choch_up", False) or bos.get("bos_up", False)
        structure_sell = structure.get("choch_down", False) or bos.get("bos_down", False)

        fake_breakout = detect_fake_breakout(df)

        high_clusters, low_clusters = identify_stop_clusters(df)
        has_stop_cluster_buy = any(abs(price - level) / price < 0.005 for level in low_clusters)
        has_stop_cluster_sell = any(abs(price - level) / price < 0.005 for level in high_clusters)

        if demand_zone is not None and sweep_low and strong_rejection_buy and structure_buy and has_stop_cluster_buy:
            reasons = ["support_zone", "sweep_low", "bullish_rejection", "structure_up", "stop_cluster_buy"]
            if fake_breakout:
                reasons.append("fake_breakout_boost")
            score = 8.0 + (2 if fake_breakout else 0)
            return "buy", score, reasons, dist_demand

        if supply_zone is not None and sweep_high and strong_rejection_sell and structure_sell and has_stop_cluster_sell:
            reasons = ["resistance_zone", "sweep_high", "bearish_rejection", "structure_down", "stop_cluster_sell"]
            if fake_breakout:
                reasons.append("fake_breakout_boost")
            score = 8.0 + (2 if fake_breakout else 0)
            return "sell", score, reasons, dist_supply

        return None

    except Exception as e:
        log_warn(f"zone hunter error: {e}")
        return None

def candle_exhaustion(df):
    try:
        if len(df) < 2:
            return False
        last = df.iloc[-1]
        prev = df.iloc[-2]

        body = abs(last['close'] - last['open'])
        prev_body = abs(prev['close'] - prev['open'])

        slowdown = body < prev_body * 0.7

        lower_wick = min(last['close'], last['open']) - last['low']
        upper_wick = last['high'] - max(last['close'], last['open'])
        strong_rejection = (lower_wick > body * 1.5) or (upper_wick > body * 1.5)

        return slowdown and strong_rejection
    except Exception as e:
        log_warn(f"candle_exhaustion error: {e}")
        return False

def footprint_confirmation(flow):
    if not flow.get("ok"):
        return False
    if flow["delta"] < 0 and not flow.get("big_sellers", False):
        return True
    if flow.get("big_buyers", False):
        return True
    return False

def smart_bottom_top_detector(df, symbol, side):
    try:
        price = float(df['close'].iloc[-1])
        flow = compute_flow_pressure(ex, symbol, price)
        exhaustion = candle_exhaustion(df)
        footprint = footprint_confirmation(flow)

        if exhaustion and footprint:
            return "STRONG"
        elif exhaustion or footprint:
            return "WEAK"
        else:
            return "NONE"
    except Exception as e:
        log_warn(f"smart_bottom_top_detector error: {e}")
        return "NONE"

def exhaustion_rejection(df, side):
    exhaustion = candle_exhaustion(df)
    rejection = detect_rejection(df)
    if side == "BUY":
        return exhaustion and (rejection == "buy")
    else:
        return exhaustion and (rejection == "sell")

def smart_entry_boost(df, side, base_score):
    boost = 0.0
    reason = None

    if exhaustion_rejection(df, side):
        boost = 2.5
        reason = "exhaustion_rejection"
    else:
        disp = detect_displacement(df)
        if disp.get("displaced"):
            boost = 2.0
            reason = "displacement"

    return base_score + boost, boost, reason

def council_votes_pro_enhanced(df, symbol=None, skip_heavy=False):
    try:
        ind = compute_indicators(df)
        rsi_ctx = rsi_ma_context(df)
        gz = golden_zone_check_pro(df, ind)
        if skip_heavy:
            flowx = {"ok": False, "big_buyers": False, "big_sellers": False}
            heatmap = {"side": None, "score": 0}
        else:
            px = get_ticker_safe(symbol) if symbol else get_ticker_safe(SYMBOL)
            flowx = compute_flow_pressure(ex, symbol or SYMBOL, px) if px else {"ok":False}
            heatmap = liquidity_heatmap_engine(symbol or SYMBOL)
        liq_pools = detect_liquidity_pools(df) if ENABLE_LIQUIDITY_POOLS else {"ok":False}
        structure = detect_structure(df) if ENABLE_STRUCTURE else {"ok":False}
        displacement = detect_displacement(df) if ENABLE_DISPLACEMENT else {"ok":False}
        ob = detect_order_block_pro(df)
        fvg = detect_fvg_pro(df)
        trap = detect_whale_trap(df) if ENABLE_WHALE_TRAP else None
        magnet = liquidity_magnet_engine_enhanced(df)
        gravity = liquidity_gravity_engine(df)
        clusters = detect_liquidity_clusters_advanced(df)
        stop_hunt = detect_stop_hunt(df) if ENABLE_STOP_HUNT else None
        void = detect_liquidity_void(df) if ENABLE_LIQUIDITY_VOID else {"ok":False}
        regime = detect_market_regime_advanced(df)
        patterns = detect_candle_patterns(df)
        bos_choch = detect_bos_choch(df)
        vwap_ctx = vwap_context(df)
        volume_explosion = detect_volume_explosion(df)
        mss = detect_mss(df)
        liquidity_map = detect_liquidity_map(df)
        sweep = liquidity_sweep(df)
        reversal = liquidity_reversal_engine(df)
        expansion = volatility_expansion_engine(df)
        sd_engine = supply_demand_engine(df, flowx if not skip_heavy else None)
        liquidity_sweep_signal = liq_pools.get("swept_low") or liq_pools.get("swept_high") if liq_pools.get("ok") else False
        order_block = ob is not None
        displacement_signal = displacement.get("displaced") if displacement.get("ok") else False
        structure_break = structure.get("choch_up") or structure.get("choch_down") if structure.get("ok") else False
        flow_buy = flowx.get("big_buyers", False)
        flow_sell = flowx.get("big_sellers", False)
        adx_trending = False
        if ind.get('adx', 0) >= 18:
            if ind.get('plus_di', 0) > ind.get('minus_di', 0):
                adx_trending = True
            if ind.get('minus_di', 0) > ind.get('plus_di', 0):
                adx_trending = True
        indicators_pool = {
            "price": vwap_ctx["price"],
            "vwap": vwap_ctx["vwap"],
            "rsi": ind.get('rsi', 50),
            "adx": ind.get('adx', 0),
            "di_plus": ind.get('plus_di', 0),
            "di_minus": ind.get('minus_di', 0),
            "atr": ind.get('atr', 0),
            "volume_spike": volume_explosion,
            "liquidity_sweep": liquidity_sweep_signal,
            "order_block": order_block,
            "displacement": displacement_signal,
            "structure_break": structure_break,
            "flow_buy": flow_buy,
            "flow_sell": flow_sell,
            "adx_trending": adx_trending,
            "price_above_vwap": vwap_ctx["buy_context"],
            "price_below_vwap": vwap_ctx["sell_context"],
            "regime": regime,
            "candle_patterns": patterns,
            "liq_pools": liq_pools,
            "structure": structure,
            "displacement": displacement,
            "ob": ob,
            "fvg": fvg,
            "whale_trap": trap,
            "stop_hunt": stop_hunt,
            "magnet": magnet,
            "gravity": gravity,
            "clusters": clusters,
            "void": void,
            "bos_choch": bos_choch,
            "rsi_ctx": rsi_ctx,
            "mss": mss,
            "gz": gz,
            "liquidity_map": liquidity_map,
            "heatmap": heatmap,
            "reversal": reversal,
            "expansion": expansion,
            "sd_engine": sd_engine
        }
        votes_b = 0
        votes_s = 0
        score_b = 0.0
        score_s = 0.0
        if liquidity_sweep_signal:
            if liq_pools.get("swept_low"):
                votes_b += 2
                score_b += 2.5
            if liq_pools.get("swept_high"):
                votes_s += 2
                score_s += 2.5
        if order_block:
            if ob and ob["type"] == "bullish":
                bonus = 1 if ob.get("fibonacci") else 0
                votes_b += 3 + bonus
                score_b += 3.0 + bonus
            elif ob and ob["type"] == "bearish":
                bonus = 1 if ob.get("fibonacci") else 0
                votes_s += 3 + bonus
                score_s += 3.0 + bonus
        if displacement_signal:
            if displacement.get("direction") == "up":
                votes_b += 2
                score_b += 2.0
            else:
                votes_s += 2
                score_s += 2.0
        if structure_break:
            if structure.get("choch_up"):
                votes_b += 2
                score_b += 2.0
            if structure.get("choch_down"):
                votes_s += 2
                score_s += 2.0
        if vwap_ctx["buy_context"]:
            votes_b += 1
            score_b += 1.0
        if vwap_ctx["sell_context"]:
            votes_s += 1
            score_s += 1.0
        if ind.get("rsi", 50) > 55:
            votes_b += 1
            score_b += 1.0
        if ind.get("rsi", 50) < 45:
            votes_s += 1
            score_s += 1.0
        if adx_trending:
            if ind.get("plus_di", 0) > ind.get("minus_di", 0):
                votes_b += 1
                score_b += 1.0
            else:
                votes_s += 1
                score_s += 1.0
        if volume_explosion:
            votes_b += 1
            votes_s += 1
        if flow_buy:
            votes_b += 2
            score_b += 2.0
        if flow_sell:
            votes_s += 2
            score_s += 2.0
        if mss == "bullish":
            votes_b += 3
            score_b += 3.0
        if mss == "bearish":
            votes_s += 3
            score_s += 3.0
        if liquidity_map.get("ok"):
            if liquidity_map.get("bullish_sweep"):
                votes_b += 3
                score_b += 3.0
            if liquidity_map.get("bearish_sweep"):
                votes_s += 3
                score_s += 3.0
            if liquidity_map.get("near_low"):
                votes_b += 1
                score_b += 1.0
            if liquidity_map.get("near_high"):
                votes_s += 1
                score_s += 1.0
        if magnet.get("score") > 0:
            if magnet.get("side") == "BUY":
                votes_b += magnet["score"]
                score_b += magnet["score"]
            elif magnet.get("side") == "SELL":
                votes_s += magnet["score"]
                score_s += magnet["score"]
        if gravity.get("side") == "BUY":
            votes_b += gravity["score"]
            score_b += gravity["score"]
        if gravity.get("side") == "SELL":
            votes_s += gravity["score"]
            score_s += gravity["score"]
        if sweep == "BUY":
            votes_b += 2
            score_b += 2.0
        elif sweep == "SELL":
            votes_s += 2
            score_s += 2.0
        if heatmap["side"] == "BUY":
            votes_b += 1
            score_b += heatmap["score"]
        elif heatmap["side"] == "SELL":
            votes_s += 1
            score_s += heatmap["score"]
        if reversal["side"] == "BUY":
            votes_b += 1
            score_b += reversal["score"]
        elif reversal["side"] == "SELL":
            votes_s += 1
            score_s += reversal["score"]
        if expansion["side"] == "BUY":
            votes_b += 1
            score_b += expansion["score"]
        elif expansion["side"] == "SELL":
            votes_s += 1
            score_s += expansion["score"]
        if sd_engine["side"] == "BUY":
            votes_b += 2
            score_b += sd_engine["score"]
        elif sd_engine["side"] == "SELL":
            votes_s += 2
            score_s += sd_engine["score"]
        return {
            "b": votes_b, "s": votes_s,
            "score_b": score_b, "score_s": score_s,
            "logs": [],
            "ind": indicators_pool,
            "institutional_trigger": False,
            "institutional_trigger_count": 0
        }
    except Exception as e:
        log_warn(f"council_votes_pro_enhanced error: {e}")
        return {
            "b": 0, "s": 0,
            "score_b": 0.0, "score_s": 0.0,
            "logs": [f"Error: {e}"],
            "ind": {
                "price": 0, "vwap": 0, "rsi": 50, "adx": 0,
                "di_plus": 0, "di_minus": 0, "atr": 0,
                "volume_spike": False, "liquidity_sweep": False,
                "order_block": False, "displacement": False,
                "structure_break": False, "flow_buy": False,
                "flow_sell": False, "adx_trending": False,
                "price_above_vwap": False, "price_below_vwap": False,
                "regime": "range", "candle_patterns": {},
                "liq_pools": {}, "structure": {}, "displacement": {},
                "ob": None, "fvg": None, "whale_trap": None,
                "stop_hunt": None, "magnet": {}, "gravity": {},
                "clusters": {}, "void": {}, "bos_choch": {}, "rsi_ctx": {},
                "mss": None, "gz": {}, "liquidity_map": {}, "heatmap": {},
                "reversal": {}, "expansion": {}, "sd_engine": {}
            },
            "institutional_trigger": False,
            "institutional_trigger_count": 0
        }

def compute_heat_score_pro(ind):
    breakdown = {}
    total = 0
    structure_score = 0
    bos = ind.get("bos_choch", {})
    if bos.get("bos_up") or bos.get("bos_down"):
        structure_score += 3
    mss = ind.get("mss")
    if mss in ("bullish", "bearish"):
        structure_score += 4
    if ind.get("mss") == "bullish" or ind.get("mss") == "bearish":
        structure_score += 4
    breakdown['Structure'] = structure_score
    total += structure_score
    liquidity_score = 0
    liq_map = ind.get("liquidity_map", {})
    if liq_map.get("ok"):
        if liq_map.get("bullish_sweep") or liq_map.get("bearish_sweep"):
            liquidity_score += 3
        if liq_map.get("near_high") or liq_map.get("near_low"):
            liquidity_score += 1
    if ind.get("liquidity_sweep"):
        liquidity_score += 2
    if ind.get("stop_hunt"):
        liquidity_score += 3
    clusters = ind.get("clusters", {})
    if clusters.get("near_high") or clusters.get("near_low"):
        liquidity_score += 1
    magnet = ind.get("magnet", {})
    if magnet.get("score", 0) > 0:
        liquidity_score += magnet["score"]
    gravity = ind.get("gravity", {})
    if gravity.get("score", 0) > 0:
        liquidity_score += gravity["score"]
    heatmap = ind.get("heatmap", {})
    if heatmap.get("score", 0) > 0:
        liquidity_score += heatmap["score"]
    reversal = ind.get("reversal", {})
    if reversal.get("score", 0) > 0:
        liquidity_score += reversal["score"]
    expansion = ind.get("expansion", {})
    if expansion.get("score", 0) > 0:
        liquidity_score += expansion["score"]
    sd = ind.get("sd_engine", {})
    if sd.get("score", 0) > 0:
        liquidity_score += sd["score"]
    liquidity_score = min(liquidity_score, 6)
    breakdown['Liquidity'] = liquidity_score
    total += liquidity_score
    vwap_score = 0
    if ind.get("price_above_vwap") or ind.get("price_below_vwap"):
        vwap_score += 1
    breakdown['VWAP'] = vwap_score
    total += vwap_score
    momentum_score = 0
    rsi = ind.get("rsi", 50)
    rsi_ctx = ind.get("rsi_ctx", {})
    trendZ = rsi_ctx.get("trendZ", "none")
    if trendZ in ("bull", "bear"):
        momentum_score += 3
    elif rsi > 55 or rsi < 45:
        momentum_score += 1
    patterns = ind.get("candle_patterns", {})
    if patterns.get("pin_bar_bottom") or patterns.get("pin_bar_top"):
        momentum_score += 1
    if patterns.get("bullish_engulfing") or patterns.get("bearish_engulfing"):
        momentum_score += 2
    breakdown['Momentum'] = momentum_score
    total += momentum_score
    adx = ind.get("adx", 0)
    adx_score = 0
    if adx >= 18:
        if adx < 25:
            adx_score = 1
        elif adx < 35:
            adx_score = 2
        else:
            adx_score = 3
    breakdown['ADX'] = adx_score
    total += adx_score
    volume_score = 0
    if ind.get("volume_spike"):
        volume_score += 2
    if ind.get("flow_buy") or ind.get("flow_sell"):
        volume_score += 2
    breakdown['Volume'] = volume_score
    total += volume_score
    gz = ind.get("gz", {})
    if gz.get("ok"):
        golden_bonus = int(gz["score"] / 2)
        breakdown['GoldenZone'] = golden_bonus
        total += golden_bonus
    return total, breakdown

def compute_signal_score(ind):
    score = 0
    if ind.get("liquidity_sweep"):
        score += 3
    if ind.get("order_block"):
        score += 3
    if ind.get("structure_break"):
        score += 2
    if ind.get("displacement"):
        score += 2
    if ind.get("price_above_vwap") or ind.get("price_below_vwap"):
        score += 1
    rsi = ind.get("rsi", 50)
    if rsi > 55 or rsi < 45:
        score += 1
    if ind.get("adx_trending"):
        score += 1
    if ind.get("volume_spike"):
        score += 1
    if ind.get("flow_buy") or ind.get("flow_sell"):
        score += 2
    return score

def classify_signal_strength(score):
    if score >= 12:
        return "ULTRA"
    elif score >= 11:
        return "VERY_STRONG"
    elif score >= 10:
        return "STRONG"
    elif score >= 9:
        return "GOOD"
    elif score >= 8:
        return "MEDIUM"
    else:
        return None

def check_vwap_context(ind):
    price = ind.get("price")
    vwap = ind.get("vwap")
    adx = ind.get("adx", 0)
    if price is None or vwap is None:
        return False
    distance = abs(price - vwap) / price
    if adx > 30:
        max_dist = 0.05
    elif adx > 22:
        max_dist = 0.04
    else:
        max_dist = 0.025
    return distance <= max_dist

def get_market_regime_from_adx(adx):
    if adx > 35:
        return "strong_trend"
    elif adx > 22:
        return "trend"
    else:
        return "range"

def classify_trend(adx, plus_di, minus_di):
    spread = abs(plus_di - minus_di)
    if adx >= 35 and spread > 10:
        return "strong"
    elif adx >= 22 and spread > 6:
        return "medium"
    else:
        return "weak"

def build_tp_plan(price, side, trend, atr):
    return {"tp1": 0.5, "tp_levels": 1}

def partial_close(ratio):
    return False

def close_all():
    strict_close_position("trailing_stop")

def update_runner_trail(price, side, atr):
    pass

def manage_runner(price, ind):
    pass

def manage_trade_dynamic(price, ind):
    pass

def _normalize_side(pos):
    side = pos.get("side") or pos.get("positionSide") or ""
    if side: return side.upper()
    qty = float(pos.get("contracts") or pos.get("positionAmt") or pos.get("size") or 0)
    return "LONG" if qty > 0 else ("SHORT" if qty < 0 else "")

def fetch_live_position(exchange, symbol: str):
    try:
        if hasattr(exchange, "fetch_positions"):
            arr = exchange.fetch_positions([symbol])
            for p in arr or []:
                sym = p.get("symbol") or p.get("info", {}).get("symbol")
                if sym and symbol.replace(":","") in sym.replace(":",""):
                    side = normalize_side(p.get("side") or p.get("positionSide"))
                    qty = abs(float(p.get("contracts") or p.get("positionAmt") or p.get("info",{}).get("size",0) or 0))
                    if qty > 0:
                        entry = float(p.get("entryPrice") or p.get("info",{}).get("entryPrice") or 0.0)
                        lev = float(p.get("leverage") or p.get("info",{}).get("leverage") or 0.0)
                        unr = float(p.get("unrealizedPnl") or 0.0)
                        return {"ok": True, "side": side, "qty": qty, "entry": entry, "unrealized": unr, "leverage": lev, "raw": p}
        if hasattr(exchange, "fetch_position"):
            p = exchange.fetch_position(symbol)
            side = normalize_side(p.get("side") or p.get("positionSide"))
            qty = abs(float(p.get("size") or 0))
            if qty > 0:
                entry = float(p.get("entryPrice") or 0.0)
                lev   = float(p.get("leverage") or 0.0)
                unr   = float(p.get("unrealizedPnl") or 0.0)
                return {"ok": True, "side": side, "qty": qty, "entry": entry, "unrealized": unr, "leverage": lev, "raw": p}
    except Exception as e:
        log_warn(f"fetch_live_position error: {e}")
    return {"ok": False, "why": "no_open_position"}

def resume_open_position(exchange, symbol: str, state: dict) -> dict:
    if not RESUME_ON_RESTART:
        log_i("resume disabled"); return state
    live = fetch_live_position(exchange, symbol)
    if not live.get("ok"):
        log_i("no live position to resume"); return state
    ts = int(time.time())
    prev = load_state()
    if prev.get("ts") and (ts - int(prev["ts"])) > RESUME_LOOKBACK_SECS:
        log_warn("found old local state — will override with exchange live snapshot")
    state.update({
        "in_position": True,
        "side": live["side"].lower(),
        "entry_price": live["entry"],
        "position_qty": live["qty"],
        "remaining_qty": live["qty"],
        "leverage": live.get("leverage") or state.get("leverage", LEVERAGE),
        "tp1_done": prev.get("tp1_done", False),
        "trail_activated": prev.get("trail_activated", False),
        "trail_stop": prev.get("trail_stop", None),
        "trail_multiplier": prev.get("trail_multiplier", TRAIL_ATR_MULT),
        "highest_profit_pct": prev.get("highest_profit_pct", 0.0),
        "opened_at": prev.get("opened_at", ts),
        "cooldown_until": prev.get("cooldown_until"),
        "daily_trades": prev.get("daily_trades", 0),
        "last_trade_day": prev.get("last_trade_day"),
        "consecutive_losses": prev.get("consecutive_losses", 0),
        "signal_strength": prev.get("signal_strength", "MEDIUM"),
        "trend_strength_entry": prev.get("trend_strength_entry", "weak"),
        "entry_score": prev.get("entry_score", 0),
        "heat_score": prev.get("heat_score", 0),
        "heat_breakdown": prev.get("heat_breakdown", {}),
        "current_market_regime": prev.get("current_market_regime", "range"),
        "supply_demand_trigger": prev.get("supply_demand_trigger", False),
        "trend": prev.get("trend"),
    })
    save_state(state)
    log_g(f"RESUME: {state['side']} qty={state['position_qty']:.6f} @ {state['entry_price']:.6f} lev={state['leverage']}x source={state.get('source','-')} signal={state['signal_strength']}")
    return state

def setup_file_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if not any(isinstance(h, RotatingFileHandler) and getattr(h, "baseFilename", "").endswith("bot.log")
               for h in logger.handlers):
        fh = RotatingFileHandler("bot.log", maxBytes=5_000_000, backupCount=7, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
        logger.addHandler(fh)
    logging.getLogger('werkzeug').setLevel(logging.ERROR)
    log_i("log rotation ready")

setup_file_logging()

# =================== EXCHANGE SETUP ===================
def make_ex():
    return ccxt.bingx({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "enableRateLimit": True,
        "timeout": 20000,
        "options": {"defaultType": "swap"}
    })

ex = make_ex()
MARKET = {}
AMT_PREC = 0
LOT_STEP = None
LOT_MIN  = None

def load_market_specs(symbol=None):
    global MARKET, AMT_PREC, LOT_STEP, LOT_MIN
    if symbol is None:
        symbol = SYMBOL
    try:
        ex.load_markets()
        MARKET = ex.markets.get(symbol, {})
        AMT_PREC = int((MARKET.get("precision", {}) or {}).get("amount", 0) or 0)
        LOT_STEP = (MARKET.get("limits", {}) or {}).get("amount", {}).get("step", None)
        LOT_MIN  = (MARKET.get("limits", {}) or {}).get("amount", {}).get("min",  None)
        log_i(f"market specs for {symbol}: precision={AMT_PREC}, step={LOT_STEP}, min={LOT_MIN}")
    except Exception as e:
        log_warn(f"load_market_specs: {e}")

def ensure_leverage_mode(symbol=None):
    if symbol is None:
        symbol = SYMBOL
    try:
        try:
            # ✅ FIXED: Hedge Mode side handling (use "ALL" instead of "BOTH")
            ex.set_leverage(LEVERAGE, symbol, params={"side": "ALL"})
            log_g(f"leverage set: {LEVERAGE}x for {symbol}")
        except Exception as e:
            log_warn(f"set_leverage warn: {e}")
        log_i(f"position mode: {POSITION_MODE}")
    except Exception as e:
        log_warn(f"ensure_leverage_mode: {e}")

def update_symbol(new_symbol):
    global SYMBOL
    if has_open_position():
        log_warn("رفض تغيير الرمز — صفقة مفتوحة")
        return False
    if SYMBOL == new_symbol:
        return True
    log_i(f"🔄 switching trading symbol from {SYMBOL} to {new_symbol}")
    SYMBOL = new_symbol
    load_market_specs()
    ensure_leverage_mode()
    return True

_consec_err = 0
last_loop_ts = time.time()

def _round_amt(q):
    if q is None: return 0.0
    try:
        d = Decimal(str(q))
        if LOT_STEP and isinstance(LOT_STEP,(int,float)) and LOT_STEP>0:
            step = Decimal(str(LOT_STEP))
            d = (d/step).to_integral_value(rounding=ROUND_DOWN)*step
        prec = int(AMT_PREC) if AMT_PREC and AMT_PREC>=0 else 0
        d = d.quantize(Decimal(1).scaleb(-prec), rounding=ROUND_DOWN)
        if LOT_MIN and isinstance(LOT_MIN,(int,float)) and LOT_MIN>0 and d < Decimal(str(LOT_MIN)):
            d = Decimal(str(LOT_MIN))
        return float(d)
    except (InvalidOperation, ValueError, TypeError):
        return max(0.0, float(q))

def safe_qty(q):
    q = _round_amt(q)
    if q<=0: log_warn(f"qty invalid after normalize → {q}")
    return q

def fmt(v, d=6, na="—"):
    try:
        if v is None or (isinstance(v,float) and (math.isnan(v) or math.isinf(v))): return na
        return f"{float(v):.{d}f}"
    except Exception:
        return na

def with_retry(fn, tries=3, base_wait=0.4):
    global _consec_err
    for i in range(tries):
        try:
            r = fn()
            _consec_err = 0
            return r
        except Exception:
            _consec_err += 1
            if i == tries-1: raise
            time.sleep(base_wait*(2**i) + random.random()*0.25)

def price_now(symbol=None):
    if symbol is None:
        symbol = SYMBOL
    return get_ticker_safe(symbol)

def balance_usdt():
    if PAPER_MODE:
        return paper["balance"]
    if not MODE_LIVE: return 100.0
    return get_real_balance(ex)

def orderbook_spread_bps(symbol=None):
    if symbol is None:
        symbol = SYMBOL
    try:
        ob = get_orderbook_safe(symbol, limit=5)
        bid = ob["bids"][0][0] if ob["bids"] else None
        ask = ob["asks"][0][0] if ob["asks"] else None
        if not (bid and ask): return None
        mid = (bid+ask)/2.0
        return ((ask-bid)/mid)*10000.0
    except Exception:
        return None

def _interval_seconds(iv: str) -> int:
    iv=(iv or "").lower().strip()
    if iv.endswith("m"): return int(float(iv[:-1]))*60
    if iv.endswith("h"): return int(float(iv[:-1]))*3600
    if iv.endswith("d"): return int(float(iv[:-1]))*86400
    return 15*60

def time_to_candle_close(df: pd.DataFrame) -> int:
    tf = _interval_seconds(INTERVAL)
    if len(df) == 0: return tf
    cur_start_ms = int(df["time"].iloc[-1])
    now_ms = int(time.time()*1000)
    next_close_ms = cur_start_ms + tf*1000
    while next_close_ms <= now_ms:
        next_close_ms += tf*1000
    left = max(0, next_close_ms - now_ms)
    return int(left/1000)

def fmt_walls(walls):
    return ", ".join([f"{p:.6f}@{q:.0f}" for p, q in walls]) if walls else "-"

def bookmap_snapshot(symbol=None, depth=BOOKMAP_DEPTH):
    try:
        ob = get_orderbook_safe(symbol, depth)
        bids = ob.get("bids", [])[:depth]; asks = ob.get("asks", [])[:depth]
        if not bids or not asks:
            return {"ok": False, "why": "empty"}
        b_sizes = np.array([b[1] for b in bids]); b_prices = np.array([b[0] for b in bids])
        a_sizes = np.array([a[1] for a in asks]); a_prices = np.array([a[0] for a in asks])
        b_idx = b_sizes.argsort()[::-1][:BOOKMAP_TOPWALLS]
        a_idx = a_sizes.argsort()[::-1][:BOOKMAP_TOPWALLS]
        buy_walls = [(float(b_prices[i]), float(b_sizes[i])) for i in b_idx]
        sell_walls = [(float(a_prices[i]), float(a_sizes[i])) for i in a_idx]
        imb = b_sizes.sum() / max(a_sizes.sum(), 1e-12)
        return {"ok": True, "buy_walls": buy_walls, "sell_walls": sell_walls, "imbalance": float(imb)}
    except Exception as e:
        return {"ok": False, "why": f"{e}"}

def compute_flow_metrics(df):
    try:
        if len(df) < max(30, FLOW_WINDOW+2):
            return {"ok": False, "why": "short_df"}
        close = df["close"].astype(float).copy()
        vol = df["volume"].astype(float).copy()
        up_mask = close.diff().fillna(0) > 0
        up_vol = (vol * up_mask).astype(float)
        dn_vol = (vol * (~up_mask)).astype(float)
        delta = up_vol - dn_vol
        cvd = delta.cumsum()
        cvd_ma = cvd.rolling(CVD_SMOOTH).mean()
        wnd = delta.tail(FLOW_WINDOW)
        mu = float(wnd.mean()); sd = float(wnd.std() or 1e-12)
        z = float((wnd.iloc[-1] - mu) / sd)
        trend = "up" if (cvd_ma.iloc[-1] - cvd_ma.iloc[-min(CVD_SMOOTH, len(cvd_ma))]) >= 0 else "down"
        return {"ok": True, "delta_last": float(delta.iloc[-1]), "delta_mean": mu, "delta_z": z,
                "cvd_last": float(cvd.iloc[-1]), "cvd_trend": trend, "spike": abs(z) >= FLOW_SPIKE_Z}
    except Exception as e:
        return {"ok": False, "why": str(e)}

def emit_snapshots(symbol, df, balance_fn=None, pnl_fn=None, precomputed_heat=None):
    try:
        bm = bookmap_snapshot(symbol)
        flow = compute_flow_metrics(df)
        cv = council_votes_pro_enhanced(df, symbol)
        if precomputed_heat is not None:
            heat_score, heat_breakdown = precomputed_heat
        else:
            heat_score, heat_breakdown = compute_heat_score_pro(cv["ind"])
        mode = decide_strategy_mode(df)
        gz = cv["ind"]["gz"]
        vwap_ctx = vwap_context(df)
        bal = None; cpnl = None
        if callable(balance_fn):
            try: bal = balance_fn()
            except: bal = None
        if callable(pnl_fn):
            try: cpnl = pnl_fn()
            except: cpnl = None
        if bm.get("ok"):
            imb_tag = "🟢" if bm["imbalance"]>=IMBALANCE_ALERT else ("🔴" if bm["imbalance"]<=1/IMBALANCE_ALERT else "⚖️")
            bm_note = f"Bookmap: {imb_tag} Imb={bm['imbalance']:.2f} | Buy[{fmt_walls(bm['buy_walls'])}] | Sell[{fmt_walls(bm['sell_walls'])}]"
        else:
            bm_note = f"Bookmap: N/A ({bm.get('why')})"
        if flow.get("ok"):
            dtag = "🟢Buy" if flow["delta_last"]>0 else ("🔴Sell" if flow["delta_last"]<0 else "⚖️Flat")
            spk = " ⚡Spike" if flow["spike"] else ""
            fl_note = f"Flow: {dtag} Δ={flow['delta_last']:.0f} z={flow['delta_z']:.2f}{spk} | CVD {'↗️' if flow['cvd_trend']=='up' else '↘️'} {flow['cvd_last']:.0f}"
        else:
            fl_note = f"Flow: N/A ({flow.get('why')})"
        side_hint = "BUY" if cv["b"]>=cv["s"] else "SELL"
        dash = (f"DASH → hint-{side_hint} | Council BUY({cv['b']},{cv['score_b']:.1f}) "
                f"SELL({cv['s']},{cv['score_s']:.1f}) | "
                f"RSI={cv['ind'].get('rsi',0):.1f} ADX={cv['ind'].get('adx',0):.1f} "
                f"DI={cv['ind'].get('di_spread',0):.1f} EVX={cv['ind'].get('evx',1.0):.2f}")
        strat_icon = "⚡" if mode["mode"]=="scalp" else "📈" if mode["mode"]=="trend" else "ℹ️"
        strat = f"Strategy: {strat_icon} {mode['mode'].upper()}"
        bal_note = f"Balance={bal:.2f}" if bal is not None else ""
        pnl_note = f"CompoundPnL={cpnl:.6f}" if cpnl is not None else ""
        wallet = (" | ".join(x for x in [bal_note, pnl_note] if x)) or ""
        gz_note = ""
        if gz and gz.get("ok"):
            gz_note = f" | 🟡 {gz['zone']['type']} s={gz['score']:.1f}"
        vwap_info = f"VWAP: {vwap_ctx['vwap']:.6f} | {'🟢' if vwap_ctx['buy_context'] else ('🔴' if vwap_ctx['sell_context'] else '⚪')} dist={vwap_ctx['distance_pct']*100:.3f}%"
        if LOG_ADDONS:
            log(f"🧱 {bm_note}")
            log(f"📦 {fl_note}")
            log(f"📊 {dash}{gz_note}")
            log(f"{strat}{(' | ' + wallet) if wallet else ''}")
            log(f"📈 {vwap_info}")
            log(f"🔥 HEAT SCORE = {heat_score} | {heat_breakdown}")
            gz_snap_note = ""
            if gz and gz.get("ok"):
                zone_type = gz["zone"]["type"]
                zone_score = gz["score"]
                gz_snap_note = f" | 🟡{zone_type} s={zone_score:.1f}"
            flow_z = flow['delta_z'] if flow and flow.get('ok') else 0.0
            bm_imb = bm['imbalance'] if bm and bm.get('ok') else 1.0
            log(f"🧠 SNAP | {side_hint} | votes={cv['b']}/{cv['s']} score={cv['score_b']:.1f}/{cv['score_s']:.1f} "
                f"| ADX={cv['ind'].get('adx',0):.1f} DI={cv['ind'].get('di_spread',0):.1f} | "
                f"z={flow_z:.2f} | imb={bm_imb:.2f}{gz_snap_note}")
            log("✅ ADDONS LIVE")
        return {"bm": bm, "flow": flow, "cv": cv, "mode": mode, "gz": gz, "vwap": vwap_ctx, "wallet": wallet, "heat_score": heat_score, "heat_breakdown": heat_breakdown}
    except Exception as e:
        log(f"🟨 AddonLog error: {e}")
        return {"bm": None, "flow": None, "cv": {"b":0,"s":0,"score_b":0.0,"score_s":0.0,"ind":{}},
                "mode": {"mode":"n/a"}, "gz": None, "vwap": None, "wallet": "", "heat_score": 0, "heat_breakdown": {}}

IND_CACHE = {}

def compute_indicators_cached(df):
    key = len(df)
    if key in IND_CACHE:
        return IND_CACHE[key]
    ind = compute_indicators(df)
    IND_CACHE[key] = ind
    return ind

def decide_strategy_mode(df, adx=None, di_plus=None, di_minus=None, rsi_ctx=None):
    if adx is None or di_plus is None or di_minus is None:
        ind = compute_indicators(df)
        adx = ind.get('adx', 0)
        di_plus = ind.get('plus_di', 0)
        di_minus = ind.get('minus_di', 0)
    if rsi_ctx is None:
        rsi_ctx = rsi_ma_context(df)
    di_spread = abs(di_plus - di_minus)
    strong_trend = (
        (adx >= ADX_TREND_MIN and di_spread >= DI_SPREAD_TREND) or
        (rsi_ctx["trendZ"] in ("bull", "bear") and not rsi_ctx["in_chop"])
    )
    mode = "trend" if strong_trend else "scalp"
    why = "adx/di_trend" if adx >= ADX_TREND_MIN else ("rsi_trendZ" if rsi_ctx["trendZ"] != "none" else "scalp_default")
    return {"mode": mode, "why": why}

def detect_htf_trend(symbol, interval='1h'):
    try:
        df = get_ohlcv_safe(symbol, interval=interval, limit=200)
        if len(df) < 200:
            return "bullish"
        ema200 = df["close"].ewm(span=200).mean().iloc[-1]
        close = df["close"].iloc[-1]
        return "bullish" if close > ema200 else "bearish"
    except Exception:
        return "bullish"

def dynamic_position_size(score):
    if score >= 9:
        return 0.7
    elif score >= 7:
        return 0.5
    elif score >= 6:
        return 0.3
    else:
        return 0.2

def calculate_position_size(symbol, price, score):
    balance = balance_usdt()
    if balance is None or balance <= 0:
        log_warn("Cannot calculate position size: invalid balance")
        return 0.0
    risk_pct = dynamic_position_size(score)
    margin = balance * risk_pct
    position_value = margin * LEVERAGE
    qty = position_value / price
    try:
        market = ex.market(symbol)
        min_qty = market['limits']['amount']['min']
        if qty < min_qty:
            log_warn(f"Calculated qty {qty:.6f} < min qty {min_qty:.6f}, using min qty")
            qty = min_qty
        qty = float(ex.amount_to_precision(symbol, qty))
    except Exception as e:
        log_warn(f"Could not adjust qty precision: {e}, using raw qty")
        qty = _round_amt(qty)
    return qty

def get_tp_config(regime, signal_strength):
    return {}

def trailing_multiplier(trend_strength, signal_strength):
    return TRAIL_ATR_MULT

EARLY_SETUP_MIN_SCORE = 3
ENABLE_ANTI_TRAP = True

def calculate_atr_simple(df, period=14):
    high = df['high'].astype(float)
    low = df['low'].astype(float)
    close = df['close'].astype(float)
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()
    return atr

def is_squeeze(df, period=50):
    if len(df) < period + 14:
        return False
    atr = calculate_atr_simple(df)
    atr_now = atr.iloc[-1]
    atr_avg = atr.iloc[-period:].mean()
    return atr_now < atr_avg * 0.7

def expansion_kick(df, lookback=20, mult=1.2):
    if len(df) < lookback + 1:
        return False
    ranges = df['high'] - df['low']
    avg_range = ranges.iloc[-lookback-1:-1].mean()
    current_range = ranges.iloc[-1]
    return current_range > avg_range * mult

def volume_shift(df, lookback=20, mult=1.5):
    if len(df) < lookback + 1:
        return False
    vol = df['volume'].astype(float)
    avg_vol = vol.iloc[-lookback-1:-1].mean()
    current_vol = vol.iloc[-1]
    return current_vol > avg_vol * mult

def range_break(df, window=20):
    if len(df) < window + 1:
        return False
    prev_high = df['high'].iloc[-window-1:-1].max()
    prev_low = df['low'].iloc[-window-1:-1].min()
    close = df['close'].iloc[-1]
    return close > prev_high or close < prev_low

def early_setup_score(df):
    score = 0
    if expansion_kick(df):
        score += 2
    if volume_shift(df):
        score += 2
    if range_break(df):
        score += 2
    if is_squeeze(df):
        score += 1
    return score

def too_many_wicks(df, lookback=5, threshold=0.6):
    if len(df) < lookback:
        return False
    bodies = abs(df['close'] - df['open'])
    ranges = df['high'] - df['low']
    wick_ratio = (ranges - bodies) / ranges.replace(0, 1e-12)
    return wick_ratio.tail(lookback).mean() > threshold

def fake_sweeps(df, lookback=5, min_sweeps=2):
    sweeps = 0
    for i in range(-lookback, 0):
        if i-1 < -len(df):
            continue
        high = df['high'].iloc[i]
        prev_high = df['high'].iloc[i-1]
        close = df['close'].iloc[i]
        if high > prev_high and close < prev_high:
            sweeps += 1
    return sweeps >= min_sweeps

def weak_volume(df, lookback=20):
    if len(df) < lookback + 1:
        return False
    vol = df['volume'].astype(float)
    avg_vol = vol.iloc[-lookback-1:-1].mean()
    return vol.iloc[-1] < avg_vol

def in_middle(df, window=20, tolerance=0.2):
    if len(df) < window:
        return False
    high = df['high'].iloc[-window:].max()
    low = df['low'].iloc[-window:].min()
    mid = (high + low) / 2
    price = df['close'].iloc[-1]
    return abs(price - mid) / mid < tolerance

def is_real_accumulation(df):
    if too_many_wicks(df):
        return False
    if fake_sweeps(df):
        return False
    if weak_volume(df):
        return False
    if in_middle(df):
        return False
    return True

def pre_move_score(df):
    if len(df) < 30:
        return 0
    score = 0
    ranges = df['high'] - df['low']
    if ranges.iloc[-5:].mean() < ranges.iloc[-20:-5].mean() * 0.7:
        score += 2
    vol = df['volume'].astype(float)
    if vol.iloc[-5:].mean() > vol.iloc[-20:-5].mean() * 1.2:
        score += 2
    high = df['high'].iloc[-20:].max()
    low = df['low'].iloc[-20:].min()
    price = df['close'].iloc[-1]
    if abs(price - high) / price < 0.01 or abs(price - low) / price < 0.01:
        score += 2
    return score

def trap_score(df):
    if len(df) < 20:
        return 0
    score = 0
    last = df.iloc[-1]
    prev = df.iloc[-2]
    high = df['high'].iloc[-20:].max()
    low = df['low'].iloc[-20:].min()
    if prev['close'] > high and last['close'] < high:
        score += 2
    if prev['close'] < low and last['close'] > low:
        score += 2
    wick_up = last['high'] - max(last['open'], last['close'])
    wick_down = min(last['open'], last['close']) - last['low']
    body = abs(last['close'] - last['open'])
    if wick_up > body * 2:
        score += 2
    if wick_down > body * 2:
        score += 2
    if abs(last['close'] - prev['close']) < body * 0.5:
        score += 1
    return score

def smart_money_score(df):
    if len(df) < 50:
        return 0
    score = 0
    close = df['close'].iloc[-1]
    high_zone = df['high'].iloc[-30:].max()
    low_zone = df['low'].iloc[-30:].min()
    if abs(close - high_zone) / close < 0.01:
        score += 2
    if abs(close - low_zone) / close < 0.01:
        score += 2
    recent_high = df['high'].iloc[-10:].max()
    recent_low = df['low'].iloc[-10:].min()
    if abs(close - recent_high) / close < 0.008:
        score += 2
    if abs(close - recent_low) / close < 0.008:
        score += 2
    last = df.iloc[-1]
    prev = df.iloc[-2]
    if last['high'] > recent_high and last['close'] < recent_high:
        score += 3
    if last['low'] < recent_low and last['close'] > recent_low:
        score += 3
    vol = df['volume'].astype(float)
    avg_vol = vol.iloc[-20:-5].mean()
    recent_vol = vol.iloc[-5:].mean()
    if recent_vol > avg_vol * 1.3:
        score += 2
    range_total = high_zone - low_zone
    if range_total > 0:
        pos = (close - low_zone) / range_total
        if pos < 0.3 or pos > 0.7:
            score += 2
    return score

def strong_rejection_candle(df):
    try:
        last = df.iloc[-1]
        body = abs(last['close'] - last['open'])
        upper_wick = last['high'] - max(last['close'], last['open'])
        lower_wick = min(last['close'], last['open']) - last['low']
        if lower_wick > body * 1.5 and last['close'] > last['open']:
            return True, "bullish"
        if upper_wick > body * 1.5 and last['close'] < last['open']:
            return True, "bearish"
        return False, None
    except:
        return False, None

def zone_reaction_engine(df):
    try:
        zones = detect_supply_demand_zones(df, min_touches=2)
        price = df['close'].iloc[-1]
        demand_zones = [z for z in zones['demand_zones'] if z['price'] < price]
        if not demand_zones:
            return None
        nearest_zone = min(demand_zones, key=lambda z: price - z['price'])
        zone_price = nearest_zone['price']
        dist_pct = abs(price - zone_price) / price * 100
        if dist_pct > 0.3:
            return None
        score = 0
        reasons = []
        ind = compute_indicators(df)
        adx = ind.get("adx", 0)
        plus_di = ind.get("plus_di", 0)
        minus_di = ind.get("minus_di", 0)
        if liquidity_sweep(df) == "BUY":
            score += 2
            reasons.append("sweep")
        rej, ctype = strong_rejection_candle(df)
        if rej:
            score += 2
            reasons.append(f"candle:{ctype}")
        if detect_volume_explosion(df):
            score += 2
            reasons.append("volume")
        if 21 <= adx <= 25:
            if plus_di > minus_di:
                score += 2
                reasons.append("adx_bull")
            else:
                score -= 1
                reasons.append("adx_conflict")
        if ind.get("rsi", 50) < 40:
            score += 1
            reasons.append("rsi_recovery")
        if score >= 5:
            return {
                "side": "buy",
                "score": score,
                "reason": f"ZONE_REACTION ({','.join(reasons)})"
            }
        return None
    except Exception as e:
        log_warn(f"zone_reaction_engine error: {e}")
        return None

def rank_zones(df, ind):
    zones = detect_supply_demand_zones(df, min_touches=2)
    price = float(df['close'].iloc[-1])
    vol = df['volume'].iloc[-1]
    vol_ma = df['volume'].rolling(20).mean().iloc[-1]

    adx = ind.get("adx", 0)
    plus_di = ind.get("plus_di", 0)
    minus_di = ind.get("minus_di", 0)

    def score_zone(z, zone_type):
        score = 0
        touches = z.get("touches", 1)
        score += min(touches, 3)

        dist = abs(price - z['price']) / price
        if dist < 0.01:
            score += 2

        if vol > vol_ma * 1.3:
            score += 2

        if zone_type == "demand" and plus_di > minus_di:
            score += 2
        if zone_type == "supply" and minus_di > plus_di:
            score += 2

        if adx >= 20:
            score += 1

        return score

    ranked = []
    for z in zones['demand_zones']:
        ranked.append({
            "type": "buy",
            "price": z['price'],
            "score": score_zone(z, "demand")
        })
    for z in zones['supply_zones']:
        ranked.append({
            "type": "sell",
            "price": z['price'],
            "score": score_zone(z, "supply")
        })

    ranked.sort(key=lambda x: x["score"], reverse=True)
    return ranked[:5]

def check_zone_proximity(price, zone_price):
    return abs(price - zone_price) / price < 0.0025

def institutional_event(ind):
    return (
        ind.get("liquidity_sweep") or
        ind.get("displacement", {}).get("displaced") or
        ind.get("bos_choch", {}).get("bos_up") or
        ind.get("bos_choch", {}).get("bos_down") or
        ind.get("structure", {}).get("choch_up") or
        ind.get("structure", {}).get("choch_down")
    )

def get_adx_series(df, period=14):
    high = df['high'].astype(float)
    low = df['low'].astype(float)
    close = df['close'].astype(float)
    tr = pd.concat([(high - low).abs(),
                    (high - close.shift(1)).abs(),
                    (low - close.shift(1)).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/period, adjust=False).mean()
    up_move = high.diff()
    down_move = low.shift(1) - low
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
    plus_di = 100 * (plus_dm.ewm(alpha=1/period, adjust=False).mean() / atr.replace(0, 1e-12))
    minus_di = 100 * (minus_dm.ewm(alpha=1/period, adjust=False).mean() / atr.replace(0, 1e-12))
    dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, 1e-12)).fillna(0.0)
    adx = dx.ewm(alpha=1/period, adjust=False).mean()
    return adx

def confirmation_ok(ind, adx_prev):
    adx = ind.get("adx", 0)
    rsi_ctx = ind.get("rsi_ctx", {})
    trendZ = rsi_ctx.get("trendZ")
    no_trap = not ind.get("stop_hunt")
    return (
        adx >= 15 and
        adx > adx_prev and
        trendZ in ("bull", "bear") and
        no_trap
    )

def decide_entry(df):
    return None, None, 0, {}

def zone_score(df):
    score = 0
    zones = detect_supply_demand_zones(df, min_touches=2)
    liq = detect_liquidity_pools(df)
    price = df["close"].iloc[-1]
    for z in zones["demand_zones"]:
        if abs(price - z["price"]) / price < 0.002:
            score += 2
            if liq.get("equal_lows"):
                score += 2
            if detect_rejection(df):
                score += 2
    for z in zones["supply_zones"]:
        if abs(price - z["price"]) / price < 0.002:
            score += 2
            if liq.get("equal_highs"):
                score += 2
            if detect_rejection(df):
                score += 2
    return score

def context_score(df):
    score = 0
    ind = compute_indicators(df)
    if ind.get("adx", 0) > 20:
        score += 2
    if not in_middle(df):
        score += 2
    if detect_volume_explosion(df):
        score += 1
    return score

def rank_candidates(symbols):
    ranked = []
    for sym in symbols:
        df = get_ohlcv_safe(sym, timeframe=INTERVAL, limit=120)
        if df is None or len(df) < 50:
            continue
        side, _, score, _ = decide_entry(df)
        if not side:
            continue
        z_score = zone_score(df)
        ctx = context_score(df)
        total = score + z_score + ctx
        ranked.append({
            "symbol": sym,
            "score": total,
            "side": side,
            "inst": score,
            "zone": z_score,
            "ctx": ctx
        })
    ranked.sort(key=lambda x: x["score"], reverse=True)
    return ranked

def pick_best_trade(ranked):
    if not ranked:
        return None
    best = ranked[0]
    if best["score"] < 6:
        return None
    return best

def move_stop_to_entry():
    if PAPER_MODE:
        log_g("🛡 [PAPER] Breakeven activated")
        return
    try:
        pos = get_real_position(SYMBOL)
        if not pos:
            return
        pos_side = pos["side"]
        params = {"positionSide": pos_side, "stopPrice": STATE["entry"]}
        ex.create_order(
            symbol=SYMBOL,
            type="stop_market",
            side="sell" if pos_side == "LONG" else "buy",
            amount=STATE["remaining_qty"],
            params=params
        )
        log_g(f"🛡 Breakeven stop placed at {STATE['entry']:.6f}")
    except Exception as e:
        log_warn(f"move_stop_to_entry error: {e}")

def update_stop_loss(new_price):
    if PAPER_MODE:
        log_i(f"📈 [PAPER] Trailing stop updated to {new_price:.6f}")
        return
    try:
        pos = get_real_position(SYMBOL)
        if not pos:
            return
        pos_side = pos["side"]
        params = {"positionSide": pos_side, "stopPrice": new_price}
        ex.create_order(
            symbol=SYMBOL,
            type="stop_market",
            side="sell" if pos_side == "LONG" else "buy",
            amount=STATE["remaining_qty"],
            params=params
        )
        log_g(f"📈 Stop loss updated to {new_price:.6f}")
    except Exception as e:
        log_warn(f"update_stop_loss error: {e}")

def close_partial_strict(qty):
    try:
        ratio = qty / STATE.get("remaining_qty", STATE.get("qty", 1))
        close_partial(ratio)

        for _ in range(3):
            pos = get_real_position(SYMBOL)
            if pos is None:
                return True
            if pos.get("amount", 0) < qty:
                return True
            time.sleep(1)

        force_close(SYMBOL)
        return True

    except Exception as e:
        log_error(f"Partial close error: {e}")
        return False

def close_partial(ratio):
    if ratio <= 0 or ratio > 1:
        return
    qty_to_close = STATE["remaining_qty"] * ratio
    if qty_to_close <= 0:
        return
    if PAPER_MODE:
        price = price_now()
        paper_close(price, qty_to_close)
        STATE["remaining_qty"] -= qty_to_close
        log_g(f"💰 Partial close {ratio*100:.0f}% at {price:.6f}")
    else:
        pos = get_real_position(SYMBOL)
        if not pos:
            return
        side = "sell" if pos["side"] == "LONG" else "buy"
        pos_side = pos["side"]
        order = execute_market(SYMBOL, side, qty_to_close, pos_side)
        if order:
            STATE["remaining_qty"] -= qty_to_close
            log_g(f"💰 Partial close {ratio*100:.0f}% executed")
        else:
            log_warn("Partial close failed")

def close_position_strict():
    for attempt in range(3):
        safe_close(ex, SYMBOL)

        pos = get_real_position(SYMBOL)
        if pos is None:
            log("✅ POSITION CLOSED (CONFIRMED)")
            return True

        time.sleep(1)

    log("🚨 FORCE CLOSE TRIGGERED")
    force_close(SYMBOL)

    pos = get_real_position(SYMBOL)
    if pos is None:
        log("✅ FORCE CLOSE SUCCESS")
        return True

    log_error("❌ FAILED TO CLOSE POSITION")
    return False

def dynamic_pme_manager(df, position, indicators):
    price = float(df['close'].iloc[-1])
    entry = position["entry"]
    side = position["side"]

    if side == "long":
        profit = (price - entry) / entry * 100
    else:
        profit = (entry - price) / entry * 100

    position["peak"] = max(position.get("peak", 0), profit)

    adx = indicators.get("adx", 20)
    target_price = STATE.get("target_price", None)

    if profit >= 0.3 and not position.get("breakeven"):
        position["stop"] = entry
        position["breakeven"] = True
        log("🛡️ Breakeven activated")

    strong_trend = adx > 22
    tp1_target = 0.8 if strong_trend else 0.5

    if profit >= tp1_target and not position.get("tp1_done"):
        close_partial(0.5)
        position["tp1_done"] = True
        log(f"💰 TP1 hit at {profit:.2f}%")

    if target_price is not None and position.get("tp1_done"):
        if side == "long" and price >= target_price:
            log(f"🎯 Liquidity target reached at {price:.6f} → exit")
            close_position_full()
            return position
        if side == "short" and price <= target_price:
            log(f"🎯 Liquidity target reached at {price:.6f} → exit")
            close_position_full()
            return position

    if position.get("tp1_done"):
        if profit < 1:
            trail_pct = 0.25
        elif profit < 2:
            trail_pct = 0.35
        else:
            trail_pct = 0.5

        trail_price = position["peak"] * (1 - trail_pct)
        if profit <= trail_price:
            log("🔒 Trailing stop hit")
            close_position_full()
            return position

    if position["peak"] > 0.8 and profit < position["peak"] * 0.6:
        log("🔒 Profit lock triggered")
        close_position_full()
        return position

    if profit >= 0.4 and adx < 18:
        log("⚠ Weak momentum exit")
        close_position_full()
        return position

    liq = detect_liquidity_pools(df)
    if position["side"] == "long" and liq.get("near_high"):
        log("🎯 liquidity target reached → exit")
        close_position_full()
        return position
    if position["side"] == "short" and liq.get("near_low"):
        log("🎯 liquidity target reached → exit")
        close_position_full()
        return position

    return position

def manage_position(df, ind, entry_price, side, state):
    pos = {
        "entry": entry_price,
        "side": side,
        "peak": state.get("peak", 0),
        "breakeven": state.get("protected", False),
        "tp1_done": state.get("tp1", False),
    }
    pos = dynamic_pme_manager(df, pos, ind)

    state["peak"] = pos["peak"]
    state["protected"] = pos.get("breakeven", False)
    state["tp1"] = pos.get("tp1_done", False)

def detect_zones(df):
    zones = []
    for i in range(30, len(df)-5):
        high = df['high'].iloc[i]
        low = df['low'].iloc[i]

        move_up = df['close'].iloc[i+3] > high
        move_down = df['close'].iloc[i+3] < low

        if move_up:
            zones.append({
                "type": "demand",
                "top": df['open'].iloc[i],
                "bottom": low,
                "index": i
            })
        if move_down:
            zones.append({
                "type": "supply",
                "top": high,
                "bottom": df['open'].iloc[i],
                "index": i
            })
    return zones[-5:]

def zone_strength(df, zone):
    score = 0

    move = abs(df['close'].iloc[zone["index"]+3] - df['close'].iloc[zone["index"]])
    avg_move = df['close'].pct_change().rolling(20).std().iloc[-1]
    if move > avg_move * 2:
        score += 2

    candle = df.iloc[-1]
    body = abs(candle['close'] - candle['open'])
    wick = candle['high'] - candle['low']
    if wick > body * 1.5:
        score += 2

    touches = 0
    for i in range(zone["index"]+1, len(df)):
        if zone["bottom"] <= df['low'].iloc[i] <= zone["top"]:
            touches += 1
    if touches <= 2:
        score += 2

    return score

def is_fake_zone(df, zone):
    fake_score = 0

    move = abs(df['close'].iloc[zone["index"]+3] - df['close'].iloc[zone["index"]])
    avg_move = df['close'].pct_change().rolling(20).std().iloc[-1]
    if move < avg_move * 1.2:
        fake_score += 2

    chop_count = 0
    for i in range(zone["index"]+1, len(df)):
        body = abs(df['close'].iloc[i] - df['open'].iloc[i])
        wick = df['high'].iloc[i] - df['low'].iloc[i]
        if wick > body * 2:
            chop_count += 1
    if chop_count >= 3:
        fake_score += 2

    touches = 0
    for i in range(zone["index"]+1, len(df)):
        if zone["bottom"] <= df['low'].iloc[i] <= zone["top"]:
            touches += 1
    if touches > 3:
        fake_score += 3

    price = df['close'].iloc[-1]
    range_high = df['high'].rolling(20).max().iloc[-1]
    range_low = df['low'].rolling(20).min().iloc[-1]
    if not (price > range_high * 0.98 or price < range_low * 1.02):
        fake_score += 2

    return fake_score >= 4

def detect_liquidity_targets(df):
    highs = []
    lows = []
    std_high = df['high'].std()
    std_low = df['low'].std()
    for i in range(10, len(df)-5):
        if abs(df['high'].iloc[i] - df['high'].iloc[i-1]) < std_high * 0.1:
            highs.append(df['high'].iloc[i])
        if abs(df['low'].iloc[i] - df['low'].iloc[i-1]) < std_low * 0.1:
            lows.append(df['low'].iloc[i])
    return highs[-5:], lows[-5:]

def get_trade_target(df, side):
    highs, lows = detect_liquidity_targets(df)
    if side == "BUY":
        return max(highs) if highs else None
    elif side == "SELL":
        return min(lows) if lows else None
    else:
        return None

# =================== TELEGRAM SYSTEM ===================
import requests

TG_TOKEN = CONFIG.TG_TOKEN
TG_CHAT  = CONFIG.TG_CHAT_ID

def tg_send(msg):
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        requests.post(url, json={
            "chat_id": TG_CHAT,
            "text": msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }, timeout=6)
    except Exception as e:
        print("TG ERROR:", e)

def tg_boot():
    now = format_time()
    tg_send(f"""
🚀 <b>BOT STARTED</b>

⏱ {now}
📡 Status: <b>ONLINE</b>
🧠 Mode: <b>{'LIVE' if MODE_LIVE else 'PAPER'}</b>

✅ Telegram Connected
""")

def tg_open(symbol, side, price, ctx):
    icon = "🟢" if side == "LONG" else "🔴"
    msg = f"""
{icon} <b>NEW TRADE</b>

📊 <b>{symbol}</b>
📈 <b>{side}</b>
💰 Entry: {price:.6f}

🧠 <b>Analysis</b>
Trend: {ctx.get('trend')}
ADX: {ctx.get('adx'):.1f}
ATR: {ctx.get('atr'):.4f}
DI Spread: {ctx.get('di_spread')}

📍 Zone: {ctx.get('zone')}
⚡ Reason:
{ctx.get('reason')}
"""
    tg_send(msg)

def tg_close(symbol, side, pnl):
    icon = "💰" if pnl >= 0 else "🔻"
    msg = f"""
{icon} <b>CLOSE TRADE</b>

📊 {symbol}
📈 {side}
📉 PNL: {pnl:.2f}%
"""
    tg_send(msg)

def tg_error(e):
    tg_send(f"🚨 <b>ERROR</b>\n{str(e)}")

def tg_mismatch(decision, real):
    tg_send(f"""
⚠️ <b>SIDE MISMATCH</b>

Decision: {decision}
Exchange: {real}
""")

def tg_summary(trades, wins, pnl):
    winrate = (wins / max(trades, 1)) * 100
    tg_send(f"""
📊 <b>SUMMARY</b>

Trades: {trades}
Wins: {wins}
Winrate: {winrate:.1f}%

💰 Total PnL: {pnl:.2f}%
""")

def play_sound(kind):
    try:
        path = {
            "open": "sounds/open.wav",
            "close": "sounds/close.wav",
            "error": "sounds/error.wav",
            "alert": "sounds/alert.wav",
        }.get(kind)
        if path and os.path.exists(path):
            if sys.platform.startswith("win"):
                import winsound
                winsound.PlaySound(path, winsound.SND_ASYNC)
            else:
                os.system(f"aplay {path} >/dev/null 2>&1 &")
    except:
        pass

# =================== INSTITUTIONAL HUNTER MODULES ===================

def update_watchlist(symbol, df):
    """Add low ADX symbols to watchlist for accumulation detection."""
    ind = compute_indicators(df)
    adx = ind.get("adx", 0)
    if adx < 25:
        if symbol not in WATCHLIST:
            WATCHLIST.append(symbol)
        WATCHLIST_META[symbol] = {
            "adx": adx,
            "added_at": time.time(),
            "phase": "accumulation",
            "ready": False,
            "adx": adx
        }
        log_warn(f"🟡 {symbol} → Added to watchlist (Accumulation ADX={adx:.1f})")
    # Clean old entries
    if len(WATCHLIST) > MAX_WATCHLIST:
        oldest = WATCHLIST.pop(0)
        WATCHLIST_META.pop(oldest, None)

def volume_spike(df):
    """Return True if current volume > 1.5x average."""
    vol = df["volume"].iloc[-1]
    avg = df["volume"].rolling(20).mean().iloc[-1]
    return vol > avg * 1.5

def candle_strength(df):
    """Classify last candle: strong, rejection, or neutral."""
    last = df.iloc[-1]
    body = abs(last["close"] - last["open"])
    wick = last["high"] - last["low"]
    if wick == 0:
        return "neutral"
    if body > wick * 0.6:
        return "strong"
    if wick > body * 1.5:
        return "rejection"
    return "neutral"

def detect_order_block_simple(df):
    """Simple order block detection based on strong institutional candle."""
    for i in range(-5, -1):
        candle = df.iloc[i]
        body = abs(candle["close"] - candle["open"])
        range_ = candle["high"] - candle["low"]
        if body > range_ * 0.6:
            if candle["close"] > candle["open"]:
                return {
                    "type": "bullish",
                    "zone_low": candle["low"],
                    "zone_high": candle["open"]
                }
            else:
                return {
                    "type": "bearish",
                    "zone_low": candle["open"],
                    "zone_high": candle["high"]
                }
    return None

def build_score(df):
    """Compute entry score based on confluence."""
    score = 0
    sweep = detect_stop_hunt(df)
    if sweep:
        score += 3
    if volume_spike(df):
        score += 2
    candle = candle_strength(df)
    if candle == "strong":
        score += 2
    elif candle == "rejection":
        score += 1
    ob = detect_order_block_simple(df)
    if ob:
        score += 2
    ind = compute_indicators(df)
    if ind.get("adx", 0) > 20:
        score += 1
    return score

def get_htf_trend(symbol):
    """Higher timeframe trend using 1h ADX/close direction."""
    try:
        df_htf = get_ohlcv_safe(symbol, timeframe="1h", limit=100)
        if df_htf.empty:
            return "RANGE"
        ind = compute_indicators(df_htf)
        adx = ind.get("adx", 0)
        last = df_htf.iloc[-1]
        if adx > 20:
            if last["close"] > last["open"]:
                return "UP"
            else:
                return "DOWN"
        return "RANGE"
    except Exception as e:
        log_warn(f"HTF trend error: {e}")
        return "RANGE"

def fake_breakout_filter(df):
    """Detect large wick relative to body (potential trap)."""
    last = df.iloc[-1]
    body = abs(last["close"] - last["open"])
    wick = last["high"] - last["low"]
    if wick > body * 2:
        return True
    return False

def evaluate_sniper(symbol, df):
    """Main sniper entry evaluation with score and HTF confirmation."""
    score = build_score(df)
    sweep = detect_stop_hunt(df)
    if not sweep:
        return None
    htf = get_htf_trend(symbol)
    # Only allow if HTF aligns
    if sweep == "LONG" and htf != "UP":
        return None
    if sweep == "SHORT" and htf != "DOWN":
        return None
    if fake_breakout_filter(df):
        log_warn(f"⚠️ {symbol} fake breakout filter triggered")
        return None
    global MIN_SCORE_THRESHOLD
    if score >= MIN_SCORE_THRESHOLD:
        log_warn(f"🔥 {symbol} → HIGH PROBABILITY SETUP (Score={score})")
        return sweep
    return None

def log_trade_memory(symbol, side, pnl, score):
    """Store trade for learning."""
    TRADE_LOG.append({
        "symbol": symbol,
        "side": side,
        "pnl": pnl,
        "score": score,
        "time": time.time()
    })
    if len(TRADE_LOG) > 100:
        TRADE_LOG.pop(0)

def analyze_performance():
    wins = [t for t in TRADE_LOG if t["pnl"] > 0]
    losses = [t for t in TRADE_LOG if t["pnl"] <= 0]
    winrate = len(wins) / len(TRADE_LOG) if TRADE_LOG else 0
    return {
        "winrate": winrate,
        "avg_win": sum(t["pnl"] for t in wins)/len(wins) if wins else 0,
        "avg_loss": sum(t["pnl"] for t in losses)/len(losses) if losses else 0
    }

def adaptive_tuning():
    global MIN_SCORE_THRESHOLD
    stats = analyze_performance()
    if stats["winrate"] < 0.4 and MIN_SCORE_THRESHOLD < 8:
        MIN_SCORE_THRESHOLD += 1
        log_warn(f"⚠️ Raising entry threshold → {MIN_SCORE_THRESHOLD}")
    elif stats["winrate"] > 0.6 and MIN_SCORE_THRESHOLD > 5:
        MIN_SCORE_THRESHOLD -= 1
        log_warn(f"🚀 Lowering entry threshold → {MIN_SCORE_THRESHOLD}")

def smart_exit(df, pos):
    """Manage exit with TP1, TP2, trend hold, and trailing protection."""
    global PEAK_PNL
    price = price_now()
    entry = pos["entry"]
    side = pos["side"]
    if side == "LONG":
        pnl = (price - entry) / entry * 100
    else:
        pnl = (entry - price) / entry * 100
    if pnl > PEAK_PNL:
        PEAK_PNL = pnl

    if pnl >= 0.5 and not STATE.get("tp1_hit"):
        close_partial(0.5)
        STATE["tp1_hit"] = True
        log_warn("🎯 TP1 HIT")
    if STATE.get("tp1_hit"):
        ind = compute_indicators(df)
        adx = ind["adx"]
        if adx > 25:
            log_warn("🚀 Strong trend → HOLD")
        if adx < 18:
            log_warn("⚠️ Weak trend → exit")
            strict_close_position("WEAK_TREND")
            return
        if PEAK_PNL - pnl >= 0.3:
            log_warn("🔒 Trailing exit")
            strict_close_position("TRAILING")
            return
    if pnl >= 1.0 and not STATE.get("tp2_hit"):
        close_partial(0.5)
        STATE["tp2_hit"] = True
        log_warn("🏁 TP2 HIT")

def can_trade_cooldown():
    global LAST_TRADE_TIME
    if time.time() - LAST_TRADE_TIME < 300:
        return False
    return True

# =================== SMART PIPELINE (existing) ===================
def smart_pipeline_scan():
    try:
        tickers = ex.fetch_tickers()
        filtered = []
        for s, t in tickers.items():
            if "USDT" in s and t.get("quoteVolume", 0) > 2_000_000:
                filtered.append(s)
        filtered = filtered[:50]

        scored = []
        for sym in filtered:
            df = get_ohlcv_safe(sym, timeframe=INTERVAL, limit=120)
            if df is None or len(df) < 50:
                continue
            ind = compute_indicators(df)

            score = 0

            range_ = (df["high"].rolling(20).max().iloc[-1] - df["low"].rolling(20).min().iloc[-1]) / df["low"].iloc[-1]
            if range_ < 0.03:
                score += 2

            if 18 < ind["adx"] < 25:
                score += 2

            vol = df["volume"].iloc[-1]
            vol_ma = df["volume"].rolling(20).mean().iloc[-1]
            if vol > vol_ma * 1.3:
                score += 2

            if score >= 4:
                scored.append((sym, score))

        scored.sort(key=lambda x: x[1], reverse=True)
        return [s[0] for s in scored[:20]]
    except Exception as e:
        log_warn(f"smart_pipeline_scan error: {e}")
        return []

def final_entry_logic(sym, df):
    try:
        ob = detect_order_block_pro(df)
        sweep = detect_liquidity_sweep_advanced(df)
        if not ob or not sweep:
            return False

        price = df["close"].iloc[-1]
        if not (ob["low"] <= price <= ob["high"]):
            return False

        decision, score = candle_master_engine(df)
        if not decision:
            return False

        if ob["type"] == "bullish" and decision != "buy":
            return False
        if ob["type"] == "bearish" and decision != "sell":
            return False

        if score < 6:
            return False

        return decision
    except Exception as e:
        log_warn(f"final_entry_logic error: {e}")
        return False

def candle_master_engine(df):
    try:
        single = analyze_candle(df)
        seq = analyze_candle_sequence(df)
        ind = compute_indicators(df)
        adx = ind.get("adx", 0)
        atr = ind.get("atr", 0)

        score = 0
        decision = None

        if seq["signal"]:
            decision = seq["signal"]
            score += seq["strength"]
        elif single["signal"]:
            decision = single["signal"]
            score += single["strength"]

        if adx > 25:
            score += 2

        candle_size = df["high"].iloc[-1] - df["low"].iloc[-1]
        if candle_size > atr * 1.2:
            score += 2

        return decision, score
    except Exception as e:
        log_warn(f"candle_master_engine error: {e}")
        return None, 0

def analyze_candle(df):
    try:
        last = df.iloc[-1]
        body = abs(last['close'] - last['open'])
        range_ = last['high'] - last['low']
        if range_ == 0:
            return {"signal": None, "strength": 0}

        upper_wick = last['high'] - max(last['close'], last['open'])
        lower_wick = min(last['close'], last['open']) - last['low']

        body_ratio = body / range_

        signal = None
        strength = 0

        if last['close'] > last['open'] and lower_wick > body * 1.2:
            signal = "buy"
            strength = 3
        elif last['close'] < last['open'] and upper_wick > body * 1.2:
            signal = "sell"
            strength = 3
        elif last['close'] > last['open'] and body_ratio > 0.6:
            signal = "buy"
            strength = 2
        elif last['close'] < last['open'] and body_ratio > 0.6:
            signal = "sell"
            strength = 2
        else:
            signal = None
            strength = 0

        return {"signal": signal, "strength": strength}
    except Exception as e:
        log_warn(f"analyze_candle error: {e}")
        return {"signal": None, "strength": 0}

def analyze_candle_sequence(df):
    try:
        if len(df) < 3:
            return {"pattern": None, "signal": None, "strength": 0}

        last3 = df.tail(3)
        closes = last3["close"].values
        opens = last3["open"].values
        highs = last3["high"].values
        lows = last3["low"].values

        result = {"pattern": None, "signal": None, "strength": 0}

        if closes[2] > closes[1] > closes[0]:
            result["pattern"] = "three_soldiers"
            result["signal"] = "buy"
            result["strength"] = 4

        elif closes[2] < closes[1] < closes[0]:
            result["pattern"] = "three_crows"
            result["signal"] = "sell"
            result["strength"] = 4

        if lows[2] < lows[1] and closes[2] > opens[2]:
            result["pattern"] = "liquidity_reversal"
            result["signal"] = "buy"
            result["strength"] = 5
        elif highs[2] > highs[1] and closes[2] < opens[2]:
            result["pattern"] = "liquidity_reversal_sell"
            result["signal"] = "sell"
            result["strength"] = 5

        return result
    except Exception as e:
        log_warn(f"analyze_candle_sequence error: {e}")
        return {"pattern": None, "signal": None, "strength": 0}

def update_memory(symbols):
    global SYMBOL_MEMORY
    for sym in symbols:
        if sym not in SYMBOL_MEMORY:
            SYMBOL_MEMORY[sym] = {"phase": "idle", "direction": None}

def smart_scan_and_trade():
    global bot_state, SYMBOL_MEMORY
    symbols = smart_pipeline_scan()
    if not symbols:
        return False

    update_memory(symbols)

    top5_opps = []

    for sym in symbols:
        df = get_ohlcv_safe(sym, timeframe=INTERVAL, limit=120)
        if df is None or len(df) < 50:
            continue

        decision = final_entry_logic(sym, df)
        if not decision:
            continue

        if has_open_position():
            log_scan(f"⚠️ {sym} صفقة مفتوحة بالفعل، تخطي")
            continue
        if cooldown_active():
            log_scan(f"🧊 {sym} كول داون نشط، تخطي")
            continue

        price = df["close"].iloc[-1]
        qty = calculate_position_size_real(sym, price, 10)
        if qty <= 0:
            continue

        if not update_symbol(sym):
            continue

        log_g(f"🔥 SMART ENTRY → {sym} {decision} score=10")
        success = open_market_enhanced(
            "buy" if decision == "buy" else "sell",
            qty, price,
            source=f"SMART_PIPELINE",
            df=df,
            institutional_score=10,
            breakdown={"engine": "smart_pipeline"}
        )
        if success:
            bot_state["best_symbol"] = sym
            bot_state["best_score"] = 10
            bot_state["decision"] = {
                "action": decision.upper(),
                "score": 10,
                "needed": MIN_ENTRY_SCORE,
                "reason": "SMART_PIPELINE"
            }
            update_top5_dashboard([{"symbol": sym, "score": 10, "zone_score": 8, "reason": "smart_pipeline"}])
            return True

        top5_opps.append({
            "symbol": sym,
            "score": 10,
            "zone_score": 8,
            "reason": f"{decision}_smart"
        })

    update_top5_dashboard(top5_opps)
    return False

# =================== SNIPER ENGINE (RADAR + WATCHLIST + DASHBOARD) ===================
# ---------- FIX: Define _ema globally for sniper functions ----------
def _ema(arr, period):
    """Exponential Moving Average helper for Sniper Engine."""
    k = 2 / (period + 1)
    ema = []
    for i, v in enumerate(arr):
        if i == 0:
            ema.append(v)
        else:
            ema.append(v * k + ema[-1] * (1 - k))
    return ema
# --------------------------------------------------------------------

SNIPER_WATCHLIST = {}   # symbol -> dict
SNIPER_LIQUIDITY_MAP = {} # symbol -> {high, low, time}
SNIPER_TOP_CACHE = []   # [(symbol, score)]
SNIPER_MAX_WATCH = 30
SNIPER_TOP_N = 5
LAST_RADAR_TIME = 0

# ✅ ADDED: Sniper Only Modification - Non‑Blocking Priority Queue
sniper_queue = []  # each item: {symbol, side, score, created_at, recheck_at}

def add_to_sniper_queue(symbol, side, score, next_candle_time):
    """Insert a sniper opportunity into the queue."""
    # avoid duplicates
    for item in sniper_queue:
        if item["symbol"] == symbol:
            # update if newer/better
            if score > item["score"]:
                item["score"] = score
                item["side"] = side
                item["recheck_at"] = next_candle_time
                item["created_at"] = time.time()
            return
    sniper_queue.append({
        "symbol": symbol,
        "side": side,
        "score": score,
        "created_at": time.time(),
        "recheck_at": next_candle_time,
    })
    # keep queue size limited and sorted by score
    sniper_queue.sort(key=lambda x: x["score"], reverse=True)
    if len(sniper_queue) > 20:
        sniper_queue.pop()

def process_sniper_queue():
    """Check queued items whose recheck time has passed, confirm and execute."""
    now = time.time()
    to_remove = []
    for item in list(sniper_queue):
        if now < item["recheck_at"]:
            continue
        symbol = item["symbol"]
        side = item["side"]
        original_score = item["score"]

        # Fetch latest data (after candle close)
        df = get_ohlcv_safe(symbol, timeframe=INTERVAL, limit=120)
        if df is None or len(df) < 3:
            log_warn(f"⚠️ Sniper queue: {symbol} data fetch failed, skipping")
            to_remove.append(item)
            continue

        # Get confirmation candles (previous = signal candle, current = confirmation candle)
        prev_candle = df.iloc[-2]   # signal candle (when opportunity was detected)
        confirm_candle = df.iloc[-1] # new closed candle

        # ✅ ADDED: Sniper confirmation logic (same as before but without wait)
        if not confirm_sniper_entry(side, prev_candle, confirm_candle, df):
            log_warn(f"⛔ SNIPER SKIPPED: {symbol} | side={side} | score={original_score} (confirmation failed)")
            to_remove.append(item)
            continue

        # All checks passed – execute trade
        price = confirm_candle.close
        qty = calculate_position_size_real(symbol, price, original_score)
        if qty <= 0:
            log_warn(f"⛔ SNIPER SKIPPED: {symbol} invalid size")
            to_remove.append(item)
            continue

        if not update_symbol(symbol):
            log_warn(f"⛔ SNIPER SKIPPED: {symbol} could not update symbol")
            to_remove.append(item)
            continue

        log_g(f"🚀 SNIPER EXECUTED: {symbol} | {side.upper()} | score={original_score}")
        success = open_market_enhanced(
            "buy" if side == "BUY" else "sell",
            qty, price,
            source="SNIPER_QUEUE",
            df=df,
            institutional_score=original_score,
            breakdown={"engine": "sniper_queue"}
        )
        if success:
            # If trade opened, we stop processing further queue items to avoid multiple trades
            to_remove.append(item)
            # Clean up the rest later (or keep them for next loop)
            break
        else:
            to_remove.append(item)

    # Remove processed items
    for item in to_remove:
        if item in sniper_queue:
            sniper_queue.remove(item)

def confirm_sniper_entry(side, prev_candle, confirm_candle, df=None):
    """
    Professional confirmation of sniper setup.
    Checks: candle strength, manipulation, continuation, momentum.
    Returns True if confirmed.
    """
    if prev_candle is None or confirm_candle is None:
        return True  # fail-safe

    body = abs(confirm_candle.close - confirm_candle.open)
    candle_range = confirm_candle.high - confirm_candle.low
    if candle_range == 0:
        return False

    body_ratio = body / candle_range
    upper_wick = confirm_candle.high - max(confirm_candle.close, confirm_candle.open)
    lower_wick = min(confirm_candle.close, confirm_candle.open) - confirm_candle.low

    is_bullish = confirm_candle.close > confirm_candle.open
    is_bearish = confirm_candle.close < confirm_candle.open

    # 1. Manipulation detection (long wicks)
    if side.lower() == "buy":
        if upper_wick > body * 1.8:
            log("⛔ Sniper manipulation: long upper wick (bull trap)")
            return False
    else:
        if lower_wick > body * 1.8:
            log("⛔ Sniper manipulation: long lower wick (bear trap)")
            return False

    # 2. Candle strength (needs solid body)
    if body_ratio < 0.5:
        log(f"⛔ Sniper weak candle: body_ratio={body_ratio:.2f}")
        return False

    # 3. Direction and continuation
    if side.lower() == "buy":
        if not is_bullish:
            log("⛔ Sniper confirmation candle is not bullish")
            return False
        if confirm_candle.close <= prev_candle.high:
            log("⛔ Sniper close did not exceed previous high")
            return False
    else:
        if not is_bearish:
            log("⛔ Sniper confirmation candle is not bearish")
            return False
        if confirm_candle.close >= prev_candle.low:
            log("⛔ Sniper close did not break previous low")
            return False

    # 4. Additional sweep check
    if df is not None and len(df) > 3:
        sweep = detect_stop_hunt(df)
        if sweep:
            if side.lower() == "buy" and sweep == "SHORT":
                log("⛔ Sniper: bearish sweep detected, possible fakeout")
                return False
            if side.lower() == "sell" and sweep == "LONG":
                log("⛔ Sniper: bullish sweep detected, possible fakeout")
                return False

    log_g(f"✅ Sniper confirmation PASSED for {side.upper()}")
    return True

def sniper_fetch_ohlcv(symbol, timeframe="15m", limit=120):
    return get_ohlcv_safe(symbol, timeframe, limit)

def sniper_adx_di(df, period=14):
    highs = df["high"].values
    lows = df["low"].values
    closes = df["close"].values

    plus_dm = [0]
    minus_dm = [0]
    tr = [highs[0] - lows[0]]

    for i in range(1, len(df)):
        up = highs[i] - highs[i-1]
        dn = lows[i-1] - lows[i]

        plus_dm.append(up if up > dn and up > 0 else 0)
        minus_dm.append(dn if dn > up and dn > 0 else 0)

        tr_i = max(highs[i] - lows[i],
                   abs(highs[i] - closes[i-1]),
                   abs(lows[i] - closes[i-1]))
        tr.append(tr_i)

    atr_vals = _ema(tr, period)
    plus_di = [100 * (_ema(plus_dm, period)[i] / (atr_vals[i] if atr_vals[i] else 1)) for i in range(len(df))]
    minus_di = [100 * (_ema(minus_dm, period)[i] / (atr_vals[i] if atr_vals[i] else 1)) for i in range(len(df))]

    dx = [100 * abs(plus_di[i] - minus_di[i]) / (plus_di[i] + minus_di[i] if (plus_di[i] + minus_di[i]) else 1) for i in range(len(df))]
    adx_vals = _ema(dx, period)

    return adx_vals[-1], plus_di[-1], minus_di[-1]

def sniper_atr(df, period=14):
    highs = df["high"].values
    lows = df["low"].values
    closes = df["close"].values

    trs = []
    for i in range(len(df)):
        if i == 0:
            trs.append(highs[i] - lows[i])
        else:
            tr = max(highs[i] - lows[i],
                     abs(highs[i] - closes[i-1]),
                     abs(lows[i] - closes[i-1]))
            trs.append(tr)
    atr_vals = _ema(trs, period)
    return atr_vals[-1]

def sniper_fibonacci_zone(df):
    high = df["high"].iloc[-50:].max()
    low = df["low"].iloc[-50:].min()
    diff = high - low
    fib_618 = high - diff * 0.618
    fib_786 = high - diff * 0.786
    price = df["close"].iloc[-1]
    return fib_786 <= price <= fib_618

def sniper_candle_pattern(df):
    last = df.iloc[-1]
    prev = df.iloc[-2]

    if (last["close"] > last["open"] and prev["close"] < prev["open"]
        and last["close"] > prev["open"] and last["open"] < prev["close"]):
        return "BULLISH"
    if (last["close"] < last["open"] and prev["close"] > prev["open"]
        and last["open"] > prev["close"] and last["close"] < prev["open"]):
        return "BEARISH"

    body = abs(last["close"] - last["open"])
    wick = last["high"] - last["low"]
    if wick > body * 2:
        return "REJECTION"

    return None

def sniper_update_liquidity(symbol, df):
    SNIPER_LIQUIDITY_MAP[symbol] = {
        "high": df["high"].iloc[-1],
        "low": df["low"].iloc[-1],
        "time": time.time()
    }

def sniper_near_liquidity(symbol, price):
    z = SNIPER_LIQUIDITY_MAP.get(symbol)
    if not z:
        return False
    if abs(price - z["high"]) / price < 0.002:
        return True
    if abs(price - z["low"]) / price < 0.002:
        return True
    return False

def sniper_radar_scan(symbols):
    radar = []
    for s in symbols:
        try:
            df = sniper_fetch_ohlcv(s)
            adx, plus_di, minus_di = sniper_adx_di(df)
            vol = df["volume"].iloc[-1]
            avg = df["volume"].rolling(20).mean().iloc[-1] or 1

            if adx < 20:
                radar.append({
                    "symbol": s,
                    "adx": adx,
                    "plus_di": plus_di,
                    "minus_di": minus_di,
                    "vol_ratio": vol / avg
                })
        except Exception as e:
            monitor_log_warning(f"{s} radar error: {e}")

    radar.sort(key=lambda x: (x["vol_ratio"]), reverse=True)
    return radar[:50]

def sniper_update_watchlist(radar):
    for c in radar:
        s = c["symbol"]
        SNIPER_WATCHLIST[s] = {
            "adx": c["adx"],
            "plus_di": c["plus_di"],
            "minus_di": c["minus_di"],
            "vol_ratio": c["vol_ratio"],
            "phase": "accumulation",
            "signal": None,
            "score": 0,
            "eye": True,
            "last_update": time.time()
        }
        monitor_log_warning(f"👁️ WATCH → {s} | ADX={c['adx']:.1f} | Vol×{c['vol_ratio']:.2f}")

    if len(SNIPER_WATCHLIST) > SNIPER_MAX_WATCH:
        items = sorted(SNIPER_WATCHLIST.items(), key=lambda kv: kv[1]["last_update"])
        for k, _ in items[:len(SNIPER_WATCHLIST) - SNIPER_MAX_WATCH]:
            SNIPER_WATCHLIST.pop(k, None)

def sniper_analyze_coin(symbol):
    df = sniper_fetch_ohlcv(symbol)
    price = df["close"].iloc[-1]

    adx, plus_di, minus_di = sniper_adx_di(df)
    a = sniper_atr(df)

    sweep = detect_stop_hunt(df)
    fib = sniper_fibonacci_zone(df)
    candle = sniper_candle_pattern(df)

    sniper_update_liquidity(symbol, df)
    liq = sniper_near_liquidity(symbol, price)

    vol_spike = df["volume"].iloc[-1] > df["volume"].rolling(20).mean().iloc[-1] * 1.5

    signal = None
    if sweep == "LONG" and plus_di > minus_di and adx > 18:
        if fib and (candle in ("BULLISH", "REJECTION")):
            signal = "BUY"
    elif sweep == "SHORT" and minus_di > plus_di and adx > 18:
        if fib and (candle in ("BEARISH", "REJECTION")):
            signal = "SELL"

    return {
        "adx": adx,
        "plus_di": plus_di,
        "minus_di": minus_di,
        "atr": a,
        "sweep": sweep,
        "fib": fib,
        "candle": candle,
        "liquidity": liq,
        "vol_spike": vol_spike,
        "signal": signal
    }

def sniper_compute_score(d):
    score = 0
    if d["sweep"]: score += 3
    if d["vol_spike"]: score += 2
    if d["liquidity"]: score += 2
    if d["fib"]: score += 2
    if d["candle"]: score += 1
    if d["adx"] > 18: score += 1
    if d["signal"]: score += 2
    return score

def sniper_refresh_top():
    global SNIPER_TOP_CACHE
    scored = []
    for s in list(SNIPER_WATCHLIST.keys()):
        try:
            d = sniper_analyze_coin(s)
            sc = sniper_compute_score(d)
            SNIPER_WATCHLIST[s].update(d)
            SNIPER_WATCHLIST[s]["score"] = sc
            scored.append((s, sc))
        except Exception as e:
            monitor_log_warning(f"{s} analyze error: {e}")

    scored.sort(key=lambda x: x[1], reverse=True)
    SNIPER_TOP_CACHE = scored[:SNIPER_TOP_N]
    return SNIPER_TOP_CACHE

def sniper_dashboard_text():
    top = sniper_refresh_top()
    lines = ["\n👁️ TOP SNIPER WATCHLIST"]
    for s, sc in top:
        d = SNIPER_WATCHLIST[s]
        icon = "👁️"
        if d["signal"] == "BUY":
            icon = "🟢⬆️"
        elif d["signal"] == "SELL":
            icon = "🔴⬇️"

        extras = []
        if d["fib"]: extras.append("FIB")
        if d["liquidity"]: extras.append("💧")
        if d["candle"]: extras.append(d["candle"])
        if d["vol_spike"]: extras.append("Vol🔥")

        line = f"{icon} {s} | {d['signal'] or 'WAIT'} | Score:{sc} | ADX:{d['adx']:.1f} | DI+:{d['plus_di']:.1f} DI-:{d['minus_di']:.1f} | ATR:{d['atr']:.4f} | {' '.join(extras)}"
        lines.append(line)
    return "\n".join(lines)

def sniper_execution():
    if has_open_position():
        return

    top = sniper_refresh_top()
    for s, sc in top:
        if sc >= 6:
            sig = SNIPER_WATCHLIST[s]["signal"]
            if sig:
                monitor_log_warning(f"🔥 SNIPER → {s} | {sig} | Score={sc}")
                # execute trade using bot's function
                df = sniper_fetch_ohlcv(s)
                price = df["close"].iloc[-1]
                qty = calculate_position_size_real(s, price, sc)
                if qty > 0 and update_symbol(s):
                    open_market_enhanced(
                        "buy" if sig == "BUY" else "sell",
                        qty, price,
                        source="SNIPER_ENGINE",
                        df=df,
                        institutional_score=sc,
                        breakdown={"engine": "sniper"}
                    )
                return

# =================== RADAR ENGINE (NEW) ===================
def radar_engine(symbols):
    """First stage: scan for low ADX coins to add to WATCHLIST."""
    global WATCHLIST, WATCHLIST_META
    for s in symbols[:50]:  # limit to 50 per radar scan
        df = get_ohlcv_safe(s)
        if df is None or df.empty:
            continue
        ind = compute_indicators(df)
        adx = ind.get("adx", 0)
        if adx < 25:
            if s not in WATCHLIST:
                WATCHLIST.append(s)
            WATCHLIST_META[s] = {
                "adx": adx,
                "time": time.time(),
                "phase": "accumulation"
            }
    log_i(f"📡 RADAR FOUND: {len(WATCHLIST)} coins")

def sniper_engine():
    """Second stage: analyze WATCHLIST for high probability setups."""
    candidates = []
    for s in list(WATCHLIST):
        df = get_ohlcv_safe(s)
        if df is None or df.empty:
            continue
        sweep = detect_liquidity_sweep_advanced(df)
        ob = detect_order_block_pro(df)
        fvg = detect_fvg_pro(df)
        score = 0
        if sweep:
            score += 3
        if ob:
            score += 2
        if fvg:
            score += 2
        if score >= 5:
            candidates.append((s, score))
    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[:5]

# =================== TRADE MANAGEMENT ===================

# ✅ ADDED: Entry Exhaustion Filter (Stochastic computation)
def compute_stoch_k(df, k_period=14, d_period=3):
    """Compute Stochastic %K line from dataframe (non-intrusive)."""
    if len(df) < k_period:
        return 50.0
    high = df['high'].astype(float)
    low = df['low'].astype(float)
    close = df['close'].astype(float)
    lowest_low = low.rolling(window=k_period).min()
    highest_high = high.rolling(window=k_period).max()
    stoch_k = 100 * (close - lowest_low) / (highest_high - lowest_low + 1e-12)
    return float(stoch_k.iloc[-1])

def is_rejection_candle(candle):
    body = abs(candle['close'] - candle['open'])
    upper_wick = candle['high'] - max(candle['close'], candle['open'])
    lower_wick = min(candle['close'], candle['open']) - candle['low']

    bearish_rejection = upper_wick > body * 2
    bullish_rejection = lower_wick > body * 2

    return bearish_rejection, bullish_rejection

def allow_entry_pro(side, rsi, stoch_k, adx, candle):
    STRONG_TREND = 25

    bear_reject, bull_reject = is_rejection_candle(candle)

    # allow in strong trend
    if adx >= STRONG_TREND:
        return True

    # block BUY at exhaustion top
    if side == "buy":
        if rsi >= 70 and stoch_k > 80 and bear_reject:
            return False

    # block SELL at exhaustion bottom
    if side == "sell":
        if rsi <= 30 and stoch_k < 20 and bull_reject:
            return False

    return True

def open_market_enhanced(side, qty, price, source="INSTITUTIONAL", df=None, institutional_score=0, breakdown=None):
    if qty <= 0:
        log_e("skip open (qty<=0)")
        return False

    if not can_open_trade():
        return False

    if not has_sufficient_margin(qty, price):
        return False

    if df is None:
        df = fetch_ohlcv_cached()
    ind = compute_indicators(df)
    adx = ind.get('adx', 0)
    rsi = ind.get('rsi', 50)
    trend_str = classify_trend_strength(adx, ind.get('plus_di',0), ind.get('minus_di',0))
    if institutional_score >= ULTRA_ENTRY_SCORE:
        signal_strength = "ULTRA"
    elif institutional_score >= STRONG_ENTRY_SCORE:
        signal_strength = "VERY_STRONG"
    elif institutional_score >= MIN_ENTRY_SCORE:
        signal_strength = "STRONG"
    else:
        signal_strength = "MEDIUM"
    current_market_regime = get_market_regime_from_adx(adx)
    target_price = get_trade_target(df, side.upper())

    # ✅ ADDED: Entry Filter Guard (Exhaustion + Rejection)
    try:
        if df is not None and len(df) > 0:
            last_candle = df.iloc[-1]
            stoch_k = compute_stoch_k(df)  # compute locally without affecting existing pipeline
            if not allow_entry_pro(side, rsi, stoch_k, adx, last_candle):
                log(f"⛔ Skip {side.upper()}: Exhaustion + Rejection Filter (RSI={rsi:.1f} StochK={stoch_k:.1f} ADX={adx:.1f})")
                return False
    except Exception as e:
        # fail-safe: if any error during filter check, allow entry
        log_warn(f"Entry filter check failed (skipping filter): {e}")

    success = execute_trade_decision(side, price, qty, "institutional", None, None, source)
    if success:
        time.sleep(1)
        sync_account_state()
        if not STATE.get("open"):
            log_e("Trade executed but position not found after sync")
            return False

        STATE.update({
            "source": source,
            "entry_score": institutional_score,
            "heat_score": institutional_score,
            "heat_breakdown": breakdown or {},
            "signal_strength": signal_strength,
            "trend_strength_entry": trend_str,
            "regime_at_entry": detect_market_regime_advanced(df),
            "current_market_regime": current_market_regime,
            "supply_demand_trigger": source == "SUPPLY_DEMAND",
            "trend": classify_trend(adx, ind.get('plus_di',0), ind.get('minus_di',0)),
            "opened_at": time.time(),
            "protected": False,
            "tp1": False,
            "tp1_wait": False,
            "peak": 0.0,
            "target_price": target_price,
            "trail_activated": False,
            "trail_stop": None,
            "trail_multiplier": TRAIL_ATR_MULT,
            "highest_profit_pct": 0.0,
            "prev_adx": adx,
            "trend_strength": trend_str,
            "tp1_hit": False,
            "tp2_hit": False,
        })
        global LAST_TRADE_TIME
        LAST_TRADE_TIME = time.time()
        color = C.GREEN if side == "buy" else C.RED
        print(f"{color}\n{'='*50}\n🚀 POSITION OPENED\n{'='*50}\n"
              f"SIDE      : {side.upper()}\n"
              f"ENTRY     : {price:.6f}\n"
              f"QTY       : {qty:.4f}\n"
              f"LEVERAGE  : {LEVERAGE}x\n"
              f"STRATEGY  : {source}\n"
              f"SIGNAL    : {signal_strength}\n"
              f"HEAT SCORE: {institutional_score:.1f}\n"
              f"ADX       : {adx:.1f}\n"
              f"RSI       : {rsi:.1f}\n"
              f"BALANCE   : {balance_usdt():.2f} USDT\n"
              f"TARGET    : {target_price if target_price else 'N/A'}\n"
              f"{'='*50}{C.RESET}\n", flush=True)
        STATE["signal"] = side.upper()
        ctx = {
            "trend": trend_str,
            "adx": adx,
            "atr": ind.get("atr", 0),
            "di_spread": abs(ind.get("plus_di",0) - ind.get("minus_di",0)),
            "zone": source,
            "reason": source
        }
        tg_open(SYMBOL, STATE["side"].upper(), price, ctx)
        play_sound("open")
        return True
    return False

open_market = open_market_enhanced

def strict_close_position(reason="CLOSE"):
    global STATE, compound_pnl, wait_for_next_signal_side, LAST_TRADE_TIME, PEAK_PNL
    if not has_open_position():
        log_warn("No position to close")
        return False
    side = STATE.get("side", "long")
    qty = STATE.get("remaining_qty", STATE.get("qty", 0))
    entry = STATE.get("entry", 0)
    px = price_now() or entry
    if side == "long":
        pnl = (px - entry) * qty
        pnl_pct = ((px - entry) / entry) * 100 if entry else 0
    else:
        pnl = (entry - px) * qty
        pnl_pct = ((entry - px) / entry) * 100 if entry else 0
    was_loss = pnl < 0
    success = close_position_full()
    if success:
        new_bal = balance_usdt()
        log_event("INFO", f"Closed {side} {qty:.4f} @ {px:.6f} | PnL={pnl:.2f}")
        update_stats_dashboard(pnl)
        log_trade_memory(SYMBOL, side.upper(), pnl_pct, STATE.get("entry_score", 0))
        sync_account_state()

        if pnl > 0:
            tg_close(SYMBOL, side.upper(), pnl_pct)
            play_sound("close")
        else:
            tg_close(SYMBOL, side.upper(), pnl_pct)
            play_sound("close")
    else:
        log_e("❌ forced close failed")
        new_bal = balance_usdt() or 0
    compound_pnl += pnl
    log(f"CLOSE {side} reason={reason} pnl={fmt(pnl)} pnl%={fmt(pnl_pct,2)}% total={fmt(compound_pnl)} source={STATE.get('source','-')}")
    logging.info(f"CLOSE {side} pnl={pnl} pnl%={pnl_pct} total={compound_pnl} source={STATE.get('source','-')}")
    if success:
        log_trade_close(side, entry, px, pnl_pct, pnl, new_bal)
    if was_loss:
        STATE["consecutive_losses"] = STATE.get("consecutive_losses", 0) + 1
        cooldown_mins = COOLDOWN_MINUTES_LOSS
        if STATE["consecutive_losses"] >= MAX_CONSECUTIVE_LOSSES:
            cooldown_mins = COOLDOWN_MINUTES_DRAWDOWN
            log_warn(f"⚠️ Drawdown protection: {MAX_CONSECUTIVE_LOSSES} consecutive losses, cooldown extended to {cooldown_mins} min")
        STATE["cooldown_until"] = datetime.now(timezone.utc) + timedelta(minutes=cooldown_mins)
        log_warn(f"🧊 Cooldown activated for {cooldown_mins} minutes due to loss")
    else:
        STATE["consecutive_losses"] = 0
    _reset_after_close(reason, prev_side=side)
    STATE["signal"] = None
    LAST_TRADE_TIME = time.time()
    PEAK_PNL = 0
    adaptive_tuning()
    return success

def close_market_strict(reason="STRICT"):
    strict_close_position(reason)

def _reset_after_close(reason, prev_side=None):
    global wait_for_next_signal_side
    prev_side = prev_side or STATE.get("side")
    # Reset all position-related fields to safe values
    STATE.update({
        "open": False,
        "side": None,
        "entry": None,          # Ensure None to prevent calculation errors
        "qty": 0.0,
        "remaining_qty": 0.0,
        "tp1_done": False,
        "trail_stop": None,
        "trail_activated": False,
        "pnl": 0.0,
        "bars": 0,
        "highest_profit_pct": 0.0,
        "trail_multiplier": TRAIL_ATR_MULT,
        "prev_adx": 0,
        "trend_strength": "weak",
        "regime_at_entry": "range",
        "signal_strength": "MEDIUM",
        "entry_score": 0,
        "heat_score": 0,
        "heat_breakdown": {},
        "current_market_regime": "range",
        "supply_demand_trigger": False,
        "trend": None,
        "protected": False,
        "tp1": False,
        "tp1_wait": False,
        "peak": 0.0,
        "target_price": None,
        "trade": None,
        "signal": None,
        "tp1_hit": False,
        "tp2_hit": False,
    })
    save_state({"in_position": False, "position_qty": 0, "cooldown_until": STATE.get("cooldown_until"),
                "daily_trades": STATE.get("daily_trades"), "last_trade_day": STATE.get("last_trade_day"),
                "consecutive_losses": STATE.get("consecutive_losses"),
                "daily_peak_balance": STATE.get("daily_peak_balance"), "daily_loss_limit_hit": STATE.get("daily_loss_limit_hit")})
    if prev_side == "long":  wait_for_next_signal_side = "sell"
    elif prev_side == "short": wait_for_next_signal_side = "buy"
    else: wait_for_next_signal_side = None
    logging.info(f"AFTER_CLOSE waiting_for={wait_for_next_signal_side}")

def log_trade_open(side, entry, qty, leverage, strategy, heat, adx, rsi, balance):
    print(f"""
🚀 NEW TRADE OPENED
────────────────────────────────
{'🟢' if side.upper() == 'BUY' else '🔴'} SIDE        : {side.upper()}
💰 ENTRY       : {entry:.5f}
📦 SIZE        : {qty:.4f}
⚡ LEVERAGE    : {leverage}x
🎯 STRATEGY    : {strategy}
🔥 HEAT SCORE  : {heat}
📊 ADX         : {adx:.1f}
📉 RSI         : {rsi:.1f}
🏦 BALANCE     : {balance:.2f} USDT
────────────────────────────────
""", flush=True)

def log_live_position(side, entry, price, pnl_pct, profit_usdt, balance, breakdown=None):
    s = f"""
📊 LIVE POSITION
────────────────────────────────
{'🟢' if side.upper() == 'BUY' else '🔴'} SIDE      : {side.upper()}
💰 ENTRY     : {entry:.5f}
📈 PRICE     : {price:.5f}
📊 PNL       : {pnl_pct:.2f} %
💵 PROFIT    : {profit_usdt:.2f} USDT
🏦 BALANCE   : {balance:.2f}"""
    if breakdown:
        s += f"""
🔥 HEAT      : {breakdown}"""
    s += """
────────────────────────────────
"""
    print(s, flush=True)

def log_trade_close(side, entry, exit_price, pnl_pct, pnl_usdt, balance):
    print(f"""
🏁 TRADE CLOSED
────────────────────────────────
{'🟢' if side.upper() == 'BUY' else '🔴'} SIDE        : {side.upper()}
💰 ENTRY       : {entry:.5f}
💰 EXIT        : {exit_price:.5f}
📉 RESULT      : {pnl_pct:.2f} %
💸 PROFIT/LOSS : {pnl_usdt:.2f} USDT
🏦 BALANCE     : {balance:.2f}
────────────────────────────────
""", flush=True)

def cooldown_active():
    if STATE["cooldown_until"] is None:
        return False
    return datetime.now(timezone.utc) < STATE["cooldown_until"]

def emergency_kill_switch_active():
    if STATE.get("daily_loss_limit_hit"):
        return True
    bal = balance_usdt()
    if bal is None:
        return False
    peak = STATE.get("daily_peak_balance")
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if STATE.get("last_trade_day") != today_str:
        STATE["daily_peak_balance"] = bal
        STATE["daily_loss_limit_hit"] = False
    else:
        if peak is None:
            STATE["daily_peak_balance"] = bal
        else:
            if bal > peak:
                STATE["daily_peak_balance"] = bal
            loss_pct = (peak - bal) / peak * 100 if peak > 0 else 0
            if loss_pct >= MAX_DAILY_LOSS_PCT:
                STATE["daily_loss_limit_hit"] = True
                log_warn(f"🚨 EMERGENCY KILL SWITCH: Daily loss {loss_pct:.2f}% >= {MAX_DAILY_LOSS_PCT}%")
                return True
    return False

def clean_cache():
    for key in list(CACHE.keys()):
        if len(CACHE[key]) > 50:
            CACHE[key].clear()
            log_i(f"Cache cleaned for {key}")
    for kind in DATA_CACHE:
        if len(DATA_CACHE[kind]) > 50:
            DATA_CACHE[kind].clear()
            log_i(f"Data cache cleaned for {kind}")

def manage_profit_system(price, ind):
    global STATE
    if not STATE.get("open"):
        return
    # Additional safety: ensure entry is not None before proceeding
    entry = STATE.get("entry")
    if entry is None:
        log_warn("manage_profit_system: entry is None, skipping")
        return
    pos = {
        "entry": entry,
        "side": STATE.get("side", "").upper(),
        "qty": STATE.get("qty", 0)
    }
    df = fetch_ohlcv_cached()
    smart_exit(df, pos)

def trade_loop():
    global wait_for_next_signal_side, compound_pnl, STATE, _scan_idx, bot_state, LAST_LOG, LAST_FULL_SCAN, SCAN_LIST, SYMBOLS, LAST_RADAR_TIME
    last_scan_time = 0
    last_snapshot_time = 0
    last_clean_time = time.time()
    last_sniper_tick = 0
    last_radar_time = 0
    try:
        new_symbols = macro_scan_all_symbols(SYMBOLS)
        if new_symbols:
            SYMBOLS = new_symbols
            SCAN_LIST = update_scan_list(SYMBOLS, 0)
    except Exception as e:
        log_error(f"Initial macro scan failed: {e}")
    LAST_FULL_SCAN = time.time()
    scan_step = 0
    while True:
        try:
            sync_account_state()

            now = time.time()
            if now - last_clean_time > 600:
                clean_cache()
                last_clean_time = now
            bal = balance_usdt()
            if bal is not None:
                STATE["balance"] = bal
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                if STATE.get("last_trade_day") != today_str:
                    STATE["daily_peak_balance"] = bal
                    STATE["daily_loss_limit_hit"] = False
                else:
                    if STATE["daily_peak_balance"] is None or bal > STATE["daily_peak_balance"]:
                        STATE["daily_peak_balance"] = bal
            if emergency_kill_switch_active():
                log_warn("⛔ Emergency Kill Switch active, skipping trading for the day")
                time.sleep(60)
                continue

            # Radar Engine (every 20 minutes)
            if now - last_radar_time >= 1200:
                if SYMBOLS:
                    radar_engine(SYMBOLS[:50])
                    last_radar_time = now
                    log("🔎 RADAR SCAN COMPLETED")

            # ✅ ADDED: Sniper Only Modification - Queue Processing (Non‑Blocking)
            process_sniper_queue()

            # Sniper Engine (every loop) - now only adds to queue, does not execute
            if not has_open_position() and WATCHLIST:
                snipes = sniper_engine()
                for sym, score in snipes:
                    log_g(f"🎯 SNIPER DETECTED → {sym} | score={score}")
                    df = get_ohlcv_safe(sym, timeframe=INTERVAL, limit=120)
                    if df is None or df.empty:
                        continue

                    # Determine side via evaluate_sniper
                    signal = evaluate_sniper(sym, df)
                    if not signal:
                        continue
                    side = "BUY" if signal == "LONG" else "SELL"

                    # Calculate next candle close time
                    sec_to_close = time_to_candle_close(df)
                    next_recheck = time.time() + sec_to_close + 5  # add small buffer

                    # ✅ ADDED: Sniper Queue Insert (Non‑Blocking + Visual Tag)
                    add_to_sniper_queue(sym, side, score, next_recheck)
                    log(f"👁️ SNIPER QUEUED: {sym} | side={side} | score={score} | recheck in ~{sec_to_close}s")

            if now - LAST_FULL_SCAN >= GLOBAL_SCAN_INTERVAL:
                try:
                    new_symbols = macro_scan_all_symbols(SYMBOLS)
                    if new_symbols:
                        SYMBOLS = new_symbols
                except Exception as e:
                    log_error(f"Macro scan failed: {e}")
                LAST_FULL_SCAN = now
                scan_step = 0
            if SYMBOLS:
                scan_step = (scan_step + 1) % (len(SYMBOLS) // 30 + 1)
                SCAN_LIST = update_scan_list(SYMBOLS, scan_step)

            if has_open_position():
                df = fetch_ohlcv_cached()
                px = price_now()
                if px is not None:
                    STATE["current_symbol"] = SYMBOL
                    ind = compute_indicators_cached(df)
                    manage_profit_system(px, ind)
                if now - LAST_LOG > LOG_INTERVAL:
                    if px:
                        # --- FIX: Guard against None entry ---
                        entry = STATE.get("entry")
                        qty = STATE.get("remaining_qty") or STATE.get("qty")
                        side = STATE.get("side")
                        if entry is None or qty is None or side is None:
                            log_warn("Invalid position state detected, resetting")
                            _reset_after_close("invalid_state")
                        else:
                            try:
                                if side == "long":
                                    pnl_usdt = (px - entry) * qty
                                else:
                                    pnl_usdt = (entry - px) * qty
                                pnl_pct = (pnl_usdt / (entry * qty)) * 100 if entry and qty else 0
                                balance = balance_usdt() or 0
                                color = C.GREEN if side == "long" else C.RED
                                print(f"{color}\n{'='*50}\n📊 LIVE POSITION\n{'='*50}\n"
                                      f"SIDE      : {side.upper()}\n"
                                      f"ENTRY     : {entry:.6f}\n"
                                      f"PRICE     : {px:.6f}\n"
                                      f"PNL       : {pnl_pct:.2f}%\n"
                                      f"PROFIT    : {pnl_usdt:.2f} USDT\n"
                                      f"BALANCE   : {balance:.2f}\n"
                                      f"{'='*50}{C.RESET}\n", flush=True)
                            except Exception as e:
                                log_e(f"PnL calculation error: {e}, resetting state")
                                _reset_after_close("calc_error")
                    LAST_LOG = now
                time.sleep(BASE_SLEEP)
                continue

            if now - last_scan_time >= SCAN_INTERVAL:
                btc_trend, btc_change = detect_btc_trend()
                bot_state["btc_trend"] = btc_trend
                bot_state["btc_1h_change"] = btc_change
                if cooldown_active() or not can_trade_cooldown():
                    bot_state["last_reject_reason"] = "Cooldown active"
                    log_i("Cooldown active, skipping scan")
                    last_scan_time = now
                    time.sleep(BASE_SLEEP)
                    continue
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                if STATE.get("last_trade_day") != today_str:
                    STATE["daily_trades"] = 0
                    STATE["last_trade_day"] = today_str
                trade_opened = False
                if SCAN_LIST:
                    for sym in SCAN_LIST[:SCAN_BATCH]:
                        df = get_ohlcv_safe(sym, timeframe=INTERVAL, limit=120)
                        if df is None or len(df) < 50:
                            continue
                        update_watchlist(sym, df)
                        signal = evaluate_sniper(sym, df)
                        if signal:
                            price = df["close"].iloc[-1]
                            qty = calculate_position_size_real(sym, price, build_score(df))
                            if qty > 0 and has_sufficient_margin(qty, price):
                                if update_symbol(sym):
                                    log_g(f"🔥 SNIPER ENTRY → {sym} {signal}")
                                    success = open_market_enhanced(
                                        "buy" if signal == "LONG" else "sell",
                                        qty, price,
                                        source="SNIPER_INSTITUTIONAL",
                                        df=df,
                                        institutional_score=build_score(df),
                                        breakdown={"engine": "sniper"}
                                    )
                                    if success:
                                        bot_state["best_symbol"] = sym
                                        bot_state["best_score"] = build_score(df)
                                        trade_opened = True
                                        break
                if not trade_opened and SMART_PIPELINE_ENABLED:
                    trade_opened = smart_scan_and_trade()
                last_scan_time = now
                if trade_opened:
                    continue
            if now - last_snapshot_time >= SNAPSHOT_INTERVAL:
                snapshot()
                last_snapshot_time = now
            time.sleep(BASE_SLEEP)
        except Exception as e:
            log_e(f"loop error: {e}\n{traceback.format_exc()}")
            log_error(f"trade_loop error: {e}\n{traceback.format_exc()}")
            logging.error(f"trade_loop error: {e}\n{traceback.format_exc()}")
            tg_error(e)
            time.sleep(BASE_SLEEP)

def smart_memory_manager():
    pass

def build_thinking():
    return []

# =================== FLASK DASHBOARD ===================
app = Flask(__name__)
START_TIME = time.time()

@app.route("/")
def dashboard():
    return """
<!DOCTYPE html>
<html>
<head>
<title>SNIPER BOT PRO v17.3</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>

<style>
body{background:#0b0f14;color:#e6edf3;font-family:Consolas;margin:0}
.header{padding:14px 16px;background:#111827;color:#00ff9f;font-size:22px;position:sticky;top:0}
.section{padding:12px 14px;border-bottom:1px solid #1f2937}
.title{color:#9ca3af;margin-bottom:6px}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
.card{background:#111827;border-radius:10px;padding:10px}
.big{font-size:20px}
.green{color:#00ffa6}
.red{color:#ff4d4d}
.yellow{color:#ffd166}
.log, .err{max-height:220px;overflow:auto;white-space:pre-wrap}
.row{display:flex;justify-content:space-between;margin:4px 0}
.badge{padding:2px 6px;border-radius:6px;font-size:12px}
.b-strong{background:#052e2b;color:#00ffa6}
.b-medium{background:#3a2e05;color:#ffd166}
.b-weak{background:#2e0505;color:#ff6b6b}
.btns{display:flex;gap:8px}
button{flex:1;padding:10px;border:0;border-radius:8px;font-weight:bold}
.buy{background:#00c853}.sell{background:#ff1744}.close{background:#ffd600}
.kpi{font-size:18px}
.small{font-size:12px;color:#9ca3af}
</style>
</head>

<body>

<div class="header">🚀 SNIPER BOT v17.3 — INSTITUTIONAL HUNTER</div>

<!-- ACCOUNT -->
<div class="section">
  <div class="title">💰 ACCOUNT</div>
  <div class="grid">
    <div class="card"><div class="small">Balance</div><div id="bal" class="kpi">-</div></div>
    <div class="card"><div class="small">Free</div><div id="free" class="kpi">-</div></div>
    <div class="card"><div class="small">Used</div><div id="used" class="kpi">-</div></div>
    <div class="card"><div class="small">Mode</div><div id="mode" class="kpi">-</div></div>
  </div>
</div>

<!-- STATS -->
<div class="section">
  <div class="title">📊 PERFORMANCE</div>
  <div class="grid">
    <div class="card"><div class="small">Trades</div><div id="trades" class="kpi">0</div></div>
    <div class="card"><div class="small">Wins</div><div id="wins" class="kpi green">0</div></div>
    <div class="card"><div class="small">Losses</div><div id="losses" class="kpi red">0</div></div>
    <div class="card"><div class="small">Total PnL</div><div id="ptotal" class="kpi">0</div></div>
  </div>
</div>

<!-- LIVE POSITION -->
<div class="section">
  <div class="title">📌 LIVE POSITION</div>
  <div id="pos" class="card big">No Trade</div>
</div>

<!-- SCANNER / TOP 5 -->
<div class="section">
  <div class="title">🧠 SCANNER — TOP 5</div>
  <div class="small">Last update: <span id="scanTime">-</span></div>
  <div id="opps"></div>
</div>

<!-- MONITORING -->
<div class="section">
  <div class="title">📡 MONITORING</div>
  <div id="monitor"></div>
</div>

<!-- CONTROLS -->
<div class="section">
  <div class="title">🎮 CONTROLS</div>
  <div class="btns">
    <button class="buy">BUY</button>
    <button class="sell">SELL</button>
    <button class="close">CLOSE</button>
  </div>
</div>

<!-- LOGS -->
<div class="section">
  <div class="title">📜 EXECUTION LOG</div>
  <div id="logs" class="card log"></div>
</div>

<!-- ERRORS -->
<div class="section">
  <div class="title">⚠ SYSTEM ERRORS</div>
  <div id="errors" class="card err"></div>
</div>

<script>
let lastErrors = "";

function errorSound(){
  new Audio("https://actions.google.com/sounds/v1/alarms/alarm_clock.ogg").play();
}
function tradeSound(){
  new Audio("https://actions.google.com/sounds/v1/cartoon/clang_and_wobble.ogg").play();
}

function zoneBadge(z){
  if(z==="STRONG") return '<span class="badge b-strong">STRONG</span>';
  if(z==="MEDIUM") return '<span class="badge b-medium">MEDIUM</span>';
  return '<span class="badge b-weak">WEAK</span>';
}

async function fetchData(){
  const r = await fetch('/data');
  const d = await r.json();

  document.getElementById("bal").innerText = d.account.balance.toFixed(2);
  document.getElementById("free").innerText = d.account.free.toFixed(2);
  document.getElementById("used").innerText = d.account.used.toFixed(2);
  document.getElementById("mode").innerText = d.account.mode;

  document.getElementById("trades").innerText = d.stats.trades;
  document.getElementById("wins").innerText = d.stats.wins;
  document.getElementById("losses").innerText = d.stats.losses;
  document.getElementById("ptotal").innerText = d.stats.profit_total.toFixed(2) + " USDT";

  if(d.position){
    const p = d.position;
    const color = p.pnl_pct >= 0 ? "green" : "red";
    document.getElementById("pos").innerHTML =
      `Symbol: ${p.symbol}<br>
       Side: ${p.side}<br>
       Entry: ${p.entry}<br>
       Price: ${p.price}<br>
       PNL: <span class="${color}">${p.pnl_pct}%</span><br>
       Profit: <span class="${color}">${p.profit} USDT</span>`;
  } else {
    document.getElementById("pos").innerText = "No Trade";
  }

  document.getElementById("scanTime").innerText = d.scanner.last_update || "-";
  let html = "";
  (d.scanner.top5 || []).forEach(o=>{
    const sColor = o.suggest === "READY" ? "green" : (o.suggest === "WATCH" ? "yellow" : "");
    html += `
      <div class="card">
        <div class="row">
          <div><b>${o.symbol}</b></div>
          <div>Score: <b>${o.score}</b></div>
        </div>
        <div class="row">
          <div>Zone: ${zoneBadge(o.zone)}</div>
          <div class="${sColor}">${o.suggest}</div>
        </div>
        <div class="small">${o.reason || ""}</div>
      </div>
    `;
  });
  document.getElementById("opps").innerHTML = html || "<div class='small'>No opportunities</div>";

  // Monitoring
  const mon = d.monitor || { errors: [], warnings: [] };
  let monHtml = "";
  mon.errors.forEach(e => monHtml += `<div style="color:#ff6b6b">🔴 ${e}</div>`);
  mon.warnings.forEach(w => monHtml += `<div style="color:#ffd166">🟡 ${w}</div>`);
  document.getElementById("monitor").innerHTML = monHtml || "<div class='small'>No alerts</div>";

  document.getElementById("logs").innerHTML = (d.logs || []).join("<br>");

  const errs = (d.errors || []).join("<br>");
  if(errs !== lastErrors && d.errors.length){
    errorSound();
  }
  lastErrors = errs;
  document.getElementById("errors").innerHTML = errs;
}

setInterval(fetchData, 2000);
</script>

</body>
</html>
    """

@app.route("/data")
def data():
    resp = DASHBOARD_STATE.copy()
    resp["monitor"] = get_monitoring_data()
    return jsonify(resp)

@app.route("/api/action", methods=["POST"])
def api_action():
    action = request.json.get("action", "").upper()
    if action == "BUY":
        if has_open_position():
            return jsonify({"status": "error", "reason": "Position already open"}), 400
        df = fetch_ohlcv_cached()
        if df is None or len(df) < 20:
            return jsonify({"status": "error", "reason": "No data"}), 400
        price = price_now()
        if not price:
            return jsonify({"status": "error", "reason": "No price"}), 400
        qty = calculate_position_size_real(SYMBOL, price, 20)
        if qty <= 0:
            return jsonify({"status": "error", "reason": "Invalid size"}), 400
        success = open_market_enhanced("buy", qty, price, source="MANUAL_BUY", df=df, institutional_score=20)
        if success:
            return jsonify({"status": "ok", "message": "Buy order executed"})
        else:
            return jsonify({"status": "error", "reason": "Execution failed"}), 500
    elif action == "SELL":
        if has_open_position():
            return jsonify({"status": "error", "reason": "Position already open"}), 400
        df = fetch_ohlcv_cached()
        if df is None or len(df) < 20:
            return jsonify({"status": "error", "reason": "No data"}), 400
        price = price_now()
        if not price:
            return jsonify({"status": "error", "reason": "No price"}), 400
        qty = calculate_position_size_real(SYMBOL, price, 20)
        if qty <= 0:
            return jsonify({"status": "error", "reason": "Invalid size"}), 400
        success = open_market_enhanced("sell", qty, price, source="MANUAL_SELL", df=df, institutional_score=20)
        if success:
            return jsonify({"status": "ok", "message": "Sell order executed"})
        else:
            return jsonify({"status": "error", "reason": "Execution failed"}), 500
    elif action == "CLOSE":
        if not has_open_position():
            return jsonify({"status": "error", "reason": "No open position"}), 400
        success = strict_close_position("MANUAL_CLOSE")
        if success:
            return jsonify({"status": "ok", "message": "Position closed"})
        else:
            return jsonify({"status": "error", "reason": "Close failed"}), 500
    else:
        return jsonify({"status": "error", "reason": "Invalid action"}), 400

@app.route("/stats")
def stats():
    try:
        trades_count = len(trade_history)
        total_pnl = sum(t["pnl"] for t in trade_history)
        winrate = (paper["wins"] / trades_count * 100) if trades_count > 0 else 0
        if MODE_LIVE and not PAPER_MODE:
            current_balance = get_balance(ex)
            mode_str = "LIVE"
        else:
            current_balance = paper["balance"]
            mode_str = "PAPER"
        response = {
            "balance": current_balance,
            "mode": mode_str,
            "profit": total_pnl,
            "trades": trades_count,
            "wins": paper["wins"],
            "losses": paper["losses"],
            "winrate": round(winrate, 2),
            "history": trade_history,
            "open_trade": paper["position"]
        }
        return jsonify(make_serializable(response))
    except Exception as e:
        log_error(f"/stats error: {e}")
        return {"error": str(e)}, 500

@app.route("/bot_state")
def bot_state_api():
    try:
        bot_state["balance"] = get_balance(ex)
        bot_state["mode"] = "LIVE" if MODE_LIVE and not PAPER_MODE else "PAPER"
        watchlist = []
        top_opps = bot_state.get("top_opportunities", [])
        if not top_opps and TOP_SYMBOLS:
            fallback_symbols = TOP_SYMBOLS[:3]
            for sym in fallback_symbols:
                df = get_ohlcv_safe(sym, timeframe=INTERVAL, limit=120)
                if df is None or len(df) < 20:
                    continue
                side, _, score, _ = decide_entry(df)
                if not side:
                    continue
                support, resistance, dist_sup, dist_res = get_support_resistance_simple(df)
                price = get_ticker_safe(sym) or df['close'].iloc[-1]
                if dist_sup is not None and dist_sup < 1.0:
                    rec = "BUY ZONE"
                elif dist_res is not None and dist_res < 1.0:
                    rec = "SELL ZONE"
                else:
                    rec = "NEUTRAL"
                ind = compute_indicators(df)
                watchlist.append({
                    "symbol": sym,
                    "price": price,
                    "support": support,
                    "resistance": resistance,
                    "dist_to_support_pct": round(dist_sup, 2) if dist_sup is not None else None,
                    "dist_to_resistance_pct": round(dist_res, 2) if dist_res is not None else None,
                    "recommendation": rec,
                    "rec_color": "green" if rec == "BUY ZONE" else ("red" if rec == "SELL ZONE" else "orange"),
                    "heat_score": score,
                    "side": side.upper(),
                    "adx": ind.get("adx", 0),
                    "plus_di": ind.get("plus_di", 0),
                    "minus_di": ind.get("minus_di", 0),
                    "total_score": score,
                    "entry_score": score,
                    "pre_score": 0,
                    "sm_score": 0,
                    "trap_score": 0,
                    "reason": "fallback"
                })
        else:
            for opp in top_opps[:3]:
                sym = opp.get("symbol")
                if not sym:
                    continue
                df = get_ohlcv_safe(sym, timeframe=INTERVAL, limit=120)
                if df is None or len(df) < 20:
                    continue
                support, resistance, dist_sup, dist_res = get_support_resistance_simple(df)
                price = get_ticker_safe(sym) or df['close'].iloc[-1]
                if dist_sup is not None and dist_sup < 1.0:
                    rec = "BUY ZONE"
                elif dist_res is not None and dist_res < 1.0:
                    rec = "SELL ZONE"
                else:
                    rec = "NEUTRAL"
                watchlist.append({
                    "symbol": sym,
                    "price": price,
                    "support": support,
                    "resistance": resistance,
                    "dist_to_support_pct": round(dist_sup, 2) if dist_sup is not None else None,
                    "dist_to_resistance_pct": round(dist_res, 2) if dist_res is not None else None,
                    "recommendation": rec,
                    "rec_color": "green" if rec == "BUY ZONE" else ("red" if rec == "SELL ZONE" else "orange"),
                    "heat_score": opp.get("total_score", 0),
                    "side": opp.get("side", "N/A").upper(),
                    "adx": opp.get("adx", 0),
                    "plus_di": opp.get("plus_di", 0),
                    "minus_di": opp.get("minus_di", 0),
                    "total_score": opp.get("total_score", 0),
                    "entry_score": opp.get("entry_score", 0),
                    "pre_score": opp.get("pre_score", 0),
                    "sm_score": opp.get("sm_score", 0),
                    "trap_score": opp.get("trap_score", 0),
                    "reason": opp.get("reason", "")
                })
        bot_state["watchlist"] = watchlist
        serializable_state = make_serializable(bot_state)
        return jsonify(serializable_state)
    except Exception as e:
        log_error(f"/bot_state error: {e}")
        return {"error": str(e)}, 500

def get_support_resistance_simple(df, window=20):
    if df is None or len(df) < window:
        return None, None, None, None
    support = df['low'].tail(window).min()
    resistance = df['high'].tail(window).max()
    price = df['close'].iloc[-1]
    dist_to_support = (price - support) / price * 100 if price != 0 else 0
    dist_to_resistance = (resistance - price) / price * 100 if price != 0 else 0
    return support, resistance, dist_to_support, dist_to_resistance

@app.route("/smart_view")
def smart_view():
    return jsonify({
        "watchlist": bot_state.get("watchlist", []),
        "thinking": build_thinking(),
        "top": bot_state.get("top_opportunities", [])
    })

@app.route("/zones")
def zones():
    return jsonify(bot_state.get("zone_watchlist", []))

@app.route("/smart_monitor")
def smart_monitor():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>🧠 Smart Monitor</title>
        <style>
            body { background: #0a0a0a; color: #eee; font-family: monospace; margin: 20px; }
            .zone-card { padding: 10px; border-radius: 8px; margin: 5px; display: inline-block; width: 300px; vertical-align: top; }
            .demand { background: rgba(0,255,0,0.1); border: 1px solid #0f0; }
            .supply { background: rgba(255,0,0,0.1); border: 1px solid #f00; }
            .thinking { background: #111; border-left: 3px solid #ffaa00; padding: 8px; margin: 5px 0; }
            .decision { background: #1e1e2e; padding: 10px; border-radius: 6px; margin-top: 20px; }
            .radar { background: #222; padding: 10px; border-radius: 6px; margin-top: 20px; }
            h2, h3 { color: #ffaa00; }
            table { width: 100%; border-collapse: collapse; }
            th, td { text-align: left; padding: 5px; border-bottom: 1px solid #333; }
            .score { font-weight: bold; }
        </style>
    </head>
    <body>
        <h2>🧠 Smart Monitor</h2>
        <div id="thinking" class="decision">
            <h3>🧠 Bot Thinking</h3>
            <div id="thinking_list"></div>
        </div>
        <div id="watchlist" class="radar">
            <h3>📡 Watchlist</h3>
            <div id="watchlist_cards"></div>
        </div>
        <div id="radar" class="radar">
            <h3>📡 Signal Radar</h3>
            <div id="radar_table"></div>
        </div>
        <script>
            async function refresh() {
                const res = await fetch('/smart_view');
                const data = await res.json();

                let thinkingHtml = '';
                if (data.thinking.length) {
                    data.thinking.forEach(t => {
                        thinkingHtml += `<div class="thinking">🤔 ${t}</div>`;
                    });
                } else {
                    thinkingHtml = '<div class="thinking">🤖 No active thinking yet. Scanning...</div>';
                }
                document.getElementById('thinking_list').innerHTML = thinkingHtml;

                let cardsHtml = '';
                for (let w of data.watchlist.slice(0,10)) {
                    let zoneClass = '';
                    let zoneText = '';
                    if (w.zone) {
                        zoneClass = w.zone.type === 'demand' ? 'demand' : 'supply';
                        zoneText = `${w.zone.type.toUpperCase()} zone (score=${w.zone.score || '?'})`;
                    } else {
                        zoneText = 'No zone';
                    }
                    cardsHtml += `
                    <div class="zone-card ${zoneClass}">
                        <div><strong>${w.symbol}</strong></div>
                        <div>Price: ${w.price.toFixed(6)}</div>
                        <div>${zoneText}</div>
                        <div>Total Score: ${w.score.toFixed(1)}</div>
                        <div>Status: ${w.status}</div>
                        <div style="font-size:12px">Pre=${w.pre} | SM=${w.sm} | Trap=${w.trap}</div>
                    </div>`;
                }
                document.getElementById('watchlist_cards').innerHTML = cardsHtml || '<div>No watchlist items</div>';

                let radarHtml = '<table><tr><th>Symbol</th><th>Score</th><th>Pre</th><th>SM</th><th>Trap</th><th>Side</th></tr>';
                for (let opp of data.top.slice(0,5)) {
                    radarHtml += `<tr>
                        <td>${opp.symbol}</td>
                        <td class="score">${opp.total_score.toFixed(1)}</td>
                        <td>${opp.pre_score}</td>
                        <td>${opp.sm_score}</td>
                        <td>${opp.trap_score}</td>
                        <td>${opp.side}</td>
                    </tr>`;
                }
                radarHtml += '</table>';
                document.getElementById('radar_table').innerHTML = radarHtml;
            }
            setInterval(refresh, 3000);
            refresh();
        </script>
    </body>
    </html>
    """

@app.route("/dashboard")
def paper_dashboard():
    try:
        trades = len(paper["trades"])
        total_pnl = sum(paper["trades"])
        winrate = (paper["wins"] / trades * 100) if trades > 0 else 0
        response = {
            "balance": paper["balance"],
            "start_balance": paper["start_balance"],
            "pnl": total_pnl,
            "trades": trades,
            "wins": paper["wins"],
            "losses": paper["losses"],
            "winrate": round(winrate, 2),
            "open_position": paper["position"]
        }
        return jsonify(make_serializable(response))
    except Exception as e:
        log_error(f"/dashboard error: {e}")
        return {"error": str(e)}, 500

@app.route("/ui")
def paper_ui():
    return """
    <!DOCTYPE html>
    <html>
    <head><title>Paper Trading Dashboard</title></head>
    <body>
        <h2>Paper Trading Dashboard</h2>
        <div id="data">Loading...</div>
        <script>
            async function load() {
                const res = await fetch('/dashboard');
                const data = await res.json();
                document.getElementById('data').innerHTML = `
                    <p><strong>Balance:</strong> $${data.balance.toFixed(2)}</p>
                    <p><strong>Start Balance:</strong> $${data.start_balance.toFixed(2)}</p>
                    <p><strong>Total PnL:</strong> $${data.pnl.toFixed(2)}</p>
                    <p><strong>Trades:</strong> ${data.trades}</p>
                    <p><strong>Wins:</strong> ${data.wins}</p>
                    <p><strong>Losses:</strong> ${data.losses}</p>
                    <p><strong>Winrate:</strong> ${data.winrate}%</p>
                    <p><strong>Open Position:</strong> ${data.open_position ? JSON.stringify(data.open_position) : 'None'}</p>
                `;
            }
            setInterval(load, 2000);
            load();
        </script>
    </body>
    </html>
    """

@app.route("/metrics")
def metrics():
    try:
        state_serial = make_serializable(STATE)
        response = {
            "symbol": SYMBOL, "interval": INTERVAL, "mode": "live" if MODE_LIVE else "paper",
            "leverage": LEVERAGE, "risk_alloc": RISK_ALLOC, "price": price_now(),
            "state": state_serial, "compound_pnl": compound_pnl,
            "entry_mode": "IMMEDIATE_SCAN", "wait_for_next_signal": wait_for_next_signal_side,
            "guards": {"max_spread_bps": MAX_SPREAD_BPS, "final_chunk_qty": FINAL_CHUNK_QTY},
            "cooldown_until": STATE["cooldown_until"].isoformat() if STATE["cooldown_until"] else None,
            "daily_trades": STATE.get("daily_trades", 0),
            "max_daily_trades": MAX_TRADES_PER_DAY,
            "consecutive_losses": STATE.get("consecutive_losses", 0),
            "max_consecutive_losses": MAX_CONSECUTIVE_LOSSES,
            "daily_peak_balance": STATE.get("daily_peak_balance"),
            "daily_loss_limit_hit": STATE.get("daily_loss_limit_hit", False)
        }
        return jsonify(make_serializable(response))
    except Exception as e:
        log_error(f"/metrics error: {e}")
        return {"error": str(e)}, 500

@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "uptime": int(time.time() - START_TIME)
    })

def keep_alive():
    while True:
        try:
            if CONFIG.SELF_URL:
                import requests
                requests.get(CONFIG.SELF_URL + "/health")
        except:
            pass
        time.sleep(300)  # كل 5 دقائق

def tg_send_start():
    try:
        import requests
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={
                "chat_id": TG_CHAT,
                "text": "🚀 BOT STARTED SUCCESSFULLY"
            },
            timeout=5
        )
    except:
        pass

tg_send_start()

@app.route("/reset_paper", methods=["POST"])
def reset_paper():
    global paper, trade_history
    paper = {
        "balance": 1000.0,
        "start_balance": 1000.0,
        "position": None,
        "trades": [],
        "wins": 0,
        "losses": 0
    }
    trade_history = []
    return jsonify({"status": "paper_reset", "balance": paper["balance"]})

def initialize_bot():
    global SYMBOLS, STATE, compound_pnl, wait_for_next_signal_side, bot_state, TOP_SYMBOLS, SCAN_LIST
    log_banner("INIT v17.3")
    load_stats()
    DASHBOARD_STATE["stats"]["trades"] = STATS["trades"]
    DASHBOARD_STATE["stats"]["wins"] = STATS["wins"]
    DASHBOARD_STATE["stats"]["losses"] = STATS["losses"]
    DASHBOARD_STATE["stats"]["profit_total"] = STATS["total_pnl"]

    state = load_state() or {}
    state.setdefault("in_position", False)
    if state.get("in_position"):
        STATE["open"] = True
        STATE["side"] = state.get("side", "").lower()
        STATE["entry"] = state.get("entry_price", 0.0)
        STATE["qty"] = state.get("position_qty", 0.0)
        STATE["remaining_qty"] = state.get("remaining_qty", STATE["qty"])
        STATE["tp1_done"] = state.get("tp1_done", False)
        STATE["leverage"] = state.get("leverage", LEVERAGE)
        STATE["trail_activated"] = state.get("trail_activated", False)
        STATE["trail_stop"] = state.get("trail_stop", None)
        STATE["trail_multiplier"] = state.get("trail_multiplier", TRAIL_ATR_MULT)
        STATE["highest_profit_pct"] = state.get("highest_profit_pct", 0.0)
        STATE["mode"] = state.get("mode", "trend")
        STATE["source"] = state.get("source", "Unknown")
        STATE["opened_at"] = state.get("opened_at", int(time.time()))
        STATE["cooldown_until"] = state.get("cooldown_until")
        STATE["daily_trades"] = state.get("daily_trades", 0)
        STATE["last_trade_day"] = state.get("last_trade_day")
        STATE["consecutive_losses"] = state.get("consecutive_losses", 0)
        STATE["signal_strength"] = state.get("signal_strength", "MEDIUM")
        STATE["entry_score"] = state.get("entry_score", 0)
        STATE["heat_score"] = state.get("heat_score", 0)
        STATE["heat_breakdown"] = state.get("heat_breakdown", {})
        STATE["current_market_regime"] = state.get("current_market_regime", "range")
        STATE["supply_demand_trigger"] = state.get("supply_demand_trigger", False)
        STATE["trend"] = state.get("trend")
        STATE["protected"] = state.get("protected", False)
        STATE["tp1"] = state.get("tp1", False)
        STATE["tp1_wait"] = state.get("tp1_wait", False)
        STATE["peak"] = state.get("peak", 0.0)
        STATE["target_price"] = state.get("target_price", None)
        STATE["tp1_hit"] = state.get("tp1_hit", False)
        STATE["tp2_hit"] = state.get("tp2_hit", False)
    else:
        STATE.update({
            "open": False, "side": None, "entry": 0.0, "qty": 0.0, "remaining_qty": 0.0,
            "tp1_done": False, "trail_stop": None, "trail_activated": False,
            "daily_trades": state.get("daily_trades", 0),
            "last_trade_day": state.get("last_trade_day"),
            "consecutive_losses": state.get("consecutive_losses", 0),
            "daily_peak_balance": state.get("daily_peak_balance"),
            "daily_loss_limit_hit": state.get("daily_loss_limit_hit", False),
            "cooldown_until": state.get("cooldown_until"),
            "heat_score": 0,
            "heat_breakdown": {},
            "current_market_regime": "range",
            "supply_demand_trigger": False,
            "trend": None,
            "protected": False,
            "tp1": False,
            "tp1_wait": False,
            "peak": 0.0,
            "target_price": None,
            "events": [],
            "errors": [],
            "price": 0.0,
            "trade": None,
            "signal": None,
            "tp1_hit": False,
            "tp2_hit": False,
        })
    bot_state["coins_scanned"] = 0
    try:
        log_i("Loading markets and building symbols...")
        all_symbols = build_symbols()
        if all_symbols:
            SYMBOLS = filter_liquid_symbols_fast(all_symbols, top_n=MAX_SCAN_COINS)
            log_g(f"🔎 Scanner loaded {len(all_symbols)} symbols, filtered to {len(SYMBOLS)} top liquid symbols")
        else:
            log_warn("No symbols loaded, using default list of 40 coins")
            SYMBOLS = [
                "BTC/USDT:USDT", "ETH/USDT:USDT", "BNB/USDT:USDT", "SOL/USDT:USDT",
                "XRP/USDT:USDT", "ADA/USDT:USDT", "DOGE/USDT:USDT", "AVAX/USDT:USDT",
                "LINK/USDT:USDT", "DOT/USDT:USDT", "MATIC/USDT:USDT", "SHIB/USDT:USDT",
                "LTC/USDT:USDT", "UNI/USDT:USDT", "ATOM/USDT:USDT", "ETC/USDT:USDT",
                "BCH/USDT:USDT", "FIL/USDT:USDT", "APT/USDT:USDT", "ARB/USDT:USDT",
                "OP/USDT:USDT", "NEAR/USDT:USDT", "INJ/USDT:USDT", "STX/USDT:USDT",
                "IMX/USDT:USDT", "RNDR/USDT:USDT", "SEI/USDT:USDT", "SUI/USDT:USDT",
                "FET/USDT:USDT", "GRT/USDT:USDT", "SAND/USDT:USDT", "MANA/USDT:USDT",
                "GALA/USDT:USDT", "AXS/USDT:USDT", "AAVE/USDT:USDT", "CRV/USDT:USDT",
                "SNX/USDT:USDT", "COMP/USDT:USDT", "MKR/USDT:USDT", "QNT/USDT:USDT"
            ]
    except Exception as e:
        log_warn(f"Error building symbols: {e}")
        SYMBOLS = [
            "BTC/USDT:USDT", "ETH/USDT:USDT", "BNB/USDT:USDT", "SOL/USDT:USDT",
            "XRP/USDT:USDT", "ADA/USDT:USDT", "DOGE/USDT:USDT", "AVAX/USDT:USDT",
            "LINK/USDT:USDT", "DOT/USDT:USDT", "MATIC/USDT:USDT", "SHIB/USDT:USDT",
            "LTC/USDT:USDT", "UNI/USDT:USDT", "ATOM/USDT:USDT", "ETC/USDT:USDT",
            "BCH/USDT:USDT", "FIL/USDT:USDT", "APT/USDT:USDT", "ARB/USDT:USDT",
            "OP/USDT:USDT", "NEAR/USDT:USDT", "INJ/USDT:USDT", "STX/USDT:USDT",
            "IMX/USDT:USDT", "RNDR/USDT:USDT", "SEI/USDT:USDT", "SUI/USDT:USDT",
            "FET/USDT:USDT", "GRT/USDT:USDT", "SAND/USDT:USDT", "MANA/USDT:USDT",
            "GALA/USDT:USDT", "AXS/USDT:USDT", "AAVE/USDT:USDT", "CRV/USDT:USDT",
            "SNX/USDT:USDT", "COMP/USDT:USDT", "MKR/USDT:USDT", "QNT/USDT:USDT"
        ]
    TOP_SYMBOLS = SYMBOLS[:30] if SYMBOLS else []
    SCAN_LIST = update_scan_list(SYMBOLS, 0) if SYMBOLS else []
    verify_execution_environment()
    mode = "🟢 LIVE" if (MODE_LIVE and not PAPER_MODE) else "🔴 PAPER"
    log(f"{mode} MODE ACTIVE")
    print(colored(f"MODE: {'LIVE' if MODE_LIVE else 'PAPER'}  •  {SYMBOL}  •  {INTERVAL}", "yellow"))
    print(colored(f"ENTRY: Smart Money Analyzer (SNIPER & TREND) v17.3", "yellow"))
    print(colored(f"FILTERS: ADX<15 NO TRADE | MID-RANGE AVOID | DISCOUNT/PREMIUM REQUIRED", "yellow"))
    print(colored(f"NEW v17.3: Safe Position, Watchlist, HTF, Fakeout Filter, Smart Exit, Memory, Adaptive Threshold, Berlin TZ", "green"))
    print(colored(f"INTEGRATED: Sniper Engine (Radar/Watchlist) + Monitoring + Auto-Recovery", "green"))
    print(colored(f"FIX: _ema defined globally – radar scanner operational", "green"))
    print(colored(f"FIX: ensure_leverage_mode uses side='ALL' for Hedge Mode compatibility", "green"))
    print(colored(f"ADDED: Entry Exhaustion Filter (RSI/StochK + Rejection) to prevent bad entries", "green"))
    print(colored(f"ADDED: Sniper Non‑Blocking Priority Queue + 👁️ Visual Tag", "green"))
    tg_boot()

if __name__ == "__main__":
    import threading
    port = CONFIG.PORT
    signal.signal(signal.SIGINT, lambda sig, frame: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda sig, frame: sys.exit(0))
    log(f"Starting web server on port {port}")
    def start_bot_async():
        try:
            initialize_bot()
            while True:
                try:
                    trade_loop()
                except Exception as e:
                    log_e(f"FATAL: trade_loop crashed: {e}\n{traceback.format_exc()}")
                    tg_error(e)
                    time.sleep(5)
        except Exception as e:
            log_e(f"FATAL ERROR in bot thread: {e}\n{traceback.format_exc()}")
            tg_error(e)
    bot_thread = threading.Thread(target=start_bot_async, daemon=True)
    bot_thread.start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
