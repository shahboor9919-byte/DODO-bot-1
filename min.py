# -*- coding: utf-8 -*-
"""
RF Futures Bot — PROFESSIONAL SNIPER EDITION v18.0 (Hybrid Liquidity Engine)
+ Sniper Engine + Monitoring + Auto-Recovery + Position Registry + Dual Scanner
STRATEGY OVERHAUL: Trend/Sniper separation, dynamic sizing, smart profit engine
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

def get_real_position_safe(exchange, symbol):
    """Safe position detection without crashing."""
    try:
        positions = exchange.fetch_positions()
        if not positions:
            return None
        for pos in positions:
            sym = pos.get("symbol") or pos.get("info", {}).get("symbol")
            if sym != symbol:
                continue
            size = float(pos.get("contracts", 0) or pos.get("positionAmt", 0))
            entry = float(pos.get("entryPrice", 0))
            if abs(size) < 1e-6:
                return None
            side = "LONG" if size > 0 else "SHORT"
            return {
                "size": abs(size),
                "entry": entry,
                "side": side
            }
        return None
    except Exception as e:
        log_error(f"❌ POSITION FETCH FAILED: {e}")
        return None

def get_real_position(exchange, symbol):
    return get_real_position_safe(exchange, symbol)

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
            pos = get_real_position_safe(exchange, symbol)
            if pos is None:
                log("✅ No position → already closed")
                return True

            size = abs(pos["size"])
            if size < 1e-6:
                log("⚠️ Dust position → ignore")
                return True

            side = "sell" if pos["side"] == "LONG" else "buy"
            position_side = pos["side"]

            amount = float(exchange.amount_to_precision(symbol, size))
            params = {"reduceOnly": True}

            mode = detect_position_mode()
            if mode == "hedge":
                params["positionSide"] = position_side

            order = exchange.create_order(
                symbol,
                "market",
                side,
                amount,
                params=params
            )

            log(f"✅ CLOSE SUCCESS → {symbol} (mode={mode})")
            return True

        except Exception as e:
            log(f"❌ CLOSE ATTEMPT {attempt+1} FAILED: {e}")
            time.sleep(1)

    return False

def force_close(symbol):
    try:
        positions = ex.fetch_positions()
        for p in positions:
            if p.get("symbol") == symbol and float(p.get("contracts", 0)) > 0:
                side = "long" if p.get("side", "").lower() == "long" else "short"
                amount = abs(float(p["contracts"]))
                log(f"⚠️ FORCE CLOSE TRIGGERED for {symbol} {side} {amount}")
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
        pos = get_real_position_safe(ex, SYMBOL)
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
API_KEY = os.getenv("BINGX_API_KEY", "")
API_SECRET = os.getenv("BINGX_API_SECRET", "")
MODE_LIVE = bool(API_KEY and API_SECRET) and not PAPER_MODE

SELF_URL = os.getenv("SELF_URL", "") or os.getenv("RENDER_EXTERNAL_URL", "")
PORT = int(os.getenv("PORT", 5000))

LOG_LEGACY = False
LOG_ADDONS = True

EXECUTE_ORDERS = True
SHADOW_MODE_DASHBOARD = False
DRY_RUN = False

BOT_VERSION = "PROFESSIONAL SNIPER v18.0 (Hybrid Liquidity Engine)"
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
LEVERAGE   = int(os.getenv("LEVERAGE", 3))  # v18.0 default 3x
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
    "trade_type": None,  # "TREND" or "SNIPER"
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

        pos = get_real_position_safe(ex, SYMBOL)
        if pos and abs(pos.get("size", 0)) > 0:
            STATE["open"] = True
            STATE["side"] = pos["side"].lower()
            STATE["entry"] = float(pos["entry"])
            STATE["qty"] = abs(pos["size"])
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
        params = {"side": "LONG" if side == "buy" else "SHORT"}
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
        order = ex.create_order(
            symbol,
            "market",
            side,
            qty
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
    log(f"🚀 v18.0: +Position Registry +Dual Scanner +Hybrid Decision +Portfolio Limits")
    log(f"🔥 INTEGRATED: Sniper Engine (Radar/Watchlist) + Monitoring + Auto-Recovery")
    log(f"🧠 STRATEGY OVERHAUL: Trend/Sniper separation, dynamic sizing, smart profit engine")
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

# =================== STRATEGY: TREND & SNIPER COUNCILS ===================
def compute_indicator_slopes(df):
    """Calculate slopes (current - previous) for key indicators."""
    if len(df) < 3:
        return {}
    adx_series = get_adx_series(df)
    di_plus_series = get_di_series(df, "plus")
    di_minus_series = get_di_series(df, "minus")
    
    adx_now = adx_series.iloc[-1] if len(adx_series) else 0
    adx_prev = adx_series.iloc[-2] if len(adx_series) > 1 else adx_now
    adx_slope = adx_now - adx_prev
    
    di_plus_now = di_plus_series.iloc[-1] if len(di_plus_series) else 0
    di_minus_now = di_minus_series.iloc[-1] if len(di_minus_series) else 0
    di_spread = di_plus_now - di_minus_now
    
    di_plus_prev = di_plus_series.iloc[-2] if len(di_plus_series) > 1 else di_plus_now
    di_minus_prev = di_minus_series.iloc[-2] if len(di_minus_series) > 1 else di_minus_now
    di_spread_prev = di_plus_prev - di_minus_prev
    di_momentum = di_spread - di_spread_prev
    
    rsi = compute_rsi(df['close'].astype(float), RSI_LEN)
    rsi_now = rsi.iloc[-1]
    rsi_prev = rsi.iloc[-2] if len(rsi) > 1 else rsi_now
    rsi_slope = rsi_now - rsi_prev
    
    return {
        "adx": adx_now,
        "adx_slope": adx_slope,
        "di_plus": di_plus_now,
        "di_minus": di_minus_now,
        "di_spread": di_spread,
        "di_momentum": di_momentum,
        "rsi": rsi_now,
        "rsi_slope": rsi_slope
    }

def get_di_series(df, di_type="plus"):
    """Helper to get DI series."""
    high = df['high'].astype(float)
    low = df['low'].astype(float)
    close = df['close'].astype(float)
    tr = pd.concat([(high - low).abs(),
                    (high - close.shift(1)).abs(),
                    (low - close.shift(1)).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/ADX_LEN, adjust=False).mean()
    up_move = high.diff()
    down_move = low.shift(1) - low
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
    plus_di = 100 * (plus_dm.ewm(alpha=1/ADX_LEN, adjust=False).mean() / atr.replace(0, 1e-12))
    minus_di = 100 * (minus_dm.ewm(alpha=1/ADX_LEN, adjust=False).mean() / atr.replace(0, 1e-12))
    return plus_di if di_type == "plus" else minus_di

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

def compute_volume_profile_zones(df, lookback=100):
    """Identify POC, VAH, VAL from volume profile."""
    if len(df) < 50:
        return {"poc": None, "vah": None, "val": None}
    try:
        # Simplified Volume Profile using price bins
        price = df['close'].astype(float)
        volume = df['volume'].astype(float)
        # Create bins
        min_price = price.min()
        max_price = price.max()
        bins = np.linspace(min_price, max_price, 20)
        hist, bin_edges = np.histogram(price, bins=bins, weights=volume)
        poc_idx = np.argmax(hist)
        poc = (bin_edges[poc_idx] + bin_edges[poc_idx+1]) / 2
        # Value Area (70% of volume)
        total_vol = hist.sum()
        target_vol = total_vol * 0.7
        sorted_indices = np.argsort(hist)[::-1]
        cum_vol = 0
        vah = None
        val = None
        prices_in_va = []
        for idx in sorted_indices:
            cum_vol += hist[idx]
            prices_in_va.append((bin_edges[idx] + bin_edges[idx+1]) / 2)
            if cum_vol >= target_vol:
                break
        if prices_in_va:
            vah = max(prices_in_va)
            val = min(prices_in_va)
        return {"poc": poc, "vah": vah, "val": val}
    except Exception as e:
        log_warn(f"volume_profile error: {e}")
        return {"poc": None, "vah": None, "val": None}

# =================== NEW HYBRID LIQUIDITY ENGINE (v18.0) ===================
# Position Registry - Single Source of Truth
class PositionRegistry:
    def __init__(self, exchange):
        self.exchange = exchange
        self.positions = {}  # symbol -> data

    def sync(self):
        try:
            raw_positions = self.exchange.fetch_positions()
            updated = {}

            for p in raw_positions:
                symbol = p.get("symbol")
                size = float(p.get("contracts", 0) or p.get("positionAmt", 0))
                entry = float(p.get("entryPrice", 0))

                if abs(size) < 1e-6:
                    continue

                side = "LONG" if size > 0 else "SHORT"

                updated[symbol] = {
                    "symbol": symbol,
                    "size": abs(size),
                    "entry": entry,
                    "side": side,
                    "type": self.positions.get(symbol, {}).get("type", None),  # TREND / SNIPER
                }

            self.positions = updated

        except Exception as e:
            log_error(f"Registry sync error: {e}")

    def set_type(self, symbol, trade_type):
        if symbol in self.positions:
            self.positions[symbol]["type"] = trade_type

    def count(self):
        trend = sum(1 for p in self.positions.values() if p.get("type") == "TREND")
        sniper = sum(1 for p in self.positions.values() if p.get("type") == "SNIPER")
        total = len(self.positions)
        return trend, sniper, total

    def exposure(self):
        total_value = 0
        balance = get_balance(self.exchange)

        for p in self.positions.values():
            price = get_ticker_safe(p["symbol"])
            total_value += price * p["size"]

        if balance == 0:
            return 0

        return total_value / balance

    def get_positions(self):
        return list(self.positions.values())

# Global registry instance (initialized later)
registry = None

# Portfolio Limits
MAX_TREND = 1
MAX_SNIPER = 2
MAX_TOTAL = 4
MAX_EXPOSURE = 0.50

# Stage1 Scanner Config
ADX_OVEREXT_MIN = 40
ADX_OVEREXT_MAX = 60
ADX_ACC_MIN = 11
ADX_ACC_MAX = 15
STAGE1_INTERVAL_SEC = 900   # 15 min
STAGE2_INTERVAL_SEC = 60     # 1 min (adjustable)
WATCHLIST_MAX = 15

# Memory System
SYMBOL_MEMORY = {}  # {symbol: {"wins":0,"losses":0,"fake":0}}

def mem_update(symbol, result):
    m = SYMBOL_MEMORY.setdefault(symbol, {"wins":0,"losses":0,"fake":0})
    if result == "win":
        m["wins"] += 1
    else:
        m["losses"] += 1
        m["fake"] += 1

def mem_penalty(symbol):
    m = SYMBOL_MEMORY.get(symbol, {})
    return 2 if m.get("fake", 0) >= 3 else 0

# Stage1 Light Scan (ADX only)
_last_stage1 = 0
_STAGE1_LIST = []

def stage1_scan(symbols):
    global _last_stage1, _STAGE1_LIST
    if time.time() - _last_stage1 < STAGE1_INTERVAL_SEC:
        return _STAGE1_LIST

    selected = []
    for s in symbols:
        df = get_ohlcv_safe(s, limit=60)
        if df is None or df.empty:
            continue
        ind = compute_indicators(df)
        adx = ind.get("adx", 0)

        if (ADX_OVEREXT_MIN <= adx <= ADX_OVEREXT_MAX) or (ADX_ACC_MIN <= adx <= ADX_ACC_MAX):
            selected.append((s, adx))

    selected.sort(key=lambda x: x[1], reverse=True)
    _STAGE1_LIST = [s for s, _ in selected[:30]]
    _last_stage1 = time.time()
    log_i(f"Stage1 scan completed: {len(_STAGE1_LIST)} symbols")
    return _STAGE1_LIST

# Stage2 Deep Scan
_last_stage2 = 0
WATCHLIST = []

def stage2_watchlist(symbols):
    global _last_stage2, WATCHLIST
    if time.time() - _last_stage2 < STAGE2_INTERVAL_SEC:
        return WATCHLIST

    base = stage1_scan(symbols)
    WATCHLIST = base[:WATCHLIST_MAX]
    _last_stage2 = time.time()
    return WATCHLIST

# Helper functions for scoring
def _indicator_dict(df):
    return compute_indicators(df)

def _vwap_ctx_simple(df):
    ctx = vwap_context(df)
    return {
        "above": ctx["price"] > ctx["vwap"],
        "below": ctx["price"] < ctx["vwap"],
        "dist": float(ctx["distance_pct"])
    }

def _volume_spike_check(df):
    vol = df["volume"].astype(float)
    if len(vol) < 20:
        return False
    avg = vol.rolling(20).mean().iloc[-1]
    return vol.iloc[-1] > avg * 1.5

def _wick_rejection_check(df):
    o = float(df["open"].iloc[-1]); c = float(df["close"].iloc[-1])
    h = float(df["high"].iloc[-1]); l = float(df["low"].iloc[-1])
    body = abs(c - o) + 1e-12
    upper = h - max(o, c)
    lower = min(o, c) - l
    return (upper > body * 1.5) or (lower > body * 1.5)

def _boc_check(df):
    s = detect_structure(df)
    return s.get("choch_up") or s.get("choch_down")

def _liquidity_sweep_check(df):
    sw = detect_liquidity_sweep_advanced(df)
    return sw is not None

def _trendline_bias_check(df):
    if len(df) < 30:
        return 0
    y = df["close"].astype(float).tail(30).values
    x = np.arange(len(y))
    slope = np.polyfit(x, y, 1)[0]
    return 1 if slope > 0 else (-1 if slope < 0 else 0)

def _fib_zone_check(df):
    high = float(df["high"].max())
    low  = float(df["low"].min())
    l618 = high - (high - low) * 0.618
    l786 = high - (high - low) * 0.786
    top = max(l618, l786); bot = min(l618, l786)
    price = float(df["close"].iloc[-1])
    return (bot <= price <= top)

def _sr_proximity_check(df):
    sup = float(df["low"].tail(50).min())
    res = float(df["high"].tail(50).max())
    price = float(df["close"].iloc[-1])
    dist_sup = abs(price - sup) / price
    dist_res = abs(res - price) / price
    return {"near_sup": dist_sup < 0.003, "near_res": dist_res < 0.003}

# Scoring function
def compute_scores(df, symbol):
    ind = _indicator_dict(df)
    vwap = _vwap_ctx_simple(df)
    vol_spike = _volume_spike_check(df)
    wick = _wick_rejection_check(df)
    sweep = _liquidity_sweep_check(df)
    boc = _boc_check(df)
    fib = _fib_zone_check(df)
    sr = _sr_proximity_check(df)
    trend_bias = _trendline_bias_check(df)

    trend_score = 0
    sniper_score = 0

    # TREND scoring
    if ind["adx"] >= 25: trend_score += 2
    if vwap["above"] or vwap["below"]: trend_score += 2
    if vol_spike: trend_score += 1
    if trend_bias != 0: trend_score += 1

    # SNIPER scoring (mandatory: sweep + boc)
    if sweep: sniper_score += 3
    if boc: sniper_score += 3
    if wick: sniper_score += 2
    if vol_spike: sniper_score += 2
    if fib: sniper_score += 2
    if sr["near_res"] or sr["near_sup"]: sniper_score += 1

    # Memory penalty
    sniper_score -= mem_penalty(symbol)

    return {
        "trend": trend_score,
        "sniper": sniper_score,
        "boc": boc,
        "sweep": sweep
    }

def decide_mode(scores):
    if scores["sniper"] >= 8 and scores["boc"] and scores["sweep"]:
        return "SNIPER"
    if scores["trend"] >= 7:
        return "TREND"
    return None

# Portfolio control
def portfolio_counts(open_positions):
    t = sum(1 for p in open_positions if p.get("type") == "TREND")
    s = sum(1 for p in open_positions if p.get("type") == "SNIPER")
    return t, s, len(open_positions)

def can_open(strategy, open_positions, exposure):
    t, s, total = portfolio_counts(open_positions)
    if total >= MAX_TOTAL: return False
    if exposure >= MAX_EXPOSURE: return False
    if strategy == "TREND" and t >= MAX_TREND: return False
    if strategy == "SNIPER" and s >= MAX_SNIPER: return False
    return True

# Position sizing
def position_notional(balance_free, strategy, score):
    if strategy == "TREND":
        base = balance_free * 0.20
    else:  # SNIPER: 10-15% scaled by score
        base = balance_free * (0.10 + min(0.05, score * 0.005))
    return base * LEVERAGE

# Execution wrapper (uses existing functions)
def open_trade_hybrid(symbol, side, strategy, score):
    price = get_ticker_safe(symbol)
    if not price:
        return False

    balance = get_balance(ex)
    notional = position_notional(balance, strategy, score)
    qty = notional / price

    if not has_sufficient_margin(qty, price):
        return False

    set_leverage_safe(symbol, LEVERAGE, side)
    ok = execute_trade_decision(side.upper(), price, qty, mode=strategy, council_data=None, gz_data=None, source=f"{strategy}_HYBRID")
    if ok:
        STATE["trade_type"] = strategy
        STATE["current_symbol"] = symbol
        # Register type in registry
        if registry:
            registry.set_type(symbol, strategy)
    return ok

# Exit management (called per position)
def manage_open_position_hybrid(symbol):
    if not STATE.get("open") or STATE.get("current_symbol") != symbol:
        return

    df = get_ohlcv_safe(symbol, limit=120)
    if df is None or df.empty:
        return

    scores = compute_scores(df, symbol)
    trade_type = STATE.get("trade_type")

    price = get_ticker_safe(symbol)
    entry = STATE.get("entry", price)

    if STATE.get("side","").upper() in ("LONG","BUY"):
        pnl_pct = (price - entry) / entry * 100
    else:
        pnl_pct = (entry - price) / entry * 100

    if trade_type == "TREND":
        # TP1: 5% partial, TP2: 10% or trailing
        if pnl_pct >= 5 and not STATE.get("tp1_hit"):
            close_partial(0.5)
            STATE["tp1_hit"] = True
        if pnl_pct >= 10:
            # Let existing trailing handle or close fully if trend weak
            if scores["trend"] < 4:
                close_position_full()
    elif trade_type == "SNIPER":
        if pnl_pct >= 3:
            close_position_full()
            return
        if scores["sniper"] < 5:
            close_position_full()
            return

def close_partial(fraction):
    # Implement partial close using existing logic (close portion of position)
    # For simplicity, we call close_position_full for now (full close)
    # In production, you'd use exchange reduce-only orders.
    log_warn("Partial close not fully implemented, using full close")
    close_position_full()

# Main hybrid tick
def hybrid_tick(all_symbols):
    global registry
    if registry is None:
        registry = PositionRegistry(ex)
    registry.sync()
    open_positions = registry.get_positions()
    exposure = registry.exposure()
    trend_count, sniper_count, total_positions = registry.count()
    log_i(f"Registry: Trend={trend_count}, Sniper={sniper_count}, Total={total_positions}, Exposure={exposure:.2%}")

    watch = stage2_watchlist(all_symbols)

    best = None
    best_pack = None

    for s in watch:
        df = get_ohlcv_safe(s, limit=120)
        if df is None or df.empty:
            continue

        scores = compute_scores(df, s)
        mode = decide_mode(scores)

        if not mode:
            continue

        score = scores["sniper"] if mode == "SNIPER" else scores["trend"]

        if can_open(mode, open_positions, exposure):
            if best is None or score > best:
                best = score
                best_pack = (s, mode, score)

    if best_pack:
        s, mode, score = best_pack
        df = get_ohlcv_safe(s, limit=120)
        vwap = _vwap_ctx_simple(df)
        # Determine side: for SNIPER, use sweep direction; for TREND, use VWAP bias
        if mode == "SNIPER":
            sweep = detect_stop_hunt(df)
            if sweep == "LONG":
                side = "buy"
            elif sweep == "SHORT":
                side = "sell"
            else:
                # fallback
                side = "buy" if vwap["above"] else "sell"
        else:
            side = "buy" if vwap["above"] else "sell"

        open_trade_hybrid(s, side, mode, score)

    # Manage existing positions
    if STATE.get("open"):
        manage_open_position_hybrid(STATE.get("current_symbol"))

# Cleanup system
_last_cleanup = 0
def hourly_cleanup():
    global _last_cleanup
    if time.time() - _last_cleanup < 3600:
        return
    CACHE["ohlcv"].clear()
    CACHE["orderbook"].clear()
    CACHE["trades"].clear()
    gc.collect()
    _last_cleanup = time.time()
    log_i("Hourly cache cleanup completed")

# =================== TRADE MANAGEMENT (existing) ===================
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
    STATE.update({
        "open": False,
        "side": None,
        "entry": None,
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
        "trade_type": None,
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

# =================== TRADE LOOP (UPDATED with HYBRID) ===================
def trade_loop():
    global wait_for_next_signal_side, compound_pnl, STATE, _scan_idx, bot_state, LAST_LOG, LAST_FULL_SCAN, SCAN_LIST, SYMBOLS, LAST_RADAR_TIME, registry
    last_scan_time = 0
    last_snapshot_time = 0
    last_clean_time = time.time()
    last_radar_time = 0

    # Initialize registry if live
    if MODE_LIVE and not PAPER_MODE:
        registry = PositionRegistry(ex)

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
                hourly_cleanup()  # v18.0 cleanup
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

            # Radar Engine (existing)
            if now - last_radar_time >= 1200:
                if SYMBOLS:
                    radar_engine(SYMBOLS[:50])
                    last_radar_time = now
                    log("🔎 RADAR SCAN COMPLETED")

            # Sniper Engine (existing watchlist)
            if not has_open_position() and WATCHLIST:
                snipes = sniper_engine()
                for sym, score in snipes:
                    log_g(f"🎯 SNIPER → {sym} | score={score}")
                    df = get_ohlcv_safe(sym, timeframe=INTERVAL, limit=120)
                    if df is None or df.empty:
                        continue
                    price = df["close"].iloc[-1]
                    sl = compute_indicator_slopes(df)
                    qty = calculate_dynamic_position_size(sym, price, "SNIPER", score, sl)
                    if qty > 0 and update_symbol(sym):
                        open_market_enhanced(
                            "buy" if score > 0 else "sell",
                            qty, price,
                            source="SNIPER_ENGINE",
                            df=df,
                            institutional_score=score,
                            breakdown={"engine": "sniper"},
                            trade_type="SNIPER"
                        )
                        break

            # Full macro scan every GLOBAL_SCAN_INTERVAL
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

            # Manage existing positions (hybrid management)
            if has_open_position():
                df = fetch_ohlcv_cached()
                px = price_now()
                if px is not None:
                    STATE["current_symbol"] = SYMBOL
                    ind = compute_indicators_cached(df)
                    # Use hybrid management
                    manage_open_position_hybrid(SYMBOL)
                if now - LAST_LOG > LOG_INTERVAL:
                    if px:
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
                                print(f"{color}\n{'='*50}\n📊 LIVE POSITION ({STATE.get('trade_type', 'UNKNOWN')})\n{'='*50}\n"
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

            # Entry scanning using hybrid engine (replaces old entry logic)
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

                # Hybrid tick: scans, decides, opens trade if conditions met
                hybrid_tick(SYMBOLS)

                last_scan_time = now

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

# =================== FLASK DASHBOARD (existing, unchanged) ===================
# ... (keep all existing Flask routes)

# =================== INITIALIZATION ===================
def initialize_bot():
    global SYMBOLS, STATE, compound_pnl, wait_for_next_signal_side, bot_state, TOP_SYMBOLS, SCAN_LIST, registry
    log_banner("INIT v18.0 HYBRID LIQUIDITY ENGINE")
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
        STATE["trade_type"] = state.get("trade_type", None)
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
            "trade_type": None,
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
    print(colored(f"ENTRY: Hybrid Liquidity Engine v18.0 - Trend/Sniper Separation", "yellow"))
    print(colored(f"FILTERS: Dual Scanner + Portfolio Limits + Position Registry", "yellow"))
    print(colored(f"NEW v18.0: Registry, Stage1/Stage2, Decision Council, Strict Limits", "green"))
    tg_boot()

if __name__ == "__main__":
    import threading
    port = int(os.environ.get("PORT", 8000))
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
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
    if SELF_URL:
        threading.Thread(target=keepalive_loop, daemon=True).start()
