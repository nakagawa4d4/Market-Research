"""
data_fetcher.py
---------------
マーケットデータ取得モジュール。

yfinance を使って主要指数・為替・コモディティデータを取得する。
日本市場モードでは J-Quants API からの注目銘柄データも活用可能。

データソース:
  - yfinance: 指数, 為替, コモディティ, セクターETF
  - J-Quants API: 日本個別株データ（オプション）
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Optional

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

# ── J-Quants 設定 ─────────────────────────────────────
JQUANTS_API_KEY: str = os.getenv("JQUANTS_API_KEY", "")
JQUANTS_BASE_URL: str = "https://api.jquants.com/v2"
MAX_RETRIES: int = 3
RETRY_BASE_SEC: int = 2


# ─────────────────────────────────────────────
# yfinance データ取得
# ─────────────────────────────────────────────

# 日本市場 ティッカーリスト
JP_TICKERS: dict[str, str] = {
    "日経225": "^N225",
    "TOPIX (ETF)": "1306.T",
    "グロース250 (ETF)": "2516.T",
    "USD/JPY": "JPY=X",
}

# 日本セクターETF
JP_SECTOR_ETFS: dict[str, str] = {
    "銀行": "1615.T",
    "自動車": "7203.T",       # トヨタを代理指標に
    "半導体": "8035.T",       # 東京エレクトロンを代理指標に
    "商社": "8058.T",         # 三菱商事を代理指標に
    "不動産": "8802.T",       # 三菱地所を代理指標に
}

# 米国市場 ティッカーリスト
US_TICKERS: dict[str, str] = {
    "S&P 500": "^GSPC",
    "NASDAQ": "^IXIC",
    "DOW": "^DJI",
    "VIX": "^VIX",
    "米10年債利回り": "^TNX",
    "WTI原油": "CL=F",
    "金": "GC=F",
    "USD/JPY": "JPY=X",
}

# 米国セクターETF
US_SECTOR_ETFS: dict[str, str] = {
    "テクノロジー": "XLK",
    "金融": "XLF",
    "エネルギー": "XLE",
    "ヘルスケア": "XLV",
    "資本財": "XLI",
    "一般消費財": "XLY",
    "通信": "XLC",
    "公益事業": "XLU",
    "素材": "XLB",
    "不動産": "XLRE",
    "生活必需品": "XLP",
}


def _safe_pct_change(current: float, previous: float) -> Optional[float]:
    """
    安全に騰落率(%)を計算する。
    Formula: pct_change = (current - previous) / previous * 100
    """
    if previous is None or previous == 0 or np.isnan(previous):
        return None
    return round((current - previous) / previous * 100, 2)


def fetch_ticker_data(
    tickers: dict[str, str],
    period: str = "2d",
) -> list[dict[str, Any]]:
    """
    yfinance 経由で複数ティッカーのデータを取得する。

    Parameters
    ----------
    tickers : dict[str, str]
        {表示名: ティッカーシンボル} の辞書
    period : str
        yfinance の取得期間（例: "2d", "5d"）

    Returns
    -------
    list[dict]
        各ティッカーの { name, ticker, close, prev_close, change_pct } リスト
    """
    results: list[dict[str, Any]] = []

    for name, ticker_symbol in tickers.items():
        try:
            ticker = yf.Ticker(ticker_symbol)
            hist = ticker.history(period=period)

            if hist.empty or len(hist) < 1:
                log.warning(f"[yfinance] {name} ({ticker_symbol}): データなし")
                results.append({
                    "name": name,
                    "ticker": ticker_symbol,
                    "close": None,
                    "prev_close": None,
                    "change_pct": None,
                })
                continue

            close = float(hist["Close"].iloc[-1])

            if len(hist) >= 2:
                prev_close = float(hist["Close"].iloc[-2])
                change_pct = _safe_pct_change(close, prev_close)
            else:
                prev_close = None
                change_pct = None

            results.append({
                "name": name,
                "ticker": ticker_symbol,
                "close": round(close, 2),
                "prev_close": round(prev_close, 2) if prev_close else None,
                "change_pct": change_pct,
            })
            log.info(f"[yfinance] {name}: {close:.2f} ({change_pct:+.2f}%)" if change_pct else f"[yfinance] {name}: {close:.2f}")

        except Exception as e:
            log.error(f"[yfinance] {name} ({ticker_symbol}) 取得エラー: {e}")
            results.append({
                "name": name,
                "ticker": ticker_symbol,
                "close": None,
                "prev_close": None,
                "change_pct": None,
            })

    return results


def fetch_jp_market_data() -> dict[str, Any]:
    """
    日本市場データを一括取得する。

    Returns
    -------
    dict
        {
            "indices": [...],
            "sectors": [...],
            "timestamp": str,
            "mode": "jp"
        }
    """
    log.info("🇯🇵 日本市場データを取得中...")

    indices = fetch_ticker_data(JP_TICKERS, period="5d")
    sectors = fetch_ticker_data(JP_SECTOR_ETFS, period="5d")

    return {
        "indices": indices,
        "sectors": sectors,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M JST"),
        "date": datetime.now().strftime("%Y年%m月%d日"),
        "weekday": ["月", "火", "水", "木", "金", "土", "日"][datetime.now().weekday()],
        "mode": "jp",
    }


def fetch_us_market_data() -> dict[str, Any]:
    """
    米国市場データを一括取得する。

    Returns
    -------
    dict
        {
            "indices": [...],
            "sectors": [...],
            "timestamp": str,
            "mode": "us"
        }
    """
    log.info("🇺🇸 米国市場データを取得中...")

    indices = fetch_ticker_data(US_TICKERS, period="5d")
    sectors = fetch_ticker_data(US_SECTOR_ETFS, period="5d")

    return {
        "indices": indices,
        "sectors": sectors,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M JST"),
        "date": datetime.now().strftime("%Y年%m月%d日"),
        "weekday": ["月", "火", "水", "木", "金", "土", "日"][datetime.now().weekday()],
        "mode": "us",
    }


# ─────────────────────────────────────────────
# J-Quants API (日本個別株用、オプション)
# ─────────────────────────────────────────────
def _jquants_get(url: str, params: dict | None = None) -> Optional[requests.Response]:
    """J-Quants API への GET リクエスト（指数バックオフ付き）。"""
    if not JQUANTS_API_KEY:
        log.debug("[J-Quants] API キーが未設定です。")
        return None

    headers = {"x-api-key": JQUANTS_API_KEY}
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 429:
                wait = RETRY_BASE_SEC * (2 ** attempt)
                log.warning(f"[J-Quants] HTTP 429 Rate limit. Waiting {wait}s...")
                time.sleep(wait)
            else:
                log.error(f"[J-Quants] HTTP {resp.status_code}: {resp.text[:200]}")
                return None
        except requests.RequestException as e:
            log.warning(f"[J-Quants] Network error (attempt {attempt+1}): {e}")
            time.sleep(RETRY_BASE_SEC * (2 ** attempt))

    return None


def fetch_jp_top_movers(target_date: str | None = None) -> list[dict[str, Any]]:
    """
    J-Quants API から当日の値動き上位銘柄を取得する。

    Parameters
    ----------
    target_date : str | None
        対象日 ("YYYY-MM-DD")。None の場合は前営業日。

    Returns
    -------
    list[dict]
        値動き上位銘柄のリスト
    """
    if not JQUANTS_API_KEY:
        log.info("[J-Quants] APIキー未設定のため、注目銘柄はスキップします。")
        return []

    if target_date is None:
        today = datetime.today()
        offset = 1 if today.weekday() not in (5, 6) else (today.weekday() - 4)
        target_date = (today - timedelta(days=max(offset, 1))).strftime("%Y-%m-%d")

    log.info(f"[J-Quants] {target_date} の株価データを取得中...")
    resp = _jquants_get(
        f"{JQUANTS_BASE_URL}/prices/daily_quotes",
        params={"date": target_date},
    )
    if resp is None:
        return []

    raw = resp.json()
    items = raw.get("daily_quotes", raw.get("data", []))
    if not items:
        log.warning("[J-Quants] データが空です。")
        return []

    df = pd.DataFrame(items)

    # 騰落率計算
    if "AdjustmentClose" in df.columns and "PreviousClose" in df.columns:
        df["close"] = df["AdjustmentClose"].astype(float)
        df["prev_close"] = df["PreviousClose"].astype(float)
    elif "Close" in df.columns and "PreviousClose" in df.columns:
        df["close"] = df["Close"].astype(float)
        df["prev_close"] = df["PreviousClose"].astype(float)
    else:
        return []

    # 騰落率 = (当日終値 - 前日終値) / 前日終値 × 100
    df["change_pct"] = (df["close"] - df["prev_close"]) / df["prev_close"] * 100
    df = df.dropna(subset=["change_pct"])

    # 上昇TOP5 + 下落TOP5
    top_gainers = df.nlargest(5, "change_pct")
    top_losers = df.nsmallest(5, "change_pct")

    results: list[dict[str, Any]] = []
    for label, sub_df in [("上昇", top_gainers), ("下落", top_losers)]:
        for _, row in sub_df.iterrows():
            results.append({
                "code": str(row.get("Code", "")),
                "name": str(row.get("CompanyName", row.get("Code", ""))),
                "close": round(float(row["close"]), 1),
                "change_pct": round(float(row["change_pct"]), 2),
                "category": label,
            })

    return results


def format_market_data_for_prompt(data: dict[str, Any]) -> str:
    """
    取得したマーケットデータを Claude プロンプト用のテキストに変換する。

    Parameters
    ----------
    data : dict
        fetch_jp_market_data() または fetch_us_market_data() の戻り値

    Returns
    -------
    str
        構造化テキスト
    """
    lines: list[str] = []
    mode = data.get("mode", "jp")

    lines.append(f"=== マーケットデータ ({data['date']} {data['weekday']}曜日) ===\n")

    # 主要指数
    lines.append("【主要指数】")
    for item in data.get("indices", []):
        close = item.get("close")
        pct = item.get("change_pct")
        if close is not None:
            pct_str = f" ({pct:+.2f}%)" if pct is not None else ""
            lines.append(f"  {item['name']}: {close:,.2f}{pct_str}")
        else:
            lines.append(f"  {item['name']}: データなし")

    lines.append("")

    # セクター
    lines.append("【セクター動向】")
    for item in data.get("sectors", []):
        close = item.get("close")
        pct = item.get("change_pct")
        if close is not None:
            pct_str = f" ({pct:+.2f}%)" if pct is not None else ""
            lines.append(f"  {item['name']}: {close:,.2f}{pct_str}")
        else:
            lines.append(f"  {item['name']}: データなし")

    # 注目銘柄（日本市場のみ）
    top_movers = data.get("top_movers", [])
    if top_movers:
        lines.append("")
        lines.append("【注目銘柄 (値動き上位)】")
        for stock in top_movers:
            lines.append(
                f"  {stock['code']} {stock.get('name', '')} "
                f"¥{stock['close']:,.1f} ({stock['change_pct']:+.2f}%) [{stock['category']}]"
            )

    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("=== 日本市場データ ===")
    jp_data = fetch_jp_market_data()
    print(format_market_data_for_prompt(jp_data))

    print("\n=== 米国市場データ ===")
    us_data = fetch_us_market_data()
    print(format_market_data_for_prompt(us_data))
