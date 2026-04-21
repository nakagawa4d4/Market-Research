"""
screener.py
-----------
J-Quants API V2 を使って東証プライム全銘柄をスクリーニングする。

スクリーニング条件:
  1. 騰落率 ±3% 以上 (前日比)
  2. RSI(14) が 30 以下（売られすぎ）または 70 以上（買われすぎ）
  3. SMA25 / SMA75 ゴールデン/デッドクロス（直近2営業日で発生）
  4. 出来高急増: 20日平均の 2 倍以上

API レート制限対策:
  - リトライ (指数バックオフ)
  - CSV キャッシュ (当日分は再取得しない)
"""

import os
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# 設定
# ─────────────────────────────────────────────
load_dotenv()

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

JQUANTS_API_KEY: str = os.getenv("JQUANTS_API_KEY", "")
BASE_URL = "https://api.jquants.com/v2"
MAX_RETRIES = 5
RETRY_BASE_SEC = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# HTTP ユーティリティ
# ─────────────────────────────────────────────
def _get(url: str, params: dict | None = None) -> Optional[requests.Response]:
    """指数バックオフ付き GET リクエスト。HTTP 429 を自動リトライ。"""
    headers = {"x-api-key": JQUANTS_API_KEY}
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
        except requests.RequestException as e:
            log.warning(f"[Attempt {attempt+1}] Network error: {e}")
            time.sleep(RETRY_BASE_SEC * (2 ** attempt))
            continue

        if resp.status_code == 200:
            return resp
        elif resp.status_code == 429:
            wait = RETRY_BASE_SEC * (2 ** attempt)
            log.warning(f"[HTTP 429] Rate limit. Waiting {wait}s...")
            time.sleep(wait)
        else:
            log.error(f"[HTTP {resp.status_code}] {url}: {resp.text[:200]}")
            return None
    return None


# ─────────────────────────────────────────────
# J-Quants データ取得
# ─────────────────────────────────────────────
def fetch_listed_info() -> pd.DataFrame:
    """東証上場銘柄一覧を取得（プライム市場でフィルタ）。"""
    cache_path = DATA_DIR / "listed_info.csv"

    # 当日キャッシュが既に存在する場合はスキップ
    if cache_path.exists():
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        if mtime.date() == datetime.today().date():
            log.info("[Cache] 銘柄一覧はキャッシュから読み込みます。")
            return pd.read_csv(cache_path, dtype=str)

    log.info("[API] 銘柄一覧を取得中...")
    resp = _get(f"{BASE_URL}/listed/info")
    if resp is None:
        log.error("銘柄一覧の取得に失敗しました。")
        return pd.DataFrame()

    data = resp.json().get("info", [])
    df = pd.DataFrame(data)

    if df.empty:
        log.warning("銘柄一覧が空です。")
        return df

    # 東証プライム市場 (MarketCodeName = "プライム" またはコード 0111)
    if "MarketCodeName" in df.columns:
        df = df[df["MarketCodeName"].str.contains("プライム", na=False)].copy()
    elif "MarketCode" in df.columns:
        df = df[df["MarketCode"] == "0111"].copy()

    df.to_csv(cache_path, index=False)
    log.info(f"[Success] {len(df)} 銘柄を保存しました → {cache_path}")
    return df


def fetch_daily_quotes(date_str: str) -> pd.DataFrame:
    """
    指定日の全銘柄日次株価を取得。
    date_str: "YYYY-MM-DD" 形式
    """
    cache_path = DATA_DIR / f"quotes_{date_str}.csv"

    if cache_path.exists():
        log.info(f"[Cache] {date_str} の株価データをキャッシュから読み込みます。")
        return pd.read_csv(cache_path, dtype={"Code": str})

    log.info(f"[API] {date_str} の全銘柄株価を取得中...")
    resp = _get(f"{BASE_URL}/prices/daily_quotes", params={"date": date_str})
    if resp is None:
        log.error(f"{date_str} の株価取得に失敗しました。")
        return pd.DataFrame()

    raw = resp.json()
    # V2 レスポンスキーを柔軟に処理
    if "daily_quotes" in raw:
        items = raw["daily_quotes"]
    elif "data" in raw:
        items = raw["data"]
    else:
        items = next((v for v in raw.values() if isinstance(v, list)), [])

    df = pd.DataFrame(items)
    if not df.empty:
        df["Code"] = df["Code"].astype(str)
        df.to_csv(cache_path, index=False)
        log.info(f"[Success] {len(df)} 件を保存 → {cache_path}")
    return df


def fetch_price_history(code: str, days: int = 100) -> pd.DataFrame:
    """
    単一銘柄の過去株価を取得（RSI / SMA 計算用）。
    code: J-Quants 形式のコード（例: "72030"）
    days: 取得する日数（RSI14 + SMA75 計算のため最低80日推奨）
    """
    cache_path = DATA_DIR / f"history_{code}.csv"
    today = datetime.today().date()

    # 当日キャッシュが存在すればそのまま使う
    if cache_path.exists():
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        if mtime.date() == today:
            df = pd.read_csv(cache_path, parse_dates=["Date"])
            return df

    date_from = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    resp = _get(
        f"{BASE_URL}/prices/daily_quotes",
        params={"code": code, "dateFrom": date_from},
    )
    if resp is None:
        return pd.DataFrame()

    raw = resp.json()
    if "daily_quotes" in raw:
        items = raw["daily_quotes"]
    elif "data" in raw:
        items = raw["data"]
    else:
        items = next((v for v in raw.values() if isinstance(v, list)), [])

    df = pd.DataFrame(items)
    if df.empty:
        return df

    # カラム名標準化 (V2: Date, Open, High, Low, Close, Volume)
    rename_map = {"AdjustmentClose": "Close", "AdjustmentOpen": "Open",
                  "AdjustmentHigh": "High", "AdjustmentLow": "Low",
                  "AdjustmentVolume": "Volume"}
    df.rename(columns=rename_map, inplace=True)

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])
        df.sort_values("Date", inplace=True)

    df.to_csv(cache_path, index=False)
    return df


# ─────────────────────────────────────────────
# テクニカル指標計算
# ─────────────────────────────────────────────
def calc_rsi(series: pd.Series, period: int = 14) -> float:
    """
    RSI(period) を計算して最新値を返す。
    Formula: RSI = 100 - 100 / (1 + RS)
             RS  = avg_gain / avg_loss (Wilder's smoothing)
    """
    delta = series.diff().dropna()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1]) if not rsi.empty else float("nan")


def calc_sma_cross(series: pd.Series, short: int = 25, long: int = 75) -> str:
    """
    直近2日間でのSMAクロスを検知。
    戻り値: "golden" | "dead" | "none"
    """
    if len(series) < long + 1:
        return "none"

    sma_s = series.rolling(short).mean()
    sma_l = series.rolling(long).mean()

    # 直前の差分符号の変化を確認
    prev_diff = sma_s.iloc[-2] - sma_l.iloc[-2]
    curr_diff = sma_s.iloc[-1] - sma_l.iloc[-1]

    if prev_diff < 0 and curr_diff >= 0:
        return "golden"
    elif prev_diff > 0 and curr_diff <= 0:
        return "dead"
    return "none"


def calc_volume_ratio(volume_series: pd.Series, window: int = 20) -> float:
    """
    直近出来高 / 20日平均出来高 の比率を計算。
    Formula: volume_ratio = latest_volume / rolling_mean(20)
    """
    if len(volume_series) < window + 1:
        return float("nan")
    avg_vol = volume_series.iloc[-window - 1 : -1].mean()
    if avg_vol == 0:
        return float("nan")
    return float(volume_series.iloc[-1] / avg_vol)


# ─────────────────────────────────────────────
# メインスクリーニング
# ─────────────────────────────────────────────
def run_screening(
    target_date: str | None = None,
    price_change_threshold: float = 3.0,
    rsi_oversold: float = 30.0,
    rsi_overbought: float = 70.0,
    volume_ratio_threshold: float = 2.0,
    max_stocks: int = 200,          # API 負荷軽減のため上限を設定
) -> pd.DataFrame:
    """
    日次スクリーニングを実行し、注目銘柄 DataFrame を返す。

    Parameters
    ----------
    target_date : str | None
        対象日 ("YYYY-MM-DD")。None の場合は前営業日。
    price_change_threshold : float
        騰落率フィルタのしきい値（%）。
    rsi_oversold / rsi_overbought : float
        RSI フィルタのしきい値。
    volume_ratio_threshold : float
        出来高急増倍率のしきい値。
    max_stocks : int
        詳細分析する銘柄数の上限（APIコール削減）。
    """
    # ── 1. 日付設定 ────────────────────────────────
    if target_date is None:
        # 土日をスキップして前営業日を推定
        today = datetime.today()
        offset = 1 if today.weekday() not in (5, 6) else (today.weekday() - 4)
        target_date = (today - timedelta(days=offset)).strftime("%Y-%m-%d")

    log.info(f"=== スクリーニング開始: {target_date} ===")

    # ── 2. 銘柄一覧取得 ────────────────────────────
    listed = fetch_listed_info()
    if listed.empty:
        log.error("銘柄一覧が取得できませんでした。終了します。")
        return pd.DataFrame()

    # ── 3. 当日全銘柄株価取得 ──────────────────────
    quotes = fetch_daily_quotes(target_date)
    if quotes.empty:
        log.error(f"{target_date} の株価データが取得できませんでした。")
        return pd.DataFrame()

    # コードを文字列に統一
    quotes["Code"] = quotes["Code"].astype(str)
    listed["Code"] = listed["Code"].astype(str)

    # プライム銘柄のみにフィルタ
    prime_codes = set(listed["Code"].tolist())
    quotes = quotes[quotes["Code"].isin(prime_codes)].copy()
    log.info(f"プライム銘柄 {len(quotes)} 件の株価データを処理します。")

    # ── 4. 騰落率計算 ──────────────────────────────
    # J-Quants V2: Close / PreviousClose または Change があれば使う
    if "PriceChangeRatio" in quotes.columns:
        quotes["change_pct"] = quotes["PriceChangeRatio"].astype(float) * 100
    elif "PreviousClose" in quotes.columns and "Close" in quotes.columns:
        quotes["Close"] = quotes["Close"].astype(float)
        quotes["PreviousClose"] = quotes["PreviousClose"].astype(float)
        # 騰落率 = (当日終値 - 前日終値) / 前日終値 × 100
        quotes["change_pct"] = (
            (quotes["Close"] - quotes["PreviousClose"]) / quotes["PreviousClose"] * 100
        )
    else:
        log.warning("騰落率計算に必要なカラムが見つかりません。change_pct=0 として続行します。")
        quotes["change_pct"] = 0.0

    # ── 5. 一次フィルタ（騰落率） ──────────────────
    notable = quotes[
        quotes["change_pct"].abs() >= price_change_threshold
    ].copy()
    log.info(f"騰落率 ±{price_change_threshold}% 以上: {len(notable)} 件")

    # 処理上限（APIコールを抑える）
    notable = notable.nlargest(max_stocks, "change_pct")

    # ── 6. 個別銘柄の詳細分析（RSI, SMA, Volume） ──
    results: list[dict] = []
    total = len(notable)

    for i, (_, row) in enumerate(notable.iterrows(), 1):
        code = str(row["Code"])
        log.info(f"[{i}/{total}] {code} を分析中...")

        hist = fetch_price_history(code, days=120)
        time.sleep(0.3)  # APIレート制限対策

        rsi_val = float("nan")
        cross = "none"
        vol_ratio = float("nan")

        if not hist.empty and "Close" in hist.columns:
            close = hist["Close"].astype(float)
            rsi_val = calc_rsi(close)
            cross = calc_sma_cross(close)

            if "Volume" in hist.columns:
                vol = hist["Volume"].astype(float)
                vol_ratio = calc_volume_ratio(vol)

        # 銘柄名取得
        info_row = listed[listed["Code"] == code]
        name = info_row["CompanyName"].iloc[0] if not info_row.empty and "CompanyName" in info_row.columns else ""
        sector = info_row["Sector33CodeName"].iloc[0] if not info_row.empty and "Sector33CodeName" in info_row.columns else ""

        results.append({
            "code": code,
            "name": name,
            "sector": sector,
            "close": row.get("Close", row.get("AdjustmentClose", float("nan"))),
            "change_pct": round(row["change_pct"], 2),
            "rsi_14": round(rsi_val, 1) if not np.isnan(rsi_val) else None,
            "sma_cross": cross,
            "volume_ratio": round(vol_ratio, 2) if not np.isnan(vol_ratio) else None,
            "volume": row.get("Volume", row.get("AdjustmentVolume", None)),
        })

    df_result = pd.DataFrame(results)

    # ── 7. 注目スコア付与 ──────────────────────────
    df_result["score"] = 0

    # 騰落率が大きいほど高スコア
    df_result["score"] += df_result["change_pct"].abs().rank(pct=True) * 30

    # RSI 過熱・過売を加点
    rsi_valid = df_result["rsi_14"].notna()
    df_result.loc[rsi_valid & (df_result["rsi_14"] <= rsi_oversold), "score"] += 20
    df_result.loc[rsi_valid & (df_result["rsi_14"] >= rsi_overbought), "score"] += 20

    # SMA クロス検知で大幅加点
    df_result.loc[df_result["sma_cross"].isin(["golden", "dead"]), "score"] += 30

    # 出来高急増加点
    vol_valid = df_result["volume_ratio"].notna()
    df_result.loc[vol_valid & (df_result["volume_ratio"] >= volume_ratio_threshold), "score"] += 20

    df_result["score"] = df_result["score"].round(1)
    df_result.sort_values("score", ascending=False, inplace=True)
    df_result.reset_index(drop=True, inplace=True)

    # ── 8. 結果保存 ────────────────────────────────
    output_path = DATA_DIR / f"screening_{target_date}.csv"
    df_result.to_csv(output_path, index=False)
    log.info(f"[Done] スクリーニング完了。{len(df_result)} 件 → {output_path}")

    return df_result


if __name__ == "__main__":
    result = run_screening()
    print(result.head(20).to_string())
