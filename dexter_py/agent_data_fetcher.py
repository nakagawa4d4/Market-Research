"""
agent_data_fetcher.py
---------------------
データ取得エージェント。

優先順位:
  1. キャッシュCSV (API節約)
  2. J-Quants API V1 (リフレッシュトークン → IDトークン自動取得)
  3. yfinance (J-Quants失敗時のフォールバック)

HTTP 429 に対してエクスポネンシャルバックオフを実装。
"""

from __future__ import annotations

import json
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import requests
import yfinance as yf
from pytrends.request import TrendReq

from config import (
    RAW_DATA_DIR,
    JQUANTS_REFRESH_TOKEN,
    JQUANTS_BASE_URL,
    MAX_RETRIES,
    RETRY_DELAY_SECONDS,
    DATA_SOURCE,
)

log = logging.getLogger(__name__)

# IDトークンのキャッシュ (プロセス内)
_ID_TOKEN_CACHE: Dict[str, str] = {"token": "", "expires_at": ""}


class AgentDataFetcher:
    """
    データ取得エージェント。
    J-Quants API と yfinance のデュアルソース対応。
    """

    def __init__(self, source: str = DATA_SOURCE):
        self.source = source
        self._id_token: str = ""

    # ─────────────────────────────────────────────
    # J-Quants 認証
    # ─────────────────────────────────────────────
    def _get_id_token(self) -> str:
        """
        リフレッシュトークンからIDトークンを取得。
        取得済みの場合はキャッシュを返す。
        """
        cached = _ID_TOKEN_CACHE.get("token", "")
        if cached:
            return cached

        if not JQUANTS_REFRESH_TOKEN:
            log.warning("[J-Quants] JQUANTS_API_KEY (RefreshToken) が未設定です。")
            return ""

        url = f"{JQUANTS_BASE_URL}/token/auth_refresh"
        try:
            resp = requests.post(
                url,
                params={"refreshtoken": JQUANTS_REFRESH_TOKEN},
                timeout=15,
            )
            if resp.status_code == 200:
                token = resp.json().get("idToken", "")
                _ID_TOKEN_CACHE["token"] = token
                log.info("[J-Quants] IDトークン取得成功。")
                return token
            else:
                log.warning(
                    f"[J-Quants] トークン取得失敗 ({resp.status_code}): {resp.text[:200]}"
                )
                return ""
        except requests.RequestException as e:
            log.warning(f"[J-Quants] トークン取得エラー: {e}")
            return ""

    def _jquants_get(self, endpoint: str, params: Dict[str, Any] = None) -> Optional[requests.Response]:
        """J-Quants API GETリクエスト (HTTP 429 エクスポネンシャルバックオフ付き)"""
        token = self._get_id_token()
        if not token:
            return None

        headers = {"Authorization": f"Bearer {token}"}
        url = f"{JQUANTS_BASE_URL}{endpoint}"

        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=30)
                if resp.status_code == 200:
                    return resp
                elif resp.status_code == 429:
                    # レート制限: エクスポネンシャルバックオフ
                    wait = RETRY_DELAY_SECONDS * (2 ** attempt)
                    log.warning(f"[HTTP 429] レート制限。{wait}秒後にリトライ ({attempt+1}/{MAX_RETRIES})")
                    time.sleep(wait)
                elif resp.status_code in (401, 403):
                    log.warning(f"[J-Quants] 認証エラー ({resp.status_code}): {resp.text[:100]}")
                    _ID_TOKEN_CACHE["token"] = ""  # キャッシュクリア
                    return None
                else:
                    log.warning(f"[J-Quants] エラー ({resp.status_code}): {resp.text[:200]}")
                    return None
            except requests.RequestException as e:
                log.warning(f"[J-Quants] リクエスト例外: {e}")
                time.sleep(RETRY_DELAY_SECONDS)

        return None

    # ─────────────────────────────────────────────
    # 株価データ取得
    # ─────────────────────────────────────────────
    def fetch_prices(
        self,
        ticker: str,
        date_from: str = None,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        日足株価データを取得。
        キャッシュ → J-Quants → yfinance の順でフォールバック。

        Parameters
        ----------
        ticker : str
            銘柄コード (例: "7974.T" または "7974")
        date_from : str
            取得開始日 (YYYY-MM-DD)。省略時は1年前。
        force_refresh : bool
            Trueの場合、キャッシュを無視して再取得。
        """
        code_no_t = ticker.replace(".T", "")
        cache_path = RAW_DATA_DIR / f"{code_no_t}_prices.csv"

        if cache_path.exists() and not force_refresh:
            log.info(f"[Cache] {ticker} の株価をキャッシュから読み込み: {cache_path}")
            df = pd.read_csv(cache_path, parse_dates=["Date"])
            return df

        if date_from is None:
            date_from = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")

        # ── J-Quants 試行 ───────────────────────────
        if self.source in ("jquants", "auto"):
            df = self._fetch_prices_jquants(code_no_t, date_from)
            if df is not None and not df.empty:
                df.to_csv(cache_path, index=False)
                log.info(f"[J-Quants] {ticker} 株価保存: {len(df)}件 → {cache_path}")
                return df
            log.info(f"[J-Quants] {ticker} 株価取得失敗。yfinanceにフォールバック。")

        # ── yfinance フォールバック ────────────────
        if self.source in ("yfinance", "auto"):
            df = self._fetch_prices_yfinance(ticker, date_from)
            if df is not None and not df.empty:
                df.to_csv(cache_path, index=False)
                log.info(f"[yfinance] {ticker} 株価保存: {len(df)}件 → {cache_path}")
                return df

        log.error(f"[Error] {ticker} 株価取得に失敗しました。")
        return pd.DataFrame()

    def _fetch_prices_jquants(self, code: str, date_from: str) -> Optional[pd.DataFrame]:
        """J-Quants V1 /prices/daily_quotes で株価取得"""
        jquants_code = f"{code}0"  # J-Quantsは末尾に0付き (例: 79740)
        resp = self._jquants_get(
            "/prices/daily_quotes",
            params={"code": jquants_code, "from": date_from},
        )
        if resp is None:
            return None
        items = resp.json().get("daily_quotes", [])
        if not items:
            return None

        df = pd.DataFrame(items)
        # J-Quants カラム名 → 標準カラム名に統一
        rename_map = {
            "Date": "Date", "Open": "Open", "High": "High",
            "Low": "Low", "Close": "Close", "Volume": "Volume",
            "AdjustmentClose": "AdjClose",
        }
        df = df.rename(columns=rename_map)
        df["Date"] = pd.to_datetime(df["Date"])
        return df

    def _fetch_prices_yfinance(self, ticker: str, date_from: str) -> Optional[pd.DataFrame]:
        """yfinance で株価取得"""
        yf_ticker = ticker if ticker.endswith(".T") else f"{ticker}.T"
        try:
            tk = yf.Ticker(yf_ticker)
            hist = tk.history(start=date_from, auto_adjust=True)
            if hist.empty:
                return None
            hist = hist.reset_index()
            # カラム名を標準化
            hist["Date"] = hist["Date"].dt.tz_localize(None)
            hist = hist.rename(columns={
                "Open": "Open", "High": "High", "Low": "Low",
                "Close": "Close", "Volume": "Volume",
            })
            return hist[["Date", "Open", "High", "Low", "Close", "Volume"]]
        except Exception as e:
            log.warning(f"[yfinance] 株価取得エラー ({yf_ticker}): {e}")
            return None

    # ─────────────────────────────────────────────
    # 財務データ取得
    # ─────────────────────────────────────────────
    def fetch_financials(self, ticker: str, force_refresh: bool = False) -> pd.DataFrame:
        """
        財務データ (PL・BS・CF) を取得。
        J-Quants → yfinance の順でフォールバック。
        """
        code_no_t = ticker.replace(".T", "")
        cache_path = RAW_DATA_DIR / f"{code_no_t}_financials.csv"

        if cache_path.exists() and not force_refresh:
            log.info(f"[Cache] {ticker} 財務データをキャッシュから読み込み")
            return pd.read_csv(cache_path)

        # ── J-Quants 試行 ───────────────────────────
        if self.source in ("jquants", "auto"):
            df = self._fetch_financials_jquants(code_no_t)
            if df is not None and not df.empty:
                df.to_csv(cache_path, index=False)
                log.info(f"[J-Quants] {ticker} 財務保存: {len(df)}件")
                return df
            log.info(f"[J-Quants] {ticker} 財務取得失敗。yfinanceにフォールバック。")

        # ── yfinance フォールバック ────────────────
        if self.source in ("yfinance", "auto"):
            df = self._fetch_financials_yfinance(ticker)
            if df is not None and not df.empty:
                df.to_csv(cache_path, index=False)
                log.info(f"[yfinance] {ticker} 財務保存")
                return df

        log.error(f"[Error] {ticker} 財務データ取得に失敗しました。")
        return pd.DataFrame()

    def _fetch_financials_jquants(self, code: str) -> Optional[pd.DataFrame]:
        """J-Quants /fins/statements で財務データ取得"""
        jquants_code = f"{code}0"
        resp = self._jquants_get("/fins/statements", params={"code": jquants_code})
        if resp is None:
            return None
        items = resp.json().get("statements", [])
        if not items:
            return None
        return pd.DataFrame(items)

    def _fetch_financials_yfinance(self, ticker: str) -> Optional[pd.DataFrame]:
        """
        yfinance で財務データ取得。
        収益計算書・貸借対照表・キャッシュフロー計算書を統合。
        """
        yf_ticker = ticker if ticker.endswith(".T") else f"{ticker}.T"
        try:
            tk = yf.Ticker(yf_ticker)
            info = tk.info

            # 主要財務指標を1行のDataFrameにまとめる
            fin_data = {
                "Ticker": yf_ticker,
                "FetchedAt": datetime.today().strftime("%Y-%m-%d"),
                # 損益計算書
                "Revenue": info.get("totalRevenue"),
                "GrossProfit": info.get("grossProfits"),
                "OperatingIncome": info.get("operatingCashflow"),
                "NetIncome": info.get("netIncomeToCommon"),
                "EPS": info.get("trailingEps"),
                # 貸借対照表
                "TotalAssets": info.get("totalAssets"),
                "TotalDebt": info.get("totalDebt"),
                "CashAndEquivalents": info.get("totalCash"),
                "NetAssets": info.get("bookValue"),  # book value per share
                "SharesOutstanding": info.get("sharesOutstanding"),
                # 財務比率 (yfinanceが直接提供)
                "ROE": info.get("returnOnEquity"),
                "ROA": info.get("returnOnAssets"),
                "PER": info.get("trailingPE"),
                "PBR": info.get("priceToBook"),
                "Beta": info.get("beta"),
                "DividendYield": info.get("dividendYield"),
                "MarketCap": info.get("marketCap"),
                # EBITDA
                "EBITDA": info.get("ebitda"),
                "FreeCashflow": info.get("freeCashflow"),
            }

            df = pd.DataFrame([fin_data])
            return df
        except Exception as e:
            log.warning(f"[yfinance] 財務データ取得エラー ({yf_ticker}): {e}")
            return None

    # ─────────────────────────────────────────────
    # Google Trends 取得
    # ─────────────────────────────────────────────
    def fetch_trends(self, keyword: str, force_refresh: bool = False) -> pd.DataFrame:
        """
        Google Trends データを取得 (日本語・日本地域)。
        HTTP 429 対策のリトライ付き。
        """
        safe_keyword = keyword.replace(" ", "_").replace("/", "-")
        cache_path = RAW_DATA_DIR / f"{safe_keyword}_trends.csv"

        if cache_path.exists() and not force_refresh:
            log.info(f"[Cache] '{keyword}' トレンドをキャッシュから読み込み")
            return pd.read_csv(cache_path, index_col=0, parse_dates=True)

        log.info(f"[PyTrends] '{keyword}' のトレンド取得中...")
        pytrends = TrendReq(hl="ja-JP", tz=540)

        for attempt in range(MAX_RETRIES):
            try:
                pytrends.build_payload(
                    kw_list=[keyword],
                    timeframe="today 3-y",
                    geo="JP",
                )
                df = pytrends.interest_over_time()
                if not df.empty:
                    if "isPartial" in df.columns:
                        df = df.drop(columns=["isPartial"])
                    df.to_csv(cache_path)
                    log.info(f"[PyTrends] '{keyword}' 保存完了: {len(df)}件")
                    return df
                break
            except Exception as e:
                if "429" in str(e) or "Too Many Requests" in str(e):
                    wait = RETRY_DELAY_SECONDS * (2 ** attempt)
                    log.warning(f"[PyTrends] Rate limit。{wait}秒後にリトライ")
                    time.sleep(wait)
                else:
                    log.warning(f"[PyTrends] エラー: {e}")
                    break

        return pd.DataFrame()

    # ─────────────────────────────────────────────
    # 銘柄情報取得
    # ─────────────────────────────────────────────
    def fetch_company_info(self, ticker: str) -> Dict[str, Any]:
        """
        yfinance から銘柄の基本情報を取得。
        """
        yf_ticker = ticker if ticker.endswith(".T") else f"{ticker}.T"
        try:
            tk = yf.Ticker(yf_ticker)
            info = tk.info
            return {
                "name": info.get("longName", info.get("shortName", yf_ticker)),
                "sector": info.get("sector", "不明"),
                "industry": info.get("industry", "不明"),
                "country": info.get("country", "Japan"),
                "currency": info.get("currency", "JPY"),
                "exchange": info.get("exchange", "TSE"),
                "description": info.get("longBusinessSummary", ""),
                "employees": info.get("fullTimeEmployees"),
                "website": info.get("website", ""),
            }
        except Exception as e:
            log.warning(f"[yfinance] 銘柄情報取得エラー ({yf_ticker}): {e}")
            return {"name": yf_ticker}
