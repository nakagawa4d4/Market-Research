"""
agent_analyst.py
----------------
アナリストペルソナ。

キャッシュ済みCSVから以下の金融指標を計算:
  - ROE (Return on Equity): 純利益 / 自己資本
  - WACC (Weighted Average Cost of Capital): (E/V)Re + (D/V)Rd(1-Tc)
  - 20日ボラティリティ (年率): std(日次リターン) * sqrt(252)
  - RSI (Relative Strength Index): 14日
  - ゴールデン/デッドクロス: SMA25 vs SMA75

可視化はすべて Plotly で生成し、HTMLレポートに組み込む。
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

from config import (
    PROCESSED_DATA_DIR,
    RAW_DATA_DIR,
    RISK_FREE_RATE,
    MARKET_RETURN,
    CORPORATE_TAX_RATE,
)

log = logging.getLogger(__name__)


class AgentAnalyst:
    """
    アナリストペルソナ。
    財務指標の計算とインタラクティブチャートの生成を担う。
    """

    def __init__(self, ticker: str, company_info: Dict[str, Any] = None):
        self.ticker = ticker.replace(".T", "")
        self.ticker_full = ticker if ticker.endswith(".T") else f"{ticker}.T"
        self.company_info = company_info or {}

        self.prices_file = RAW_DATA_DIR / f"{self.ticker}_prices.csv"
        self.fins_file = RAW_DATA_DIR / f"{self.ticker}_financials.csv"

        self.prices: pd.DataFrame = pd.DataFrame()
        self.fins: pd.DataFrame = pd.DataFrame()
        self._load_data()

    # ─────────────────────────────────────────────
    # データロード
    # ─────────────────────────────────────────────
    def _load_data(self) -> None:
        """キャッシュCSVからデータを読み込む。"""
        if self.prices_file.exists():
            self.prices = pd.read_csv(self.prices_file, parse_dates=["Date"])
            self.prices.sort_values("Date", inplace=True)
            self.prices.set_index("Date", inplace=True)
            log.info(f"[Analyst] {self.ticker} 株価データ読み込み: {len(self.prices)}件")
        else:
            log.warning(f"[Analyst] 株価ファイルが見つかりません: {self.prices_file}")

        if self.fins_file.exists():
            self.fins = pd.read_csv(self.fins_file)
            log.info(f"[Analyst] {self.ticker} 財務データ読み込み: {len(self.fins)}件")
        else:
            log.warning(f"[Analyst] 財務ファイルが見つかりません: {self.fins_file}")

    # ─────────────────────────────────────────────
    # 財務指標計算
    # ─────────────────────────────────────────────
    def calculate_volatility(self, window: int = 20) -> float:
        """
        年率換算の歴史的ボラティリティを計算。
        Formula: std(直近{window}日の日次リターン) × √252
        """
        if self.prices.empty or "Close" not in self.prices.columns:
            return 0.0
        returns = self.prices["Close"].pct_change().dropna()
        if len(returns) < window:
            return 0.0
        vol = returns.tail(window).std() * np.sqrt(252)
        return float(vol)

    def calculate_rsi(self, window: int = 14) -> float:
        """
        RSI (Relative Strength Index) を計算。
        RSI = 100 - 100 / (1 + RS)  ※ RS = 平均上昇幅 / 平均下落幅
        """
        if self.prices.empty or "Close" not in self.prices.columns:
            return 0.0
        delta = self.prices["Close"].diff().dropna()
        gain = delta.clip(lower=0).rolling(window).mean()
        loss = -delta.clip(upper=0).rolling(window).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi = 100 - 100 / (1 + rs)
        latest_rsi = rsi.iloc[-1]
        return float(latest_rsi) if not np.isnan(latest_rsi) else 0.0

    def detect_sma_cross(self) -> str:
        """
        SMA25とSMA75のゴールデン/デッドクロスを検出。
        Returns: "golden" | "dead" | "none"
        """
        if self.prices.empty or len(self.prices) < 75:
            return "none"
        sma25 = self.prices["Close"].rolling(25).mean()
        sma75 = self.prices["Close"].rolling(75).mean()
        if len(sma25) < 2:
            return "none"
        # 直前2日で交差しているか確認
        if sma25.iloc[-2] < sma75.iloc[-2] and sma25.iloc[-1] > sma75.iloc[-1]:
            return "golden"
        elif sma25.iloc[-2] > sma75.iloc[-2] and sma25.iloc[-1] < sma75.iloc[-1]:
            return "dead"
        return "none"

    def calculate_roe(self) -> float:
        """
        ROE (Return on Equity) を計算。
        Formula: 純利益 (NetIncome) / 自己資本 (NetAssets)

        J-Quants: ProfitLossAttributableToOwnersOfParent / NetAssets
        yfinance:  ROEを直接参照 (returnOnEquity)
        """
        if self.fins.empty:
            return 0.0
        latest = self.fins.iloc[-1]

        # yfinanceの場合: ROEが直接入っている
        if "ROE" in latest and pd.notna(latest["ROE"]):
            return float(latest["ROE"])

        # J-Quantsの場合: 計算
        try:
            net_income = float(latest.get("ProfitLossAttributableToOwnersOfParent", 0) or 0)
            equity = float(
                latest.get("NetAssets", 0) or latest.get("Equity", 0) or 0
            )
            if equity == 0:
                return 0.0
            return net_income / equity
        except (ValueError, TypeError):
            return 0.0

    def calculate_wacc(self) -> float:
        """
        WACC (加重平均資本コスト) を計算。

        Formula:
          Re = Rf + β(Rm - Rf)          ← CAPM: 株主資本コスト
          Rd = 有利子負債コスト (固定1.5% or 実績)
          WACC = (E/V)×Re + (D/V)×Rd×(1−Tc)
            E = 時価総額、D = 総負債、V = E + D、Tc = 法人税率
        """
        if self.fins.empty or self.prices.empty:
            return 0.0

        latest_fin = self.fins.iloc[-1]
        latest_price = float(self.prices["Close"].iloc[-1])

        try:
            beta = float(latest_fin.get("Beta", 1.0) or 1.0)

            # Re: 株主資本コスト (CAPM)
            re = RISK_FREE_RATE + beta * (MARKET_RETURN - RISK_FREE_RATE)

            # Rd: 負債コスト (1.5%固定 — 日本の社債利回り平均)
            rd = 0.015

            # E: 時価総額
            market_cap = float(latest_fin.get("MarketCap", 0) or 0)
            if market_cap == 0:
                shares = float(latest_fin.get("SharesOutstanding", 0) or 0)
                market_cap = shares * latest_price

            # D: 総負債
            total_debt = float(latest_fin.get("TotalDebt", 0) or 0)

            v = market_cap + total_debt
            if v == 0:
                return 0.0

            weight_e = market_cap / v
            weight_d = total_debt / v

            # WACC = (E/V)×Re + (D/V)×Rd×(1−Tc)
            wacc = (weight_e * re) + (weight_d * rd * (1 - CORPORATE_TAX_RATE))
            return float(wacc)

        except (ValueError, TypeError) as e:
            log.warning(f"[Analyst] WACC計算エラー: {e}")
            return 0.0

    def calculate_momentum(self, days: int = 60) -> float:
        """
        {days}日間のモメンタム(騰落率)を計算。
        Formula: (現在値 - {days}日前値) / {days}日前値
        """
        if self.prices.empty or len(self.prices) < days:
            return 0.0
        past = float(self.prices["Close"].iloc[-days])
        current = float(self.prices["Close"].iloc[-1])
        if past == 0:
            return 0.0
        return (current - past) / past

    # ─────────────────────────────────────────────
    # チャート生成
    # ─────────────────────────────────────────────
    def generate_price_chart(self) -> str:
        """
        ローソク足 + SMA25/75 + 出来高 のインタラクティブチャートを生成。
        plotly_dark テーマ。HTMLとして保存。
        """
        if self.prices.empty:
            return ""

        name = self.company_info.get("name", self.ticker)

        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            row_heights=[0.75, 0.25],
            vertical_spacing=0.03,
            subplot_titles=(f"{name} ({self.ticker}) 株価推移", "出来高"),
        )

        # ── ローソク足 ──────────────────────────
        fig.add_trace(
            go.Candlestick(
                x=self.prices.index,
                open=self.prices["Open"],
                high=self.prices["High"],
                low=self.prices["Low"],
                close=self.prices["Close"],
                name="株価",
                increasing_line_color="#26a69a",
                decreasing_line_color="#ef5350",
            ),
            row=1, col=1,
        )

        # ── 移動平均線 ──────────────────────────
        sma25 = self.prices["Close"].rolling(25).mean()
        sma75 = self.prices["Close"].rolling(75).mean()

        fig.add_trace(
            go.Scatter(
                x=self.prices.index, y=sma25,
                line=dict(color="#ffa726", width=1.5),
                name="SMA 25",
            ),
            row=1, col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=self.prices.index, y=sma75,
                line=dict(color="#42a5f5", width=1.5),
                name="SMA 75",
            ),
            row=1, col=1,
        )

        # ── 出来高 ──────────────────────────────
        if "Volume" in self.prices.columns:
            colors = [
                "#26a69a" if c >= o else "#ef5350"
                for c, o in zip(self.prices["Close"], self.prices["Open"])
            ]
            fig.add_trace(
                go.Bar(
                    x=self.prices.index, y=self.prices["Volume"],
                    marker_color=colors, name="出来高", opacity=0.7,
                ),
                row=2, col=1,
            )

        fig.update_layout(
            template="plotly_dark",
            xaxis_rangeslider_visible=False,
            height=600,
            font=dict(family="Noto Sans JP, sans-serif"),
            margin=dict(l=50, r=30, t=60, b=30),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            paper_bgcolor="#1a1a2e",
            plot_bgcolor="#1a1a2e",
        )
        fig.update_xaxes(
            rangebreaks=[dict(bounds=["sat", "mon"])],  # 土日ギャップ除去
            showgrid=True, gridcolor="#2a2a3e",
        )
        fig.update_yaxes(showgrid=True, gridcolor="#2a2a3e")

        chart_path = PROCESSED_DATA_DIR / f"{self.ticker}_price_chart.html"
        fig.write_html(str(chart_path), full_html=False, include_plotlyjs="cdn")
        log.info(f"[Analyst] 価格チャート生成: {chart_path}")
        return str(chart_path)

    def generate_rsi_chart(self) -> str:
        """RSIチャートを生成。"""
        if self.prices.empty:
            return ""

        delta = self.prices["Close"].diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = -delta.clip(upper=0).rolling(14).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi = 100 - 100 / (1 + rs)

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=self.prices.index, y=rsi,
            line=dict(color="#7e57c2", width=2),
            name="RSI (14)",
        ))
        # 水平線
        fig.add_hline(y=70, line=dict(color="#ef5350", dash="dash"), annotation_text="過買い (70)")
        fig.add_hline(y=30, line=dict(color="#26a69a", dash="dash"), annotation_text="過売り (30)")
        fig.add_hline(y=50, line=dict(color="#90a4ae", dash="dot", width=0.8))

        fig.update_layout(
            template="plotly_dark",
            title="RSI (14日)",
            height=280,
            paper_bgcolor="#1a1a2e",
            plot_bgcolor="#1a1a2e",
            margin=dict(l=50, r=30, t=40, b=30),
        )
        fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])], showgrid=True, gridcolor="#2a2a3e")
        fig.update_yaxes(range=[0, 100], showgrid=True, gridcolor="#2a2a3e")

        path = PROCESSED_DATA_DIR / f"{self.ticker}_rsi_chart.html"
        fig.write_html(str(path), full_html=False, include_plotlyjs=False)
        return str(path)

    def generate_returns_histogram(self) -> str:
        """日次リターンのヒストグラムを生成。"""
        if self.prices.empty:
            return ""

        returns = self.prices["Close"].pct_change().dropna() * 100

        fig = px.histogram(
            returns, nbins=60,
            title="日次リターン分布",
            labels={"value": "日次リターン (%)", "count": "頻度"},
            color_discrete_sequence=["#7e57c2"],
            template="plotly_dark",
        )
        fig.update_layout(
            height=280,
            paper_bgcolor="#1a1a2e",
            plot_bgcolor="#1a1a2e",
            margin=dict(l=50, r=30, t=40, b=30),
            showlegend=False,
        )

        path = PROCESSED_DATA_DIR / f"{self.ticker}_returns_hist.html"
        fig.write_html(str(path), full_html=False, include_plotlyjs=False)
        return str(path)

    # ─────────────────────────────────────────────
    # サマリー出力
    # ─────────────────────────────────────────────
    def output_summary(self) -> Dict[str, Any]:
        """
        全指標を計算し、Fund Managerに渡すサマリーを返す。
        """
        roe = self.calculate_roe()
        wacc = self.calculate_wacc()
        vol = self.calculate_volatility()
        rsi = self.calculate_rsi()
        cross = self.detect_sma_cross()
        mom_60 = self.calculate_momentum(60)

        latest_price = float(self.prices["Close"].iloc[-1]) if not self.prices.empty else 0.0
        latest_date = str(self.prices.index[-1].date()) if not self.prices.empty else "N/A"

        chart_path = self.generate_price_chart()
        rsi_path = self.generate_rsi_chart()
        hist_path = self.generate_returns_histogram()

        summary = {
            "ticker": self.ticker_full,
            "name": self.company_info.get("name", self.ticker),
            "sector": self.company_info.get("sector", "不明"),
            "industry": self.company_info.get("industry", "不明"),
            "latest_price": latest_price,
            "latest_date": latest_date,
            # 財務指標
            "roe": roe,                     # Return on Equity
            "wacc": wacc,                   # Weighted Average Cost of Capital
            # テクニカル指標
            "volatility_20d_ann": vol,      # 20日年率ボラティリティ
            "rsi_14": rsi,                  # RSI (14日)
            "sma_cross": cross,             # ゴールデン/デッドクロス
            "momentum_60d": mom_60,         # 60日モメンタム
            # チャートパス
            "chart_paths": {
                "price": chart_path,
                "rsi": rsi_path,
                "histogram": hist_path,
            },
        }

        log.info(f"[Analyst] {self.ticker} サマリー生成完了")
        return summary
