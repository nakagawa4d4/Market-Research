"""
agent_fund_manager.py
---------------------
ファンドマネージャーペルソナ。

アナリストのサマリーを受け取り、批判的・独立的な視点から
投資判断（Buy / Hold / Sell）とアロケーション提案を行う。

判断ロジック:
  1. バリュエーション (WACC vs ROE のスプレッド)
  2. モメンタム (60日騰落率, RSI)
  3. リスク (ボラティリティ, SMAクロス)
  4. 定性的シグナル (セクター・トレンド) ← 将来拡張用

スコアリングモデル:
  - 各指標を +2 / +1 / 0 / -1 / -2 でスコア化
  - 合計スコアで Buy(≥4) / Hold(0-3) / Sell(≤-1) を判定
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from config import REPORTS_DIR

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────
# スコアリング関数
# ─────────────────────────────────────────────────────────

def _score_roe_vs_wacc(roe: float, wacc: float) -> Tuple[int, str]:
    """
    ROE - WACC スプレッド評価。
    スプレッド > 0 → 株主価値創出。
    """
    if wacc == 0:
        return 0, "WACCデータなし"
    spread = roe - wacc
    if spread > 0.10:
        return 2, f"ROE-WACC スプレッド {spread*100:.1f}pt (非常に優秀)"
    elif spread > 0.03:
        return 1, f"ROE-WACC スプレッド {spread*100:.1f}pt (良好)"
    elif spread > 0:
        return 0, f"ROE-WACC スプレッド {spread*100:.1f}pt (やや弱い)"
    else:
        return -1, f"ROE-WACC スプレッド {spread*100:.1f}pt (価値破壊)"


def _score_rsi(rsi: float) -> Tuple[int, str]:
    """
    RSIによるモメンタム評価。
    """
    if 40 <= rsi <= 60:
        return 1, f"RSI {rsi:.1f}: 中立ゾーン"
    elif 60 < rsi <= 70:
        return 2, f"RSI {rsi:.1f}: 強気モメンタム"
    elif rsi > 70:
        return -1, f"RSI {rsi:.1f}: 過買い圏 (調整リスク)"
    elif 30 <= rsi < 40:
        return 0, f"RSI {rsi:.1f}: やや弱い"
    else:  # rsi < 30
        return 1, f"RSI {rsi:.1f}: 過売り圏 (反転期待)"


def _score_volatility(vol: float) -> Tuple[int, str]:
    """
    ボラティリティ (年率) によるリスク評価。
    日本株平均: ~20-25%
    """
    if vol < 0.15:
        return 1, f"ボラティリティ {vol*100:.1f}% (低リスク)"
    elif vol < 0.30:
        return 0, f"ボラティリティ {vol*100:.1f}% (標準的)"
    elif vol < 0.50:
        return -1, f"ボラティリティ {vol*100:.1f}% (高リスク)"
    else:
        return -2, f"ボラティリティ {vol*100:.1f}% (非常に高リスク)"


def _score_momentum(momentum_60d: float) -> Tuple[int, str]:
    """
    60日モメンタム評価。
    """
    pct = momentum_60d * 100
    if pct > 15:
        return 2, f"60日モメンタム +{pct:.1f}% (強い上昇トレンド)"
    elif pct > 5:
        return 1, f"60日モメンタム +{pct:.1f}% (緩やかな上昇)"
    elif pct > -5:
        return 0, f"60日モメンタム {pct:.1f}% (横ばい)"
    elif pct > -15:
        return -1, f"60日モメンタム {pct:.1f}% (下落トレンド)"
    else:
        return -2, f"60日モメンタム {pct:.1f}% (急落)"


def _score_sma_cross(cross: str) -> Tuple[int, str]:
    """
    SMAクロスシグナル評価。
    """
    if cross == "golden":
        return 2, "SMA25がSMA75を上抜け (ゴールデンクロス: 買いシグナル)"
    elif cross == "dead":
        return -2, "SMA25がSMA75を下抜け (デッドクロス: 売りシグナル)"
    else:
        return 0, "SMAクロスなし (トレンド継続)"


# ─────────────────────────────────────────────────────────
# ファンドマネージャー本体
# ─────────────────────────────────────────────────────────

class AgentFundManager:
    """
    ファンドマネージャーペルソナ。
    アナリストのサマリーを受け取り、投資判断を下す。
    """

    VERDICT_THRESHOLDS = {
        "Strong Buy": 6,
        "Buy": 3,
        "Hold": 0,
        "Sell": -3,
        "Strong Sell": float("-inf"),
    }

    def __init__(self, analyst_summary: Dict[str, Any]):
        self.summary = analyst_summary
        self.ticker = analyst_summary.get("ticker", "N/A")
        self.name = analyst_summary.get("name", self.ticker)

    def evaluate(self) -> Dict[str, Any]:
        """
        アナリストサマリーを評価し、投資判断を返す。
        """
        scores: List[Tuple[str, int, str]] = []

        # 1. ROE vs WACC スプレッド
        score, reason = _score_roe_vs_wacc(
            self.summary.get("roe", 0),
            self.summary.get("wacc", 0),
        )
        scores.append(("バリュエーション (ROE-WACC)", score, reason))

        # 2. RSI モメンタム
        score, reason = _score_rsi(self.summary.get("rsi_14", 50))
        scores.append(("テクニカル (RSI)", score, reason))

        # 3. ボラティリティ
        score, reason = _score_volatility(self.summary.get("volatility_20d_ann", 0.25))
        scores.append(("リスク (ボラティリティ)", score, reason))

        # 4. 60日モメンタム
        score, reason = _score_momentum(self.summary.get("momentum_60d", 0))
        scores.append(("モメンタム (60日)", score, reason))

        # 5. SMAクロス
        score, reason = _score_sma_cross(self.summary.get("sma_cross", "none"))
        scores.append(("SMAクロス", score, reason))

        total_score = sum(s[1] for s in scores)

        # ── 投資判断 (Verdict) ──────────────────
        verdict = "Hold"
        for v, threshold in self.VERDICT_THRESHOLDS.items():
            if total_score >= threshold:
                verdict = v
                break

        # ── ポジションサイズ提案 ─────────────────
        position_pct = self._suggest_position(verdict, self.summary.get("volatility_20d_ann", 0.25))

        return {
            "ticker": self.ticker,
            "name": self.name,
            "verdict": verdict,
            "total_score": total_score,
            "max_score": len(scores) * 2,
            "scores": scores,
            "position_suggestion_pct": position_pct,
            "evaluated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "analyst_summary": self.summary,
        }

    def _suggest_position(self, verdict: str, volatility: float) -> float:
        """
        Kelly基準の簡易版で推奨ポジションサイズ(%)を算出。
        """
        base = {"Strong Buy": 8.0, "Buy": 5.0, "Hold": 2.0, "Sell": 0.0, "Strong Sell": 0.0}
        pos = base.get(verdict, 2.0)
        # ボラティリティが高い場合はポジション縮小
        if volatility > 0.40:
            pos *= 0.5
        elif volatility > 0.30:
            pos *= 0.75
        return round(pos, 1)

    def generate_report_text(self, evaluation: Dict[str, Any]) -> str:
        """
        ファンドマネージャーの評価をテキストレポートとして生成。
        """
        verdict_emoji = {
            "Strong Buy": "🟢🟢",
            "Buy": "🟢",
            "Hold": "🟡",
            "Sell": "🔴",
            "Strong Sell": "🔴🔴",
        }
        emoji = verdict_emoji.get(evaluation["verdict"], "⚪")

        lines = [
            f"{'='*60}",
            f"  [{evaluation['ticker']}] {evaluation['name']}",
            f"  ファンドマネージャー評価: {emoji} {evaluation['verdict'].upper()}",
            f"  スコア: {evaluation['total_score']} / {evaluation['max_score']}",
            f"  推奨ポジション: {evaluation['position_suggestion_pct']}%",
            f"  評価日時: {evaluation['evaluated_at']}",
            f"{'='*60}",
            "",
            "【スコアリング詳細】",
        ]

        for name, score, reason in evaluation["scores"]:
            sign = "+" if score > 0 else ""
            lines.append(f"  • {name}: {sign}{score}pt → {reason}")

        lines += [
            "",
            "【アナリスト提供データ】",
            f"  現在株価: ¥{evaluation['analyst_summary'].get('latest_price', 0):,.0f}",
            f"  ROE: {evaluation['analyst_summary'].get('roe', 0)*100:.2f}%",
            f"  WACC: {evaluation['analyst_summary'].get('wacc', 0)*100:.2f}%",
            f"  20日年率ボラティリティ: {evaluation['analyst_summary'].get('volatility_20d_ann', 0)*100:.2f}%",
            f"  RSI(14): {evaluation['analyst_summary'].get('rsi_14', 0):.1f}",
            f"  60日モメンタム: {evaluation['analyst_summary'].get('momentum_60d', 0)*100:.2f}%",
            f"  SMAクロス: {evaluation['analyst_summary'].get('sma_cross', 'none')}",
            "",
        ]

        return "\n".join(lines)
