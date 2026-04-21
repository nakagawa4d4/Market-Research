"""
report_generator.py
-------------------
アナリスト + ファンドマネージャーの評価結果から
インタラクティブなHTMLレポートを生成する。

デザイン: ダークテーマ + glassmorphism
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from config import PROCESSED_DATA_DIR, REPORTS_DIR

log = logging.getLogger(__name__)


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{ticker} エクイティリサーチ | dexter_py</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@300;400;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
<style>
  :root {{
    --bg: #0d0d1a;
    --surface: rgba(255,255,255,0.04);
    --surface-hover: rgba(255,255,255,0.08);
    --border: rgba(255,255,255,0.1);
    --accent: #7c4dff;
    --accent2: #00e5ff;
    --green: #26a69a;
    --red: #ef5350;
    --yellow: #ffa726;
    --text: #e0e0e0;
    --text-muted: #9e9e9e;
    --radius: 16px;
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: 'Noto Sans JP', sans-serif;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
    padding: 32px 16px;
  }}
  /* 背景グラデーション */
  body::before {{
    content: '';
    position: fixed; inset: 0; z-index: -1;
    background:
      radial-gradient(ellipse at 20% 20%, rgba(124,77,255,0.15) 0%, transparent 50%),
      radial-gradient(ellipse at 80% 80%, rgba(0,229,255,0.1) 0%, transparent 50%);
  }}

  .container {{ max-width: 1200px; margin: 0 auto; }}

  /* ヘッダー */
  .header {{
    text-align: center;
    padding: 48px 24px;
    margin-bottom: 32px;
  }}
  .header .badge {{
    display: inline-block;
    padding: 6px 18px;
    border-radius: 100px;
    background: rgba(124,77,255,0.2);
    border: 1px solid var(--accent);
    font-size: 12px;
    color: #b39ddb;
    letter-spacing: 2px;
    text-transform: uppercase;
    margin-bottom: 16px;
  }}
  .header h1 {{
    font-size: clamp(28px, 5vw, 48px);
    font-weight: 700;
    background: linear-gradient(135deg, #fff 0%, #b39ddb 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    margin-bottom: 8px;
  }}
  .header .subtitle {{
    color: var(--text-muted);
    font-size: 15px;
  }}

  /* グラスカード */
  .card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 28px;
    margin-bottom: 24px;
    backdrop-filter: blur(12px);
    transition: border-color 0.3s, background 0.3s;
  }}
  .card:hover {{
    border-color: rgba(124,77,255,0.3);
    background: var(--surface-hover);
  }}
  .card-title {{
    font-size: 13px;
    font-weight: 600;
    color: var(--accent2);
    text-transform: uppercase;
    letter-spacing: 1.5px;
    margin-bottom: 20px;
    display: flex;
    align-items: center;
    gap: 8px;
  }}
  .card-title::after {{
    content: '';
    flex: 1;
    height: 1px;
    background: var(--border);
  }}

  /* ベルディクト (投資判断) */
  .verdict-card {{
    background: linear-gradient(135deg, rgba(124,77,255,0.15) 0%, rgba(0,229,255,0.08) 100%);
    border: 1px solid rgba(124,77,255,0.4);
    border-radius: var(--radius);
    padding: 40px;
    text-align: center;
    margin-bottom: 24px;
  }}
  .verdict-label {{
    font-size: 13px;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 2px;
    margin-bottom: 12px;
  }}
  .verdict-text {{
    font-size: clamp(36px, 6vw, 64px);
    font-weight: 700;
    letter-spacing: 2px;
  }}
  .verdict-buy {{ color: #26a69a; }}
  .verdict-strong-buy {{ color: #00e676; }}
  .verdict-hold {{ color: #ffa726; }}
  .verdict-sell {{ color: #ef5350; }}
  .verdict-strong-sell {{ color: #d50000; }}
  .verdict-score {{
    margin-top: 16px;
    font-size: 15px;
    color: var(--text-muted);
  }}
  .verdict-position {{
    margin-top: 12px;
    display: inline-block;
    padding: 8px 24px;
    border-radius: 100px;
    background: rgba(255,255,255,0.06);
    border: 1px solid var(--border);
    font-size: 14px;
  }}

  /* KPIグリッド */
  .kpi-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
  }}
  .kpi-item {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 20px;
    text-align: center;
    transition: transform 0.2s, border-color 0.2s;
  }}
  .kpi-item:hover {{
    transform: translateY(-2px);
    border-color: rgba(124,77,255,0.3);
  }}
  .kpi-label {{
    font-size: 11px;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 1.2px;
    margin-bottom: 8px;
  }}
  .kpi-value {{
    font-size: 24px;
    font-weight: 700;
    font-family: 'JetBrains Mono', monospace;
  }}
  .kpi-value.positive {{ color: var(--green); }}
  .kpi-value.negative {{ color: var(--red); }}
  .kpi-value.neutral {{ color: var(--accent2); }}
  .kpi-sub {{
    font-size: 11px;
    color: var(--text-muted);
    margin-top: 4px;
  }}

  /* スコアリングテーブル */
  .score-table {{ width: 100%; border-collapse: collapse; }}
  .score-table tr {{ border-bottom: 1px solid var(--border); }}
  .score-table tr:last-child {{ border-bottom: none; }}
  .score-table td {{
    padding: 14px 8px;
    font-size: 14px;
    vertical-align: middle;
  }}
  .score-table .label {{ color: var(--text-muted); width: 40%; }}
  .score-table .reason {{ flex: 1; color: var(--text); }}
  .score-badge {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 40px; height: 28px;
    border-radius: 6px;
    font-weight: 700;
    font-family: 'JetBrains Mono', monospace;
    font-size: 13px;
  }}
  .score-pos {{ background: rgba(38,166,154,0.2); color: #26a69a; border: 1px solid rgba(38,166,154,0.4); }}
  .score-neg {{ background: rgba(239,83,80,0.2); color: #ef5350; border: 1px solid rgba(239,83,80,0.4); }}
  .score-zero {{ background: rgba(158,158,158,0.15); color: #9e9e9e; border: 1px solid rgba(158,158,158,0.3); }}

  /* チャートコンテナ */
  .chart-container {{ width: 100%; margin-bottom: 8px; }}

  /* 2カラムレイアウト */
  .two-col {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 24px;
    margin-bottom: 24px;
  }}
  @media (max-width: 768px) {{ .two-col {{ grid-template-columns: 1fr; }} }}

  /* フッター */
  .footer {{
    text-align: center;
    padding: 32px;
    color: var(--text-muted);
    font-size: 12px;
    border-top: 1px solid var(--border);
    margin-top: 48px;
  }}

  /* アニメーション */
  @keyframes fadeInUp {{
    from {{ opacity: 0; transform: translateY(20px); }}
    to {{ opacity: 1; transform: translateY(0); }}
  }}
  .animate {{ animation: fadeInUp 0.5s ease forwards; }}
  .delay-1 {{ animation-delay: 0.1s; opacity: 0; }}
  .delay-2 {{ animation-delay: 0.2s; opacity: 0; }}
  .delay-3 {{ animation-delay: 0.3s; opacity: 0; }}
</style>
</head>
<body>
<div class="container">

  <!-- ヘッダー -->
  <div class="header animate">
    <div class="badge">dexter_py · Autonomous Equity Research</div>
    <h1>{name} ({ticker})</h1>
    <div class="subtitle">{sector} · {industry} &nbsp;|&nbsp; 生成日時: {generated_at}</div>
  </div>

  <!-- 投資判断 -->
  <div class="verdict-card animate delay-1">
    <div class="verdict-label">ファンドマネージャー判断</div>
    <div class="verdict-text {verdict_class}">{verdict}</div>
    <div class="verdict-score">総合スコア: {total_score} / {max_score}点</div>
    <div class="verdict-position">推奨ポジションサイズ: {position_pct}%</div>
  </div>

  <!-- KPI グリッド -->
  <div class="kpi-grid animate delay-2">
    <div class="kpi-item">
      <div class="kpi-label">現在株価</div>
      <div class="kpi-value neutral">¥{latest_price:,.0f}</div>
      <div class="kpi-sub">{latest_date}</div>
    </div>
    <div class="kpi-item">
      <div class="kpi-label">ROE</div>
      <div class="kpi-value {roe_class}">{roe_pct:.2f}%</div>
      <div class="kpi-sub">Return on Equity</div>
    </div>
    <div class="kpi-item">
      <div class="kpi-label">WACC</div>
      <div class="kpi-value neutral">{wacc_pct:.2f}%</div>
      <div class="kpi-sub">加重平均資本コスト</div>
    </div>
    <div class="kpi-item">
      <div class="kpi-label">ROE-WACC スプレッド</div>
      <div class="kpi-value {spread_class}">{spread_pct:+.2f}%</div>
      <div class="kpi-sub">価値創出能力</div>
    </div>
    <div class="kpi-item">
      <div class="kpi-label">RSI (14日)</div>
      <div class="kpi-value neutral">{rsi:.1f}</div>
      <div class="kpi-sub">{rsi_signal}</div>
    </div>
    <div class="kpi-item">
      <div class="kpi-label">20日ボラティリティ</div>
      <div class="kpi-value neutral">{vol_pct:.1f}%</div>
      <div class="kpi-sub">年率換算</div>
    </div>
    <div class="kpi-item">
      <div class="kpi-label">60日モメンタム</div>
      <div class="kpi-value {mom_class}">{mom_pct:+.2f}%</div>
      <div class="kpi-sub">騰落率</div>
    </div>
    <div class="kpi-item">
      <div class="kpi-label">SMAクロス</div>
      <div class="kpi-value neutral">{sma_cross_label}</div>
      <div class="kpi-sub">SMA25 vs SMA75</div>
    </div>
  </div>

  <!-- スコアリング詳細 -->
  <div class="card animate delay-3">
    <div class="card-title">📊 スコアリング詳細</div>
    <table class="score-table">
      {score_rows}
    </table>
  </div>

  <!-- 株価チャート -->
  <div class="card">
    <div class="card-title">📈 株価チャート (ローソク足 + SMA)</div>
    <div class="chart-container">
      {price_chart_html}
    </div>
  </div>

  <!-- RSI & ヒストグラム -->
  <div class="two-col">
    <div class="card">
      <div class="card-title">📉 RSI (14日)</div>
      <div class="chart-container">
        {rsi_chart_html}
      </div>
    </div>
    <div class="card">
      <div class="card-title">📊 日次リターン分布</div>
      <div class="chart-container">
        {hist_chart_html}
      </div>
    </div>
  </div>

  <!-- フッター -->
  <div class="footer">
    <p>このレポートは <strong>dexter_py</strong> によって自動生成されました。</p>
    <p>投資判断はスコアリングモデルに基づく参考情報であり、投資助言ではありません。</p>
    <p>Generated at {generated_at} · {ticker}</p>
  </div>

</div>
</body>
</html>"""


class ReportGenerator:
    """
    アナリスト + ファンドマネージャーの評価からHTMLレポートを生成。
    """

    def __init__(self, evaluation: Dict[str, Any]):
        self.ev = evaluation
        self.summary = evaluation.get("analyst_summary", {})

    def _read_chart(self, path: str) -> str:
        """チャートHTMLを読み込む。"""
        if not path:
            return "<p style='color:#666;text-align:center'>チャートデータなし</p>"
        p = Path(path)
        if not p.exists():
            return "<p style='color:#666;text-align:center'>チャートファイル未生成</p>"
        return p.read_text(encoding="utf-8")

    def _score_rows_html(self) -> str:
        rows = []
        for label, score, reason in self.ev.get("scores", []):
            if score > 0:
                badge_class = "score-pos"
                badge_text = f"+{score}"
            elif score < 0:
                badge_class = "score-neg"
                badge_text = str(score)
            else:
                badge_class = "score-zero"
                badge_text = "0"

            rows.append(
                f"<tr>"
                f"<td class='label'>{label}</td>"
                f"<td><span class='score-badge {badge_class}'>{badge_text}</span></td>"
                f"<td class='reason'>{reason}</td>"
                f"</tr>"
            )
        return "\n".join(rows)

    def _verdict_css_class(self) -> str:
        mapping = {
            "Strong Buy": "verdict-strong-buy",
            "Buy": "verdict-buy",
            "Hold": "verdict-hold",
            "Sell": "verdict-sell",
            "Strong Sell": "verdict-strong-sell",
        }
        return mapping.get(self.ev.get("verdict", "Hold"), "verdict-hold")

    def _rsi_signal_text(self, rsi: float) -> str:
        if rsi > 70:
            return "過買い"
        elif rsi > 60:
            return "強気"
        elif rsi > 40:
            return "中立"
        elif rsi > 30:
            return "弱気"
        else:
            return "過売り"

    def _sma_cross_label(self, cross: str) -> str:
        return {"golden": "🟢 GC", "dead": "🔴 DC", "none": "―"}.get(cross, "―")

    def generate(self) -> str:
        """HTMLレポートを生成し、ファイルパスを返す。"""
        roe = self.summary.get("roe", 0) or 0
        wacc = self.summary.get("wacc", 0) or 0
        spread = roe - wacc
        vol = self.summary.get("volatility_20d_ann", 0) or 0
        rsi = self.summary.get("rsi_14", 50) or 50
        mom = self.summary.get("momentum_60d", 0) or 0

        # チャート読み込み
        chart_paths = self.summary.get("chart_paths", {})
        price_html = self._read_chart(chart_paths.get("price", ""))
        rsi_html = self._read_chart(chart_paths.get("rsi", ""))
        hist_html = self._read_chart(chart_paths.get("histogram", ""))

        ticker = self.ev.get("ticker", "N/A")
        safe_ticker = ticker.replace(".", "_")

        html = HTML_TEMPLATE.format(
            ticker=ticker,
            name=self.ev.get("name", ticker),
            sector=self.summary.get("sector", "不明"),
            industry=self.summary.get("industry", "不明"),
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M JST"),
            # 判断
            verdict=self.ev.get("verdict", "Hold"),
            verdict_class=self._verdict_css_class(),
            total_score=self.ev.get("total_score", 0),
            max_score=self.ev.get("max_score", 10),
            position_pct=self.ev.get("position_suggestion_pct", 0),
            # KPI
            latest_price=self.summary.get("latest_price", 0),
            latest_date=self.summary.get("latest_date", "N/A"),
            roe_pct=roe * 100,
            roe_class="positive" if roe > 0 else "negative",
            wacc_pct=wacc * 100,
            spread_pct=spread * 100,
            spread_class="positive" if spread > 0 else "negative",
            rsi=rsi,
            rsi_signal=self._rsi_signal_text(rsi),
            vol_pct=vol * 100,
            mom_pct=mom * 100,
            mom_class="positive" if mom > 0 else "negative",
            sma_cross_label=self._sma_cross_label(self.summary.get("sma_cross", "none")),
            # スコア詳細
            score_rows=self._score_rows_html(),
            # チャート
            price_chart_html=price_html,
            rsi_chart_html=rsi_html,
            hist_chart_html=hist_html,
        )

        # 保存
        filename = f"{safe_ticker}_equity_report_{datetime.now().strftime('%Y%m%d_%H%M')}.html"
        report_path = REPORTS_DIR / filename
        report_path.write_text(html, encoding="utf-8")
        log.info(f"[Report] HTMLレポート生成: {report_path}")
        return str(report_path)
