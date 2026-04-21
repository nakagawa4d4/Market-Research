"""
report_generator.py
-------------------
スクリーニング結果を受け取り、インタラクティブな HTML レポートを生成する。

出力: daily_screener/reports/report_YYYY-MM-DD.html
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
REPORTS_DIR = BASE_DIR / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────
# HTML テンプレート（Jinja2 非依存の f-string 版）
# ─────────────────────────────────────────────
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>🇯🇵 日本株 注目銘柄レポート — {date}</title>
  <meta name="description" content="J-Quants APIを使った日本株の注目銘柄スクリーニングレポート。{date}分。" />
  <link
    href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&family=Noto+Sans+JP:wght@300;400;700&display=swap"
    rel="stylesheet"
  />
  <style>
    :root {{
      --bg: #0d1117;
      --surface: #161b22;
      --surface2: #21262d;
      --border: #30363d;
      --primary: #58a6ff;
      --green: #3fb950;
      --red: #f85149;
      --gold: #d29922;
      --text: #e6edf3;
      --text-sub: #8b949e;
      --radius: 12px;
    }}

    * {{ box-sizing: border-box; margin: 0; padding: 0; }}

    body {{
      font-family: 'Inter', 'Noto Sans JP', sans-serif;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
    }}

    /* ── Header ── */
    .header {{
      background: linear-gradient(135deg, #0d1117 0%, #1a2332 50%, #0d1117 100%);
      border-bottom: 1px solid var(--border);
      padding: 32px 48px;
      position: relative;
      overflow: hidden;
    }}
    .header::before {{
      content: '';
      position: absolute;
      top: -50%;
      left: -10%;
      width: 400px;
      height: 400px;
      background: radial-gradient(circle, rgba(88,166,255,0.08) 0%, transparent 70%);
      pointer-events: none;
    }}
    .header-top {{
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      flex-wrap: wrap;
      gap: 16px;
    }}
    .header h1 {{
      font-size: 28px;
      font-weight: 700;
      letter-spacing: -0.5px;
      background: linear-gradient(90deg, #58a6ff, #79c0ff);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
    }}
    .header .date-badge {{
      background: var(--surface2);
      border: 1px solid var(--border);
      border-radius: 20px;
      padding: 6px 16px;
      font-size: 13px;
      color: var(--text-sub);
    }}
    .header .subtitle {{
      margin-top: 8px;
      color: var(--text-sub);
      font-size: 14px;
      font-weight: 300;
    }}

    /* ── Stats Bar ── */
    .stats-bar {{
      display: flex;
      gap: 16px;
      padding: 24px 48px;
      flex-wrap: wrap;
      background: var(--surface);
      border-bottom: 1px solid var(--border);
    }}
    .stat-card {{
      flex: 1;
      min-width: 140px;
      background: var(--surface2);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 16px 20px;
      transition: transform 0.2s, box-shadow 0.2s;
    }}
    .stat-card:hover {{
      transform: translateY(-2px);
      box-shadow: 0 8px 24px rgba(0,0,0,0.4);
    }}
    .stat-card .label {{ font-size: 12px; color: var(--text-sub); margin-bottom: 6px; }}
    .stat-card .value {{ font-size: 24px; font-weight: 700; }}
    .stat-card .value.up {{ color: var(--green); }}
    .stat-card .value.down {{ color: var(--red); }}
    .stat-card .value.neutral {{ color: var(--primary); }}

    /* ── Main ── */
    .main {{ padding: 32px 48px; }}

    /* ── Section ── */
    .section {{ margin-bottom: 40px; }}
    .section-title {{
      font-size: 18px;
      font-weight: 600;
      margin-bottom: 20px;
      display: flex;
      align-items: center;
      gap: 8px;
    }}
    .section-title::after {{
      content: '';
      flex: 1;
      height: 1px;
      background: var(--border);
      margin-left: 12px;
    }}

    /* ── Table ── */
    .table-wrap {{
      overflow-x: auto;
      border-radius: var(--radius);
      border: 1px solid var(--border);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    thead tr {{
      background: var(--surface2);
    }}
    thead th {{
      padding: 12px 16px;
      text-align: left;
      font-weight: 600;
      color: var(--text-sub);
      cursor: pointer;
      user-select: none;
      white-space: nowrap;
    }}
    thead th:hover {{ color: var(--primary); }}
    tbody tr {{
      border-top: 1px solid var(--border);
      transition: background 0.15s;
    }}
    tbody tr:hover {{ background: var(--surface2); }}
    tbody td {{
      padding: 12px 16px;
      white-space: nowrap;
    }}
    .rank {{ font-weight: 700; color: var(--text-sub); }}
    .code {{ font-family: monospace; color: var(--primary); font-weight: 600; }}
    .name {{ font-weight: 500; max-width: 200px; overflow: hidden; text-overflow: ellipsis; }}
    .change-up {{ color: var(--green); font-weight: 600; }}
    .change-down {{ color: var(--red); font-weight: 600; }}
    .change-neutral {{ color: var(--text-sub); }}
    .score-bar {{
      display: flex;
      align-items: center;
      gap: 8px;
    }}
    .score-bar-bg {{
      flex: 1;
      height: 6px;
      background: var(--border);
      border-radius: 3px;
      min-width: 80px;
    }}
    .score-bar-fill {{
      height: 100%;
      border-radius: 3px;
      background: linear-gradient(90deg, #58a6ff, #a5d6ff);
    }}
    .badge {{
      display: inline-block;
      padding: 2px 8px;
      border-radius: 20px;
      font-size: 11px;
      font-weight: 600;
    }}
    .badge-golden {{ background: rgba(210,153,34,0.2); color: var(--gold); border: 1px solid rgba(210,153,34,0.3); }}
    .badge-dead {{ background: rgba(248,81,73,0.15); color: var(--red); border: 1px solid rgba(248,81,73,0.3); }}
    .badge-rsi-low {{ background: rgba(63,185,80,0.15); color: var(--green); border: 1px solid rgba(63,185,80,0.3); }}
    .badge-rsi-high {{ background: rgba(248,81,73,0.15); color: var(--red); border: 1px solid rgba(248,81,73,0.3); }}
    .badge-vol {{ background: rgba(88,166,255,0.15); color: var(--primary); border: 1px solid rgba(88,166,255,0.3); }}

    /* ── Chart container ── */
    .chart-container {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 8px;
    }}

    /* ── Footer ── */
    .footer {{
      text-align: center;
      padding: 32px;
      color: var(--text-sub);
      font-size: 12px;
      border-top: 1px solid var(--border);
    }}
    .footer a {{ color: var(--primary); text-decoration: none; }}

    /* ── Responsive ── */
    @media (max-width: 768px) {{
      .header, .stats-bar, .main {{ padding: 20px 16px; }}
      .header h1 {{ font-size: 20px; }}
    }}
  </style>
</head>
<body>

<!-- ── Header ── -->
<div class="header">
  <div class="header-top">
    <div>
      <h1>🇯🇵 日本株 注目銘柄レポート</h1>
      <p class="subtitle">J-Quants API × テクニカル分析による自動スクリーニング</p>
    </div>
    <span class="date-badge">📅 {date}</span>
  </div>
</div>

<!-- ── Stats Bar ── -->
<div class="stats-bar">
  {stats_cards}
</div>

<!-- ── Main ── -->
<div class="main">

  <!-- 注目銘柄テーブル -->
  <div class="section">
    <h2 class="section-title">🔍 注目銘柄ランキング</h2>
    <div class="table-wrap">
      <table id="screener-table">
        <thead>
          <tr>
            <th onclick="sortTable(0)">#</th>
            <th onclick="sortTable(1)">コード</th>
            <th onclick="sortTable(2)">銘柄名</th>
            <th onclick="sortTable(3)">セクター</th>
            <th onclick="sortTable(4)">終値 (円)</th>
            <th onclick="sortTable(5)">騰落率 (%)</th>
            <th onclick="sortTable(6)">RSI(14)</th>
            <th onclick="sortTable(7)">SMAクロス</th>
            <th onclick="sortTable(8)">出来高比</th>
            <th onclick="sortTable(9)">スコア</th>
          </tr>
        </thead>
        <tbody>
          {table_rows}
        </tbody>
      </table>
    </div>
  </div>

  <!-- チャート -->
  <div class="section">
    <h2 class="section-title">📊 スコア分布 × 騰落率</h2>
    <div class="chart-container">
      {scatter_chart}
    </div>
  </div>

  <div class="section">
    <h2 class="section-title">📈 セクター別 騰落率ヒートマップ</h2>
    <div class="chart-container">
      {sector_chart}
    </div>
  </div>

</div>

<!-- ── Footer ── -->
<div class="footer">
  <p>本レポートは情報提供のみを目的としています。投資判断は自己責任でお願いします。</p>
  <p style="margin-top:6px;">Generated by <a href="#">Dexter — AI Equity Screener</a> | {generated_at}</p>
</div>

<script>
  // ── ソート機能 ──
  let sortDir = {{}};
  function sortTable(col) {{
    const table = document.getElementById('screener-table');
    const tbody = table.querySelector('tbody');
    const rows = Array.from(tbody.querySelectorAll('tr'));
    sortDir[col] = !sortDir[col];
    rows.sort((a, b) => {{
      const aVal = a.cells[col].getAttribute('data-val') ?? a.cells[col].textContent.trim();
      const bVal = b.cells[col].getAttribute('data-val') ?? b.cells[col].textContent.trim();
      const aNum = parseFloat(aVal);
      const bNum = parseFloat(bVal);
      if (!isNaN(aNum) && !isNaN(bNum)) {{
        return sortDir[col] ? aNum - bNum : bNum - aNum;
      }}
      return sortDir[col] ? aVal.localeCompare(bVal, 'ja') : bVal.localeCompare(aVal, 'ja');
    }});
    rows.forEach(r => tbody.appendChild(r));
  }}
</script>

</body>
</html>
"""


# ─────────────────────────────────────────────
# ヘルパー関数
# ─────────────────────────────────────────────
def _change_class(val: float) -> str:
    if val > 0:
        return "change-up"
    elif val < 0:
        return "change-down"
    return "change-neutral"


def _fmt_change(val: float) -> str:
    if val > 0:
        return f"+{val:.2f}%"
    return f"{val:.2f}%"


def _rsi_badge(rsi: float | None) -> str:
    if rsi is None:
        return "—"
    if rsi <= 30:
        return f'<span class="badge badge-rsi-low">売られ過ぎ {rsi:.0f}</span>'
    elif rsi >= 70:
        return f'<span class="badge badge-rsi-high">買われ過ぎ {rsi:.0f}</span>'
    return f"{rsi:.0f}"


def _cross_badge(cross: str | None) -> str:
    if cross == "golden":
        return '<span class="badge badge-golden">⬆ ゴールデン</span>'
    elif cross == "dead":
        return '<span class="badge badge-dead">⬇ デッド</span>'
    return "—"


def _vol_badge(ratio: float | None) -> str:
    if ratio is None:
        return "—"
    if ratio >= 2.0:
        return f'<span class="badge badge-vol">🔥 {ratio:.1f}x</span>'
    return f"{ratio:.1f}x"


def _score_bar(score: float, max_score: float = 100) -> str:
    pct = min(score / max_score * 100, 100)
    return (
        f'<div class="score-bar">'
        f'  <span>{score:.0f}</span>'
        f'  <div class="score-bar-bg">'
        f'    <div class="score-bar-fill" style="width:{pct:.0f}%"></div>'
        f'  </div>'
        f"</div>"
    )


def _build_table_rows(df: pd.DataFrame) -> str:
    rows: list[str] = []
    max_score = df["score"].max() if not df.empty else 100
    for i, row in df.iterrows():
        rank = i + 1
        change = row.get("change_pct", 0) or 0
        rsi = row.get("rsi_14")
        cross = row.get("sma_cross", "none")
        vol_r = row.get("volume_ratio")
        score = row.get("score", 0) or 0
        close = row.get("close")
        close_str = f"{float(close):,.0f}" if close and not pd.isna(close) else "—"

        rows.append(f"""
        <tr>
          <td class="rank" data-val="{rank}">{rank}</td>
          <td class="code">{row.get('code','—')}</td>
          <td class="name">{row.get('name','—')}</td>
          <td style="color:var(--text-sub);font-size:12px">{row.get('sector','—')}</td>
          <td data-val="{close or 0}">{close_str}</td>
          <td class="{_change_class(change)}" data-val="{change}">{_fmt_change(change)}</td>
          <td data-val="{rsi or 0}">{_rsi_badge(rsi)}</td>
          <td>{_cross_badge(cross)}</td>
          <td data-val="{vol_r or 0}">{_vol_badge(vol_r)}</td>
          <td data-val="{score}">{_score_bar(score, max_score)}</td>
        </tr>
        """)
    return "\n".join(rows)


def _build_stats_cards(df: pd.DataFrame, target_date: str) -> str:
    total = len(df)
    up = (df["change_pct"] > 0).sum()
    down = (df["change_pct"] < 0).sum()
    cross_cnt = df[df["sma_cross"].isin(["golden", "dead"])].shape[0]

    cards = [
        ("対象銘柄数", str(total), "neutral"),
        ("騰落 ↑", str(up), "up"),
        ("騰落 ↓", str(down), "down"),
        ("クロス検知", str(cross_cnt), "neutral"),
    ]
    html = ""
    for label, value, cls in cards:
        html += f"""
        <div class="stat-card">
          <div class="label">{label}</div>
          <div class="value {cls}">{value}</div>
        </div>
        """
    return html


def _build_scatter_chart(df: pd.DataFrame) -> str:
    """スコア vs 騰落率の散布図（Plotly）。"""
    if df.empty:
        return "<p style='color:var(--text-sub);padding:20px;'>データなし</p>"

    hover_text = df.apply(
        lambda r: f"{r.get('code','')} {r.get('name','')}<br>騰落率: {r.get('change_pct',0):.2f}%<br>スコア: {r.get('score',0):.0f}",
        axis=1,
    )

    fig = go.Figure(
        go.Scatter(
            x=df["change_pct"],
            y=df["score"],
            mode="markers+text",
            text=df["code"],
            textposition="top center",
            textfont=dict(size=9, color="#8b949e"),
            marker=dict(
                size=10,
                color=df["change_pct"],
                colorscale=[
                    [0, "#f85149"],
                    [0.5, "#58a6ff"],
                    [1, "#3fb950"],
                ],
                showscale=True,
                colorbar=dict(title="騰落率(%)", tickfont=dict(color="#8b949e"), titlefont=dict(color="#8b949e")),
                line=dict(width=1, color="#30363d"),
            ),
            hovertext=hover_text,
            hoverinfo="text",
        )
    )
    fig.update_layout(
        paper_bgcolor="#161b22",
        plot_bgcolor="#0d1117",
        font=dict(color="#e6edf3", family="Inter, Noto Sans JP"),
        xaxis=dict(
            title="騰落率 (%)",
            gridcolor="#30363d",
            zerolinecolor="#58a6ff",
            zerolinewidth=1,
        ),
        yaxis=dict(title="注目スコア", gridcolor="#30363d"),
        margin=dict(l=60, r=20, t=20, b=60),
        height=420,
    )
    return fig.to_html(full_html=False, include_plotlyjs="cdn")


def _build_sector_chart(df: pd.DataFrame) -> str:
    """セクター別の平均騰落率を横棒グラフで表示。"""
    if df.empty or "sector" not in df.columns:
        return "<p style='color:var(--text-sub);padding:20px;'>データなし</p>"

    sector_avg = (
        df.groupby("sector")["change_pct"]
        .mean()
        .sort_values()
        .reset_index()
    )
    sector_avg.columns = ["sector", "avg_change"]

    colors = ["#3fb950" if v >= 0 else "#f85149" for v in sector_avg["avg_change"]]

    fig = go.Figure(
        go.Bar(
            x=sector_avg["avg_change"],
            y=sector_avg["sector"],
            orientation="h",
            marker_color=colors,
            text=[f"{v:+.2f}%" for v in sector_avg["avg_change"]],
            textposition="outside",
            textfont=dict(size=11),
        )
    )
    fig.update_layout(
        paper_bgcolor="#161b22",
        plot_bgcolor="#0d1117",
        font=dict(color="#e6edf3", family="Inter, Noto Sans JP"),
        xaxis=dict(title="平均騰落率 (%)", gridcolor="#30363d", zerolinecolor="#58a6ff"),
        yaxis=dict(gridcolor="#30363d"),
        margin=dict(l=120, r=80, t=20, b=50),
        height=max(300, len(sector_avg) * 32),
    )
    return fig.to_html(full_html=False, include_plotlyjs="cdn")


# ─────────────────────────────────────────────
# メイン関数
# ─────────────────────────────────────────────
def generate_report(df: pd.DataFrame, target_date: str) -> Path:
    """
    スクリーニング結果 DataFrame から HTML レポートを生成。

    Parameters
    ----------
    df : pd.DataFrame
        screener.run_screening() の戻り値
    target_date : str
        レポート対象日 ("YYYY-MM-DD")

    Returns
    -------
    Path
        生成した HTML ファイルのパス
    """
    log.info(f"[ReportGenerator] HTMLレポートを生成中 ({target_date})...")

    html = HTML_TEMPLATE.format(
        date=target_date,
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S JST"),
        stats_cards=_build_stats_cards(df, target_date),
        table_rows=_build_table_rows(df),
        scatter_chart=_build_scatter_chart(df),
        sector_chart=_build_sector_chart(df),
    )

    output_path = REPORTS_DIR / f"report_{target_date}.html"
    output_path.write_text(html, encoding="utf-8")
    log.info(f"[Success] レポート生成完了 → {output_path}")
    return output_path


if __name__ == "__main__":
    # テスト: CSV が存在すれば読み込んでレポート生成
    import sys

    date = sys.argv[1] if len(sys.argv) > 1 else datetime.today().strftime("%Y-%m-%d")
    csv_path = BASE_DIR / "data" / f"screening_{date}.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        path = generate_report(df, date)
        print(f"レポート生成完了: {path}")
    else:
        print(f"CSVが見つかりません: {csv_path}")
        print("先に screener.py を実行してください。")
