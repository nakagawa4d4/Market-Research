"""
run_workflow.py
---------------
dexter_py エンドツーエンドワークフロー。

Usage:
  python run_workflow.py <TICKER> [--force-refresh]

Example:
  python run_workflow.py 7974.T           # 任天堂 (キャッシュ使用)
  python run_workflow.py 7974.T --force   # 強制再取得
  python run_workflow.py 8725.T           # MS&AD

出力:
  - data/raw/{ticker}_prices.csv
  - data/raw/{ticker}_financials.csv
  - data/processed/{ticker}_price_chart.html
  - data/processed/{ticker}_rsi_chart.html
  - data/processed/{ticker}_returns_hist.html
  - reports/{ticker}_equity_report_{datetime}.html  ← メインレポート
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

# ロギング設定 (タイムスタンプ + レベル付き)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("dexter_py")


def run(ticker: str, force_refresh: bool = False) -> str:
    """
    フルワークフローを実行し、レポートパスを返す。
    """
    from agent_data_fetcher import AgentDataFetcher
    from agent_analyst import AgentAnalyst
    from agent_fund_manager import AgentFundManager
    from report_generator import ReportGenerator

    log.info("=" * 60)
    log.info(f"  dexter_py 自律型エクイティリサーチ開始")
    log.info(f"  対象銘柄: {ticker}")
    log.info("=" * 60)

    # ────────────────────────────────────────────
    # Step 1: データ取得 (DataFetcher)
    # ────────────────────────────────────────────
    log.info("\n[Step 1] データ取得開始 (AgentDataFetcher)")
    fetcher = AgentDataFetcher()

    company_info = fetcher.fetch_company_info(ticker)
    log.info(f"  銘柄名: {company_info.get('name', 'N/A')}")
    log.info(f"  セクター: {company_info.get('sector', 'N/A')}")

    prices_df = fetcher.fetch_prices(ticker, force_refresh=force_refresh)
    fins_df = fetcher.fetch_financials(ticker, force_refresh=force_refresh)

    if prices_df.empty:
        log.error("[Error] 株価データの取得に失敗しました。終了します。")
        sys.exit(1)
    log.info(f"  株価データ: {len(prices_df)}件")
    log.info(f"  財務データ: {len(fins_df)}件")

    # ── Google Trends (オプショナル、--trends フラグ付きのみ実行) ────
    log.info("  Googleトレンド: スキップ (--trends フラグで有効化可能)")

    # ────────────────────────────────────────────
    # Step 2: 分析 (Analyst)
    # ────────────────────────────────────────────
    log.info("\n[Step 2] 財務分析 & チャート生成 (AgentAnalyst)")
    analyst = AgentAnalyst(ticker, company_info=company_info)
    analyst_summary = analyst.output_summary()

    log.info("  【アナリストサマリー】")
    log.info(f"  現在株価:         ¥{analyst_summary['latest_price']:>10,.0f}")
    log.info(f"  ROE:              {analyst_summary['roe']*100:>8.2f}%")
    log.info(f"  WACC:             {analyst_summary['wacc']*100:>8.2f}%")
    log.info(f"  ROE-WACC スプレッド:{(analyst_summary['roe']-analyst_summary['wacc'])*100:>+7.2f}%")
    log.info(f"  20日ボラティリティ:{analyst_summary['volatility_20d_ann']*100:>8.2f}%")
    log.info(f"  RSI (14日):       {analyst_summary['rsi_14']:>8.1f}")
    log.info(f"  60日モメンタム:   {analyst_summary['momentum_60d']*100:>+8.2f}%")
    log.info(f"  SMAクロス:        {analyst_summary['sma_cross']:>10}")

    # ────────────────────────────────────────────
    # Step 3: 投資判断 (Fund Manager)
    # ────────────────────────────────────────────
    log.info("\n[Step 3] 投資判断 (AgentFundManager)")
    manager = AgentFundManager(analyst_summary)
    evaluation = manager.evaluate()
    report_text = manager.generate_report_text(evaluation)
    print("\n" + report_text)

    # ────────────────────────────────────────────
    # Step 4: HTMLレポート生成
    # ────────────────────────────────────────────
    log.info("\n[Step 4] HTMLレポート生成")
    gen = ReportGenerator(evaluation)
    report_path = gen.generate()

    log.info("\n" + "=" * 60)
    log.info(f"  ✅ 完了! レポートを開いてください:")
    log.info(f"  {report_path}")
    log.info("=" * 60)

    return report_path


# ─────────────────────────────────────────────────────────
# CLI エントリーポイント
# ─────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="dexter_py: 自律型日本株エクイティリサーチ",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "ticker",
        help="銘柄コード (例: 7974.T, 8725.T, 7203.T)",
    )
    parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="キャッシュを無視してデータを再取得",
    )
    args = parser.parse_args()

    report_path = run(args.ticker, force_refresh=args.force)

    # macOS: 自動でブラウザを開く
    import subprocess, platform
    if platform.system() == "Darwin":
        subprocess.run(["open", report_path], check=False)


if __name__ == "__main__":
    main()
