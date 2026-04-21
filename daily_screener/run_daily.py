"""
run_daily.py
------------
毎日のスクリーニングパイプラインのエントリポイント。

実行フロー:
  1. スクリーニング実行 (screener.py)
  2. HTMLレポート生成 (report_generator.py)
  3. 通知送信 (notifier.py)

使い方:
  # 前営業日（デフォルト）
  python run_daily.py

  # 日付指定
  python run_daily.py --date 2026-04-04

  # 通知スキップ
  python run_daily.py --no-notify
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# ── ロギング設定 ──────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            Path(__file__).parent / "logs" / f"run_{datetime.today().strftime('%Y%m%d')}.log",
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)

# ── ログディレクトリ作成 ───────────────────────────
(Path(__file__).parent / "logs").mkdir(parents=True, exist_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="日本株 毎日注目銘柄スクリーニング",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="対象日 (YYYY-MM-DD)。省略時は前営業日。",
    )
    parser.add_argument(
        "--no-notify",
        action="store_true",
        help="通知をスキップする。",
    )
    parser.add_argument(
        "--report-url",
        type=str,
        default="",
        help="通知に含める公開レポートURL (GitHub Pages等)。",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=50,
        help="レポートに掲載する上位銘柄数。デフォルト: 50。",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    log.info("=" * 60)
    log.info("🇯🇵 日本株 注目銘柄スクリーニング 開始")
    log.info("=" * 60)

    # ── Step 1: スクリーニング ───────────────────────
    try:
        from screener import run_screening
    except ImportError:
        log.error("screener.py が見つかりません。daily_screener/ ディレクトリから実行してください。")
        return 1

    df = run_screening(target_date=args.date)

    if df.empty:
        log.error("スクリーニング結果が空でした。APIキーや接続を確認してください。")
        return 1

    target_date = args.date or df.index.name or datetime.today().strftime("%Y-%m-%d")

    # 上位 N 件に絞る
    df_report = df.head(args.top_n).copy()
    log.info(f"上位 {len(df_report)} 銘柄をレポートに掲載します。")

    # ── Step 2: レポート生成 ─────────────────────────
    try:
        from report_generator import generate_report
    except ImportError:
        log.error("report_generator.py が見つかりません。")
        return 1

    # target_date は screener から取得
    # CSV のファイル名から日付を推定
    import re
    from pathlib import Path as P

    data_dir = P(__file__).parent / "data"
    csv_files = sorted(data_dir.glob("screening_*.csv"), reverse=True)
    if csv_files:
        match = re.search(r"screening_(\d{4}-\d{2}-\d{2})\.csv", csv_files[0].name)
        if match:
            target_date = match.group(1)

    report_path = generate_report(df_report, target_date)

    # ── Step 3: 通知 ─────────────────────────────────
    if not args.no_notify:
        try:
            from notifier import send_all_notifications
        except ImportError:
            log.warning("notifier.py が見つかりません。通知をスキップします。")
        else:
            results = send_all_notifications(
                df_report,
                target_date,
                report_path=report_path,
                report_url=args.report_url,
            )
            log.info(f"通知結果: {results}")
    else:
        log.info("--no-notify フラグが指定されているため通知をスキップします。")

    log.info("=" * 60)
    log.info(f"✅ 全処理完了。レポート: {report_path}")
    log.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
