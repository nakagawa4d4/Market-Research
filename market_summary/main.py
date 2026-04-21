"""
main.py
-------
マーケットサマリー自動配信のエントリポイント。

使い方:
  # 日本市場サマリーを生成・投稿
  python main.py --mode jp

  # 米国市場サマリーを生成・投稿
  python main.py --mode us

  # Discord 投稿をスキップ（テスト用）
  python main.py --mode jp --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
import traceback
from datetime import datetime

# ── ロギング設定 ──────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="📈 マーケットサマリー自動配信 (Discord)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["jp", "us"],
        required=True,
        help="市場モード: jp (日本市場) or us (米国市場)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discord 投稿をスキップし、サマリーをコンソールに表示する。",
    )
    return parser.parse_args()


def run(mode: str, dry_run: bool = False) -> int:
    """
    メイン実行フロー。

    1. マーケットデータ取得
    2. Claude API でサマリー生成
    3. Discord Webhook に投稿

    Parameters
    ----------
    mode : str
        "jp" or "us"
    dry_run : bool
        True の場合は Discord 投稿をスキップ

    Returns
    -------
    int
        終了コード (0=成功, 1=失敗)
    """
    from data_fetcher import (
        fetch_jp_market_data,
        fetch_jp_top_movers,
        fetch_us_market_data,
        format_market_data_for_prompt,
    )
    from claude_summarizer import generate_fallback_summary, generate_summary
    from discord_poster import post_error_to_discord, post_to_discord

    market_label = "🇯🇵 日本市場" if mode == "jp" else "🇺🇸 米国市場"

    log.info("=" * 60)
    log.info(f"{market_label} サマリー生成 開始")
    log.info(f"時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S JST')}")
    log.info("=" * 60)

    # ── Step 1: データ取得 ─────────────────────────
    log.info("Step 1: マーケットデータ取得")

    try:
        if mode == "jp":
            data = fetch_jp_market_data()
            # J-Quants の注目銘柄も追加（オプション）
            try:
                top_movers = fetch_jp_top_movers()
                if top_movers:
                    data["top_movers"] = top_movers
            except Exception as e:
                log.warning(f"J-Quants 注目銘柄取得をスキップ: {e}")
        else:
            data = fetch_us_market_data()
    except Exception as e:
        error_msg = f"データ取得エラー: {e}\n{traceback.format_exc()}"
        log.error(error_msg)
        if not dry_run:
            post_error_to_discord(error_msg, mode=mode)
        return 1

    # データの有効性チェック
    valid_indices = [
        item for item in data.get("indices", [])
        if item.get("close") is not None
    ]
    if not valid_indices:
        error_msg = "有効なマーケットデータが取得できませんでした。市場が閉まっている可能性があります。"
        log.error(error_msg)
        if not dry_run:
            post_error_to_discord(error_msg, mode=mode)
        return 1

    log.info(f"  → {len(valid_indices)} 件の指数データを取得")

    # テキスト変換
    market_text = format_market_data_for_prompt(data)
    log.info(f"  → プロンプトテキスト: {len(market_text)} 文字")

    # ── Step 2: Claude サマリー生成 ────────────────
    log.info("Step 2: Claude API でサマリー生成")

    summary = generate_summary(market_text, mode=mode)

    if summary is None:
        log.warning("Claude API サマリー生成失敗。フォールバックを使用します。")
        summary = generate_fallback_summary(market_text, mode=mode)

    log.info(f"  → サマリー: {len(summary)} 文字")

    # ── [NEW] サマリーをローカルに保存 ────────────────
    from pathlib import Path
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    out_dir = Path(__file__).parent / "reports" / mode
    out_dir.mkdir(parents=True, exist_ok=True)
    
    out_file = out_dir / f"{date_str}_{mode}.md"
    out_file.write_text(summary, encoding="utf-8")
    log.info(f"レポート保存完了: {out_file}")

    # ── Step 3: Discord 投稿 ──────────────────────
    if dry_run:
        log.info("Step 3: [DRY RUN] Discord 投稿スキップ")
        print("\n" + "=" * 60)
        print(f"{market_label} サマリー")
        print("=" * 60)
        print(summary)
        print("=" * 60 + "\n")
        return 0

    log.info("Step 3: Discord に投稿")

    success = post_to_discord(summary, mode=mode)

    if success:
        log.info("✅ Discord 投稿成功！")
    else:
        log.error("❌ Discord 投稿に失敗しました。")
        return 1

    log.info("=" * 60)
    log.info(f"✅ {market_label} サマリー配信完了")
    log.info("=" * 60)
    return 0


def main() -> int:
    args = parse_args()
    return run(mode=args.mode, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
