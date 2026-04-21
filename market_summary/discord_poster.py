"""
discord_poster.py
-----------------
Discord Webhook 経由でリッチな Embed メッセージを投稿する。

Discord Embed の制限:
  - description: 最大 4096 文字
  - 全体 Embed: 最大 6000 文字
  - フィールド数: 最大 25
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

# ── 設定 ──────────────────────────────────────
DISCORD_WEBHOOK_URL: str = os.getenv("DISCORD_WEBHOOK_URL", "")

# Embed カラー
COLOR_JP: int = 0xBC002D   # 日本国旗レッド
COLOR_US: int = 0x3C3B6E   # 米国国旗ブルー
COLOR_ERROR: int = 0xFF0000  # エラー時


def _truncate(text: str, max_len: int = 4000) -> str:
    """Discord Embed の description 上限に合わせて切り詰める。"""
    if len(text) <= max_len:
        return text
    return text[:max_len - 20] + "\n\n... (省略されました)"


def post_to_discord(
    summary: str,
    mode: str = "jp",
    webhook_url: str | None = None,
) -> bool:
    """
    Discord Webhook にマーケットサマリーを投稿する。

    Parameters
    ----------
    summary : str
        投稿するサマリーテキスト
    mode : str
        "jp" (日本市場) or "us" (米国市場)
    webhook_url : str | None
        Webhook URL。None の場合は環境変数から取得。

    Returns
    -------
    bool
        投稿成功なら True
    """
    url = webhook_url or DISCORD_WEBHOOK_URL
    if not url:
        log.error("[Discord] DISCORD_WEBHOOK_URL が設定されていません。")
        return False

    # Embed 設定
    if mode == "jp":
        title = "日本市場 デイリーサマリー"
        color = COLOR_JP
        footer_text = "日本市場サマリー | Market Summary Bot"
    else:
        title = "米国市場 デイリーサマリー"
        color = COLOR_US
        footer_text = "米国市場サマリー | Market Summary Bot"

    now = datetime.now()
    date_str = now.strftime("%Y年%m月%d日")
    weekday = ["月", "火", "水", "木", "金", "土", "日"][now.weekday()]

    embed: dict[str, Any] = {
        "title": f"{title} ({date_str} {weekday})",
        "description": _truncate(summary),
        "color": color,
        "footer": {
            "text": footer_text,
        },
        "timestamp": now.isoformat(),
    }

    payload = {
        "embeds": [embed],
    }

    try:
        resp = requests.post(url, json=payload, timeout=15)

        if resp.status_code in (200, 204):
            log.info(f"[Discord] 投稿成功 ✅ (mode={mode})")
            return True
        elif resp.status_code == 429:
            # レートリミット
            retry_after = resp.json().get("retry_after", 5)
            log.warning(f"[Discord] Rate limited. Retry after {retry_after}s")
            import time
            time.sleep(retry_after)
            # リトライ
            resp2 = requests.post(url, json=payload, timeout=15)
            if resp2.status_code in (200, 204):
                log.info(f"[Discord] リトライ投稿成功 ✅ (mode={mode})")
                return True
            else:
                log.error(f"[Discord] リトライ失敗 ({resp2.status_code})")
                return False
        else:
            log.error(f"[Discord] 投稿失敗 ({resp.status_code}): {resp.text[:300]}")
            return False

    except requests.RequestException as e:
        log.error(f"[Discord] ネットワークエラー: {e}")
        return False


def post_error_to_discord(
    error_message: str,
    mode: str = "jp",
    webhook_url: str | None = None,
) -> bool:
    """
    エラー発生時にDiscordへエラー通知を送る。

    Parameters
    ----------
    error_message : str
        エラーメッセージ
    mode : str
        "jp" or "us"
    webhook_url : str | None
        Webhook URL

    Returns
    -------
    bool
        投稿成功なら True
    """
    url = webhook_url or DISCORD_WEBHOOK_URL
    if not url:
        return False

    market_name = "日本市場" if mode == "jp" else "米国市場"

    embed: dict[str, Any] = {
        "title": f"⚠️ {market_name}サマリー - エラー発生",
        "description": f"```\n{error_message[:3000]}\n```",
        "color": COLOR_ERROR,
        "footer": {"text": "Market Summary Bot - Error"},
        "timestamp": datetime.now().isoformat(),
    }

    try:
        resp = requests.post(url, json={"embeds": [embed]}, timeout=15)
        return resp.status_code in (200, 204)
    except requests.RequestException:
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # テスト投稿
    test_summary = (
        "**📊 主要指数**\n"
        "• 日経225: 38,450.12 (+1.23%)\n"
        "• TOPIX: 2,720.45 (+0.89%)\n\n"
        "**💱 為替**\n"
        "• USD/JPY: 154.32 (+0.15%)\n\n"
        "**📈 本日の注目ポイント**\n"
        "• テスト投稿です。\n"
    )
    result = post_to_discord(test_summary, mode="jp")
    print(f"投稿結果: {'成功' if result else '失敗'}")
