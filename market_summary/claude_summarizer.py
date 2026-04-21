"""
claude_summarizer.py
--------------------
Claude API を使ってマーケットデータから自然言語サマリーを生成する。

使用モデル: claude-sonnet-4-6
出力形式: Discord Embed に適したマークダウン
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

# ── 設定 ──────────────────────────────────────
ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL: str = "claude-sonnet-4-6"
CLAUDE_API_URL: str = "https://api.anthropic.com/v1/messages"
MAX_TOKENS: int = 2048
MAX_RETRIES: int = 3
RETRY_BASE_SEC: int = 2

# ── システムプロンプト ─────────────────────────
JP_SYSTEM_PROMPT: str = """あなたはトップクラスの金融機関で働くシニア・クオンツアナリストおよびEquity Researchアナリストです。
提供されたマーケットデータを基に、機関投資家向けの高品質でプロフェッショナルな日本市場のデイリーサマリーを作成してください。

## ルール
- 全て日本語で記述し、敬体（です/ます）ではなく、レポートに適した常体（だ/である/した）を使用すること。
- AIが生成したとわかるような絵文字（📊、📉、📈、💱、💡など）は一切使用禁止。
- 数値は必ず提供されたデータから引用すること（捏造厳禁）。
- 提供されたデータから推測可能な背景や、一般的なマクロ経済の動向、地政学的要因を織り交ぜて深く考察すること。
- 以下の4つのセクション構成で出力すること。マークダウンの太字などは適宜使用してよいが、見出しは指定の括弧（〈〉）を使用すること。

## 出力構成
[月/日]のマーケット振り返り （※日付はデータの日付を記載）

〈株式〉
日経平均やTOPIXなどの主要指数の動向、セクター別の動き、注目銘柄（データにある場合）の値動き、またその背景（米国市場の流れや固有の材料など）を2〜3段落で記述。

〈債券〉
日本の長期金利の動向や、日銀の政策に対する市場の期待、イールドカーブの変化などについて推測を交えて1〜2段落で記述。（※データに直接的な債券利回りがない場合は、株式・為替の動きからマクロ環境を推測して記述）

〈為替〉
ドル円（USD/JPY）などの推移と、日米金利差、為替介入への警戒感、原油価格などのマクロ要因との関連を1段落で記述。

〈今後の展望〉
翌日以降の東京市場の展望、物色の広がり、注目されるマクロ指標やイベント、テクニカルな水準への言及などを2段落程度で深く考察して記述。
"""

US_SYSTEM_PROMPT: str = """あなたはトップクラスの金融機関で働くシニア・クオンツアナリストおよびエコノミストです。
提供されたマーケットデータを基に、機関投資家向けの高品質でプロフェッショナルな米国市場のデイリーサマリーを作成してください。

## ルール
- 全て日本語で記述し、敬体（です/ます）ではなく、レポートに適した常体（だ/である/した）を使用すること。
- AIが生成したとわかるような絵文字（📊、📉、📈、💱、💡など）は一切使用禁止。
- 数値は必ず提供されたデータから引用すること（捏造厳禁）。
- 提供されたデータから推測可能な背景や、一般的なマクロ経済の動向、地政学的要因を織り交ぜて深く考察すること。
- 以下の4つのセクション構成で出力すること。

## 出力構成
[月/日]の米国マーケット振り返り （※日付はデータの日付を記載）

〈株式〉
S&P500、NASDAQ、DOWなどの主要指数の動向、セクター別の動き、VIXの推移やその背景（経済指標、決算、FRB高官発言など推測を含む）を2〜3段落で記述。

〈債券・コモディティ〉
米10年債利回りの動向、FRBの利下げ/利上げ期待、原油（WTI）や金価格の推移についてマクロ的な視点から1〜2段落で記述。

〈為替〉
ドル円（USD/JPY）の推移と、日米経済のファンダメンタルズや地政学リスクなどの要因を1段落で記述。

〈今後の展望〉
今後の米国市場の展望や、翌日の東京市場への波及効果・示唆について、深く分析して記述。
"""


def generate_summary(
    market_data_text: str,
    mode: str = "jp",
) -> Optional[str]:
    """
    Claude API を使ってマーケットサマリーを生成する。

    Parameters
    ----------
    market_data_text : str
        format_market_data_for_prompt() で生成した構造化テキスト
    mode : str
        "jp" (日本市場) or "us" (米国市場)

    Returns
    -------
    str | None
        生成されたサマリーテキスト。失敗時は None。
    """
    if not ANTHROPIC_API_KEY:
        log.error("[Claude] ANTHROPIC_API_KEY が設定されていません。")
        return None

    system_prompt = JP_SYSTEM_PROMPT if mode == "jp" else US_SYSTEM_PROMPT

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    payload = {
        "model": CLAUDE_MODEL,
        "max_tokens": MAX_TOKENS,
        "system": system_prompt,
        "messages": [
            {
                "role": "user",
                "content": (
                    f"以下のマーケットデータを基に、本日の{'日本' if mode == 'jp' else '米国'}市場の"
                    f"デイリーサマリーを作成してください。\n\n{market_data_text}"
                ),
            }
        ],
    }

    for attempt in range(MAX_RETRIES):
        try:
            log.info(f"[Claude] サマリー生成中... (attempt {attempt + 1}/{MAX_RETRIES})")
            resp = requests.post(
                CLAUDE_API_URL,
                headers=headers,
                json=payload,
                timeout=60,
            )

            if resp.status_code == 200:
                data = resp.json()
                # Claude API レスポンス形式: {"content": [{"type": "text", "text": "..."}]}
                content_blocks = data.get("content", [])
                text_parts = [
                    block["text"]
                    for block in content_blocks
                    if block.get("type") == "text"
                ]
                summary = "\n".join(text_parts)
                log.info(f"[Claude] サマリー生成成功 ({len(summary)} 文字)")
                return summary

            elif resp.status_code == 429:
                wait = RETRY_BASE_SEC * (2 ** attempt)
                log.warning(f"[Claude] Rate limit (429). Waiting {wait}s...")
                time.sleep(wait)

            elif resp.status_code == 529:
                wait = RETRY_BASE_SEC * (2 ** attempt) * 2
                log.warning(f"[Claude] Overloaded (529). Waiting {wait}s...")
                time.sleep(wait)

            else:
                log.error(f"[Claude] HTTP {resp.status_code}: {resp.text[:300]}")
                return None

        except requests.RequestException as e:
            log.error(f"[Claude] Network error (attempt {attempt + 1}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BASE_SEC * (2 ** attempt))

    log.error("[Claude] 最大リトライ回数に達しました。")
    return None


def generate_fallback_summary(
    market_data_text: str,
    mode: str = "jp",
) -> str:
    """
    Claude API が利用不可の場合のフォールバックサマリーを生成する。
    データをそのまま整形して返す。

    Parameters
    ----------
    market_data_text : str
        構造化テキスト
    mode : str
        "jp" or "us"

    Returns
    -------
    str
        フォールバックサマリー
    """
    flag = "🇯🇵" if mode == "jp" else "🇺🇸"
    market_name = "日本" if mode == "jp" else "米国"

    return (
        f"{flag} **{market_name}市場サマリー**\n\n"
        f"⚠️ AI サマリー生成に失敗したため、データのみ表示します。\n\n"
        f"```\n{market_data_text}\n```"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # テスト: ダミーデータでサマリー生成
    test_data = """=== マーケットデータ (2026年04月21日 月曜日) ===

【主要指数】
  日経225: 38,450.12 (+1.23%)
  TOPIX: 2,720.45 (+0.89%)
  グロース250: 680.30 (-0.42%)

【為替】
  USD/JPY: 154.32 (+0.15%)

【セクター動向】
  銀行: +1.5%
  半導体: +3.2%
  自動車: -0.8%
"""

    result = generate_summary(test_data, mode="jp")
    if result:
        print(result)
    else:
        print("サマリー生成に失敗しました。")
