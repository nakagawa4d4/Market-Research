"""
notifier.py
-----------
スクリーニング結果のサマリーを各チャンネルに通知する。

対応チャンネル:
  - Slack Incoming Webhook   (SLACK_WEBHOOK_URL)
  - LINE Notify              (LINE_NOTIFY_TOKEN)
  - Gmail SMTP               (GMAIL_USER, GMAIL_APP_PASSWORD, NOTIFY_TO_EMAIL)

各チャンネルは環境変数が設定されている場合のみ有効化されます。
"""

from __future__ import annotations

import logging
import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

# ── 環境変数 ───────────────────────────────────
SLACK_WEBHOOK_URL: str = os.getenv("SLACK_WEBHOOK_URL", "")
LINE_NOTIFY_TOKEN: str = os.getenv("LINE_NOTIFY_TOKEN", "")
GMAIL_USER: str = os.getenv("GMAIL_USER", "")
GMAIL_APP_PASSWORD: str = os.getenv("GMAIL_APP_PASSWORD", "")
NOTIFY_TO_EMAIL: str = os.getenv("NOTIFY_TO_EMAIL", "")


# ─────────────────────────────────────────────
# サマリーテキスト生成
# ─────────────────────────────────────────────
def build_summary(df: pd.DataFrame, target_date: str, report_url: str = "") -> dict:
    """
    通知用のサマリー情報を生成する。

    Returns
    -------
    dict
        {"title": str, "body": str, "top_stocks": list}
    """
    total = len(df)
    up = int((df["change_pct"] > 0).sum())
    down = int((df["change_pct"] < 0).sum())

    # 上位5銘柄 (スコア順)
    top5 = df.head(5)
    top_lines: list[str] = []
    for _, row in top5.iterrows():
        change = row.get("change_pct", 0) or 0
        sign = "+" if change >= 0 else ""
        rsi = row.get("rsi_14")
        rsi_str = f" RSI:{rsi:.0f}" if rsi is not None else ""
        cross = row.get("sma_cross", "none")
        cross_str = " ⬆GC" if cross == "golden" else (" ⬇DC" if cross == "dead" else "")
        top_lines.append(
            f"  • {row.get('code','')} {row.get('name','')[:12]}"
            f" {sign}{change:.2f}%{rsi_str}{cross_str}"
        )

    body = (
        f"📅 {target_date}\n"
        f"東証プライム注目銘柄: {total}件\n"
        f"騰落 ↑{up} / ↓{down}\n\n"
        f"🏆 上位5銘柄:\n"
        + "\n".join(top_lines)
    )
    if report_url:
        body += f"\n\n📄 詳細レポート: {report_url}"

    return {
        "title": f"🇯🇵 日本株 注目銘柄 {target_date}",
        "body": body,
        "top_stocks": top5.to_dict("records"),
    }


# ─────────────────────────────────────────────
# Slack 通知
# ─────────────────────────────────────────────
def notify_slack(summary: dict) -> bool:
    """Slack Incoming Webhook に通知する。"""
    if not SLACK_WEBHOOK_URL:
        log.debug("[Slack] SLACK_WEBHOOK_URL が未設定のためスキップします。")
        return False

    # Slack Block Kit を使ったリッチメッセージ
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": summary["title"], "emoji": True},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"```{summary['body']}```"},
        },
    ]

    payload = {"blocks": blocks, "text": summary["title"]}

    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=15)
        if resp.status_code == 200:
            log.info("[Slack] 通知送信成功。")
            return True
        else:
            log.error(f"[Slack] 送信失敗 ({resp.status_code}): {resp.text}")
            return False
    except requests.RequestException as e:
        log.error(f"[Slack] 送信エラー: {e}")
        return False


# ─────────────────────────────────────────────
# LINE Notify 通知
# ─────────────────────────────────────────────
def notify_line(summary: dict) -> bool:
    """LINE Notify に通知する。"""
    if not LINE_NOTIFY_TOKEN:
        log.debug("[LINE] LINE_NOTIFY_TOKEN が未設定のためスキップします。")
        return False

    message = f"\n{summary['title']}\n\n{summary['body']}"

    try:
        resp = requests.post(
            "https://notify-api.line.me/api/notify",
            headers={"Authorization": f"Bearer {LINE_NOTIFY_TOKEN}"},
            data={"message": message},
            timeout=15,
        )
        if resp.status_code == 200:
            log.info("[LINE] 通知送信成功。")
            return True
        else:
            log.error(f"[LINE] 送信失敗 ({resp.status_code}): {resp.text}")
            return False
    except requests.RequestException as e:
        log.error(f"[LINE] 送信エラー: {e}")
        return False


# ─────────────────────────────────────────────
# Gmail メール通知
# ─────────────────────────────────────────────
def notify_email(summary: dict, report_path: Optional[Path] = None) -> bool:
    """Gmail SMTP を使ってメール通知する。"""
    if not all([GMAIL_USER, GMAIL_APP_PASSWORD, NOTIFY_TO_EMAIL]):
        log.debug("[Email] Gmail 設定が不完全なためスキップします。")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = summary["title"]
    msg["From"] = GMAIL_USER
    msg["To"] = NOTIFY_TO_EMAIL

    # テキスト本文
    msg.attach(MIMEText(summary["body"], "plain", "utf-8"))

    # HTML本文（レポートがあれば埋め込む）
    if report_path and report_path.exists():
        html_content = report_path.read_text(encoding="utf-8")
        msg.attach(MIMEText(html_content, "html", "utf-8"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            smtp.sendmail(GMAIL_USER, NOTIFY_TO_EMAIL, msg.as_string())
        log.info(f"[Email] メール送信成功 → {NOTIFY_TO_EMAIL}")
        return True
    except smtplib.SMTPException as e:
        log.error(f"[Email] 送信エラー: {e}")
        return False


# ─────────────────────────────────────────────
# 統合通知関数
# ─────────────────────────────────────────────
def send_all_notifications(
    df: pd.DataFrame,
    target_date: str,
    report_path: Optional[Path] = None,
    report_url: str = "",
) -> dict[str, bool]:
    """
    設定されている全通知チャンネルに一括送信する。

    Parameters
    ----------
    df : pd.DataFrame
        screener.run_screening() の戻り値
    target_date : str
        対象日 ("YYYY-MM-DD")
    report_path : Path | None
        生成した HTML レポートのパス（メール添付用）
    report_url : str
        レポートの公開 URL（GitHub Pages 等）

    Returns
    -------
    dict[str, bool]
        各チャンネルの送信結果 {"slack": bool, "line": bool, "email": bool}
    """
    summary = build_summary(df, target_date, report_url)

    log.info("=== 通知送信開始 ===")
    results = {
        "slack": notify_slack(summary),
        "line": notify_line(summary),
        "email": notify_email(summary, report_path),
    }

    # どれも設定されていない場合はコンソール出力
    if not any(results.values()):
        log.info("[Console] 通知設定なし。コンソールにサマリーを出力します。")
        print("\n" + "=" * 60)
        print(summary["title"])
        print("=" * 60)
        print(summary["body"])
        print("=" * 60 + "\n")

    return results


if __name__ == "__main__":
    # 動作確認用: ダミーデータで通知テスト
    test_df = pd.DataFrame([
        {"code": "7974", "name": "任天堂", "sector": "その他製品", "close": 8500, "change_pct": 4.2, "rsi_14": 68, "sma_cross": "golden", "volume_ratio": 2.5, "score": 95},
        {"code": "8725", "name": "MS&AD", "sector": "保険業", "close": 3200, "change_pct": -3.1, "rsi_14": 28, "sma_cross": "none", "volume_ratio": 1.8, "score": 72},
    ])
    results = send_all_notifications(test_df, datetime.today().strftime("%Y-%m-%d"))
    print("通知結果:", results)
