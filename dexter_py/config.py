"""
config.py
---------
設定とパス管理。
J-Quants API V2 および yfinance のデュアルソース対応。
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── パス設定 ──────────────────────────────────────────
BASE_DIR = Path(__file__).parent.resolve()
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
REPORTS_DIR = BASE_DIR / "reports"

for d in [RAW_DATA_DIR, PROCESSED_DATA_DIR, REPORTS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── J-Quants API 設定 ──────────────────────────────────
# リフレッシュトークン（期限: 1週間）
JQUANTS_REFRESH_TOKEN: str = os.getenv("JQUANTS_API_KEY", "")
JQUANTS_BASE_URL: str = "https://api.jquants.com/v1"

# ── API リトライ設定 ─────────────────────────────────
MAX_RETRIES: int = 5
RETRY_DELAY_SECONDS: int = 3

# ── 財務計算パラメータ ─────────────────────────────────
# 無リスク金利: 日本国債10年利回り (2025年基準)
RISK_FREE_RATE: float = 0.015

# 市場期待リターン: TOPIX長期平均(配当込みリターン)
MARKET_RETURN: float = 0.075

# 実効法人税率 (日本: 約30.62%)
CORPORATE_TAX_RATE: float = 0.3062

# ── データソース優先設定 ────────────────────────────
# "jquants" | "yfinance" | "auto" (J-Quants失敗時にyfinanceへフォールバック)
DATA_SOURCE: str = "auto"


def get_jquants_base_url() -> str:
    return JQUANTS_BASE_URL
