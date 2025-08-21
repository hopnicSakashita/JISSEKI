import logging
import sys

# ロギング設定
logging.basicConfig(
    filename='app.log',
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def log_error(message):
    """エラーログを記録"""
    logger.error(message)
    # 標準エラー出力にも出力（デバッグ用）
    print(f"[ERROR] {message}", file=sys.stderr) 