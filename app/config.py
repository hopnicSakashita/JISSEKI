import os
from dotenv import load_dotenv
from datetime import timedelta

# .envファイルの読み込み
load_dotenv()

class Config:
    # データベース設定
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
        f"mysql+pymysql://{os.getenv('DB_USER', 'username')}:" \
        f"{os.getenv('DB_PASSWORD', 'password')}@" \
        f"{os.getenv('DB_HOST', 'localhost')}/" \
        f"{os.getenv('DB_NAME', 'dbname')}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # アプリケーション設定
    SECRET_KEY = os.environ.get('SECRET_KEY', 'default-secret-key-for-development')
    
    # アップロード設定
    UPLOAD_DIR = os.getenv('UPLOAD_DIR', 'uploads')
    CSV_ENCODING = os.getenv('CSV_ENCODING', 'shift_jis')
    
    PERMANENT_SESSION_LIFETIME = timedelta(hours=24) 