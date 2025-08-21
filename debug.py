from dotenv import load_dotenv
import os

def check_env():
    load_dotenv()
    
    # 環境変数の確認
    env_vars = [
        'DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME',
        'FLASK_APP', 'FLASK_ENV', 'FLASK_DEBUG',
        'UPLOAD_DIR', 'CSV_ENCODING'
    ]
    
    print("環境変数の確認:")
    for var in env_vars:
        value = os.getenv(var)
        if value:
            # パスワードなど機密情報は値を隠す
            if 'PASSWORD' in var:
                print(f"{var}: ***")
            else:
                print(f"{var}: {value}")
        else:
            print(f"{var}: 未設定")

if __name__ == "__main__":
    check_env() 