# create_user.py
from flask import Flask
from werkzeug.security import generate_password_hash
import sys
import os
from dotenv import load_dotenv

# .envファイルを読み込む
load_dotenv()

def create_app():
    """アプリケーションインスタンスを作成"""
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = (
        f"mysql+pymysql://{os.getenv('DB_USER', 'username')}:"
        f"{os.getenv('DB_PASSWORD', 'password')}@"
        f"{os.getenv('DB_HOST', 'localhost')}/"
        f"{os.getenv('DB_NAME', 'dbname')}"
    )
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    # db拡張機能を初期化
    from app import db
    db.init_app(app)
    
    return app

def create_user(user_id, username, password, custom_db_params=None):
    """データベースに新しいユーザーを作成する"""
    try:
        # アプリケーションインスタンスを作成
        app = create_app()
        
        # カスタムデータベース接続パラメータが提供されている場合
        if custom_db_params:
            app.config['SQLALCHEMY_DATABASE_URI'] = (
                f"mysql+pymysql://{custom_db_params['user']}:"
                f"{custom_db_params['password']}@"
                f"{custom_db_params['host']}:{custom_db_params['port']}/"
                f"{custom_db_params['database']}"
            )
        
        # アプリケーションコンテキストでデータベース操作を実行
        with app.app_context():
            # インポートはアプリケーションコンテキスト内で行う
            from app.models import User
            from app import db
            
            # パスワードをハッシュ化
            hashed_password = generate_password_hash(password)
            
            # ユーザーの存在確認
            existing_user = User.query.filter_by(USER_ID=user_id).first()
            
            if existing_user:
                print(f"警告: ユーザーID '{user_id}' は既に存在します。上書きします。")
                
                # 既存ユーザーの更新
                existing_user.USERNAME = username
                existing_user.PASSWORD = hashed_password
                existing_user.USER_FLG = 0
            else:
                # 新規ユーザーの作成
                new_user = User(
                    USER_ID=user_id,
                    USERNAME=username,
                    PASSWORD=hashed_password,
                    USER_FLG=0
                )
                db.session.add(new_user)
            
            # 変更をコミット
            db.session.commit()
            
        print(f"ユーザー '{user_id}' を正常に登録しました。")
        return True
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        return False

if __name__ == "__main__":
    # コマンドライン引数のチェック
    if len(sys.argv) < 4:
        print("使用方法: python create_user.py <ユーザーID> <ユーザー名> <パスワード>")
        print("例: python create_user.py admin 管理者 admin123")
        
        # デフォルトは.envファイルの設定を使用するオプション
        use_env = input("環境変数（.env）のデータベース設定を使用しますか？ (y/n): ").lower() == 'y'
        
        custom_db_params = None
        if not use_env:
            # カスタム接続パラメータ
            custom_db_params = {
                'host': input("ホスト名 (デフォルト: localhost): ") or 'localhost',
                'user': input("データベースユーザー名: "),
                'password': input("データベースパスワード: "),
                'database': input("データベース名: "),
                'port': int(input("ポート番号 (デフォルト: 3306): ") or 3306)
            }
        
        user_id = input("ユーザーID: ")
        username = input("ユーザー名: ")
        password = input("パスワード: ")
    else:
        user_id = sys.argv[1]
        username = sys.argv[2]
        password = sys.argv[3]
        
        # デフォルトは.envファイルの設定を使用
        use_env = input("環境変数（.env）のデータベース設定を使用しますか？ (y/n): ").lower() == 'y'
        
        custom_db_params = None
        if not use_env:
            # カスタム接続パラメータ
            custom_db_params = {
                'host': input("ホスト名 (デフォルト: localhost): ") or 'localhost',
                'user': input("データベースユーザー名: "),
                'password': input("データベースパスワード: "),
                'database': input("データベース名: "),
                'port': int(input("ポート番号 (デフォルト: 3306): ") or 3306)
            }
    
    # ユーザー作成の実行
    create_user(user_id, username, password, custom_db_params)

    # 主要なユーザーを一括登録する例
    create_more = input("他のユーザーも作成しますか？(y/n): ").lower() == 'y'
    if create_more:
        sample_users = [
            ('user1', '一般ユーザー1', 'user123'),
            ('guest', 'ゲストユーザー', 'guest123')
        ]
        
        for user in sample_users:
            create_more = input(f"{user[0]}（{user[1]}）を作成しますか？(y/n): ").lower() == 'y'
            if create_more:
                create_user(user[0], user[1], user[2], custom_db_params)

    print("処理が完了しました。")