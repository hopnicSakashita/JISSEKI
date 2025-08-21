from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from datetime import datetime
from app.config import Config

# グローバルなインスタンスを初期化
db = SQLAlchemy()
login_manager = LoginManager()

def create_app(config_class=None):
    app = Flask(__name__)
    
    # configの設定
    if config_class is None:
        from app.config import Config
        config_class = Config
    
    app.config.from_object(config_class)
    
    # セッションの設定
    app.config['SESSION_TYPE'] = 'filesystem'
    app.config['SESSION_PERMANENT'] = False
    
    # 拡張機能を初期化
    db.init_app(app)
    login_manager.init_app(app)
    
    login_manager.login_view = 'auth.login'
    
    # ブループリントを登録
    from app.routes import main as main_blueprint
    from app.auth import auth as auth_blueprint
    from app.upload_routes import upload_bp as upload_blueprint
    from app.analyse_routes import analyse as analyse_blueprint
    from app.ishida1_routes import ishida1 as ishida1_blueprint
    from app.ishida2_routes import ishida2 as ishida2_blueprint
    from app.master_routes import master as master_blueprint
    
    app.register_blueprint(main_blueprint)
    app.register_blueprint(auth_blueprint)
    app.register_blueprint(upload_blueprint)
    app.register_blueprint(analyse_blueprint)
    app.register_blueprint(ishida1_blueprint)
    app.register_blueprint(ishida2_blueprint)
    app.register_blueprint(master_blueprint)
    
    # アプリケーションコンテキストをプッシュしてモデルを初期化
    with app.app_context():
        # マスタモデルをインポートしてテーブルを作成
        from .master_models import PrdMstModel, KbnMst, MnoMstModel
        db.create_all()
    
    return app 