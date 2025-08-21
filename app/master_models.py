from sqlalchemy import create_engine, distinct
from sqlalchemy.orm import sessionmaker, declarative_base
import csv
import os
from dotenv import load_dotenv
from app import db
from decimal import Decimal
from .utils import log_error

# .envファイルの読み込み
load_dotenv()

Base = declarative_base()

def get_db_session():
    """データベースセッションを取得"""
    try:
        DATABASE_URL = (
            f"mysql+pymysql://"
            f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
            f"{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        )
        
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        return Session()
    except Exception as e:
        log_error(f'データベースセッションの取得中にエラーが発生しました: {str(e)}')
        return None

# ==================== PRD_MST (製品マスタ) モデル ====================

class PrdMstModel(db.Model):
    """製品マスタテーブルのSQLAlchemyモデル"""
    __tablename__ = 'PRD_MST'
    
    PRD_ID = db.Column(db.String(5), primary_key=True, comment='製品ID')
    PRD_KBN = db.Column(db.DECIMAL(2), comment='商品分類')
    PRD_TYP = db.Column(db.String(1), comment='識別ID')
    PRD_NM = db.Column(db.String(60), comment='製品名')
    PRD_COLOR = db.Column(db.String(20), comment='膜カラー')
    PRD_PLY_DAYS = db.Column(db.DECIMAL(2), comment='重合日数')
    
    @staticmethod
    def get_distinct_prd_nm():
        """製品名をDISTINCTで取得する"""
        try:
            session = get_db_session()
            if not session:
                return []
            # PRD_TYPとPRD_NMでDISTINCTを取得
            distinct_items = session.query(
                distinct(PrdMstModel.PRD_NM)
            ).all()
            session.close()
            
            # タプルのリストから値のリストに変換
            return [item[0] for item in distinct_items if item[0]]
        except Exception as e:
            log_error(f'製品名の取得中にエラーが発生しました: {str(e)}')
            return []
        
    @staticmethod
    def get_distinct_prd_color():
        """膜カラーをDISTINCTで取得する"""
        try:
            session = get_db_session()
            if not session:
                return []
            # PRD_COLORでDISTINCTを取得
            distinct_items = session.query(
                distinct(PrdMstModel.PRD_COLOR)
            ).all()
            session.close()
            
            # タプルのリストから値のリストに変換
            return [item[0] for item in distinct_items if item[0]]
        except Exception as e:
            log_error(f'膜カラーの取得中にエラーが発生しました: {str(e)}')
            return []
    
    @staticmethod
    def get_all():
        """全ての製品マスタデータを取得"""
        try:
            session = get_db_session()
            if not session:
                return []
            
            records = session.query(PrdMstModel).order_by(PrdMstModel.PRD_ID).all()
            session.close()
            return records
        except Exception as e:
            log_error(f'製品マスタデータの取得中にエラーが発生しました: {str(e)}')
            return []
    
    @staticmethod
    def get_by_id(prd_id):
        """製品IDで製品マスタデータを取得"""
        try:
            session = get_db_session()
            if not session:
                return None
            
            record = session.query(PrdMstModel).filter(PrdMstModel.PRD_ID == prd_id).first()
            session.close()
            return record
        except Exception as e:
            log_error(f'製品マスタデータの取得中にエラーが発生しました: {str(e)}')
            return None
    
    def save(self):
        """製品マスタデータを保存"""
        try:
            session = get_db_session()
            if not session:
                return False
            
            if self.PRD_ID:
                # 既存レコードを確認
                existing_record = session.query(PrdMstModel).filter(PrdMstModel.PRD_ID == self.PRD_ID).first()
                if existing_record:
                    # 更新
                    existing_record.PRD_KBN = self.PRD_KBN
                    existing_record.PRD_TYP = self.PRD_TYP
                    existing_record.PRD_NM = self.PRD_NM
                    existing_record.PRD_COLOR = self.PRD_COLOR
                    existing_record.PRD_PLY_DAYS = self.PRD_PLY_DAYS
                else:
                    # 新規作成
                    session.add(self)
            else:
                # 新規作成
                session.add(self)
            
            session.commit()
            session.close()
            return True
        except Exception as e:
            log_error(f'製品マスタデータの保存中にエラーが発生しました: {str(e)}')
            return False
    
    def delete(self):
        """製品マスタデータを削除"""
        try:
            session = get_db_session()
            if not session:
                return False
            
            record = session.query(PrdMstModel).filter(PrdMstModel.PRD_ID == self.PRD_ID).first()
            if record:
                session.delete(record)
                session.commit()
            
            session.close()
            return True
        except Exception as e:
            log_error(f'製品マスタデータの削除中にエラーが発生しました: {str(e)}')
            return False
    
    @staticmethod
    def search(prd_id=None, prd_nm=None, prd_color=None, prd_typ=None):
        """製品マスタデータを検索"""
        try:
            session = get_db_session()
            if not session:
                return []
            
            query = session.query(PrdMstModel)
            
            if prd_id:
                query = query.filter(PrdMstModel.PRD_ID.like(f'%{prd_id}%'))
            if prd_nm:
                query = query.filter(PrdMstModel.PRD_NM.like(f'%{prd_nm}%'))
            if prd_color:
                query = query.filter(PrdMstModel.PRD_COLOR.like(f'%{prd_color}%'))
            if prd_typ:
                query = query.filter(PrdMstModel.PRD_TYP == prd_typ)
            
            records = query.order_by(PrdMstModel.PRD_ID).all()
            session.close()
            return records
        except Exception as e:
            log_error(f'製品マスタデータの検索中にエラーが発生しました: {str(e)}')
            return []
    
    @staticmethod
    def import_from_csv(file_path, encoding='shift_jis'):
        """CSVファイルから製品マスタデータをインポートする（ヘッダーなし、列順固定）"""
        try:
            session = get_db_session()
            if not session:
                return False, 'データベースセッションの作成に失敗しました'
            
            imported_count = 0
            updated_count = 0
            error_count = 0
            
            with open(file_path, 'r', encoding=encoding) as f:
                reader = csv.reader(f)
                
                # 列順固定: PRD_ID, PRD_KBN, PRD_TYP, PRD_NM, PRD_COLOR, PRD_PLY_DAYS
                for row_num, row in enumerate(reader, start=1):
                    try:
                        # 最低限PRD_IDが必要
                        if len(row) < 1:
                            error_count += 1
                            continue
                        
                        prd_id = row[0].strip() if len(row) > 0 and row[0].strip() else None
                        
                        # PRD_IDが空の場合はスキップ
                        if not prd_id:
                            error_count += 1
                            continue
                        
                        # 既存レコードを確認
                        existing_record = session.query(PrdMstModel).filter(
                            PrdMstModel.PRD_ID == prd_id
                        ).first()
                        
                        # データの準備（列順に従って取得）
                        prd_kbn = None
                        if len(row) > 1 and row[1].strip():
                            try:
                                prd_kbn = Decimal(row[1].strip())
                            except:
                                pass
                        
                        prd_typ = row[2].strip() if len(row) > 2 and row[2].strip() else None
                        prd_nm = row[3].strip() if len(row) > 3 and row[3].strip() else None
                        prd_color = row[4].strip() if len(row) > 4 and row[4].strip() else None
                        
                        prd_ply_days = None
                        if len(row) > 5 and row[5].strip():
                            try:
                                prd_ply_days = Decimal(row[5].strip())
                            except:
                                pass
                        
                        if existing_record:
                            # 既存レコードを更新
                            if prd_kbn is not None:
                                existing_record.PRD_KBN = prd_kbn
                            if prd_typ is not None:
                                existing_record.PRD_TYP = prd_typ
                            if prd_nm is not None:
                                existing_record.PRD_NM = prd_nm
                            if prd_color is not None:
                                existing_record.PRD_COLOR = prd_color
                            if prd_ply_days is not None:
                                existing_record.PRD_PLY_DAYS = prd_ply_days
                            updated_count += 1
                        else:
                            # 新規レコードを作成
                            prd_record = PrdMstModel(
                                PRD_ID=prd_id,
                                PRD_KBN=prd_kbn,
                                PRD_TYP=prd_typ,
                                PRD_NM=prd_nm,
                                PRD_COLOR=prd_color,
                                PRD_PLY_DAYS=prd_ply_days
                            )
                            session.add(prd_record)
                            imported_count += 1
                    
                    except Exception as e:
                        log_error(f'行 {row_num} の処理中にエラーが発生しました: {str(e)}')
                        error_count += 1
                        continue
                        
            session.commit()
            session.close()
            
            message = f'データのインポートが完了しました。新規追加: {imported_count}件、更新: {updated_count}件'
            if error_count > 0:
                message += f'、エラー: {error_count}件'
            
            return True, message
            
        except Exception as e:
            log_error(f'製品マスタのインポート中にエラーが発生しました: {str(e)}')
            return False, f'製品マスタのインポート中にエラーが発生しました: {str(e)}'

# ==================== KBN_MST (区分マスタ) モデル ====================

class KbnMst(db.Model):
    __tablename__ = 'KBN_MST'
    
    KBN_TYP = db.Column(db.String(4), primary_key=True, comment='区分種別')
    KBN_ID = db.Column(db.DECIMAL(3), primary_key=True, comment='区分ID')
    KBN_NM = db.Column(db.String(30), comment='区分名')
    
    @staticmethod
    def get_kbn_list(kbn_typ):
        return KbnMst.query.filter_by(KBN_TYP=kbn_typ).all()
    
    @staticmethod
    def get_all():
        """全てのレコードを取得"""
        try:
            return KbnMst.query.order_by(KbnMst.KBN_TYP, KbnMst.KBN_ID).all()
        except Exception as e:
            log_error(f'KBN_MSTの全件取得中にエラーが発生しました: {str(e)}')
            return []
    
    @staticmethod
    def get_by_keys(kbn_typ, kbn_id):
        """主キーによるレコード取得"""
        try:
            return KbnMst.query.filter_by(KBN_TYP=kbn_typ, KBN_ID=kbn_id).first()
        except Exception as e:
            log_error(f'KBN_MST(KBN_TYP:{kbn_typ}, KBN_ID:{kbn_id})の取得中にエラーが発生しました: {str(e)}')
            return None
    
    @staticmethod
    def exists(kbn_typ, kbn_id):
        """データが存在するかどうかを確認"""
        try:
            return db.session.query(
                db.session.query(KbnMst).filter_by(
                    KBN_TYP=kbn_typ,
                    KBN_ID=kbn_id
                ).exists()
            ).scalar()
        except Exception as e:
            log_error(f'KBN_MSTの存在確認中にエラーが発生しました: {str(e)}')
            return False
    
    def save(self):
        """レコードの保存"""
        try:
            db.session.add(self)
            db.session.commit()
            return True
        except Exception as e:
            log_error(f'KBN_MSTの保存中にエラーが発生しました: {str(e)}')
            db.session.rollback()
            return False
    
    def delete(self):
        """レコードの削除"""
        try:
            db.session.delete(self)
            db.session.commit()
            return True
        except Exception as e:
            log_error(f'KBN_MSTの削除中にエラーが発生しました: {str(e)}')
            db.session.rollback()
            return False
    
    @staticmethod
    def get_distinct_types():
        """区分種別の一覧を取得"""
        try:
            return db.session.query(distinct(KbnMst.KBN_TYP)).order_by(KbnMst.KBN_TYP).all()
        except Exception as e:
            log_error(f'区分種別の取得中にエラーが発生しました: {str(e)}')
            return [] 
 
# ==================== MNO_MST (モノマーマスタ) モデル ====================

class MnoMstModel(db.Model):
    """モノマーマスタテーブルのSQLAlchemyモデル"""
    __tablename__ = 'MNO_MST'
    
    MNO_SYU = db.Column(db.String(1), primary_key=True, comment='モノマー種別')
    MNO_NM = db.Column(db.String(30), comment='モノマー名')
    MNO_TARGET = db.Column(db.DECIMAL(4, 1), comment='目標値')
    
    @staticmethod
    def get_all():
        """全てのモノマーマスタデータを取得"""
        try:
            session = get_db_session()
            if not session:
                return []
            
            records = session.query(MnoMstModel).order_by(MnoMstModel.MNO_SYU).all()
            session.close()
            return records
        except Exception as e:
            log_error(f'モノマーマスタデータの取得中にエラーが発生しました: {str(e)}')
            return []
    
    @staticmethod
    def get_by_id(mno_syu):
        """モノマー種別でモノマーマスタデータを取得"""
        try:
            session = get_db_session()
            if not session:
                return None
            
            record = session.query(MnoMstModel).filter(MnoMstModel.MNO_SYU == mno_syu).first()
            session.close()
            return record
        except Exception as e:
            log_error(f'モノマーマスタデータの取得中にエラーが発生しました: {str(e)}')
            return None
    
    def save(self):
        """モノマーマスタデータを保存"""
        try:
            session = get_db_session()
            if not session:
                return False
            
            if self.MNO_SYU:
                # 既存レコードを確認
                existing_record = session.query(MnoMstModel).filter(MnoMstModel.MNO_SYU == self.MNO_SYU).first()
                if existing_record:
                    # 更新
                    existing_record.MNO_NM = self.MNO_NM
                    existing_record.MNO_TARGET = self.MNO_TARGET
                else:
                    # 新規作成
                    session.add(self)
            else:
                # 新規作成
                session.add(self)
            
            session.commit()
            session.close()
            return True
        except Exception as e:
            log_error(f'モノマーマスタデータの保存中にエラーが発生しました: {str(e)}')
            return False
    
    def delete(self):
        """モノマーマスタデータを削除"""
        try:
            session = get_db_session()
            if not session:
                return False
            
            record = session.query(MnoMstModel).filter(MnoMstModel.MNO_SYU == self.MNO_SYU).first()
            if record:
                session.delete(record)
                session.commit()
            
            session.close()
            return True
        except Exception as e:
            log_error(f'モノマーマスタデータの削除中にエラーが発生しました: {str(e)}')
            return False
    
    @staticmethod
    def search(mno_syu=None, mno_nm=None):
        """モノマーマスタデータを検索"""
        try:
            session = get_db_session()
            if not session:
                return []
            
            query = session.query(MnoMstModel)
            
            if mno_syu:
                query = query.filter(MnoMstModel.MNO_SYU.like(f'%{mno_syu}%'))
            if mno_nm:
                query = query.filter(MnoMstModel.MNO_NM.like(f'%{mno_nm}%'))
            
            records = query.order_by(MnoMstModel.MNO_SYU).all()
            session.close()
            return records
        except Exception as e:
            log_error(f'モノマーマスタデータの検索中にエラーが発生しました: {str(e)}')
            return [] 