from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime, timedelta
import csv
import os
from dotenv import load_dotenv
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from app import db, login_manager


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


    
class WorkerModel(db.Model):
    """従業員テーブルのSQLAlchemyモデル"""
    __tablename__ = 'WRK_MST'
    
    WRK_ID = db.Column(db.DECIMAL(3), primary_key=True, comment='従業員ID')
    WRK_NM = db.Column(db.String(100), comment='従業員名')
    


class SjiDatModel(db.Model):
    """指示データテーブルのSQLAlchemyモデル"""
    __tablename__ = 'SJI_DAT'
    # 複合主キーの設定
    __table_args__ = (
        db.PrimaryKeyConstraint('SJI_PRD_ID', 'SJI_DATE'),
    )
    SJI_PRD_ID = db.Column(db.String(5), db.ForeignKey('PRD_MST.PRD_ID'), comment='製品ID')
    SJI_DATE = db.Column(db.DateTime, comment='指示日')
    SJI_QTY = db.Column(db.DECIMAL(4), comment='指示数')
    
    # 外部キーリレーション
    prd_mst = db.relationship('PrdMstModel', backref='sji_records', foreign_keys=[SJI_PRD_ID])
    
    @staticmethod
    def import_from_csv(file_path, encoding='shift_jis'):
        """CSVファイルから指示データをインポートする"""
        try:
            session = get_db_session()
            if not session:
                return False, 'データベースセッションの作成に失敗しました'
            
            with open(file_path, 'r', encoding=encoding) as f:
                reader = csv.reader(f)
                next(reader)  # ヘッダー行をスキップ
                
                for row in reader:
                    prd_id = row[1]
                    sji_date = row[2]
                    sji_qty = row[13]
                    
                    query = session.query(SjiDatModel).filter(
                        SjiDatModel.SJI_PRD_ID == prd_id, 
                        SjiDatModel.SJI_DATE == sji_date).first()
                    if query:
                        continue
                    
                    sji_record = SjiDatModel(
                        SJI_PRD_ID=prd_id,
                        SJI_DATE=sji_date,
                        SJI_QTY=sji_qty
                    )
                    session.add(sji_record)     
                    
            session.commit()
            session.close()
            return True, 'データのインポートが完了しました'
        except Exception as e:
            log_error(f'指示データのインポート中にエラーが発生しました: {str(e)}')
            return False, f'指示データのインポート中にエラーが発生しました: {str(e)}'
                    
class PrdRecordModel(db.Model):
    """生産実績テーブルのSQLAlchemyモデル"""
    __tablename__ = 'PRD_RECORD'
    
    PRR_LOT_NO = db.Column(db.String(17), primary_key=True, comment='ロットNo')
    PRR_PRD_ID = db.Column(db.String(5), db.ForeignKey('PRD_MST.PRD_ID'), comment='製品ID')
    PRR_R1_IN_DATE = db.Column(db.DateTime, comment='R1注入日')
    PRR_R1_TANK = db.Column(db.DECIMAL(2), comment='R1重合槽')
    PRR_R2_TANK = db.Column(db.DECIMAL(2), comment='R2重合槽')
    PRR_MONO_BATCH = db.Column(db.String(10), comment='モノマーバッチ')
    PRR_R2_INJECT = db.Column(db.DECIMAL(3), comment='R2注入者')
    PRR_FILM_DATE = db.Column(db.DateTime, comment='膜加工日')
    PRR_R1_INJECT = db.Column(db.DECIMAL(3), comment='R1注入者')
    PRR_INJECT_QTY = db.Column(db.DECIMAL(4), comment='注入数')
    PRR_ROLL_MISS = db.Column(db.DECIMAL(4), comment='巻きミス')
    PRR_R1_BUB_CHK = db.Column(db.DECIMAL(4), comment='R1泡(検品)')
    PRR_CURL_INS = db.Column(db.DECIMAL(4), comment='カール(検品)')
    PRR_FILM_FLT_CK = db.Column(db.DECIMAL(4), comment='膜浮き(検品)')
    PRR_LEAK = db.Column(db.DECIMAL(4), comment='モレ')
    PRR_FILM_PULL = db.Column(db.DECIMAL(4), comment='膜ひっぱり')
    PRR_FILM_NG_CK = db.Column(db.DECIMAL(4), comment='膜不良(検品)')
    PRR_R2_BUB_REK = db.Column(db.DECIMAL(4), comment='R2泡(離型)')
    PRR_CRACK = db.Column(db.DECIMAL(4), comment='ワレ')
    PRR_TEAR_RLS = db.Column(db.DECIMAL(4), comment='チギレ(離型)')
    PRR_TEAR = db.Column(db.DECIMAL(4), comment='チギレ')
    PRR_PEEL = db.Column(db.DECIMAL(4), comment='ハガレ')
    PRR_CHIP = db.Column(db.DECIMAL(4), comment='カケ')
    PRR_POLY_CRK = db.Column(db.DECIMAL(4), comment='重合ワレ')
    PRR_MOLD_SCR = db.Column(db.DECIMAL(4), comment='型キズ')
    PRR_LENS_SCR = db.Column(db.DECIMAL(4), comment='レンズキズ')
    PRR_R1_BUBBLE = db.Column(db.DECIMAL(4), comment='R1泡')
    PRR_R2_BUBBLE = db.Column(db.DECIMAL(4), comment='R2泡')
    PRR_DEFECT = db.Column(db.DECIMAL(4), comment='ブツ')
    PRR_ELUTION = db.Column(db.DECIMAL(4), comment='溶出')
    PRR_HAZE = db.Column(db.DECIMAL(4), comment='モヤ')
    PRR_CURL = db.Column(db.DECIMAL(4), comment='カール')
    PRR_FILM_FLOAT = db.Column(db.DECIMAL(4), comment='膜浮き')
    PRR_R1_DEFECT = db.Column(db.DECIMAL(4), comment='R1不良')
    PRR_FILM_NG = db.Column(db.DECIMAL(4), comment='膜不良')
    PRR_FOREIGN = db.Column(db.DECIMAL(4), comment='イブツ')
    PRR_CUT_WASTE = db.Column(db.DECIMAL(4), comment='カットくず')
    PRR_FIBER = db.Column(db.DECIMAL(4), comment='センイ')
    PRR_MOLD_DIRT = db.Column(db.DECIMAL(4), comment='モールド汚れ')
    PRR_FILM_DIRT = db.Column(db.DECIMAL(4), comment='膜汚れ')
    PRR_AXIS_1ST = db.Column(db.DECIMAL(4), comment='片軸(一次)')
    PRR_STRIPE_1ST = db.Column(db.DECIMAL(4), comment='脈理(一次)')
    PRR_EDGE_DEFECT = db.Column(db.DECIMAL(4), comment='コバスリ不良')
    PRR_ECC_1ST = db.Column(db.DECIMAL(4), comment='偏心不良(一次)')
    PRR_WASH_DROP = db.Column(db.DECIMAL(4), comment='洗浄落下')
    PRR_UNKNOWN = db.Column(db.DECIMAL(4), comment='不明')
    PRR_OTHER_1 = db.Column(db.DECIMAL(4), comment='その他1')
    PRR_OTHER_2 = db.Column(db.DECIMAL(4), comment='その他2')
    PRR_ECC_DEFECT = db.Column(db.DECIMAL(4), comment='偏心不良')
    PRR_DROP = db.Column(db.DECIMAL(4), comment='落下')
    PRR_COUNT_ERR = db.Column(db.DECIMAL(4), comment='員数違い')
    PRR_OTHER_1ST = db.Column(db.DECIMAL(4), comment='その他(一次)')
    PRR_PEEL_2ND = db.Column(db.DECIMAL(4), comment='ハガレ(二次)')
    PRR_STRIPE_2ND = db.Column(db.DECIMAL(4), comment='脈理(二次)')
    PRR_SUCTION = db.Column(db.DECIMAL(4), comment='吸い込み')
    PRR_MOLD_2ND = db.Column(db.DECIMAL(4), comment='型キズ(二次)')
    PRR_FILM_2ND = db.Column(db.DECIMAL(4), comment='膜不良(二次)')
    PRR_DEFECT_2ND = db.Column(db.DECIMAL(4), comment='ブツ(二次)')
    PRR_OTHER_2ND = db.Column(db.DECIMAL(4), comment='その他(二次)')
    PRR_AXIS_DEF = db.Column(db.DECIMAL(4), comment='軸不良')
    PRR_FILM_3RD = db.Column(db.DECIMAL(4), comment='膜浮き(三次)')
    PRR_COLOR_DEF = db.Column(db.DECIMAL(4), comment='カラー不良')
    PRR_TRANS_DEF = db.Column(db.DECIMAL(4), comment='透過率不良')
    PRR_CURVE_DEF = db.Column(db.DECIMAL(4), comment='カーブ不良')
    PRR_CEN_TH_DEF = db.Column(db.DECIMAL(4), comment='中心厚不良')
    PRR_DIAM_DEF = db.Column(db.DECIMAL(4), comment='径不良')
    PRR_R1_TH_DEF = db.Column(db.DECIMAL(4), comment='R1厚み不良')
    PRR_ECC_3RD = db.Column(db.DECIMAL(4), comment='偏心不良(三次)')
    PRR_EDGE_DEF_3 = db.Column(db.DECIMAL(4), comment='膜汚れ(三次)')
    PRR_AXIS_3RD = db.Column(db.DECIMAL(4), comment='片軸(三次)')
    PRR_OTHER_3RD = db.Column(db.DECIMAL(4), comment='その他(三次)')
    PRR_A_GRADE = db.Column(db.DECIMAL(4), comment='A品')
    PRR_B_GRADE = db.Column(db.DECIMAL(4), comment='B品')
    PRR_R1_IN_COM = db.Column(db.String(100), comment='R1注入コメント')
    PRR_R1_CHK_COM = db.Column(db.String(100), comment='R1検品コメント')
    PRR_R2_IN_COM = db.Column(db.String(100), comment='R2注入コメント')
    PRR_REL_COM = db.Column(db.String(100), comment='離型コメント')
    PRR_RELEASE_BY = db.Column(db.DECIMAL(3), comment='離型者')
    PRR_ANNEAL_BY = db.Column(db.DECIMAL(3), comment='アニール者')
    PRR_CHK1_BY = db.Column(db.DECIMAL(3), comment='一次検査員')
    PRR_CHK2_BY = db.Column(db.DECIMAL(3), comment='二次検査員')
    PRR_CHK3_BY = db.Column(db.DECIMAL(3), comment='三次検査員')
    PRR_R1_GOOD_CNT = db.Column(db.DECIMAL(4), comment='R1良品数')
    PRR_ANNEAL_TNK = db.Column(db.DECIMAL(2), comment='アニール槽')
    PRR_R2_DATE = db.Column(db.DateTime, comment='R2注入日')
    PRR_R2__QTY = db.Column(db.DECIMAL(4), comment='R2注入数')
    PRR_RELEASE_DT = db.Column(db.DateTime, comment='離型日')
    PRR_CHK_DT = db.Column(db.DateTime, comment='検査日')
    PRR_MONO_SYU = db.Column(db.String(1), db.ForeignKey('MNO_MST.MNO_SYU'), comment='モノマー種')
    PRR_R2_JG_DT = db.Column(db.DateTime, comment='R2重合日')
    PRR_ANNEAL_DT = db.Column(db.DateTime, comment='アニール日')
    PRR_CHK1_DT = db.Column(db.DateTime, comment='一次検査日')
    PRR_CHK2_DT = db.Column(db.DateTime, comment='二次検査日')
    PRR_R1_JG_DT = db.Column(db.DateTime, comment='R1重合日')
    PRR_R1_CHK_DT = db.Column(db.DateTime, comment='R1検査日')
    PRR_R2_DATETIME = db.Column(db.DateTime, comment='R2注入日時')
    
    # リレーションシップ定義
    prd_mst = db.relationship('PrdMstModel', foreign_keys=[PRR_PRD_ID])
    mono_mst = db.relationship('MnoMstModel', foreign_keys=[PRR_MONO_SYU])
    
    @staticmethod
    def get_incomplete_inspections():
        """
        検査が終了していないデータを取得する
        
        条件:
        - PRR_CHK3_BYがNULL (三次検査員が未設定 = 三次検査が未完了)
        - PRR_RELEASE_BY.isnot(None) (離型担当者が設定されている = 離型が完了している)
        - 直近2週間のR1注入日のデータに限定
        - 2日以上前に離型が完了したデータのみ対象
        
        詳細説明:
        - このメソッドは離型工程は完了しているが、三次検査工程がまだ完了していないレコードを抽出します
        - 工程管理上、離型後に検査が滞っているデータを把握するために使用されます
        - 直近2週間のデータのみを表示することで、現在の作業状況に焦点を当てています
        - 2日以上前に離型が完了したデータに限定することで、実際に検査可能なデータのみを表示します
        - R1注入日の順にソートされるため、古い順に処理を進めることができます
        
        技術的注意点:
        - 検査完了の判断はPRR_CHK3_BYの有無で行っています（三次検査員が記録されていれば検査完了）
        - 離型完了の判断はPRR_RELEASE_BYの有無で行っています（離型担当者が記録されていれば離型完了）
        
        SQLAlchemyのフィルタリング:
        - PRR_CHK3_BY.is_(None): 三次検査員がNULLのレコードに限定
        - PRR_RELEASE_BY.isnot(None): 離型担当者が設定されているレコードに限定
        - PRR_R1_IN_DATE >= two_weeks_ago: 2週間以内のデータに限定
        - PRR_RELEASE_DT <= two_days_ago: 2日以上前に離型が完了したデータに限定
        - order_by(): R1注入日の昇順でソート

        戻り値:
            list: 検査未完了データのリスト。何もなければ空のリストを返します。
        
        例外処理:
            データベース接続エラーなどの例外が発生した場合はエラーログに記録し、空のリストを返します。
        """
        try:
            query = PrdRecordModel.query.filter(PrdRecordModel.PRR_CHK3_BY.is_(None))
            query = query.filter(PrdRecordModel.PRR_RELEASE_BY.isnot(None))
                        
            two_weeks_ago = datetime.now() - timedelta(days=14)
            two_days_ago = datetime.now() - timedelta(days=2)
            query = query.filter(PrdRecordModel.PRR_R1_IN_DATE >= two_weeks_ago)
            query = query.filter(PrdRecordModel.PRR_RELEASE_DT <= two_days_ago)
            
            # 日付順で並び替え
            query = query.order_by(PrdRecordModel.PRR_R1_IN_DATE)
            
            return query.all()
        except Exception as e:
            log_error(f'未検査品データの取得中にエラーが発生しました: {str(e)}')
            return []

class PrdRecord:
    @staticmethod
    def import_from_csv(file_path, encoding='shift_jis'):
        """CSVファイルから生産実績データをインポート"""
        session = None
        try:
            session = get_db_session()
            
            # マスタデータを一度だけ取得
            worker_map = {}
            worker_query = text("SELECT WRK_ID, WRK_NM FROM WRK_MST")
            try:
                for row in session.execute(worker_query):
                    worker_map[row.WRK_NM] = row.WRK_ID
            except Exception as e:
                log_error(f'ワーカー情報の取得中にエラーが発生しました: {str(e)}')
                raise
            
            machine_map = {}
            machine_query = text("SELECT MCN_ID, MCN_NM FROM MCN_MST")
            try:
                for row in session.execute(machine_query):
                    machine_map[row.MCN_NM] = row.MCN_ID
            except Exception as e:
                log_error(f'マシン情報の取得中にエラーが発生しました: {str(e)}')
                raise
            log_error(f'読み取り開始')
            
            # CSVファイルを読み込みながら処理
            records = []
            delete_records = []
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    reader = csv.reader(f)
                    
                    for row in reader:
                        try:
                            # 行が空でないことを確認
                            if not row or len(row) < 89:  # 必要な列数
                                continue
                            
                            siji_no = row[0].split('-')[0]
                            if len(siji_no) == 10:
                            
                                edit_row = {}
                                monoSyu = row[0][:1] if len(row[0]) > 1 else row[0]
                                prdId = row[0][:4] if len(row[0]) > 4 else row[0]
                                from .master_models import PrdMstModel
                                prdMst = PrdMstModel.query.filter_by(PRD_ID=prdId).first()
                                if prdMst:
                                    monoSyu = prdMst.PRD_TYP
                                    
                                if len(row[0]) > 13:
                                    if not row[0][:13] in delete_records:
                                        delete_records.append(row[0][:13])
                            elif len(siji_no) == 11:
                                edit_row = {}
                                monoSyu = row[0][:1] if len(row[0]) > 1 else row[0]
                                prdId = row[0][:5] if len(row[0]) > 5 else row[0]
                                from .master_models import PrdMstModel
                                prdMst = PrdMstModel.query.filter_by(PRD_ID=prdId).first()
                                if prdMst:
                                    monoSyu = prdMst.PRD_TYP
                                    
                                if len(row[0]) > 14:
                                    if not row[0][:14] in delete_records:
                                        delete_records.append(row[0][:14])
                                        
                            else:
                                raise ValueError(f'ロットNoの形式が不正です: {row[0]}')
                                    
                            try:
                                r2_in = row[5].split(',')
                                r2_str = r2_in[0]
                            except:
                                r2_str = row[5]
                            
                            # データの変換と格納（元のコードを維持）
                            edit_row['PRR_LOT_NO'] = row[0]
                            edit_row['PRR_PRD_ID'] = prdId
                            edit_row['PRR_R1_IN_DATE'] = parse_date(row[1])
                            edit_row['PRR_R1_TANK'] = machine_map.get(row[2], None)
                            edit_row['PRR_R2_TANK'] = machine_map.get(row[3], None)
                            edit_row['PRR_MONO_BATCH'] = row[4]
                            edit_row['PRR_R2_INJECT'] = worker_map.get(r2_str, None)
                            edit_row['PRR_FILM_DATE'] = parse_date(row[6])
                            edit_row['PRR_R1_INJECT'] = worker_map.get(row[7], None)
                            edit_row['PRR_INJECT_QTY'] = str_to_flt(row[12])
                            edit_row['PRR_ROLL_MISS'] = str_to_flt(row[13])
                            edit_row['PRR_R1_BUB_CHK'] = str_to_flt(row[14])
                            edit_row['PRR_CURL_INS'] = str_to_flt(row[15])
                            edit_row['PRR_FILM_FLT_CK'] = str_to_flt(row[16])   
                            edit_row['PRR_LEAK'] = str_to_flt(row[17])
                            edit_row['PRR_FILM_PULL'] = str_to_flt(row[18])
                            edit_row['PRR_FILM_NG_CK'] = str_to_flt(row[19])
                            edit_row['PRR_R2_BUB_REK'] = str_to_flt(row[20])
                            edit_row['PRR_CRACK'] = str_to_flt(row[21])
                            edit_row['PRR_TEAR_RLS'] = str_to_flt(row[22])
                            edit_row['PRR_TEAR'] = str_to_flt(row[23])
                            edit_row['PRR_PEEL'] = str_to_flt(row[24])  
                            edit_row['PRR_CHIP'] = str_to_flt(row[25])
                            edit_row['PRR_POLY_CRK'] = str_to_flt(row[26])
                            edit_row['PRR_MOLD_SCR'] = str_to_flt(row[27])
                            edit_row['PRR_LENS_SCR'] = str_to_flt(row[28])
                            edit_row['PRR_R1_BUBBLE'] = str_to_flt(row[29])
                            edit_row['PRR_R2_BUBBLE'] = str_to_flt(row[30])
                            edit_row['PRR_DEFECT'] = str_to_flt(row[31])
                            edit_row['PRR_ELUTION'] = str_to_flt(row[32])    
                            edit_row['PRR_HAZE'] = str_to_flt(row[33])
                            edit_row['PRR_CURL'] = str_to_flt(row[34])
                            edit_row['PRR_FILM_FLOAT'] = str_to_flt(row[35])
                            edit_row['PRR_R1_DEFECT'] = str_to_flt(row[36])
                            edit_row['PRR_FILM_NG'] = str_to_flt(row[37])
                            edit_row['PRR_FOREIGN'] = str_to_flt(row[38])
                            edit_row['PRR_CUT_WASTE'] = str_to_flt(row[39])
                            edit_row['PRR_FIBER'] = str_to_flt(row[40])  
                            edit_row['PRR_MOLD_DIRT'] = str_to_flt(row[41])
                            edit_row['PRR_FILM_DIRT'] = str_to_flt(row[42])
                            edit_row['PRR_AXIS_1ST'] = str_to_flt(row[43])
                            edit_row['PRR_STRIPE_1ST'] = str_to_flt(row[44])
                            edit_row['PRR_EDGE_DEFECT'] = str_to_flt(row[45])
                            edit_row['PRR_ECC_1ST'] = str_to_flt(row[46])    
                            edit_row['PRR_WASH_DROP'] = str_to_flt(row[47])
                            edit_row['PRR_UNKNOWN'] = str_to_flt(row[48])
                            edit_row['PRR_OTHER_1'] = str_to_flt(row[49])
                            edit_row['PRR_OTHER_2'] = str_to_flt(row[50])
                            edit_row['PRR_ECC_DEFECT'] = str_to_flt(row[51])
                            edit_row['PRR_DROP'] = str_to_flt(row[52])   
                            edit_row['PRR_COUNT_ERR'] = str_to_flt(row[53])
                            edit_row['PRR_OTHER_1ST'] = str_to_flt(row[54])
                            edit_row['PRR_PEEL_2ND'] = str_to_flt(row[55])
                            edit_row['PRR_STRIPE_2ND'] = str_to_flt(row[56])
                            edit_row['PRR_SUCTION'] = str_to_flt(row[57])
                            edit_row['PRR_MOLD_2ND'] = str_to_flt(row[58])   
                            edit_row['PRR_FILM_2ND'] = str_to_flt(row[59])
                            edit_row['PRR_DEFECT_2ND'] = str_to_flt(row[60])
                            edit_row['PRR_OTHER_2ND'] = str_to_flt(row[61])
                            edit_row['PRR_AXIS_DEF'] = str_to_flt(row[62])
                            edit_row['PRR_FILM_3RD'] = str_to_flt(row[63])   
                            edit_row['PRR_COLOR_DEF'] = str_to_flt(row[64])
                            edit_row['PRR_TRANS_DEF'] = str_to_flt(row[65])
                            edit_row['PRR_CURVE_DEF'] = str_to_flt(row[66])
                            edit_row['PRR_CEN_TH_DEF'] = str_to_flt(row[67])
                            edit_row['PRR_DIAM_DEF'] = str_to_flt(row[68])
                            edit_row['PRR_R1_TH_DEF'] = str_to_flt(row[69])  
                            edit_row['PRR_ECC_3RD'] = str_to_flt(row[70])
                            edit_row['PRR_EDGE_DEF_3'] = str_to_flt(row[71])
                            edit_row['PRR_AXIS_3RD'] = str_to_flt(row[72])
                            edit_row['PRR_OTHER_3RD'] = str_to_flt(row[73])
                            edit_row['PRR_A_GRADE'] = str_to_flt(row[74])
                            edit_row['PRR_B_GRADE'] = str_to_flt(row[75])    
                            edit_row['PRR_R1_IN_COM'] = row[76]
                            edit_row['PRR_R1_CHK_COM'] = row[77]
                            edit_row['PRR_R2_IN_COM'] = row[78]
                            edit_row['PRR_REL_COM'] = row[79]
                            edit_row['PRR_RELEASE_BY'] = worker_map.get(row[80], None)
                            edit_row['PRR_ANNEAL_BY'] = worker_map.get(row[81], None)  
                            edit_row['PRR_CHK1_BY'] = worker_map.get(row[82], None)
                            edit_row['PRR_CHK2_BY'] = worker_map.get(row[83], None)
                            edit_row['PRR_CHK3_BY'] = worker_map.get(row[84], None)
                            edit_row['PRR_R1_GOOD_CNT'] = str_to_flt(row[87])
                            edit_row['PRR_ANNEAL_TNK'] = machine_map.get(row[88], None)
                            edit_row['PRR_R2_DATE'] = None
                            edit_row['PRR_R2__QTY'] = 0
                            edit_row['PRR_RELEASE_DT'] = None
                            edit_row['PRR_CHK_DT'] = None
                            edit_row['PRR_MONO_SYU'] = monoSyu
                            edit_row['PRR_R2_JG_DT'] = None
                            edit_row['PRR_ANNEAL_DT'] = None
                            edit_row['PRR_CHK1_DT'] = None
                            edit_row['PRR_CHK2_DT'] = None
                            
                            records.append(edit_row)
                            
                        except Exception as e:
                            log_error(f'行の処理中にエラーが発生しました: {str(e)}, 行: {row}')
                            continue
                
            except UnicodeDecodeError as e:
                log_error(f'CSVファイルの文字コード変換中にエラーが発生しました: {str(e)}')
                return False, 'CSVファイルの文字コードが正しくありません。Shift-JIS形式であることを確認してください。'

            if not records:
                return False, 'データが読み込めませんでした。CSVファイルの形式を確認してください。'
            
            total_processed = 0
            
            log_error(f'レコード処理')
            # レコードを一括で処理
            for record in records:
                try:
                    stmt = text("""
                        INSERT INTO PRD_RECORD (
                            PRR_LOT_NO, PRR_PRD_ID, PRR_R1_IN_DATE, PRR_R1_TANK, PRR_R2_TANK,
                            PRR_MONO_BATCH, PRR_R2_INJECT, PRR_FILM_DATE, PRR_R1_INJECT,
                            PRR_INJECT_QTY, PRR_ROLL_MISS, PRR_R1_BUB_CHK, PRR_CURL_INS,
                            PRR_FILM_FLT_CK, PRR_LEAK, PRR_FILM_PULL, PRR_FILM_NG_CK,
                            PRR_R2_BUB_REK, PRR_CRACK, PRR_TEAR_RLS, PRR_TEAR, PRR_PEEL,
                            PRR_CHIP, PRR_POLY_CRK, PRR_MOLD_SCR, PRR_LENS_SCR, PRR_R1_BUBBLE,
                            PRR_R2_BUBBLE, PRR_DEFECT, PRR_ELUTION, PRR_HAZE, PRR_CURL,
                            PRR_FILM_FLOAT, PRR_R1_DEFECT, PRR_FILM_NG, PRR_FOREIGN,
                            PRR_CUT_WASTE, PRR_FIBER, PRR_MOLD_DIRT, PRR_FILM_DIRT,
                            PRR_AXIS_1ST, PRR_STRIPE_1ST, PRR_EDGE_DEFECT, PRR_ECC_1ST,
                            PRR_WASH_DROP, PRR_UNKNOWN, PRR_OTHER_1, PRR_OTHER_2,
                            PRR_ECC_DEFECT, PRR_DROP, PRR_COUNT_ERR, PRR_OTHER_1ST,
                            PRR_PEEL_2ND, PRR_STRIPE_2ND, PRR_SUCTION, PRR_MOLD_2ND,
                            PRR_FILM_2ND, PRR_DEFECT_2ND, PRR_OTHER_2ND, PRR_AXIS_DEF,
                            PRR_FILM_3RD, PRR_COLOR_DEF, PRR_TRANS_DEF, PRR_CURVE_DEF,
                            PRR_CEN_TH_DEF, PRR_DIAM_DEF, PRR_R1_TH_DEF, PRR_ECC_3RD,
                            PRR_EDGE_DEF_3, PRR_AXIS_3RD, PRR_OTHER_3RD, PRR_A_GRADE,
                            PRR_B_GRADE, PRR_R1_IN_COM, PRR_R1_CHK_COM, PRR_R2_IN_COM,
                            PRR_REL_COM, PRR_RELEASE_BY, PRR_ANNEAL_BY, PRR_CHK1_BY,
                            PRR_CHK2_BY, PRR_CHK3_BY, PRR_R1_GOOD_CNT, PRR_ANNEAL_TNK,
                            PRR_R2_DATE, PRR_R2__QTY, PRR_RELEASE_DT, PRR_CHK_DT, PRR_MONO_SYU
                        ) VALUES (
                            :lot_no, :prd_id, :r1_in_date, :r1_tank, :r2_tank,
                            :mono_batch, :r2_inject, :film_date, :r1_inject,
                            :inject_qty, :roll_miss, :r1_bubble_chk, :curl_ins,
                            :film_float_ck, :leak, :film_pull, :film_ng_ck,
                            :r2_bubble_rek, :crack, :tear_rls, :tear, :peel,
                            :chip, :poly_crk, :mold_scr, :lens_scr, :r1_bubble,
                            :r2_bubble, :defect, :elution, :haze, :curl,
                            :film_float, :r1_defect, :film_ng, :foreign,
                            :cut_waste, :fiber, :mold_dirt, :film_dirt,
                            :axis_1st, :stripe_1st, :edge_defect, :ecc_1st,
                            :wash_drop, :unknown, :other_1, :other_2,
                            :ecc_defect, :drop, :count_err, :other_1st,
                            :peel_2nd, :stripe_2nd, :suction, :mold_2nd,
                            :film_2nd, :defect_2nd, :other_2nd, :axis_def,
                            :film_3rd, :color_def, :trans_def, :curve_def,
                            :cen_th_def, :diam_def, :r1_th_def, :ecc_3rd,
                            :edge_def_3, :axis_3rd, :other_3rd, :a_grade,
                            :b_grade, :r1_in_com, :r1_chk_com, :r2_in_com,
                            :rel_com, :release_by, :anneal_by, :chk1_by,
                            :chk2_by, :chk3_by, :r1_good_cnt, :anneal_tnk,
                            :r2_date, :r2__qty, :release_dt, :chk_dt, :mono_syu
                        )
                        ON DUPLICATE KEY UPDATE
                            PRR_PRD_ID = VALUES(PRR_PRD_ID),
                            PRR_R1_IN_DATE = VALUES(PRR_R1_IN_DATE),
                            PRR_R1_TANK = VALUES(PRR_R1_TANK),
                            PRR_R2_TANK = VALUES(PRR_R2_TANK),
                            PRR_MONO_BATCH = VALUES(PRR_MONO_BATCH),
                            PRR_R2_INJECT = VALUES(PRR_R2_INJECT),
                            PRR_FILM_DATE = VALUES(PRR_FILM_DATE),
                            PRR_R1_INJECT = VALUES(PRR_R1_INJECT),
                            PRR_INJECT_QTY = VALUES(PRR_INJECT_QTY),
                            PRR_ROLL_MISS = VALUES(PRR_ROLL_MISS),
                            PRR_R1_BUB_CHK = VALUES(PRR_R1_BUB_CHK),
                            PRR_CURL_INS = VALUES(PRR_CURL_INS),
                            PRR_FILM_FLT_CK = VALUES(PRR_FILM_FLT_CK),
                            PRR_LEAK = VALUES(PRR_LEAK),
                            PRR_FILM_PULL = VALUES(PRR_FILM_PULL),
                            PRR_FILM_NG_CK = VALUES(PRR_FILM_NG_CK),
                            PRR_R2_BUB_REK = VALUES(PRR_R2_BUB_REK),
                            PRR_CRACK = VALUES(PRR_CRACK),
                            PRR_TEAR_RLS = VALUES(PRR_TEAR_RLS),
                            PRR_TEAR = VALUES(PRR_TEAR),
                            PRR_PEEL = VALUES(PRR_PEEL),
                            PRR_CHIP = VALUES(PRR_CHIP),
                            PRR_POLY_CRK = VALUES(PRR_POLY_CRK),
                            PRR_MOLD_SCR = VALUES(PRR_MOLD_SCR),
                            PRR_LENS_SCR = VALUES(PRR_LENS_SCR),
                            PRR_R1_BUBBLE = VALUES(PRR_R1_BUBBLE),
                            PRR_R2_BUBBLE = VALUES(PRR_R2_BUBBLE),
                            PRR_DEFECT = VALUES(PRR_DEFECT),
                            PRR_ELUTION = VALUES(PRR_ELUTION),
                            PRR_HAZE = VALUES(PRR_HAZE),
                            PRR_CURL = VALUES(PRR_CURL),
                            PRR_FILM_FLOAT = VALUES(PRR_FILM_FLOAT),
                            PRR_R1_DEFECT = VALUES(PRR_R1_DEFECT),
                            PRR_FILM_NG = VALUES(PRR_FILM_NG),
                            PRR_FOREIGN = VALUES(PRR_FOREIGN),
                            PRR_CUT_WASTE = VALUES(PRR_CUT_WASTE),
                            PRR_FIBER = VALUES(PRR_FIBER),
                            PRR_MOLD_DIRT = VALUES(PRR_MOLD_DIRT),
                            PRR_FILM_DIRT = VALUES(PRR_FILM_DIRT),
                            PRR_AXIS_1ST = VALUES(PRR_AXIS_1ST),
                            PRR_STRIPE_1ST = VALUES(PRR_STRIPE_1ST),
                            PRR_EDGE_DEFECT = VALUES(PRR_EDGE_DEFECT),
                            PRR_ECC_1ST = VALUES(PRR_ECC_1ST),
                            PRR_WASH_DROP = VALUES(PRR_WASH_DROP),
                            PRR_UNKNOWN = VALUES(PRR_UNKNOWN),
                            PRR_OTHER_1 = VALUES(PRR_OTHER_1),
                            PRR_OTHER_2 = VALUES(PRR_OTHER_2),
                            PRR_ECC_DEFECT = VALUES(PRR_ECC_DEFECT),
                            PRR_DROP = VALUES(PRR_DROP),
                            PRR_COUNT_ERR = VALUES(PRR_COUNT_ERR),
                            PRR_OTHER_1ST = VALUES(PRR_OTHER_1ST),
                            PRR_PEEL_2ND = VALUES(PRR_PEEL_2ND),
                            PRR_STRIPE_2ND = VALUES(PRR_STRIPE_2ND),
                            PRR_SUCTION = VALUES(PRR_SUCTION),
                            PRR_MOLD_2ND = VALUES(PRR_MOLD_2ND),
                            PRR_FILM_2ND = VALUES(PRR_FILM_2ND),
                            PRR_DEFECT_2ND = VALUES(PRR_DEFECT_2ND),
                            PRR_OTHER_2ND = VALUES(PRR_OTHER_2ND),
                            PRR_AXIS_DEF = VALUES(PRR_AXIS_DEF),
                            PRR_FILM_3RD = VALUES(PRR_FILM_3RD),
                            PRR_COLOR_DEF = VALUES(PRR_COLOR_DEF),
                            PRR_TRANS_DEF = VALUES(PRR_TRANS_DEF),
                            PRR_CURVE_DEF = VALUES(PRR_CURVE_DEF),
                            PRR_CEN_TH_DEF = VALUES(PRR_CEN_TH_DEF),
                            PRR_DIAM_DEF = VALUES(PRR_DIAM_DEF),
                            PRR_R1_TH_DEF = VALUES(PRR_R1_TH_DEF),
                            PRR_ECC_3RD = VALUES(PRR_ECC_3RD),
                            PRR_EDGE_DEF_3 = VALUES(PRR_EDGE_DEF_3),
                            PRR_AXIS_3RD = VALUES(PRR_AXIS_3RD),
                            PRR_OTHER_3RD = VALUES(PRR_OTHER_3RD),
                            PRR_A_GRADE = VALUES(PRR_A_GRADE),
                            PRR_B_GRADE = VALUES(PRR_B_GRADE),
                            PRR_R1_IN_COM = VALUES(PRR_R1_IN_COM),
                            PRR_R1_CHK_COM = VALUES(PRR_R1_CHK_COM),
                            PRR_R2_IN_COM = VALUES(PRR_R2_IN_COM),
                            PRR_REL_COM = VALUES(PRR_REL_COM),
                            PRR_RELEASE_BY = VALUES(PRR_RELEASE_BY),
                            PRR_ANNEAL_BY = VALUES(PRR_ANNEAL_BY),
                            PRR_CHK1_BY = VALUES(PRR_CHK1_BY),
                            PRR_CHK2_BY = VALUES(PRR_CHK2_BY),
                            PRR_CHK3_BY = VALUES(PRR_CHK3_BY),
                            PRR_R1_GOOD_CNT = VALUES(PRR_R1_GOOD_CNT),
                            PRR_ANNEAL_TNK = VALUES(PRR_ANNEAL_TNK),
                            PRR_MONO_SYU = VALUES(PRR_MONO_SYU)
                    """)
                    session.execute(stmt, {
                        'lot_no': record['PRR_LOT_NO'],
                        'prd_id': record['PRR_PRD_ID'],
                        'r1_in_date': record['PRR_R1_IN_DATE'],
                        'r1_tank': record['PRR_R1_TANK'],
                        'r2_tank': record['PRR_R2_TANK'],
                        'mono_batch': record['PRR_MONO_BATCH'],
                        'r2_inject': record['PRR_R2_INJECT'],
                        'film_date': record['PRR_FILM_DATE'],
                        'r1_inject': record['PRR_R1_INJECT'],
                        'inject_qty': record['PRR_INJECT_QTY'],
                        'roll_miss': record['PRR_ROLL_MISS'],
                        'r1_bubble_chk': record['PRR_R1_BUB_CHK'],
                        'curl_ins': record['PRR_CURL_INS'],
                        'film_float_ck': record['PRR_FILM_FLT_CK'],
                        'leak': record['PRR_LEAK'],
                        'film_pull': record['PRR_FILM_PULL'],
                        'film_ng_ck': record['PRR_FILM_NG_CK'],
                        'r2_bubble_rek': record['PRR_R2_BUB_REK'],
                        'crack': record['PRR_CRACK'],
                        'tear_rls': record['PRR_TEAR_RLS'],
                        'tear': record['PRR_TEAR'],
                        'peel': record['PRR_PEEL'],
                        'chip': record['PRR_CHIP'],
                        'poly_crk': record['PRR_POLY_CRK'],
                        'mold_scr': record['PRR_MOLD_SCR'],
                        'lens_scr': record['PRR_LENS_SCR'],
                        'r1_bubble': record['PRR_R1_BUBBLE'],
                        'r2_bubble': record['PRR_R2_BUBBLE'],
                        'defect': record['PRR_DEFECT'],
                        'elution': record['PRR_ELUTION'],
                        'haze': record['PRR_HAZE'],
                        'curl': record['PRR_CURL'],
                        'film_float': record['PRR_FILM_FLOAT'],
                        'r1_defect': record['PRR_R1_DEFECT'],
                        'film_ng': record['PRR_FILM_NG'],
                        'foreign': record['PRR_FOREIGN'],
                        'cut_waste': record['PRR_CUT_WASTE'],
                        'fiber': record['PRR_FIBER'],
                        'mold_dirt': record['PRR_MOLD_DIRT'],
                        'film_dirt': record['PRR_FILM_DIRT'],
                        'axis_1st': record['PRR_AXIS_1ST'],
                        'stripe_1st': record['PRR_STRIPE_1ST'],
                        'edge_defect': record['PRR_EDGE_DEFECT'],
                        'ecc_1st': record['PRR_ECC_1ST'],
                        'wash_drop': record['PRR_WASH_DROP'],
                        'unknown': record['PRR_UNKNOWN'],
                        'other_1': record['PRR_OTHER_1'],
                        'other_2': record['PRR_OTHER_2'],
                        'ecc_defect': record['PRR_ECC_DEFECT'],
                        'drop': record['PRR_DROP'],
                        'count_err': record['PRR_COUNT_ERR'],
                        'other_1st': record['PRR_OTHER_1ST'],
                        'peel_2nd': record['PRR_PEEL_2ND'],
                        'stripe_2nd': record['PRR_STRIPE_2ND'],
                        'suction': record['PRR_SUCTION'],
                        'mold_2nd': record['PRR_MOLD_2ND'],
                        'film_2nd': record['PRR_FILM_2ND'],
                        'defect_2nd': record['PRR_DEFECT_2ND'],
                        'other_2nd': record['PRR_OTHER_2ND'],
                        'axis_def': record['PRR_AXIS_DEF'],
                        'film_3rd': record['PRR_FILM_3RD'],
                        'color_def': record['PRR_COLOR_DEF'],
                        'trans_def': record['PRR_TRANS_DEF'],
                        'curve_def': record['PRR_CURVE_DEF'],
                        'cen_th_def': record['PRR_CEN_TH_DEF'],
                        'diam_def': record['PRR_DIAM_DEF'],
                        'r1_th_def': record['PRR_R1_TH_DEF'],
                        'ecc_3rd': record['PRR_ECC_3RD'],
                        'edge_def_3': record['PRR_EDGE_DEF_3'],
                        'axis_3rd': record['PRR_AXIS_3RD'],
                        'other_3rd': record['PRR_OTHER_3RD'],
                        'a_grade': record['PRR_A_GRADE'],
                        'b_grade': record['PRR_B_GRADE'],
                        'r1_in_com': record['PRR_R1_IN_COM'],
                        'r1_chk_com': record['PRR_R1_CHK_COM'],
                        'r2_in_com': record['PRR_R2_IN_COM'],
                        'rel_com': record['PRR_REL_COM'],
                        'release_by': record['PRR_RELEASE_BY'],
                        'anneal_by': record['PRR_ANNEAL_BY'],
                        'chk1_by': record['PRR_CHK1_BY'],
                        'chk2_by': record['PRR_CHK2_BY'],
                        'chk3_by': record['PRR_CHK3_BY'],
                        'r1_good_cnt': record['PRR_R1_GOOD_CNT'],
                        'anneal_tnk': record['PRR_ANNEAL_TNK'],
                        'r2_date': record['PRR_R2_DATE'],
                        'r2__qty': record['PRR_R2__QTY'],
                        'release_dt': record['PRR_RELEASE_DT'],
                        'chk_dt': record['PRR_CHK_DT'],
                        'mono_syu': record['PRR_MONO_SYU']
                    })
                    
                    session.commit()
                    total_processed += 1
                except Exception as e:
                    session.rollback()
                    log_error(f'生産実績データのインポート/更新中にエラーが発生しました: {str(e)}')
                    raise
            
            if delete_records:
                for record in delete_records:
                    stmt = text("""
                        DELETE FROM PRD_RECORD WHERE PRR_LOT_NO = :lot_no
                    """)
                    session.execute(stmt, {'lot_no': record})
                    session.commit()
                    
            SetMst.set_csv_import_time()    
            
            return True, f'{total_processed}件のデータをインポートしました。'   
        
        except Exception as e:
            log_error(f'CSVファイルの読み込み中にエラーが発生しました: {str(e)}')
            return False, str(e)
        finally:
            if session:
                session.close()
                
    @staticmethod
    def import_from_csv2(file_path, encoding='shift_jis'):
        """CSVファイルから生産実績データをインポート"""
        session = None
        try:
            session = get_db_session()
            
            # CSVファイルを読み込み、11列目の値に基づいて振り分け
            r2_inject_records = []  # 11列目が4（R2注入）
            check_records = []      # 11列目が10（検査）
            r2_jg_records = []      # 11列目が5（R2重合）
            anneal_records = []     # 11列目が7（アニール）
            chk1_records = []       # 11列目が8（一次検査）
            chk2_records = []       # 11列目が9（二次検査）
            release_records = []   # 11列目が6（離型）
            r1_jg_records = []     # 11列目が2（R1重合）
            r1_chk_records = []    # 11列目が3（R1検品）

            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    reader = csv.reader(f)
                    next(reader)  # ヘッダー行をスキップ
                    
                    for row in reader:
                        try:
                            # 行が空でないことを確認
                            if not row or len(row) < 13:  # 必要な列数
                                continue
                            
                            # 11列目のデータで振り分け
                            process_type = row[10] if len(row) > 10 else None
                            
                            edit_row = {
                                'PRR_LOT_NO': row[1],
                                'DATE': parse_date(row[0])
                            }
                            
                            if process_type == '4':  # R2注入
                                edit_row['PRR_R2__QTY'] = str_to_flt(row[12]) if len(row) > 12 else 0
                                edit_row['PRR_R2_DATETIME'] = parse_datetime(row[15]) if len(row) > 15 else None
                                r2_inject_records.append(edit_row)
                            elif process_type == '5':  # R2重合
                                r2_jg_records.append(edit_row)
                            elif process_type == '6':  # 離型
                                release_records.append(edit_row)
                            elif process_type == '7':  # アニール
                                anneal_records.append(edit_row)
                            elif process_type == '8':  # 一次検査
                                chk1_records.append(edit_row)
                            elif process_type == '9':  # 二次検査
                                chk2_records.append(edit_row)
                            elif process_type == '10': # 検査
                                check_records.append(edit_row)
                            elif process_type == '2': # R1重合
                                r1_jg_records.append(edit_row)
                            elif process_type == '3': # R1検品
                                r1_chk_records.append(edit_row)

                        except Exception as e:
                            log_error(f'行の処理中にエラーが発生しました: {str(e)}, 行: {row}')
                            continue
                
            except UnicodeDecodeError as e:
                log_error(f'CSVファイルの文字コード変換中にエラーが発生しました: {str(e)}')
                return False, 'CSVファイルの文字コードが正しくありません。Shift-JIS形式であることを確認してください。'

            if not (r2_inject_records or check_records or r2_jg_records or anneal_records or chk1_records or chk2_records):
                return False, 'データが読み込めませんでした。CSVファイルの形式を確認してください。'
            
            total_processed = 0
            
            log_error(f'レコード処理')
            
            # R2注入データの処理
            for record in r2_inject_records:
                try:
                    stmt = text("""
                        UPDATE PRD_RECORD SET
                            PRR_R2_DATE = :date,
                            PRR_R2__QTY = :r2__qty,
                            PRR_R2_DATETIME = :r2_datetime
                        WHERE PRR_LOT_NO = :lot_no
                    """)
                    session.execute(stmt, {
                        'lot_no': record['PRR_LOT_NO'],
                        'date': record['DATE'],
                        'r2__qty': record.get('PRR_R2__QTY', 0),
                        'r2_datetime': record.get('PRR_R2_DATETIME', None)
                    })
                    
                    session.commit()
                    total_processed += 1
                except Exception as e:
                    session.rollback()
                    log_error(f'R2注入データの更新中にエラーが発生しました: {str(e)}')
                    continue
            
            # 検査データの処理
            for record in check_records:
                try:
                    stmt = text("""
                        UPDATE PRD_RECORD SET
                            PRR_CHK_DT = :date
                        WHERE PRR_LOT_NO = :lot_no
                    """)
                    session.execute(stmt, {
                        'lot_no': record['PRR_LOT_NO'],
                        'date': record['DATE']
                    })
                    
                    session.commit()
                    total_processed += 1
                except Exception as e:
                    session.rollback()
                    log_error(f'検査データの更新中にエラーが発生しました: {str(e)}')
                    continue
            
            # R2重合データの処理
            for record in r2_jg_records:
                try:
                    stmt = text("""
                        UPDATE PRD_RECORD SET
                            PRR_R2_JG_DT = :date
                        WHERE PRR_LOT_NO = :lot_no
                    """)
                    session.execute(stmt, {
                        'lot_no': record['PRR_LOT_NO'],
                        'date': record['DATE']
                    })
                    
                    session.commit()
                    total_processed += 1
                except Exception as e:
                    session.rollback()
                    log_error(f'R2重合データの更新中にエラーが発生しました: {str(e)}')
                    continue
            
            # 離型データの処理
            for record in release_records:
                try:
                    stmt = text("""
                        UPDATE PRD_RECORD SET
                            PRR_RELEASE_DT = :date
                        WHERE PRR_LOT_NO = :lot_no
                    """)
                    session.execute(stmt, {
                        'lot_no': record['PRR_LOT_NO'],
                        'date': record['DATE']
                    })

                    session.commit()
                    total_processed += 1
                except Exception as e:
                    session.rollback()
                    log_error(f'離型データの更新中にエラーが発生しました: {str(e)}')
                    continue
                
            # アニールデータの処理
            for record in anneal_records:
                try:
                    stmt = text("""
                        UPDATE PRD_RECORD SET
                            PRR_ANNEAL_DT = :date
                        WHERE PRR_LOT_NO = :lot_no
                    """)
                    session.execute(stmt, {
                        'lot_no': record['PRR_LOT_NO'],
                        'date': record['DATE']
                    })
                    
                    session.commit()
                    total_processed += 1
                except Exception as e:
                    session.rollback()
                    log_error(f'アニールデータの更新中にエラーが発生しました: {str(e)}')
                    continue
            
            # 一次検査データの処理
            for record in chk1_records:
                try:
                    stmt = text("""
                        UPDATE PRD_RECORD SET
                            PRR_CHK1_DT = :date
                        WHERE PRR_LOT_NO = :lot_no
                    """)
                    session.execute(stmt, {
                        'lot_no': record['PRR_LOT_NO'],
                        'date': record['DATE']
                    })
                    
                    session.commit()
                    total_processed += 1
                except Exception as e:
                    session.rollback()
                    log_error(f'一次検査データの更新中にエラーが発生しました: {str(e)}')
                    continue
            
            # 二次検査データの処理
            for record in chk2_records:
                try:
                    stmt = text("""
                        UPDATE PRD_RECORD SET
                            PRR_CHK2_DT = :date
                        WHERE PRR_LOT_NO = :lot_no
                    """)
                    session.execute(stmt, {
                        'lot_no': record['PRR_LOT_NO'],
                        'date': record['DATE']
                    })
                    
                    session.commit()
                    total_processed += 1
                except Exception as e:
                    session.rollback()
                    log_error(f'二次検査データの更新中にエラーが発生しました: {str(e)}')
                    continue
            
            # R1重合データの処理
            for record in r1_jg_records:
                try:
                    stmt = text("""
                        UPDATE PRD_RECORD SET
                            PRR_R1_JG_DT = :date
                        WHERE PRR_LOT_NO = :lot_no
                    """)
                    session.execute(stmt, {
                        'lot_no': record['PRR_LOT_NO'],
                        'date': record['DATE']
                    })
                    
                    session.commit()
                    total_processed += 1
                except Exception as e:
                    session.rollback()
                    log_error(f'R1重合データの更新中にエラーが発生しました: {str(e)}')
                    continue
            
            # R1検品データの処理
            for record in r1_chk_records:
                try:
                    stmt = text("""
                        UPDATE PRD_RECORD SET
                            PRR_R1_CHK_DT = :date
                        WHERE PRR_LOT_NO = :lot_no
                    """)
                    session.execute(stmt, {
                        'lot_no': record['PRR_LOT_NO'],
                        'date': record['DATE']
                    })
                    
                    session.commit()
                    total_processed += 1
                except Exception as e:
                    session.rollback()
                    log_error(f'R1検品データの更新中にエラーが発生しました: {str(e)}')
                    continue
            
            return True, f'{total_processed}件のデータをインポートしました。'
                
        except Exception as e:
            log_error(f'CSVファイルの読み込み中にエラーが発生しました: {str(e)}')
            return False, str(e)
        finally:
            if session:
                session.close()

                
def parse_date(date_str):
    """日付文字列をdatetimeオブジェクトに変換する関数"""
    try:
        return datetime.strptime(date_str, '%Y/%m/%d')
    except ValueError:
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return None
def parse_datetime(date_str):
    """日付文字列をdatetimeオブジェクトに変換する関数"""
    try:
        return datetime.strptime(date_str, '%Y/%m/%d %H:%M:%S')
    except ValueError:
        try:
            return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None
                            
def str_to_flt(value):
    """文字列を数値に変換する関数"""
    try:
        return float(value)
    except ValueError:
        return 0

class User(db.Model, UserMixin):
    __tablename__ = 'USERS'
    
    USER_ID = db.Column(db.String(20), primary_key=True)
    USERNAME = db.Column(db.String(50), nullable=False)
    PASSWORD = db.Column(db.String(255), nullable=False)
    LAST_LOGIN = db.Column(db.DateTime, nullable=True)
    USER_FLG = db.Column(db.Numeric(1, 0), default=0)
    
    def get_id(self):
        return self.USER_ID
        
    def set_password(self, password):
        self.PASSWORD = generate_password_hash(password)
        
    def check_password(self, password):
        return check_password_hash(self.PASSWORD, password)
        
    def update_last_login(self):
        self.LAST_LOGIN = datetime.now()
        db.session.commit()

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(user_id)

# MonoMstはmaster_models.pyに移動しました
    
class SetMst(db.Model):
    __tablename__ = 'SET_MST'
    
    SET_ID = db.Column(db.Numeric(1), primary_key=True, comment='ID')
    SET_DRW_INT = db.Column(db.Numeric(6), comment='描画間隔')
    SET_CHS_TM = db.Column(db.Numeric(2), comment='抽出期間')
    SET_JS_RD_DT = db.Column(db.DateTime, comment='CSV取込時間')
    SET_INFO_H1 = db.Column(db.String(40), comment='お知らせヘッダ１')
    SET_INFO_1 = db.Column(db.String(200), comment='お知らせ１')
    SET_INFO_H2 = db.Column(db.String(40), comment='お知らせヘッダ２')
    SET_INFO_2 = db.Column(db.String(200), comment='お知らせ２')
    SET_INFO_H3 = db.Column(db.String(40), comment='お知らせヘッダ３')
    SET_INFO_3 = db.Column(db.String(200), comment='お知らせ３')
    
    @classmethod
    def set_csv_import_time(cls):
        """
        SET_JS_RD_DT（CSV取込時間）のみを取得するメソッド
        
        Returns:
            DateTime: CSV取込時間
        """
        dt = datetime.now()
        setting = cls.query.first()
        if setting:
            setting.SET_JS_RD_DT = dt
            db.session.commit()
        else:
            return None
    
    @classmethod
    def get_csv_import_time(cls):
        setting = cls.query.first()
        return setting.SET_JS_RD_DT.strftime('%Y/%m/%d %H:%M:%S') if setting else None

class NoteDatModel(db.Model):
    """特記事項データテーブルのSQLAlchemyモデル"""
    __tablename__ = 'NOTE_DAT'
    
    NOTE_ID = db.Column(db.Integer, primary_key=True, autoincrement=True, comment='ID')
    NOTE_LOT_NO = db.Column(db.String(17), comment='ロットNo.')
    NOTE_DATE = db.Column(db.DateTime, comment='入力日')
    NOTE_USER = db.Column(db.DECIMAL(4), db.ForeignKey('WRK_MST.WRK_ID'), comment='入力者')
    NOTE_TITLE = db.Column(db.String(40), comment='項目名')
    NOTE_CNTNT = db.Column(db.String(200), comment='内容')
    NOTE_PATH = db.Column(db.String(255), comment='画像パス')
    
    # 外部キーリレーション
    worker = db.relationship('WorkerModel', backref='notes')
    
    @staticmethod
    def get_all():
        """全ての特記事項データを取得"""
        try:
            session = get_db_session()
            if not session:
                return []
            
            notes = session.query(NoteDatModel).options(db.joinedload(NoteDatModel.worker)).order_by(NoteDatModel.NOTE_DATE.desc()).all()
            session.close()
            return notes
        except Exception as e:
            log_error(f'特記事項データの取得中にエラーが発生しました: {str(e)}')
            return []
    
    @staticmethod
    def get_by_id(note_id):
        """IDで特記事項データを取得"""
        try:
            session = get_db_session()
            if not session:
                return None
            
            note = session.query(NoteDatModel).options(db.joinedload(NoteDatModel.worker)).filter(NoteDatModel.NOTE_ID == note_id).first()
            session.close()
            return note
        except Exception as e:
            log_error(f'特記事項データの取得中にエラーが発生しました: {str(e)}')
            return None
    
    @staticmethod
    def search(lot_no=None, start_date=None, end_date=None, user_id=None, title=None, page=1, per_page=10):
        """特記事項データを検索（ページネーション対応）"""
        try:
            session = get_db_session()
            if not session:
                return [], 0
            
            query = session.query(NoteDatModel).options(db.joinedload(NoteDatModel.worker))
            
            if lot_no:
                query = query.filter(NoteDatModel.NOTE_LOT_NO.like(f'%{lot_no}%'))
            if start_date:
                query = query.filter(NoteDatModel.NOTE_DATE >= start_date)
            if end_date:
                query = query.filter(NoteDatModel.NOTE_DATE <= end_date)
            if user_id:
                query = query.filter(NoteDatModel.NOTE_USER == user_id)
            if title:
                query = query.filter(NoteDatModel.NOTE_TITLE.like(f'%{title}%'))
            
            total_count = query.count()
            offset = (page - 1) * per_page
            notes = query.order_by(NoteDatModel.NOTE_ID.desc()).offset(offset).limit(per_page).all()
            
            # 各特記事項に対して返信の有無を確認
            for note in notes:
                reply_count = session.query(NansDatModel).filter(
                    NansDatModel.NANS_NOTE_ID == note.NOTE_ID
                ).count()
                note.has_replies = reply_count > 0
                note.reply_count = reply_count
            
            session.close()
            return notes, total_count
        except Exception as e:
            log_error(f'特記事項データの検索中にエラーが発生しました: {str(e)}')
            return [], 0
    
    def save(self):
        """特記事項データを保存"""
        try:
            session = get_db_session()
            if not session:
                return False
            
            if self.NOTE_ID:
                # 更新
                existing_note = session.query(NoteDatModel).filter(NoteDatModel.NOTE_ID == self.NOTE_ID).first()
                if existing_note:
                    existing_note.NOTE_LOT_NO = self.NOTE_LOT_NO
                    existing_note.NOTE_DATE = self.NOTE_DATE
                    existing_note.NOTE_USER = self.NOTE_USER
                    existing_note.NOTE_TITLE = self.NOTE_TITLE
                    existing_note.NOTE_CNTNT = self.NOTE_CNTNT
                    existing_note.NOTE_PATH = self.NOTE_PATH
            else:
                # 新規作成
                session.add(self)
            
            session.commit()
            session.close()
            return True
        except Exception as e:
            log_error(f'特記事項データの保存中にエラーが発生しました: {str(e)}')
            return False
    
    def delete(self):
        """特記事項データを削除"""
        try:
            session = get_db_session()
            if not session:
                return False
            
            note = session.query(NoteDatModel).filter(NoteDatModel.NOTE_ID == self.NOTE_ID).first()
            if note:
                session.delete(note)
                session.commit()
            
            session.close()
            return True
        except Exception as e:
            log_error(f'特記事項データの削除中にエラーが発生しました: {str(e)}')
            return False

class NansDatModel(db.Model):
    """特記返信データテーブルのSQLAlchemyモデル"""
    __tablename__ = 'NANS_DAT'
    NANS_ID = db.Column(db.Integer, primary_key=True, autoincrement=True, comment='特記返信ID')
    NANS_NOTE_ID = db.Column(db.Integer, db.ForeignKey('NOTE_DAT.NOTE_ID'), comment='特記事項ID')
    NANS_DATE = db.Column(db.DateTime, comment='入力日')
    NANS_USER = db.Column(db.DECIMAL(4), db.ForeignKey('WRK_MST.WRK_ID'), comment='入力者')
    NANS_CNTNT = db.Column(db.String(200), comment='内容')

    # 外部キーリレーション
    note = db.relationship('NoteDatModel', backref='replies')
    worker = db.relationship('WorkerModel', backref='nans_replies')

    @staticmethod
    def get_by_note_id(note_id):
        """特記事項IDで返信データを取得"""
        try:
            session = get_db_session()
            if not session:
                return []
            replies = session.query(NansDatModel).options(
                db.joinedload(NansDatModel.worker)
            ).filter(
                NansDatModel.NANS_NOTE_ID == note_id
            ).order_by(NansDatModel.NANS_DATE.asc()).all()
            session.close()
            return replies
        except Exception as e:
            log_error(f'特記返信データの取得中にエラーが発生しました: {str(e)}')
            return []

    @staticmethod
    def get_by_id(nans_id):
        """返信データをIDで取得"""
        try:
            session = get_db_session()
            if not session:
                return None
            reply = session.query(NansDatModel).options(
                db.joinedload(NansDatModel.worker)
            ).filter(NansDatModel.NANS_ID == nans_id).first()
            session.close()
            return reply
        except Exception as e:
            log_error(f'特記返信データの取得中にエラーが発生しました: {str(e)}')
            return None

    def save(self):
        """返信データを保存"""
        try:
            session = get_db_session()
            if not session:
                return False
            if self.NANS_ID:
                # 更新
                existing_reply = session.query(NansDatModel).filter(NansDatModel.NANS_ID == self.NANS_ID).first()
                if existing_reply:
                    existing_reply.NANS_NOTE_ID = self.NANS_NOTE_ID
                    existing_reply.NANS_DATE = self.NANS_DATE
                    existing_reply.NANS_USER = self.NANS_USER
                    existing_reply.NANS_CNTNT = self.NANS_CNTNT
            else:
                # 新規作成
                session.add(self)
            session.commit()
            session.close()
            return True
        except Exception as e:
            log_error(f'特記返信データの保存中にエラーが発生しました: {str(e)}')
            return False

    def delete(self):
        """返信データを削除"""
        try:
            session = get_db_session()
            if not session:
                return False
            reply = session.query(NansDatModel).filter(NansDatModel.NANS_ID == self.NANS_ID).first()
            if reply:
                session.delete(reply)
                session.commit()
            session.close()
            return True
        except Exception as e:
            log_error(f'特記返信データの削除中にエラーが発生しました: {str(e)}')
            return False

    @staticmethod
    def get_next_id():
        """次の返信IDを取得"""
        try:
            session = get_db_session()
            if not session:
                return 1
            max_id = session.query(db.func.max(NansDatModel.NANS_ID)).scalar()
            session.close()
            return int(max_id) + 1 if max_id else 1
        except Exception as e:
            log_error(f'次の返信ID取得中にエラーが発生しました: {str(e)}')
            return 1

