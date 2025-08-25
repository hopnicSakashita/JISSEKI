from sqlalchemy import text
from sqlalchemy.orm import declarative_base
from datetime import datetime, timedelta
import logging
import csv
from app import db

from app.models import PrdRecordModel, get_db_session, parse_date, str_to_flt
from .master_models import PrdMstModel
from .utils import log_error

# ロギング設定
logging.basicConfig(
    filename='app.log',
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

Base = declarative_base()


    
class FmcDat(db.Model):
    __tablename__ = 'FMC_DAT'
    
    FMC_ID = db.Column(db.Integer, primary_key=True, autoincrement=True, comment='ID')
    FMC_CUT_DATE = db.Column(db.DateTime, comment='カット日')
    FMC_R1_INJ_DATE = db.Column(db.DateTime, comment='R1注入日')
    FMC_MONOMER = db.Column(db.Numeric(2), comment='モノマー')
    FMC_ANNEAL_NO = db.Column(db.Numeric(2), comment='アニール№')
    FMC_CUT_MACH_NO = db.Column(db.Numeric(2), comment='カット機№')
    FMC_ITEM = db.Column(db.Numeric(3), comment='アイテム')
    FMC_CUT_MENU = db.Column(db.Numeric(3), comment='カットメニュー')
    FMC_FILM_PROC_DT = db.Column(db.DateTime, comment='膜加工日')
    FMC_CR_FILM = db.Column(db.Numeric(1), comment='CR膜')
    FMC_HEAT_PROC_DT = db.Column(db.DateTime, comment='熱処理日')
    FMC_FILM_CURVE = db.Column(db.Numeric(2), comment='膜カーブ')
    FMC_COLOR = db.Column(db.Numeric(2), comment='色')
    FMC_AMPM = db.Column(db.Numeric(1), comment='AM/PM')
    FMC_INPUT_QTY = db.Column(db.Numeric(4), comment='投入数')
    FMC_CUT_FOREIGN = db.Column(db.Numeric(4), comment='カットブツ')
    FMC_CUT_WRINKLE = db.Column(db.Numeric(4), comment='カットシワ')
    FMC_CUT_WAVE = db.Column(db.Numeric(4), comment='カットウエーブ')
    FMC_CUT_ERR = db.Column(db.Numeric(4), comment='カットミス')
    FMC_CUT_CRACK = db.Column(db.Numeric(4), comment='カットサケ')
    FMC_CUT_SCRATCH = db.Column(db.Numeric(4), comment='カットキズ')
    FMC_CUT_OTHERS = db.Column(db.Numeric(4), comment='カットその他')
    FMC_GOOD_QTY = db.Column(db.Numeric(4), comment='良品数')
    FMC_WASH_WRINKLE = db.Column(db.Numeric(4), comment='洗浄シワ')
    FMC_WASH_SCRATCH = db.Column(db.Numeric(4), comment='洗浄キズ')
    FMC_WASH_FOREIGN = db.Column(db.Numeric(4), comment='洗浄イブツ')
    FMC_WASH_ACETONE = db.Column(db.Numeric(4), comment='洗浄アセトン')
    FMC_WASH_ERR = db.Column(db.Numeric(4), comment='洗浄ミス')
    FMC_WASH_CUT_ERR = db.Column(db.Numeric(4), comment='洗浄カットミス')
    FMC_WASH_OTHERS = db.Column(db.Numeric(4), comment='洗浄その他')
    FMC_PASS_QTY = db.Column(db.Numeric(4), comment='合格数')
    FMC_MONTH = db.Column(db.Numeric(2), comment='月')
    
    @staticmethod
    def import_from_csv(file_path, encoding='shift_jis'):
        """CSVファイルから膜カットデータをインポート"""
        session = None
        try:
            session = get_db_session()
            
            # マスタデータを一度だけ取得
            item_map = {}
            item_query = text("SELECT KBN_ID, KBN_NM FROM KBN_MST WHERE KBN_TYP = 'MITM'")
            try:
                for row in session.execute(item_query):
                    item_map[row.KBN_NM] = row.KBN_ID
            except Exception as e:
                log_error(f'アイテム情報の取得中にエラーが発生しました: {str(e)}')
                raise
            
            cut_menu_map = {}
            cut_menu_query = text("SELECT KBN_ID, KBN_NM FROM KBN_MST WHERE KBN_TYP = 'MCUT'")
            try:
                for row in session.execute(cut_menu_query):
                    cut_menu_map[row.KBN_NM] = row.KBN_ID
            except Exception as e:
                log_error(f'カットメニュー情報の取得中にエラーが発生しました: {str(e)}')
                raise
            
            monomer_map = {}
            monomer_query = text("SELECT KBN_ID, KBN_NM FROM KBN_MST WHERE KBN_TYP = 'MMNO'")
            try:
                for row in session.execute(monomer_query):
                    monomer_map[row.KBN_NM] = row.KBN_ID
            except Exception as e:
                log_error(f'モノマー情報の取得中にエラーが発生しました: {str(e)}')
                raise
            
            film_curve_map = {}
            film_curve_query = text("SELECT KBN_ID, KBN_NM FROM KBN_MST WHERE KBN_TYP = 'MCRB'")
            try:
                for row in session.execute(film_curve_query):
                    film_curve_map[row.KBN_NM] = row.KBN_ID
            except Exception as e:
                log_error(f'膜カーブ情報の取得中にエラーが発生しました: {str(e)}')
                raise
            
            color_map = {}
            color_query = text("SELECT KBN_ID, KBN_NM FROM KBN_MST WHERE KBN_TYP = 'MCLR'")
            try:
                for row in session.execute(color_query):
                    color_map[row.KBN_NM] = row.KBN_ID
            except Exception as e:
                log_error(f'色情報の取得中にエラーが発生しました: {str(e)}')
                raise
            
            log_error(f'読み取り開始')
            
            # CSVファイルを読み込みながら処理
            records = []
            error_rows = []
            row_number = 0
            
            import codecs
            with codecs.open(file_path, 'r', encoding='shift_jis', errors='replace') as f:
                reader = csv.reader(f)
                next(reader)  # ヘッダー行をスキップ
                
                for row in reader:
                    row_number += 1
                    try:
                        # 行が空でないことを確認
                        if not row or len(row) < 30:  # 必要な列数
                            error_rows.append(f"行 {row_number}: 列数が不足しています ({len(row)} 列)")
                            continue
                        
                        # データの検証
                        if not row[0]:  # 日付が空
                            error_rows.append(f"行 {row_number}: 日付が空です")
                            continue
                            
                        try:
                            parse_date(row[0])
                        except:
                            error_rows.append(f"行 {row_number}: 日付の形式が不正です")
                            continue
                        
                        # アイテム、カットメニュー、モノマー、膜カーブ、色の検証
                        if row[5] and row[5] not in item_map:
                            error_rows.append(f"行 {row_number}: アイテム '{row[5]}' が見つかりません")
                        if row[6] and row[6] not in cut_menu_map:
                            error_rows.append(f"行 {row_number}: カットメニュー '{row[6]}' が見つかりません")
                        if row[2] and row[2] not in monomer_map:
                            error_rows.append(f"行 {row_number}: モノマー '{row[2]}' が見つかりません")
                        if row[10] and row[10] not in film_curve_map:
                            error_rows.append(f"行 {row_number}: 膜カーブ '{row[10]}' が見つかりません")
                        if row[11] and row[11] not in color_map:
                            error_rows.append(f"行 {row_number}: 色 '{row[11]}' が見つかりません")
                        
                        edit_row = {}
                        # データの変換と格納（元のコードを維持）
                        edit_row['FMC_CUT_DATE'] = parse_date(row[0])
                        edit_row['FMC_R1_INJ_DATE'] = parse_date(row[1])
                        edit_row['FMC_MONOMER'] = monomer_map.get(row[2], None)
                        edit_row['FMC_ANNEAL_NO'] = str_to_flt(row[3])
                        edit_row['FMC_CUT_MACH_NO'] = str_to_flt(row[4])
                        edit_row['FMC_ITEM'] = item_map.get(row[5], None)
                        edit_row['FMC_CUT_MENU'] = cut_menu_map.get(row[6], None)
                        edit_row['FMC_FILM_PROC_DT'] = parse_date(row[7])
                        edit_row['FMC_CR_FILM'] = row[8] if row[8] else 0
                        edit_row['FMC_HEAT_PROC_DT'] = parse_date(row[9])
                        edit_row['FMC_FILM_CURVE'] = film_curve_map.get(row[10], None)
                        edit_row['FMC_COLOR'] = color_map.get(row[11], None)
                        edit_row['FMC_INPUT_QTY'] = str_to_flt(row[12])
                        edit_row['FMC_CUT_FOREIGN'] = str_to_flt(row[13])
                        edit_row['FMC_CUT_WRINKLE'] = str_to_flt(row[14])
                        edit_row['FMC_CUT_WAVE'] = str_to_flt(row[15])
                        edit_row['FMC_CUT_ERR'] = str_to_flt(row[16])
                        edit_row['FMC_CUT_CRACK'] = str_to_flt(row[17])
                        edit_row['FMC_CUT_SCRATCH'] = str_to_flt(row[18])
                        edit_row['FMC_CUT_OTHERS'] = str_to_flt(row[19])
                        edit_row['FMC_GOOD_QTY'] = str_to_flt(row[20])
                        edit_row['FMC_WASH_WRINKLE'] = str_to_flt(row[21])
                        edit_row['FMC_WASH_SCRATCH'] = str_to_flt(row[22])
                        edit_row['FMC_WASH_FOREIGN'] = str_to_flt(row[23])
                        edit_row['FMC_WASH_ACETONE'] = str_to_flt(row[24])
                        edit_row['FMC_WASH_ERR'] = str_to_flt(row[25])
                        edit_row['FMC_WASH_CUT_ERR'] = str_to_flt(row[26])
                        edit_row['FMC_WASH_OTHERS'] = str_to_flt(row[27])
                        edit_row['FMC_PASS_QTY'] = str_to_flt(row[28])
                        edit_row['FMC_MONTH'] = str_to_flt(row[29].replace('月', ''))
                        
                        existing = session.query(FmcDat).filter(
                            FmcDat.FMC_CUT_DATE == edit_row['FMC_CUT_DATE'],
                            FmcDat.FMC_R1_INJ_DATE == edit_row['FMC_R1_INJ_DATE'],
                            FmcDat.FMC_MONOMER == edit_row['FMC_MONOMER'],
                            FmcDat.FMC_ANNEAL_NO == edit_row['FMC_ANNEAL_NO'],
                            FmcDat.FMC_CUT_MACH_NO == edit_row['FMC_CUT_MACH_NO'],
                            FmcDat.FMC_ITEM == edit_row['FMC_ITEM'],
                            FmcDat.FMC_CUT_MENU == edit_row['FMC_CUT_MENU'],
                            FmcDat.FMC_FILM_PROC_DT == edit_row['FMC_FILM_PROC_DT'],
                            FmcDat.FMC_CR_FILM == edit_row['FMC_CR_FILM'],
                            FmcDat.FMC_HEAT_PROC_DT == edit_row['FMC_HEAT_PROC_DT'],
                            FmcDat.FMC_FILM_CURVE == edit_row['FMC_FILM_CURVE'],
                            FmcDat.FMC_COLOR == edit_row['FMC_COLOR']
                        ).first()
                        if existing:
                            for k, v in edit_row.items():
                                setattr(existing, k, v)
                        else:
                            session.add(FmcDat(**edit_row))
                        session.commit()
                        records.append(edit_row)
                        
                    except Exception as e:
                        session.rollback()
                        error_rows.append(f"行 {row_number}: {str(e)}")
                        continue
            if error_rows:
                log_error("以下の行でエラーが発生しました:")
                for error in error_rows:
                    log_error(error)
                return False, '\n'.join(error_rows)
            return True, f'{row_number - len(error_rows)}件のデータをインポート/更新しました。'
        except Exception as e:
            log_error(f'FMC_DAT CSVファイルの読み込み中にエラーが発生しました: {str(e)}')
            return False, str(e)
        finally:
            if session:
                session.close()
                
    @staticmethod
    def get_defect_analysis(start_date=None, end_date=None, month=None, r1_inj_date=None,
                          monomer=None, anneal_no=None, cut_mach_no=None, item=None,
                          cut_menu=None, film_proc_dt=None, cr_film=None, heat_proc_dt=None,
                          film_curve=None, color=None):
        """
        不良率分析を行うメソッド
        
        Parameters:
            start_date (datetime): カット日開始日
            end_date (datetime): カット日終了日
            month (int): 月
            r1_inj_date (datetime): R1注入日
            monomer (int): モノマー
            anneal_no (int): アニール№
            cut_mach_no (int): カット機№
            item (int): アイテム
            cut_menu (int): カットメニュー
            film_proc_dt (datetime): 膜加工日
            cr_film (int): CR膜
            heat_proc_dt (datetime): 熱処理日
            film_curve (int): 膜カーブ
            color (int): 色
            
        Returns:
            dict: 不良率分析結果
        """
        try:
            query = FmcDat.query
            
            # 検索条件の適用
            if start_date:
                query = query.filter(FmcDat.FMC_CUT_DATE >= start_date)
            if end_date:
                query = query.filter(FmcDat.FMC_CUT_DATE <= end_date)
            if month:
                query = query.filter(FmcDat.FMC_MONTH == month)
            if r1_inj_date:
                query = query.filter(FmcDat.FMC_R1_INJ_DATE == r1_inj_date)
            if monomer:
                query = query.filter(FmcDat.FMC_MONOMER == monomer)
            if anneal_no:
                query = query.filter(FmcDat.FMC_ANNEAL_NO == anneal_no)
            if cut_mach_no:
                query = query.filter(FmcDat.FMC_CUT_MACH_NO == cut_mach_no)
            if item:
                query = query.filter(FmcDat.FMC_ITEM == item)
            if cut_menu:
                query = query.filter(FmcDat.FMC_CUT_MENU == cut_menu)
            if film_proc_dt:
                query = query.filter(FmcDat.FMC_FILM_PROC_DT == film_proc_dt)
            if cr_film:
                query = query.filter(FmcDat.FMC_CR_FILM == cr_film)
            if heat_proc_dt:
                query = query.filter(FmcDat.FMC_HEAT_PROC_DT == heat_proc_dt)
            if film_curve:
                query = query.filter(FmcDat.FMC_FILM_CURVE == film_curve)
            if color:
                query = query.filter(FmcDat.FMC_COLOR == color)
            query = query.filter(FmcDat.FMC_MONOMER != 8)#色目を除く
            query = query.filter(FmcDat.FMC_CUT_MENU != 62)#色目を除く
            
            # データの集計
            results = query.all()
            
            total_input = sum(r.FMC_INPUT_QTY for r in results)
            total_good = sum(r.FMC_GOOD_QTY for r in results)
            
            if total_input == 0 or total_good == 0:
                return {
                    'total_input': 0,
                    'total_good': 0,
                    'cut_defect_rates': {},
                    'wash_defect_rates': {},
                    'total_cut_defect_rate': 0,
                    'total_wash_defect_rate': 0
                }
            
            # カット工程の不良率計算（投入数を分母）
            cut_defect_rates = {
                'カットブツ': sum(r.FMC_CUT_FOREIGN for r in results) / total_input * 100,
                'カットシワ': sum(r.FMC_CUT_WRINKLE for r in results) / total_input * 100,
                'カットウエーブ': sum(r.FMC_CUT_WAVE for r in results) / total_input * 100,
                'カットミス': sum(r.FMC_CUT_ERR for r in results) / total_input * 100,
                'カットサケ': sum(r.FMC_CUT_CRACK for r in results) / total_input * 100,
                'カットキズ': sum(r.FMC_CUT_SCRATCH for r in results) / total_input * 100,
                'カットその他': sum(r.FMC_CUT_OTHERS for r in results) / total_input * 100
            }
            
            # 洗浄工程の不良率計算（良品数を分母）
            wash_defect_rates = {
                '洗浄シワ': sum(r.FMC_WASH_WRINKLE for r in results) / total_good * 100,
                '洗浄キズ': sum(r.FMC_WASH_SCRATCH for r in results) / total_good * 100,
                '洗浄イブツ': sum(r.FMC_WASH_FOREIGN for r in results) / total_good * 100,
                '洗浄アセトン': sum(r.FMC_WASH_ACETONE for r in results) / total_good * 100,
                '洗浄ミス': sum(r.FMC_WASH_ERR for r in results) / total_good * 100,
                '洗浄カットミス': sum(r.FMC_WASH_CUT_ERR for r in results) / total_good * 100,
                '洗浄その他': sum(r.FMC_WASH_OTHERS for r in results) / total_good * 100
            }
            
            # カット工程の総不良率計算
            total_cut_defects = sum(
                r.FMC_CUT_FOREIGN + r.FMC_CUT_WRINKLE + r.FMC_CUT_WAVE + r.FMC_CUT_ERR +
                r.FMC_CUT_CRACK + r.FMC_CUT_SCRATCH + r.FMC_CUT_OTHERS for r in results
            )
            total_cut_defect_rate = total_cut_defects / total_input * 100
            
            # 洗浄工程の総不良率計算
            total_wash_defects = sum(
                r.FMC_WASH_WRINKLE + r.FMC_WASH_SCRATCH + r.FMC_WASH_FOREIGN +
                r.FMC_WASH_ACETONE + r.FMC_WASH_ERR + r.FMC_WASH_CUT_ERR +
                r.FMC_WASH_OTHERS for r in results
            )
            total_wash_defect_rate = total_wash_defects / total_good * 100
            
            return {
                'total_input': total_input,
                'total_good': total_good,
                'cut_defect_rates': cut_defect_rates,
                'wash_defect_rates': wash_defect_rates,
                'total_cut_defect_rate': total_cut_defect_rate,
                'total_wash_defect_rate': total_wash_defect_rate
            }
            
        except Exception as e:
            log_error(f'不良率分析中にエラーが発生しました: {str(e)}')
            return None

    @staticmethod
    def get_all():
        """全てのレコードを取得"""
        try:
            return FmcDat.query.order_by(FmcDat.FMC_CUT_DATE.desc()).all()
        except Exception as e:
            log_error(f'FMC_DATの全件取得中にエラーが発生しました: {str(e)}')
            return []

    @staticmethod
    def get_by_id(fmc_id):
        """IDによるレコード取得"""
        try:
            return FmcDat.query.get(fmc_id)
        except Exception as e:
            log_error(f'FMC_DAT(ID:{fmc_id})の取得中にエラーが発生しました: {str(e)}')
            return None

    def save(self):
        """レコードの保存"""
        try:
            db.session.add(self)
            db.session.commit()
            return True
        except Exception as e:
            log_error(f'FMC_DATの保存中にエラーが発生しました: {str(e)}')
            db.session.rollback()
            return False

    def delete(self):
        """レコードの削除"""
        try:
            db.session.delete(self)
            db.session.commit()
            return True
        except Exception as e:
            log_error(f'FMC_DATの削除中にエラーが発生しました: {str(e)}')
            db.session.rollback()
            return False

    @staticmethod
    def search(cut_date_start=None, cut_date_end=None, r1_inj_date=None, monomer=None, cut_menu=None, film_proc_date_start=None, film_proc_date_end=None, color=None, film_curve=None):
        """検索条件に基づくレコード取得"""
        try:
            session = get_db_session()
            query = session.query(FmcDat,
                                  (FmcDat.FMC_CUT_FOREIGN + 
                                  FmcDat.FMC_CUT_WRINKLE + 
                                  FmcDat.FMC_CUT_WAVE + 
                                  FmcDat.FMC_CUT_ERR + 
                                  FmcDat.FMC_CUT_CRACK + 
                                  FmcDat.FMC_CUT_SCRATCH + 
                                  FmcDat.FMC_CUT_OTHERS).label('cut_defect_qty'),
                                  (FmcDat.FMC_WASH_WRINKLE + 
                                  FmcDat.FMC_WASH_SCRATCH + 
                                  FmcDat.FMC_WASH_FOREIGN + 
                                  FmcDat.FMC_WASH_ACETONE + 
                                  FmcDat.FMC_WASH_ERR + 
                                  FmcDat.FMC_WASH_CUT_ERR + 
                                  FmcDat.FMC_WASH_OTHERS).label('wash_defect_qty'))
            
            if cut_date_start:
                query = query.filter(FmcDat.FMC_CUT_DATE >= cut_date_start)
            if cut_date_end:
                query = query.filter(FmcDat.FMC_CUT_DATE <= cut_date_end)
            if r1_inj_date:
                query = query.filter(FmcDat.FMC_R1_INJ_DATE == r1_inj_date)
            if monomer:
                query = query.filter(FmcDat.FMC_MONOMER == monomer)
            if cut_menu:
                query = query.filter(FmcDat.FMC_CUT_MENU == cut_menu)
            if film_proc_date_start:
                query = query.filter(FmcDat.FMC_FILM_PROC_DT >= film_proc_date_start)
            if film_proc_date_end:
                query = query.filter(FmcDat.FMC_FILM_PROC_DT <= film_proc_date_end)
            if color:
                query = query.filter(FmcDat.FMC_COLOR == color)
            if film_curve:
                query = query.filter(FmcDat.FMC_FILM_CURVE == film_curve)
            query = query.order_by(FmcDat.FMC_CUT_DATE.desc(), FmcDat.FMC_MONOMER)
                
            return query.all()
        except Exception as e:
            log_error(f'FMC_DATの検索中にエラーが発生しました: {str(e)}')
            return []

    @staticmethod
    def get_monomer_summary(cut_date_start=None, cut_date_end=None):
        """
        モノマー別の投入数、良品数、合格数を集計するメソッド
        
        Parameters:
            cut_date_start (datetime): カット日開始日
            cut_date_end (datetime): カット日終了日
            
        Returns:
            list: モノマー別集計結果
        """
        try:
            from sqlalchemy import func
            session = get_db_session()
            
            query = session.query(
                FmcDat.FMC_MONOMER,
                func.sum(FmcDat.FMC_INPUT_QTY).label('total_input_qty'),
                func.sum(FmcDat.FMC_GOOD_QTY).label('total_good_qty'),
                func.sum(FmcDat.FMC_PASS_QTY).label('total_pass_qty'),
                func.count(FmcDat.FMC_ID).label('record_count')
            )
            
            # 検索条件の適用
            if cut_date_start:
                query = query.filter(FmcDat.FMC_CUT_DATE >= cut_date_start)
            if cut_date_end:
                query = query.filter(FmcDat.FMC_CUT_DATE <= cut_date_end)
            
            # 色目を除く（既存の処理と統一）
            query = query.filter(FmcDat.FMC_MONOMER != 8)
            query = query.filter(FmcDat.FMC_CUT_MENU != 62)
            
            # モノマー別にグループ化
            query = query.group_by(FmcDat.FMC_MONOMER)
            query = query.order_by(FmcDat.FMC_MONOMER)
            
            results = query.all()
            
            # 結果を辞書形式に変換
            summary_data = []
            for result in results:
                monomer_code = result.FMC_MONOMER
                
                # モノマー名を取得
                monomer_name = '不明'
                if monomer_code:
                    from .master_models import KbnMst
                    kbn_data = session.query(KbnMst).filter_by(
                        KBN_TYP='MMNO', 
                        KBN_ID=str(int(monomer_code))
                    ).first()
                    if kbn_data:
                        monomer_name = kbn_data.KBN_NM
                
                input_qty = result.total_input_qty or 0
                good_qty = result.total_good_qty or 0
                pass_qty = result.total_pass_qty or 0
                
                # 歩留率計算（良品数／投入数）
                good_rate = (good_qty / input_qty * 100) if input_qty > 0 else 0
                # 合格率計算（合格数／良品数）
                pass_rate = (pass_qty / good_qty * 100) if good_qty > 0 else 0
                # 総合歩留率計算（合格数／投入数）
                total_rate = (pass_qty / input_qty * 100) if input_qty > 0 else 0
                
                summary_data.append({
                    'monomer_code': monomer_code,
                    'monomer_name': monomer_name,
                    'input_qty': input_qty,
                    'good_qty': good_qty,
                    'pass_qty': pass_qty,
                    'good_rate': round(good_rate, 2),
                    'pass_rate': round(pass_rate, 2),
                    'total_rate': round(total_rate, 2),
                    'record_count': result.record_count
                })
            
            return summary_data
            
        except Exception as e:
            log_error(f'モノマー別集計中にエラーが発生しました: {str(e)}')
            return []
        finally:
            if session:
                session.close()

    @staticmethod
    def get_pass_qty_cross_table(cut_date_start=None, cut_date_end=None, monomer=None, cr_film=None):
        """
        合格数を色×膜カーブのクロス集計表で取得するメソッド
        
        Parameters:
            cut_date_start (datetime): カット日開始日
            cut_date_end (datetime): カット日終了日
            monomer (int): モノマー
            
        Returns:
            dict: クロス集計表データ
        """
        try:
            from sqlalchemy import func
            session = get_db_session()
            
            # ベースクエリの構築
            query = session.query(
                FmcDat.FMC_COLOR,
                FmcDat.FMC_FILM_CURVE,
                func.sum(FmcDat.FMC_PASS_QTY).label('total_pass_qty')
            )
            
            # 検索条件の適用
            if cut_date_start:
                query = query.filter(FmcDat.FMC_CUT_DATE >= cut_date_start)
            if cut_date_end:
                query = query.filter(FmcDat.FMC_CUT_DATE <= cut_date_end)
            if monomer:
                query = query.filter(FmcDat.FMC_MONOMER == monomer)
            if cr_film:
                query = query.filter(FmcDat.FMC_CR_FILM == cr_film)
                
            # 色目を除く（既存の処理と統一）
            query = query.filter(FmcDat.FMC_MONOMER != 8)
            query = query.filter(FmcDat.FMC_CUT_MENU != 62)
            
            # グループ化と並び順の設定
            query = query.group_by(FmcDat.FMC_COLOR, FmcDat.FMC_FILM_CURVE)
            query = query.order_by(FmcDat.FMC_COLOR, FmcDat.FMC_FILM_CURVE)
            
            results = query.all()
            
            # 色と膜カーブの値を収集（ソート用）
            colors = set()
            film_curves = set()
            for row in results:
                if row.FMC_COLOR is not None:
                    colors.add(int(row.FMC_COLOR))
                if row.FMC_FILM_CURVE is not None:
                    film_curves.add(int(row.FMC_FILM_CURVE))
            
            # ソートして順序を保証
            colors = sorted(colors)
            film_curves = sorted(film_curves)
            
            # クロス集計表の作成
            cross_table = {}
            for row in results:
                color_key = int(row.FMC_COLOR) if row.FMC_COLOR is not None else 0
                curve_key = int(row.FMC_FILM_CURVE) if row.FMC_FILM_CURVE is not None else 0
                
                if color_key not in cross_table:
                    cross_table[color_key] = {}
                
                cross_table[color_key][curve_key] = float(row.total_pass_qty or 0)
            
            # 不足分を0で埋める
            for color in colors:
                if color not in cross_table:
                    cross_table[color] = {}
                for curve in film_curves:
                    if curve not in cross_table[color]:
                        cross_table[color][curve] = 0
            
            # 合計値の計算
            color_totals = {}  # 行合計
            curve_totals = {}  # 列合計
            grand_total = 0   # 総合計
            
            for color in colors:
                color_totals[color] = sum(cross_table[color][curve] for curve in film_curves)
                
            for curve in film_curves:
                curve_totals[curve] = sum(cross_table[color][curve] for color in colors)
                
            grand_total = sum(color_totals.values())
            
            return {
                'cross_table': cross_table,
                'colors': colors,
                'film_curves': film_curves,
                'color_totals': color_totals,
                'curve_totals': curve_totals,
                'grand_total': grand_total
            }
            
        except Exception as e:
            log_error(f'クロス集計表の取得中にエラーが発生しました: {str(e)}')
            return {
                'cross_table': {},
                'colors': [],
                'film_curves': [],
                'color_totals': {},
                'curve_totals': {},
                'grand_total': 0
            }
        finally:
            if session:
                session.close()

class FmpDat(db.Model):
    """膜加工データテーブルのSQLAlchemyモデル"""
    __tablename__ = 'FMP_DAT'
    
    FMP_ID = db.Column(db.Integer, primary_key=True, autoincrement=True, comment='ID')
    FMP_INSP_DATE = db.Column(db.DateTime, comment='検査日')
    FMP_PROC_DATE = db.Column(db.DateTime, comment='加工日')
    FMP_COLOR = db.Column(db.DECIMAL(2), comment='色')
    FMP_PVA_LOT_NO = db.Column(db.DECIMAL(10), comment='PVAロットNo')
    FMP_BIKO = db.Column(db.String(30), comment='備考')
    FMP_FILM_CURVE = db.Column(db.DECIMAL(2), comment='膜カーブ')
    FMP_PROC_SHTS = db.Column(db.DECIMAL(5), comment='加工枚数')
    FMP_WRINKLE_A = db.Column(db.DECIMAL(5), comment='シワA')
    FMP_WRINKLE_B = db.Column(db.DECIMAL(5), comment='シワB')
    FMP_TEAR = db.Column(db.DECIMAL(5), comment='裂け')
    FMP_FOREIGN = db.Column(db.DECIMAL(5), comment='ブツ')
    FMP_FIBER = db.Column(db.DECIMAL(5), comment='繊維')
    FMP_SCRATCH = db.Column(db.DECIMAL(5), comment='キズ')
    FMP_HOLE = db.Column(db.DECIMAL(5), comment='穴')
    FMP_PRM_OTHERS = db.Column(db.DECIMAL(5), comment='一次その他')
    FMP_PRM_GOOD_QTY = db.Column(db.DECIMAL(5), comment='一次良品数')
    FMP_CLR_FADE = db.Column(db.DECIMAL(5), comment='色抜け')
    FMP_CLR_IRREG = db.Column(db.DECIMAL(5), comment='色ムラ')
    FMP_DYE_STREAK = db.Column(db.DECIMAL(5), comment='染スジ')
    FMP_DIRT = db.Column(db.DECIMAL(5), comment='汚れ')
    FMP_OTHERS = db.Column(db.DECIMAL(5), comment='その他')
    FMP_GRADE_A = db.Column(db.DECIMAL(5), comment='A品')
    FMP_GRADE_B = db.Column(db.DECIMAL(5), comment='B品')
    FMP_GRADE_C = db.Column(db.DECIMAL(5), comment='C品')

    @staticmethod
    def get_defect_analysis(start_date=None, end_date=None, proc_date=None,
                          color=None, pva_lot_no=None, film_curve=None):
        """
        不良率分析を行うメソッド
        
        Parameters:
            start_date (datetime): 検査日開始日
            end_date (datetime): 検査日終了日
            proc_date (datetime): 加工日
            color (int): 色
            pva_lot_no (int): PVAロットNo
            film_curve (int): 膜カーブ
            
        Returns:
            dict: 不良率分析結果
        """
        try:
            query = FmpDat.query
            
            # 検索条件の適用
            if start_date:
                query = query.filter(FmpDat.FMP_INSP_DATE >= start_date)
            if end_date:
                query = query.filter(FmpDat.FMP_INSP_DATE <= end_date)
            if proc_date:
                query = query.filter(FmpDat.FMP_PROC_DATE == proc_date)
            if color:
                query = query.filter(FmpDat.FMP_COLOR == color)
            if pva_lot_no:
                query = query.filter(FmpDat.FMP_PVA_LOT_NO == pva_lot_no)
            if film_curve:
                query = query.filter(FmpDat.FMP_FILM_CURVE == film_curve)
            
            # データの集計
            results = query.all()
            
            total_sheets = sum(r.FMP_PROC_SHTS for r in results)
            primary_good = sum(r.FMP_PRM_GOOD_QTY for r in results)
            
            if total_sheets == 0:
                return {
                    'total_sheets': 0,
                    'primary_good': 0,
                    'primary_good_rate': 0,
                    'primary_defect_rates': {},
                    'secondary_defect_rates': {},
                    'grade_info': {}
                }
            
            # 一次検査の不良率計算（加工枚数を分母）
            primary_defect_rates = {
                'シワA': sum(r.FMP_WRINKLE_A for r in results) / total_sheets * 100,
                'シワB': sum(r.FMP_WRINKLE_B for r in results) / total_sheets * 100,
                '裂け': sum(r.FMP_TEAR for r in results) / total_sheets * 100,
                'ブツ': sum(r.FMP_FOREIGN for r in results) / total_sheets * 100,
                '繊維': sum(r.FMP_FIBER for r in results) / total_sheets * 100,
                'キズ': sum(r.FMP_SCRATCH for r in results) / total_sheets * 100,
                '穴': sum(r.FMP_HOLE for r in results) / total_sheets * 100,
                'その他': sum(r.FMP_PRM_OTHERS for r in results) / total_sheets * 100
            }
            
            if primary_good == 0:
                return {
                    'total_sheets': total_sheets,
                    'primary_good': 0,
                    'primary_good_rate': 0,
                    'primary_defect_rates': primary_defect_rates,
                    'secondary_defect_rates': {},
                    'grade_info': {}
                }
            
            # 二次検査の不良率計算（一次良品数を分母）
            secondary_defect_rates = {
                '色抜け': sum(r.FMP_CLR_FADE for r in results) / primary_good * 100,
                '色ムラ': sum(r.FMP_CLR_IRREG for r in results) / primary_good * 100,
                '染スジ': sum(r.FMP_DYE_STREAK for r in results) / primary_good * 100,
                '汚れ': sum(r.FMP_DIRT for r in results) / primary_good * 100,
                'その他': sum(r.FMP_OTHERS for r in results) / primary_good * 100
            }
            
            # 等級別情報の計算
            total_graded = sum(
                (r.FMP_GRADE_A or 0) + (r.FMP_GRADE_B or 0) + (r.FMP_GRADE_C or 0)
                for r in results
            )
            
            grade_info = {
                'A品': {
                    'quantity': sum(r.FMP_GRADE_A or 0 for r in results),
                    'rate': sum(r.FMP_GRADE_A or 0 for r in results) / total_graded * 100 if total_graded > 0 else 0
                },
                'B品': {
                    'quantity': sum(r.FMP_GRADE_B or 0 for r in results),
                    'rate': sum(r.FMP_GRADE_B or 0 for r in results) / total_graded * 100 if total_graded > 0 else 0
                },
                'C品': {
                    'quantity': sum(r.FMP_GRADE_C or 0 for r in results),
                    'rate': sum(r.FMP_GRADE_C or 0 for r in results) / total_graded * 100 if total_graded > 0 else 0
                }
            }
            
            # 収率の計算（FMP_GRADE_A + FMP_GRADE_B + FMP_GRADE_C）/ FMP_PROC_SHTS
            total_grade_a = sum(r.FMP_GRADE_A or 0 for r in results)
            total_grade_b = sum(r.FMP_GRADE_B or 0 for r in results)
            total_grade_c = sum(r.FMP_GRADE_C or 0 for r in results)
            total_good = total_grade_a + total_grade_b + total_grade_c
            good_rate = total_good / total_sheets * 100 if total_sheets > 0 else 0
            
            return {
                'total_sheets': total_sheets,
                'primary_good': primary_good,
                'primary_good_rate': good_rate,  # 正しい収率計算
                'primary_defect_rates': primary_defect_rates,
                'secondary_defect_rates': secondary_defect_rates,
                'grade_info': grade_info
            }
            
        except Exception as e:
            log_error(f'不良率分析中にエラーが発生しました: {str(e)}')
            return None

    @staticmethod
    def get_recent_defect_trend(days=10):
        """
        直近の不良率推移データを取得するメソッド
        
        Parameters:
            days (int): 取得する日数（デフォルト: 5日）
            
        Returns:
            list: 日付ごとの不良率データ
        """
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days-1)
            
            # 日付ごとのデータを取得
            daily_data = []
            current_date = start_date
            
            while current_date <= end_date:
                next_date = current_date + timedelta(days=1)
                
                # その日のデータを取得
                day_results = FmpDat.query.filter(
                    FmpDat.FMP_INSP_DATE >= current_date,
                    FmpDat.FMP_INSP_DATE < next_date
                ).all()
                
                if day_results:
                    total_sheets = sum(r.FMP_PROC_SHTS for r in day_results)
                    primary_good = sum(r.FMP_PRM_GOOD_QTY for r in day_results)
                    
                    if total_sheets > 0:
                        # 一次検査の不良率
                        primary_defect_rates = {
                            'シワA': sum(r.FMP_WRINKLE_A for r in day_results) / total_sheets * 100,
                            'シワB': sum(r.FMP_WRINKLE_B for r in day_results) / total_sheets * 100,
                            '裂け': sum(r.FMP_TEAR for r in day_results) / total_sheets * 100,
                            'ブツ': sum(r.FMP_FOREIGN for r in day_results) / total_sheets * 100,
                            '繊維': sum(r.FMP_FIBER for r in day_results) / total_sheets * 100,
                            'キズ': sum(r.FMP_SCRATCH for r in day_results) / total_sheets * 100,
                            '穴': sum(r.FMP_HOLE for r in day_results) / total_sheets * 100,
                            'その他': sum(r.FMP_PRM_OTHERS for r in day_results) / total_sheets * 100
                        }
                        
                        # 二次検査の不良率（一次良品数が0の場合は0%）
                        secondary_defect_rates = {
                            '色抜け': sum(r.FMP_CLR_FADE for r in day_results) / primary_good * 100 if primary_good > 0 else 0,
                            '色ムラ': sum(r.FMP_CLR_IRREG for r in day_results) / primary_good * 100 if primary_good > 0 else 0,
                            '染スジ': sum(r.FMP_DYE_STREAK for r in day_results) / primary_good * 100 if primary_good > 0 else 0,
                            '汚れ': sum(r.FMP_DIRT for r in day_results) / primary_good * 100 if primary_good > 0 else 0,
                            'その他': sum(r.FMP_OTHERS for r in day_results) / primary_good * 100 if primary_good > 0 else 0
                        }
                        
                        daily_data.append({
                            'date': current_date.strftime('%Y-%m-%d'),
                            'defect_rates': {**primary_defect_rates, **secondary_defect_rates}
                        })
                
                current_date = next_date
            
            return daily_data
            
        except Exception as e:
            log_error(f'不良率推移データの取得中にエラーが発生しました: {str(e)}')
            return []

    @staticmethod
    def import_from_csv(file_path, encoding='shift_jis'):
        """CSVファイルから膜加工データをインポート"""
        session = None
        try:
            session = get_db_session()
            
            # マスタデータを一度だけ取得
            color_map = {}
            color_query = text("SELECT KBN_ID, KBN_NM FROM KBN_MST WHERE KBN_TYP = 'MCLR'")
            try:
                for row in session.execute(color_query):
                    color_map[row.KBN_NM] = row.KBN_ID
            except Exception as e:
                log_error(f'色情報の取得中にエラーが発生しました: {str(e)}')
                raise
            
            film_curve_map = {}
            film_curve_query = text("SELECT KBN_ID, KBN_NM FROM KBN_MST WHERE KBN_TYP = 'MCRB'")
            try:
                for row in session.execute(film_curve_query):
                    film_curve_map[row.KBN_NM] = row.KBN_ID
            except Exception as e:
                log_error(f'膜カーブ情報の取得中にエラーが発生しました: {str(e)}')
                raise
            
            log_error(f'読み取り開始')
            
            # CSVファイルを読み込みながら処理
            records = []
            error_rows = []
            row_number = 0
            
            with open(file_path, 'r', encoding=encoding) as f:
                reader = csv.reader(f)
                
                for row in reader:
                    row_number += 1
                    try:
                        # 行が空でないことを確認
                        if not row or len(row) < 23:  # 必要な列数
                            error_rows.append(f"行 {row_number}: 列数が不足しています ({len(row)} 列)")
                            continue
                        
                        # データの検証
                        if not row[0]:  # 日付が空
                            error_rows.append(f"行 {row_number}: 日付が空です")
                            continue
                            
                        try:
                            parse_date(row[0])
                        except:
                            error_rows.append(f"行 {row_number}: 日付の形式が不正です")
                            continue
                        
                        # 色、膜カーブの検証
                        if row[2] and row[2] not in color_map:
                            error_rows.append(f"行 {row_number}: 色 '{row[2]}' が見つかりません")
                        if row[5] and row[5] not in film_curve_map:
                            error_rows.append(f"行 {row_number}: 膜カーブ '{row[5]}' が見つかりません")
                        
                        edit_row = {}
                        # データの変換と格納
                        edit_row['FMP_INSP_DATE'] = parse_date(row[0])
                        edit_row['FMP_PROC_DATE'] = parse_date(row[1])
                        edit_row['FMP_COLOR'] = color_map.get(row[2], None)
                        edit_row['FMP_PVA_LOT_NO'] = str_to_flt(row[3])
                        edit_row['FMP_BIKO'] = row[4]
                        edit_row['FMP_FILM_CURVE'] = film_curve_map.get(row[5], None)
                        edit_row['FMP_PROC_SHTS'] = str_to_flt(row[6])
                        edit_row['FMP_WRINKLE_A'] = str_to_flt(row[7])
                        edit_row['FMP_WRINKLE_B'] = str_to_flt(row[8])
                        edit_row['FMP_TEAR'] = str_to_flt(row[9])
                        edit_row['FMP_FOREIGN'] = str_to_flt(row[10])
                        edit_row['FMP_FIBER'] = str_to_flt(row[11])
                        edit_row['FMP_SCRATCH'] = str_to_flt(row[12])
                        edit_row['FMP_HOLE'] = str_to_flt(row[13])
                        edit_row['FMP_PRM_OTHERS'] = str_to_flt(row[14])
                        edit_row['FMP_PRM_GOOD_QTY'] = str_to_flt(row[15])
                        edit_row['FMP_CLR_FADE'] = str_to_flt(row[16])
                        edit_row['FMP_CLR_IRREG'] = str_to_flt(row[17])
                        edit_row['FMP_DYE_STREAK'] = str_to_flt(row[18])
                        edit_row['FMP_DIRT'] = str_to_flt(row[19])
                        edit_row['FMP_OTHERS'] = str_to_flt(row[20])
                        edit_row['FMP_GRADE_A'] = str_to_flt(row[21])
                        edit_row['FMP_GRADE_B'] = str_to_flt(row[22])
                        edit_row['FMP_GRADE_C'] = str_to_flt(row[23])
                        
                        existing = session.query(FmpDat).filter(
                            FmpDat.FMP_INSP_DATE == edit_row['FMP_INSP_DATE'],
                            FmpDat.FMP_PROC_DATE == edit_row['FMP_PROC_DATE'],
                            FmpDat.FMP_COLOR == edit_row['FMP_COLOR'],
                            FmpDat.FMP_PVA_LOT_NO == edit_row['FMP_PVA_LOT_NO'],
                            FmpDat.FMP_BIKO == edit_row['FMP_BIKO'],
                            FmpDat.FMP_FILM_CURVE == edit_row['FMP_FILM_CURVE']
                        ).first()
                        if existing:
                            for k, v in edit_row.items():
                                setattr(existing, k, v)
                        else:
                            session.add(FmpDat(**edit_row))
                        session.commit()
                    except Exception as e:
                        session.rollback()
                        error_rows.append(f"行 {row_number}: {str(e)}")
                        continue
            if error_rows:
                log_error("以下の行でエラーが発生しました:")
                for error in error_rows:
                    log_error(error)
                return False, '\n'.join(error_rows)
            return True, f'{row_number - len(error_rows)}件のデータをインポート/更新しました。'
        except Exception as e:
            log_error(f'FMP_DAT CSVファイルの読み込み中にエラーが発生しました: {str(e)}')
            return False, str(e)
        finally:
            if session:
                session.close()

class FngDat(db.Model):
    """膜不良データテーブルのSQLAlchemyモデル"""
    __tablename__ = 'FNG_DAT'
    
    FNG_LOT_NO = db.Column(db.String(16), primary_key=True, comment='ロットNo')
    FNG_NG_ID = db.Column(db.DECIMAL(1), primary_key=True, comment='不良項目')
    FNG_INS_QTY = db.Column(db.DECIMAL(4), comment='検査数')
    FNG_NG_QTY = db.Column(db.DECIMAL(4), comment='不良数')
    FNG_BIKO = db.Column(db.String(50), comment='備考')

    @staticmethod
    def get_by_lot_and_ng(lot_no, ng_id):
        """ロットNoと不良項目IDからデータを取得"""
        try:
            return FngDat.query.filter_by(
                FNG_LOT_NO=lot_no,
                FNG_NG_ID=ng_id
            ).first()
        except Exception as e:
            log_error(f'不良データの取得中にエラーが発生しました: {str(e)}')
            return None

    @staticmethod
    def exists(lot_no, ng_id):
        """データが存在するかどうかを確認"""
        try:
            return db.session.query(
                db.session.query(FngDat).filter_by(
                    FNG_LOT_NO=lot_no,
                    FNG_NG_ID=ng_id
                ).exists()
            ).scalar()
        except Exception as e:
            log_error(f'不良データの存在確認中にエラーが発生しました: {str(e)}')
            return False

    @staticmethod
    def get_color_trans_defects(start_date=None, end_date=None):
        """カラー不良と透過率不良のデータを取得"""
        try:
            # カラー不良のクエリ
            color_query = db.session.query(
                PrdRecordModel.PRR_LOT_NO.label('lot_no'),
                PrdRecordModel.PRR_R1_IN_DATE.label('r1_in_date'),
                PrdRecordModel.PRR_CHK_DT.label('check_date'),
                PrdMstModel.PRD_NM.label('product_name'),
                PrdRecordModel.PRR_FILM_DATE.label('film_date'),
                PrdMstModel.PRD_COLOR.label('film_color'),
                db.literal('カラー不良').label('defect_type'),
                PrdRecordModel.PRR_COLOR_DEF.label('defect_qty'),
                db.literal(1).label('ng_id'),
                FngDat.FNG_INS_QTY.label('ins_qty'),
                FngDat.FNG_NG_QTY.label('ng_qty'),
                FngDat.FNG_BIKO.label('biko'),
                db.case(
                    (FngDat.FNG_INS_QTY > 0,
                     db.cast(FngDat.FNG_NG_QTY * 100, db.Numeric(5, 1)) /
                     db.cast(FngDat.FNG_INS_QTY, db.Numeric(5, 1))),
                    else_=None
                ).label('defect_rate')
            ).join(
                PrdMstModel,
                PrdRecordModel.PRR_PRD_ID == PrdMstModel.PRD_ID
            ).outerjoin(
                FngDat,
                db.and_(
                    FngDat.FNG_LOT_NO == PrdRecordModel.PRR_LOT_NO,
                    FngDat.FNG_NG_ID == 1
                )
            ).filter(
                PrdRecordModel.PRR_COLOR_DEF > 0,
                PrdMstModel.PRD_COLOR != ""
            )

            # 透過率不良のクエリ
            trans_query = db.session.query(
                PrdRecordModel.PRR_LOT_NO.label('lot_no'),
                PrdRecordModel.PRR_R1_IN_DATE.label('r1_in_date'),
                PrdRecordModel.PRR_CHK_DT.label('check_date'),
                PrdMstModel.PRD_NM.label('product_name'),
                PrdRecordModel.PRR_FILM_DATE.label('film_date'),
                PrdMstModel.PRD_COLOR.label('film_color'),
                db.literal('透過率不良').label('defect_type'),
                PrdRecordModel.PRR_TRANS_DEF.label('defect_qty'),
                db.literal(2).label('ng_id'),
                FngDat.FNG_INS_QTY.label('ins_qty'),
                FngDat.FNG_NG_QTY.label('ng_qty'),
                FngDat.FNG_BIKO.label('biko'),
                db.case(
                    (FngDat.FNG_INS_QTY > 0,
                     db.cast(FngDat.FNG_NG_QTY * 100, db.Numeric(5, 1)) /
                     db.cast(FngDat.FNG_INS_QTY, db.Numeric(5, 1))),
                    else_=None
                ).label('defect_rate')
            ).join(
                PrdMstModel,
                PrdRecordModel.PRR_PRD_ID == PrdMstModel.PRD_ID
            ).outerjoin(
                FngDat,
                db.and_(
                    FngDat.FNG_LOT_NO == PrdRecordModel.PRR_LOT_NO,
                    FngDat.FNG_NG_ID == 2
                )
            ).filter(PrdRecordModel.PRR_TRANS_DEF > 0)

            # 日付フィルターの適用（R1注入日でフィルタリング）
            if start_date:
                color_query = color_query.filter(PrdRecordModel.PRR_R1_IN_DATE >= start_date)
                trans_query = trans_query.filter(PrdRecordModel.PRR_R1_IN_DATE >= start_date)
            if end_date:
                color_query = color_query.filter(PrdRecordModel.PRR_R1_IN_DATE <= end_date)
                trans_query = trans_query.filter(PrdRecordModel.PRR_R1_IN_DATE <= end_date)

            # UNIONして結果を取得
            query = color_query.union(trans_query).order_by(
                db.text('check_date desc'),
                db.text('lot_no'),
                db.text('defect_type')
            )
            results = query.all()
            
            # 結果の整形
            processed_results = []
            for r in results:
                processed_results.append({
                    'lot_no': r.lot_no,
                    'r1_in_date': r.r1_in_date,
                    'check_date': r.check_date,
                    'product_name': r.product_name,
                    'film_date': r.film_date,
                    'film_color': r.film_color,
                    'defect_type': r.defect_type,
                    'defect_qty': r.defect_qty,
                    'ng_id': r.ng_id,
                    'has_fng_data': r.ins_qty is not None,
                    'ins_qty': r.ins_qty,
                    'ng_qty': r.ng_qty,
                    'defect_rate': r.defect_rate,
                    'biko': r.biko
                })
            
            return processed_results
            
        except Exception as e:
            log_error(f'不良データの取得中にエラーが発生しました: {str(e)}')
            return []

class SpcDat(db.Model):
    """スピンコートデータテーブルのSQLAlchemyモデル"""
    __tablename__ = 'SPC_DAT'

    SPC_ID = db.Column(db.Integer, primary_key=True, autoincrement=True, comment='ID')
    SPC_COAT_DATE = db.Column(db.DateTime, comment='コート日')
    SPC_INSTR = db.Column(db.Numeric(3), comment='指図')
    SPC_BRANCH_NO = db.Column(db.String(4), comment='枝番')
    SPC_TYPE = db.Column(db.Numeric(2), comment='種類')
    SPC_NAME_1 = db.Column(db.Numeric(3), comment='呼び名1')
    SPC_NAME_2 = db.Column(db.String(30), comment='呼び名2')
    SPC_REF_IDX = db.Column(db.Numeric(4), comment='屈折率')
    SPC_COAT_COLOR = db.Column(db.Numeric(3), comment='コート色')
    SPC_MACHINE = db.Column(db.Numeric(2), comment='機')
    SPC_TIMES = db.Column(db.Numeric(2), comment='回数')
    SPC_SHEETS = db.Column(db.Numeric(5), comment='枚数')
    SPC_PRE_BLK_DUST = db.Column(db.Numeric(5), comment='硬化前黒ブツ')
    SPC_PRE_WHT_DUST = db.Column(db.Numeric(5), comment='硬化前白ブツ')
    SPC_PRE_EDGE_FAIL = db.Column(db.Numeric(5), comment='硬化前外周不良')
    SPC_PRE_COAT_FAIL = db.Column(db.Numeric(5), comment='硬化前コート不良')
    SPC_PRE_DARK_SPOT = db.Column(db.Numeric(5), comment='硬化前ダークスポット')
    SPC_PRE_SNAIL = db.Column(db.Numeric(5), comment='硬化前スネイル')
    SPC_PRE_MIST = db.Column(db.Numeric(5), comment='硬化前ミスト')
    SPC_PRE_WRINKLE = db.Column(db.Numeric(5), comment='硬化前シワ')
    SPC_PRE_BRRL_BUB = db.Column(db.Numeric(5), comment='硬化前バレル泡')
    SPC_PRE_STICK = db.Column(db.Numeric(5), comment='硬化前付着物')
    SPC_PRE_TRBL_FIL = db.Column(db.Numeric(5), comment='硬化前トラブル不')
    SPC_PRE_BASE_FIL = db.Column(db.Numeric(5), comment='硬化前基材不良')
    SPC_PRE_GOOD_QTY = db.Column(db.Numeric(5), comment='硬化前良品数')
    SPC_PRE_NOTE = db.Column(db.String(40), comment='硬化前備考')
    SPC_PST_INSP_DATE = db.Column(db.DateTime, comment='硬化後検査日')
    SPC_PST_SCRATCH = db.Column(db.Numeric(5), comment='硬化後キズ')
    SPC_PST_COAT_FIL = db.Column(db.Numeric(5), comment='硬化後コート不良')
    SPC_PST_SNAIL = db.Column(db.Numeric(5), comment='硬化後スネイル')
    SPC_PST_DARK_SPOT = db.Column(db.Numeric(5), comment='硬化後ダークスポット')
    SPC_PST_WRINKLE = db.Column(db.Numeric(5), comment='硬化後シワ')
    SPC_PST_BUBBLE = db.Column(db.Numeric(5), comment='硬化後泡')
    SPC_PST_EDGE_FAIL = db.Column(db.Numeric(5), comment='硬化後外周不良')
    SPC_PST_WHT_DUST = db.Column(db.Numeric(5), comment='硬化後白ブツ')
    SPC_PST_BLK_DUST = db.Column(db.Numeric(5), comment='硬化後黒ブツ')
    SPC_PST_STICK = db.Column(db.Numeric(5), comment='硬化後付着物')
    SPC_PST_PRM_STICK = db.Column(db.Numeric(5), comment='硬化後プライマー付着跡')
    SPC_PST_BASE_FAIL = db.Column(db.Numeric(5), comment='硬化後基材不良')
    SPC_PST_OTHERS = db.Column(db.Numeric(5), comment='硬化後その他')
    SPC_FNL_GD_QTY = db.Column(db.Numeric(5), comment='最終良品数')

    @staticmethod
    def import_from_csv(file_path, encoding='shift_jis'):
        """CSVファイルからスピンコートデータをインポート（8項目一致ならUPDATE、なければINSERT、区分マスタ変換あり）"""
        session = None
        try:
            session = get_db_session()
            # 区分マスタのマッピングを作成
            type_map = {}
            name1_map = {}
            color_map = {}
            # SPC_TYPE: KBN_TYP='SPKD'
            for row in session.execute(text("SELECT KBN_NM, KBN_ID FROM KBN_MST WHERE KBN_TYP = 'SPKD'")):
                type_map[row.KBN_NM] = row.KBN_ID
            # SPC_NAME_1: KBN_TYP='SPCN'
            for row in session.execute(text("SELECT KBN_NM, KBN_ID FROM KBN_MST WHERE KBN_TYP = 'SPNM'")):
                name1_map[row.KBN_NM] = row.KBN_ID
            # SPC_COAT_COLOR: KBN_TYP='SPCC'
            for row in session.execute(text("SELECT KBN_NM, KBN_ID FROM KBN_MST WHERE KBN_TYP = 'SPCL'")):
                color_map[row.KBN_NM] = row.KBN_ID

            error_rows = []
            row_number = 0
            import csv
            with open(file_path, 'r', encoding=encoding) as f:
                reader = csv.reader(f)
                next(reader, None)  # ヘッダー行スキップ
                for row in reader:
                    row_number += 1
                    try:
                        if not row or len(row) < 41:
                            error_rows.append(f"行 {row_number}: 列数が不足しています ({len(row)} 列)")
                            continue
                        edit_row = {}
                        edit_row['SPC_COAT_DATE'] = parse_date(row[0])
                        edit_row['SPC_INSTR'] = str_to_flt(row[1])
                        edit_row['SPC_BRANCH_NO'] = row[2]
                        # 区分マスタ変換
                        spc_type_val = row[3].strip() if row[3] is not None else ''
                        if spc_type_val == '':
                            spc_type = 0
                        else:
                            spc_type = type_map.get(spc_type_val)
                            if spc_type is None:
                                error_rows.append(f"行 {row_number}: SPC_TYPE区分変換失敗 [{spc_type_val}]")
                                continue
                        spc_name1_val = row[4].strip() if row[4] is not None else ''
                        if spc_name1_val == '':
                            spc_name1 = 0
                        else:
                            spc_name1 = name1_map.get(spc_name1_val)
                            if spc_name1 is None:
                                error_rows.append(f"行 {row_number}: SPC_NAME_1区分変換失敗 [{spc_name1_val}]")
                                continue
                        spc_color_val = row[7].strip() if row[7] is not None else ''
                        if spc_color_val == '':
                            spc_color = 0
                        else:
                            spc_color = color_map.get(spc_color_val)
                            if spc_color is None:
                                error_rows.append(f"行 {row_number}: SPC_COAT_COLOR区分変換失敗 [{spc_color_val}]")
                                continue
                        edit_row['SPC_TYPE'] = spc_type
                        edit_row['SPC_NAME_1'] = spc_name1
                        edit_row['SPC_NAME_2'] = row[5]
                        edit_row['SPC_REF_IDX'] = str_to_flt(row[6])
                        edit_row['SPC_COAT_COLOR'] = spc_color
                        edit_row['SPC_MACHINE'] = str_to_flt(row[8])
                        edit_row['SPC_TIMES'] = str_to_flt(row[9])
                        edit_row['SPC_SHEETS'] = str_to_flt(row[12])
                        edit_row['SPC_PRE_BLK_DUST'] = str_to_flt(row[13])
                        edit_row['SPC_PRE_WHT_DUST'] = str_to_flt(row[14])
                        edit_row['SPC_PRE_EDGE_FAIL'] = str_to_flt(row[15])
                        edit_row['SPC_PRE_COAT_FAIL'] = str_to_flt(row[16])
                        edit_row['SPC_PRE_DARK_SPOT'] = str_to_flt(row[17])
                        edit_row['SPC_PRE_SNAIL'] = str_to_flt(row[18])
                        edit_row['SPC_PRE_MIST'] = str_to_flt(row[19])
                        edit_row['SPC_PRE_WRINKLE'] = str_to_flt(row[20])
                        edit_row['SPC_PRE_BRRL_BUB'] = str_to_flt(row[21])
                        edit_row['SPC_PRE_STICK'] = str_to_flt(row[22])
                        edit_row['SPC_PRE_TRBL_FIL'] = str_to_flt(row[23])
                        edit_row['SPC_PRE_BASE_FIL'] = str_to_flt(row[24])
                        edit_row['SPC_PRE_GOOD_QTY'] = str_to_flt(row[25])
                        edit_row['SPC_PRE_NOTE'] = row[26]
                        edit_row['SPC_PST_INSP_DATE'] = parse_date(row[27])
                        edit_row['SPC_PST_SCRATCH'] = str_to_flt(row[28])
                        edit_row['SPC_PST_COAT_FIL'] = str_to_flt(row[29])
                        edit_row['SPC_PST_SNAIL'] = str_to_flt(row[30])
                        edit_row['SPC_PST_DARK_SPOT'] = str_to_flt(row[31])
                        edit_row['SPC_PST_WRINKLE'] = str_to_flt(row[32])
                        edit_row['SPC_PST_BUBBLE'] = str_to_flt(row[33])
                        edit_row['SPC_PST_EDGE_FAIL'] = str_to_flt(row[34])
                        edit_row['SPC_PST_WHT_DUST'] = str_to_flt(row[35])
                        edit_row['SPC_PST_BLK_DUST'] = str_to_flt(row[36])
                        edit_row['SPC_PST_STICK'] = str_to_flt(row[37])
                        edit_row['SPC_PST_PRM_STICK'] = str_to_flt(row[38])
                        edit_row['SPC_PST_BASE_FAIL'] = str_to_flt(row[39])
                        edit_row['SPC_PST_OTHERS'] = str_to_flt(row[40])
                        edit_row['SPC_FNL_GD_QTY'] = str_to_flt(row[41])
                        # 8項目一致で既存レコード検索
                        existing = session.query(SpcDat).filter(
                            SpcDat.SPC_COAT_DATE == edit_row['SPC_COAT_DATE'],
                            SpcDat.SPC_INSTR == edit_row['SPC_INSTR'],
                            SpcDat.SPC_BRANCH_NO == edit_row['SPC_BRANCH_NO'],
                            SpcDat.SPC_TYPE == edit_row['SPC_TYPE'],
                            SpcDat.SPC_NAME_1 == edit_row['SPC_NAME_1'],
                            SpcDat.SPC_REF_IDX == edit_row['SPC_REF_IDX'],
                            SpcDat.SPC_COAT_COLOR == edit_row['SPC_COAT_COLOR'],
                            SpcDat.SPC_TIMES == edit_row['SPC_TIMES']
                        ).first()
                        if existing:
                            for k, v in edit_row.items():
                                setattr(existing, k, v)
                        else:
                            session.add(SpcDat(**edit_row))
                        session.commit()
                    except Exception as e:
                        session.rollback()
                        error_rows.append(f"行 {row_number}: {str(e)}")
                        continue
            if error_rows:
                log_error("以下の行でエラーが発生しました:")
                for error in error_rows:
                    log_error(error)
                return False, '\n'.join(error_rows)
            return True, f'{row_number - len(error_rows)}件のデータをインポート/更新しました。'
        except Exception as e:
            log_error(f'SPC_DAT CSVファイルの読み込み中にエラーが発生しました: {str(e)}')
            return False, str(e)
        finally:
            if session:
                session.close()

    @staticmethod
    def get_defect_analysis(start_date=None, end_date=None, ct_type=None, color=None):
        """
        スピンコート不良率分析
        - SPC_SHEETS分母の硬化前不良率
        - SPC_PRE_GOOD_QTY分母の硬化後不良率
        Returns:
            dict: {
                'total_sheets': ...,
                'total_pre_good': ...,
                'pre_defect_rates': {項目名: 不良率, ...},
                'pst_defect_rates': {項目名: 不良率, ...}
            }
        """
        try:
            query = SpcDat.query
            if start_date:
                query = query.filter(SpcDat.SPC_COAT_DATE >= start_date)
            if end_date:
                query = query.filter(SpcDat.SPC_COAT_DATE <= end_date)
            if ct_type:
                query = query.filter(SpcDat.SPC_TYPE == ct_type)
            if color:
                query = query.filter(SpcDat.SPC_COAT_COLOR == color)
            results = query.all()

            # 分母
            total_sheets = sum(r.SPC_SHEETS or 0 for r in results)
            total_pre_good = sum(r.SPC_PRE_GOOD_QTY or 0 for r in results)

            # 硬化前不良項目
            pre_items = [
                ('SPC_PRE_BLK_DUST', '硬化前黒ブツ'),
                ('SPC_PRE_WHT_DUST', '硬化前白ブツ'),
                ('SPC_PRE_EDGE_FAIL', '硬化前外周不良'),
                ('SPC_PRE_COAT_FAIL', '硬化前コート不良'),
                ('SPC_PRE_DARK_SPOT', '硬化前ダークスポット'),
                ('SPC_PRE_SNAIL', '硬化前スネイル'),
                ('SPC_PRE_MIST', '硬化前ミスト'),
                ('SPC_PRE_WRINKLE', '硬化前シワ'),
                ('SPC_PRE_BRRL_BUB', '硬化前バレル泡'),
                ('SPC_PRE_STICK', '硬化前付着物'),
                ('SPC_PRE_TRBL_FIL', '硬化前トラブル不'),
                ('SPC_PRE_BASE_FIL', '硬化前基材不良'),
            ]
            # 硬化後不良項目
            pst_items = [
                ('SPC_PST_SCRATCH', '硬化後キズ'),
                ('SPC_PST_COAT_FIL', '硬化後コート不良'),
                ('SPC_PST_SNAIL', '硬化後スネイル'),
                ('SPC_PST_DARK_SPOT', '硬化後ダークスポット'),
                ('SPC_PST_WRINKLE', '硬化後シワ'),
                ('SPC_PST_BUBBLE', '硬化後泡'),
                ('SPC_PST_EDGE_FAIL', '硬化後外周不良'),
                ('SPC_PST_WHT_DUST', '硬化後白ブツ'),
                ('SPC_PST_BLK_DUST', '硬化後黒ブツ'),
                ('SPC_PST_STICK', '硬化後付着物'),
                ('SPC_PST_PRM_STICK', '硬化後プライマー付着跡'),
                ('SPC_PST_BASE_FAIL', '硬化後基材不良'),
                ('SPC_PST_OTHERS', '硬化後その他'),
            ]

            pre_defect_rates = {}
            for attr, label in pre_items:
                total = sum(getattr(r, attr) or 0 for r in results)
                rate = (total / total_sheets * 100) if total_sheets else 0
                pre_defect_rates[label] = round(rate, 2)

            pst_defect_rates = {}
            for attr, label in pst_items:
                total = sum(getattr(r, attr) or 0 for r in results)
                rate = (total / total_pre_good * 100) if total_pre_good else 0
                pst_defect_rates[label] = round(rate, 2)

            return {
                'total_sheets': total_sheets,
                'total_pre_good': total_pre_good,
                'pre_defect_rates': pre_defect_rates,
                'pst_defect_rates': pst_defect_rates
            }
        except Exception as e:
            log_error(f'SPC不良率分析中にエラーが発生しました: {str(e)}')
            return None

    @staticmethod
    def get_daily_defect_summary_by_times(start_date=None, end_date=None, ct_type=None, color=None):
        """
        SPC_TIMESごとに日別の合計不良率・各不良項目不良率を返す
        Returns:
            dict: {SPC_TIMES: {日付: { 'pre_total_rate':..., 'pst_total_rate':..., 'pre_items':..., 'pst_items':..., 'total_sheets':..., 'total_pre_good':... }, ...}, ...}
        """
        try:
            pre_items = [
                'SPC_PRE_BLK_DUST', 'SPC_PRE_WHT_DUST', 'SPC_PRE_EDGE_FAIL', 'SPC_PRE_COAT_FAIL',
                'SPC_PRE_DARK_SPOT', 'SPC_PRE_SNAIL', 'SPC_PRE_MIST', 'SPC_PRE_WRINKLE',
                'SPC_PRE_BRRL_BUB', 'SPC_PRE_STICK', 'SPC_PRE_TRBL_FIL', 'SPC_PRE_BASE_FIL'
            ]
            pst_items = [
                'SPC_PST_SCRATCH', 'SPC_PST_COAT_FIL', 'SPC_PST_SNAIL', 'SPC_PST_DARK_SPOT',
                'SPC_PST_WRINKLE', 'SPC_PST_BUBBLE', 'SPC_PST_EDGE_FAIL', 'SPC_PST_WHT_DUST',
                'SPC_PST_BLK_DUST', 'SPC_PST_STICK', 'SPC_PST_PRM_STICK', 'SPC_PST_BASE_FAIL', 'SPC_PST_OTHERS'
            ]
            query = SpcDat.query
            if start_date:
                query = query.filter(SpcDat.SPC_COAT_DATE >= start_date)
            if end_date:
                query = query.filter(SpcDat.SPC_COAT_DATE <= end_date)
            if ct_type:
                query = query.filter(SpcDat.SPC_TYPE == ct_type)
            if color:
                query = query.filter(SpcDat.SPC_COAT_COLOR == color)
            results = query.all()
            from collections import defaultdict
            summary = defaultdict(lambda: defaultdict(lambda: {
                'total_sheets': 0, 'total_pre_good': 0,
                'pre_items': defaultdict(float), 'pst_items': defaultdict(float),
                'pre_total_defect': 0, 'pst_total_defect': 0
            }))
            for r in results:
                date_str = r.SPC_COAT_DATE.strftime('%Y-%m-%d') if r.SPC_COAT_DATE else None
                if not date_str:
                    continue
                times = r.SPC_TIMES
                sheets = r.SPC_SHEETS or 0
                pre_good = r.SPC_PRE_GOOD_QTY or 0
                summary[times][date_str]['total_sheets'] += sheets
                summary[times][date_str]['total_pre_good'] += pre_good
                for item in pre_items:
                    val = float(getattr(r, item, 0) or 0)
                    summary[times][date_str]['pre_items'][item] += val
                    summary[times][date_str]['pre_total_defect'] += val
                for item in pst_items:
                    val = float(getattr(r, item, 0) or 0)
                    summary[times][date_str]['pst_items'][item] += val
                    summary[times][date_str]['pst_total_defect'] += val
            # 不良率計算
            result = {}
            for times, daily in summary.items():
                result[times] = {}
                for date_str, v in daily.items():
                    total_sheets = float(v['total_sheets'])
                    total_pre_good = float(v['total_pre_good'])
                    pre_items_rate = {}
                    pst_items_rate = {}
                    for item, val in v['pre_items'].items():
                        pre_items_rate[item] = round((val / total_sheets * 100) if total_sheets else 0, 2)
                    for item, val in v['pst_items'].items():
                        pst_items_rate[item] = round((val / total_pre_good * 100) if total_pre_good else 0, 2)
                    pre_total_rate = round((v['pre_total_defect'] / total_sheets * 100) if total_sheets else 0, 2)
                    pst_total_rate = round((v['pst_total_defect'] / total_pre_good * 100) if total_pre_good else 0, 2)
                    result[times][date_str] = {
                        'pre_total_rate': pre_total_rate,
                        'pst_total_rate': pst_total_rate,
                        'pre_items': pre_items_rate,
                        'pst_items': pst_items_rate,
                        'total_sheets': total_sheets,
                        'total_pre_good': total_pre_good
                    }
            return result
        except Exception as e:
            log_error(f'SPC_TIMESごと日別不良率集計中にエラーが発生しました: {str(e)}')
            return None

class HdcDat(db.Model):
    """ハードコートデータテーブルのSQLAlchemyモデル"""
    __tablename__ = 'HDC_DAT'

    HDC_ID = db.Column(db.Integer, primary_key=True, autoincrement=True, comment='ID')
    HDC_COAT_DATE = db.Column(db.DateTime, comment='コート日')
    HDC_TIMES = db.Column(db.Float, comment='回数')
    HDC_TYPE = db.Column(db.String(10), comment='種類')
    HDC_BASE = db.Column(db.Float, comment='ベース')
    HDC_ADD_PWR = db.Column(db.Float, comment='加入度数')
    HDC_LR = db.Column(db.String(1), comment='L/R')
    HDC_COLOR = db.Column(db.String(5), comment='色')
    HDC_COAT_CNT = db.Column(db.Float, comment='コート数')
    HDC_PRE_FOREIGN = db.Column(db.Float, comment='硬化前ブツ')
    HDC_PRE_DROP = db.Column(db.Float, comment='硬化前タレ')
    HDC_PRE_CHIP = db.Column(db.Float, comment='硬化前カケ')
    HDC_PRE_STREAK = db.Column(db.Float, comment='硬化前スジ')
    HDC_PRE_OTHERS = db.Column(db.Float, comment='硬化前その他')
    HDC_TRS_BASE_FAIL = db.Column(db.Float, comment='透過基材不良')
    HDC_TRS_FOREIGN = db.Column(db.Float, comment='透過ブツ')
    HDC_TRS_INCL = db.Column(db.Float, comment='透過イブツ')
    HDC_TRS_SCRATCH = db.Column(db.Float, comment='透過キズ')
    HDC_TRS_COAT_FAIL = db.Column(db.Float, comment='透過コート不良')
    HDC_TRS_DROP = db.Column(db.Float, comment='透過タレ')
    HDC_TRS_STREAK = db.Column(db.Float, comment='透過スジ')
    HDC_TRS_DIRT = db.Column(db.Float, comment='透過汚れ')
    HDC_TRS_CHIP = db.Column(db.Float, comment='透過カケ')
    HDC_PRJ_BASE = db.Column(db.Float, comment='投影基材')
    HDC_PRJ_FOREIGN = db.Column(db.Float, comment='投影ブツ')
    HDC_PRJ_DUST = db.Column(db.Float, comment='投影ごみ')
    HDC_PRJ_SCRATCH = db.Column(db.Float, comment='投影キズ')
    HDC_PRJ_DROP = db.Column(db.Float, comment='投影タレ')
    HDC_PRJ_CHIP = db.Column(db.Float, comment='投影カケ')
    HDC_PRJ_STREAK = db.Column(db.Float, comment='投影スジ')
    HDC_PASS_QTY = db.Column(db.Float, comment='合格数')

    @staticmethod
    def import_from_csv(file_path, encoding='shift_jis'):
        """CSVファイルからハードコートデータをインポート（区分マスタ変換あり、7項目一致ならUPDATE、なければINSERT）"""
        session = None
        try:
            session = get_db_session()
            # 区分マスタのマッピングを作成
            type_map = {}
            color_map = {}
            # HDC_TYPE: KBN_TYP='HDCT'
            for row in session.execute(text("SELECT KBN_NM, KBN_ID FROM KBN_MST WHERE KBN_TYP = 'HCKD'")):
                type_map[row.KBN_NM] = row.KBN_ID
            # HDC_COLOR: KBN_TYP='HDCC'
            for row in session.execute(text("SELECT KBN_NM, KBN_ID FROM KBN_MST WHERE KBN_TYP = 'HCCL'")):
                color_map[row.KBN_NM] = row.KBN_ID

            error_rows = []
            row_number = 0
            with open(file_path, 'r', encoding=encoding) as f:
                reader = csv.reader(f)
                next(reader, None)  # ヘッダー行スキップ
                for row in reader:
                    row_number += 1
                    try:
                        if not row or len(row) < 37:
                            error_rows.append(f"行 {row_number}: 列数が不足しています ({len(row)} 列)")
                            continue
                        edit_row = {}
                        edit_row['HDC_COAT_DATE'] = parse_date(row[0])
                        edit_row['HDC_TIMES'] = str_to_flt(row[7])
                        # 区分マスタ変換
                        hdc_type_val = row[8].strip() if row[8] is not None else ''
                        if hdc_type_val == '':
                            hdc_type = 0
                        else:
                            hdc_type = type_map.get(hdc_type_val)
                            if hdc_type is None:
                                error_rows.append(f"行 {row_number}: HDC_TYPE区分変換失敗 [{hdc_type_val}]")
                                continue
                        hdc_color_val = row[13].strip() if row[13] is not None else ''
                        if hdc_color_val == '':
                            hdc_color = 0
                        else:
                            hdc_color = color_map.get(hdc_color_val)
                            if hdc_color is None:
                                error_rows.append(f"行 {row_number}: HDC_COLOR区分変換失敗 [{hdc_color_val}]")
                                continue
                        edit_row['HDC_TYPE'] = hdc_type
                        edit_row['HDC_BASE'] = str_to_flt(row[10])
                        edit_row['HDC_ADD_PWR'] = str_to_flt(row[11])
                        edit_row['HDC_LR'] = row[12]
                        edit_row['HDC_COLOR'] = hdc_color
                        edit_row['HDC_COAT_CNT'] = str_to_flt(row[14])
                        edit_row['HDC_PRE_FOREIGN'] = str_to_flt(row[15])
                        edit_row['HDC_PRE_DROP'] = str_to_flt(row[16])
                        edit_row['HDC_PRE_CHIP'] = str_to_flt(row[17])
                        edit_row['HDC_PRE_STREAK'] = str_to_flt(row[18])
                        edit_row['HDC_PRE_OTHERS'] = str_to_flt(row[19])
                        edit_row['HDC_TRS_BASE_FAIL'] = str_to_flt(row[20])
                        edit_row['HDC_TRS_FOREIGN'] = str_to_flt(row[21])
                        edit_row['HDC_TRS_INCL'] = str_to_flt(row[22])
                        edit_row['HDC_TRS_SCRATCH'] = str_to_flt(row[23])
                        edit_row['HDC_TRS_COAT_FAIL'] = str_to_flt(row[24])
                        edit_row['HDC_TRS_DROP'] = str_to_flt(row[25])
                        edit_row['HDC_TRS_STREAK'] = str_to_flt(row[26])
                        edit_row['HDC_TRS_DIRT'] = str_to_flt(row[27])
                        edit_row['HDC_TRS_CHIP'] = str_to_flt(row[28])
                        edit_row['HDC_PRJ_BASE'] = str_to_flt(row[29])
                        edit_row['HDC_PRJ_FOREIGN'] = str_to_flt(row[30])
                        edit_row['HDC_PRJ_DUST'] = str_to_flt(row[31])
                        edit_row['HDC_PRJ_SCRATCH'] = str_to_flt(row[32])
                        edit_row['HDC_PRJ_DROP'] = str_to_flt(row[33])
                        edit_row['HDC_PRJ_CHIP'] = str_to_flt(row[34])
                        edit_row['HDC_PRJ_STREAK'] = str_to_flt(row[35])
                        edit_row['HDC_PASS_QTY'] = str_to_flt(row[36])
                        # 7項目一致で既存レコード検索
                        existing = session.query(HdcDat).filter(
                            HdcDat.HDC_COAT_DATE == edit_row['HDC_COAT_DATE'],
                            HdcDat.HDC_TIMES == edit_row['HDC_TIMES'],
                            HdcDat.HDC_TYPE == edit_row['HDC_TYPE'],
                            HdcDat.HDC_BASE == edit_row['HDC_BASE'],
                            HdcDat.HDC_ADD_PWR == edit_row['HDC_ADD_PWR'],
                            HdcDat.HDC_LR == edit_row['HDC_LR'],
                            HdcDat.HDC_COLOR == edit_row['HDC_COLOR']
                        ).first()
                        if existing:
                            for k, v in edit_row.items():
                                setattr(existing, k, v)
                        else:
                            session.add(HdcDat(**edit_row))
                        session.commit()
                    except Exception as e:
                        session.rollback()
                        error_rows.append(f"行 {row_number}: {str(e)}")
                        continue
            if error_rows:
                log_error("以下の行でエラーが発生しました:")
                for error in error_rows:
                    log_error(error)
                return False, '\n'.join(error_rows)
            return True, f'{row_number - len(error_rows)}件のデータをインポート/更新しました。'
        except Exception as e:
            log_error(f'HDC_DAT CSVファイルの読み込み中にエラーが発生しました: {str(e)}')
            return False, str(e)
        finally:
            if session:
                session.close()

    @staticmethod
    def get_defect_trend(item='HDC_PRE_FOREIGN', start_date=None, end_date=None, ct_type=None, color=None):
        """
        指定不良項目について、日ごと・HDC_TIMESごとの不良率推移を返す
        Returns:
            dict: {日付: {HDC_TIMES: 不良率, ...}, ...}
        """
        try:
            query = HdcDat.query
            if start_date:
                query = query.filter(HdcDat.HDC_COAT_DATE >= start_date)
            if end_date:
                query = query.filter(HdcDat.HDC_COAT_DATE <= end_date)
            if ct_type:
                query = query.filter(HdcDat.HDC_TYPE == ct_type)
            if color:
                query = query.filter(HdcDat.HDC_COLOR == color)
            results = query.all()

            # 日付・HDC_TIMESごとに集計
            from collections import defaultdict
            trend = defaultdict(lambda: defaultdict(lambda: {'defect': 0, 'cnt': 0}))
            for r in results:
                date_str = r.HDC_COAT_DATE.strftime('%Y-%m-%d') if r.HDC_COAT_DATE else None
                if not date_str:
                    continue
                times = r.HDC_TIMES
                cnt = r.HDC_COAT_CNT or 0
                defect = getattr(r, item, 0) or 0
                trend[date_str][times]['defect'] += defect
                trend[date_str][times]['cnt'] += cnt

            # 不良率計算
            trend_rates = {}
            for date_str, times_dict in trend.items():
                trend_rates[date_str] = {}
                for times, v in times_dict.items():
                    rate = (v['defect'] / v['cnt'] * 100) if v['cnt'] else 0
                    trend_rates[date_str][times] = round(rate, 2)
            return trend_rates
        except Exception as e:
            log_error(f'HDC不良率推移分析中にエラーが発生しました: {str(e)}')
            return None

    @staticmethod
    def get_daily_defect_summary(start_date=None, end_date=None, ct_type=None, color=None):
        """
        日ごとに全不良項目合計不良率と各不良項目不良率を返す
        Returns:
            dict: {日付: { 'total_rate': 合計不良率, 'items': {項目名: 不良率, ...}, 'total_cnt': コート数 }, ...}
        """
        try:
            defect_items = [
                'HDC_PRE_FOREIGN', 'HDC_PRE_DROP', 'HDC_PRE_CHIP', 'HDC_PRE_STREAK', 'HDC_PRE_OTHERS',
                'HDC_TRS_BASE_FAIL', 'HDC_TRS_FOREIGN', 'HDC_TRS_INCL', 'HDC_TRS_SCRATCH', 'HDC_TRS_COAT_FAIL',
                'HDC_TRS_DROP', 'HDC_TRS_STREAK', 'HDC_TRS_DIRT', 'HDC_TRS_CHIP', 'HDC_PRJ_BASE',
                'HDC_PRJ_FOREIGN', 'HDC_PRJ_DUST', 'HDC_PRJ_SCRATCH', 'HDC_PRJ_DROP', 'HDC_PRJ_CHIP', 'HDC_PRJ_STREAK'
            ]
            query = HdcDat.query
            if start_date:
                query = query.filter(HdcDat.HDC_COAT_DATE >= start_date)
            if end_date:
                query = query.filter(HdcDat.HDC_COAT_DATE <= end_date)
            if ct_type:
                query = query.filter(HdcDat.HDC_TYPE == ct_type)
            if color:
                query = query.filter(HdcDat.HDC_COLOR == color)
            results = query.all()
            from collections import defaultdict
            daily = defaultdict(lambda: {'total_cnt': 0, 'items': defaultdict(float), 'total_defect': 0})
            for r in results:
                date_str = r.HDC_COAT_DATE.strftime('%Y-%m-%d') if r.HDC_COAT_DATE else None
                if not date_str:
                    continue
                cnt = r.HDC_COAT_CNT or 0
                daily[date_str]['total_cnt'] += cnt
                for item in defect_items:
                    val = getattr(r, item, 0) or 0
                    daily[date_str]['items'][item] += val
                    daily[date_str]['total_defect'] += val
            # 不良率計算
            summary = {}
            for date_str, v in daily.items():
                total_cnt = v['total_cnt']
                total_defect = v['total_defect']
                items_rate = {}
                for item, val in v['items'].items():
                    items_rate[item] = round((val / total_cnt * 100) if total_cnt else 0, 2)
                total_rate = round((total_defect / total_cnt * 100) if total_cnt else 0, 2)
                summary[date_str] = {
                    'total_rate': total_rate,
                    'items': items_rate,
                    'total_cnt': total_cnt
                }
            return summary
        except Exception as e:
            log_error(f'HDC日別不良率集計中にエラーが発生しました: {str(e)}')
            return None

    @staticmethod
    def get_daily_defect_summary_by_times(start_date=None, end_date=None, ct_type=None, color=None):
        """
        HDC_TIMESごとに日別の合計不良率・各不良項目不良率を返す
        Returns:
            dict: {HDC_TIMES: {日付: { 'total_rate':..., 'items':..., 'total_cnt':... }, ...}, ...}
        """
        try:
            defect_items = [
                'HDC_PRE_FOREIGN', 'HDC_PRE_DROP', 'HDC_PRE_CHIP', 'HDC_PRE_STREAK', 'HDC_PRE_OTHERS',
                'HDC_TRS_BASE_FAIL', 'HDC_TRS_FOREIGN', 'HDC_TRS_INCL', 'HDC_TRS_SCRATCH', 'HDC_TRS_COAT_FAIL',
                'HDC_TRS_DROP', 'HDC_TRS_STREAK', 'HDC_TRS_DIRT', 'HDC_TRS_CHIP', 'HDC_PRJ_BASE',
                'HDC_PRJ_FOREIGN', 'HDC_PRJ_DUST', 'HDC_PRJ_SCRATCH', 'HDC_PRJ_DROP', 'HDC_PRJ_CHIP', 'HDC_PRJ_STREAK'
            ]
            query = HdcDat.query
            if start_date:
                query = query.filter(HdcDat.HDC_COAT_DATE >= start_date)
            if end_date:
                query = query.filter(HdcDat.HDC_COAT_DATE <= end_date)
            if ct_type:
                query = query.filter(HdcDat.HDC_TYPE == ct_type)
            if color:
                query = query.filter(HdcDat.HDC_COLOR == color)
            results = query.all()
            from collections import defaultdict
            summary = defaultdict(lambda: defaultdict(lambda: {'total_cnt': 0, 'items': defaultdict(float), 'total_defect': 0}))
            for r in results:
                date_str = r.HDC_COAT_DATE.strftime('%Y-%m-%d') if r.HDC_COAT_DATE else None
                if not date_str:
                    continue
                times = r.HDC_TIMES
                cnt = r.HDC_COAT_CNT or 0
                summary[times][date_str]['total_cnt'] += cnt
                for item in defect_items:
                    val = getattr(r, item, 0) or 0
                    summary[times][date_str]['items'][item] += val
                    summary[times][date_str]['total_defect'] += val
            # 不良率計算
            result = {}
            for times, daily in summary.items():
                result[times] = {}
                for date_str, v in daily.items():
                    total_cnt = v['total_cnt']
                    total_defect = v['total_defect']
                    items_rate = {}
                    for item, val in v['items'].items():
                        items_rate[item] = round((val / total_cnt * 100) if total_cnt else 0, 2)
                    total_rate = round((total_defect / total_cnt * 100) if total_cnt else 0, 2)
                    result[times][date_str] = {
                        'total_rate': total_rate,
                        'items': items_rate,
                        'total_cnt': total_cnt
                    }
            return result
        except Exception as e:
            log_error(f'HDC_TIMESごと日別不良率集計中にエラーが発生しました: {str(e)}')
            return None


