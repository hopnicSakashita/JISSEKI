import csv
import io
from datetime import datetime, timedelta
import locale
from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app as app, jsonify

from app.forms import FmcDatForm, SetMstForm
from app.ishida_models import FmcDat
from .models import WorkerModel, db, PrdRecordModel, SetMst, NoteDatModel, NansDatModel
from .master_models import MnoMstModel
from .master_models import KbnMst, PrdMstModel
from .utils import log_error
import tempfile
import os
from flask_login import login_required, current_user
from sqlalchemy import Integer, case, func, text
import uuid
import base64

main = Blueprint('main', __name__)

@main.route('/')
@main.route('/index')
@login_required
def index():
    return redirect(url_for('main.note_dat_list'))

def save_image_from_base64(base64_data):
    """Base64データから画像を保存する関数"""
    try:
        print('save_image_from_base64開始')
        
        # Base64データの基本チェック
        if not base64_data:
            print('Base64データが空です')
            return None
        
        print(f'Base64データ長: {len(base64_data)}')
        
        # Base64データからヘッダーを削除
        if ',' in base64_data:
            base64_data = base64_data.split(',')[1]
            print('Base64ヘッダーを削除しました')
        
        # Base64データをデコード
        image_data = base64.b64decode(base64_data)
        print(f'デコード後のデータサイズ: {len(image_data)} bytes')
        
        # 保存先ディレクトリを作成
        base_dir = os.path.dirname(os.path.abspath(__file__))
        save_dir = os.path.join(base_dir, 'static', 'images', 'notes')
        print(f'保存先ディレクトリ: {save_dir}')
        
        if not os.path.exists(save_dir):
            os.makedirs(save_dir, exist_ok=True)
            print('ディレクトリを作成しました')
        
        # ファイル名を生成（日付ベース）
        from datetime import datetime
        date_str = datetime.now().strftime('%y%m%d')
        filename = f"note_{date_str}_{uuid.uuid4().hex[:8]}.jpg"
        
        print(f'生成されたファイル名: {filename}')
        
        # ファイルパス
        file_path = os.path.join(save_dir, filename)
        print(f'保存先パス: {file_path}')
        
        # ファイルに保存
        with open(file_path, 'wb') as f:
            f.write(image_data)
        
        # ファイルが実際に保存されたかチェック
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            print(f'ファイル保存完了: サイズ={file_size} bytes')
            return filename
        else:
            print('ファイル保存失敗: ファイルが存在しません')
            return None
        
    except Exception as e:
        print(f'画像保存エラー: {str(e)}')
        import traceback
        print(f'エラー詳細: {traceback.format_exc()}')
        return None

def delete_image_file(filename):
    """画像ファイルを削除する関数"""
    try:
        if filename:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            file_path = os.path.join(base_dir, 'static', 'images', 'notes', filename)
            if os.path.exists(file_path):
                os.remove(file_path)
                return True
    except Exception as e:
        print(f'画像ファイル削除エラー: {str(e)}')
    return False

@main.route('/results', methods=['GET'])
@login_required
def results():
    # 検索パラメータの有無をチェック
    has_search_params = any([
        request.args.get('date_from'),
        request.args.get('date_to'),
        request.args.get('date_from2'),
        request.args.get('date_to2'),
        request.args.get('injector'),
        request.args.get('injector2')
    ])
    
    # 検索パラメータがある場合のみデータを取得
    if has_search_params:
        # クエリの構築
        query = PrdRecordModel.query
        
        # 検索条件の適用
        if request.args.get('date_from'):
            query = query.filter(PrdRecordModel.PRR_R1_IN_DATE >= datetime.strptime(request.args.get('date_from'), '%Y-%m-%d'))
        if request.args.get('date_to'):
            query = query.filter(PrdRecordModel.PRR_R1_IN_DATE <= datetime.strptime(request.args.get('date_to'), '%Y-%m-%d'))
        if request.args.get('date_from2'):
            query = query.filter(PrdRecordModel.PRR_R2_DATE >= datetime.strptime(request.args.get('date_from2'), '%Y-%m-%d'))
        if request.args.get('date_to2'):
            query = query.filter(PrdRecordModel.PRR_R2_DATE <= datetime.strptime(request.args.get('date_to2'), '%Y-%m-%d'))
        if request.args.get('injector'):
            query = query.filter(PrdRecordModel.PRR_R1_INJECT == request.args.get('injector'))
        if request.args.get('injector2'):
            query = query.filter(PrdRecordModel.PRR_R2_INJECT == request.args.get('injector2'))
        
        # 日付の降順でソート
        query = query.order_by(PrdRecordModel.PRR_R1_IN_DATE.desc())
        
        # 結果の取得
        performances = query.all()
    else:
        # 検索パラメータがない場合は空のリストを返す
        performances = []
    
    # workerの取得
    workers = WorkerModel.query.all()
    
    csv_import_time = SetMst.get_csv_import_time()  
    
    return render_template('results.html', performances=performances, workers=workers, csv_import_time=csv_import_time)

@main.route('/results2', methods=['GET'])
@login_required
def results2():
    # モノマー種別マスタを取得
    mono_list = MnoMstModel.get_all()
    
    # 検索パラメータの有無をチェック
    has_search_params = any([
        request.args.get('date_from'),
        request.args.get('date_to'),
        request.args.get('date_from2'),
        request.args.get('date_to2'),
        request.args.get('film_date_from'),
        request.args.get('film_date_to'),
        request.args.get('prd_id'),
        request.args.get('prd_nm'),
        request.args.get('mono_syu'),
        request.args.get('lot_no'),
        request.args.get('prd_color')
    ])
    
    # 検索パラメータがある場合のみデータを取得
    if has_search_params:
        # クエリの構築
        query = PrdRecordModel.query
        
        # 検索条件の適用
        if request.args.get('date_from'):
            query = query.filter(PrdRecordModel.PRR_R1_IN_DATE >= datetime.strptime(request.args.get('date_from'), '%Y-%m-%d'))
        if request.args.get('date_to'):
            query = query.filter(PrdRecordModel.PRR_R1_IN_DATE <= datetime.strptime(request.args.get('date_to'), '%Y-%m-%d'))
        if request.args.get('date_from2'):
            query = query.filter(PrdRecordModel.PRR_CHK_DT >= datetime.strptime(request.args.get('date_from2'), '%Y-%m-%d'))
        if request.args.get('date_to2'):
            query = query.filter(PrdRecordModel.PRR_CHK_DT <= datetime.strptime(request.args.get('date_to2'), '%Y-%m-%d'))
        if request.args.get('film_date_from'):
            query = query.filter(PrdRecordModel.PRR_FILM_DATE >= datetime.strptime(request.args.get('film_date_from'), '%Y-%m-%d'))
        if request.args.get('film_date_to'):
            query = query.filter(PrdRecordModel.PRR_FILM_DATE <= datetime.strptime(request.args.get('film_date_to'), '%Y-%m-%d'))
        if request.args.get('prd_id'):
            query = query.filter(PrdRecordModel.PRR_PRD_ID == request.args.get('prd_id'))
        if request.args.get('prd_nm'):
            query = query.join(PrdMstModel, PrdRecordModel.PRR_PRD_ID == PrdMstModel.PRD_ID)
            query = query.filter(PrdMstModel.PRD_NM.like(f"%{request.args.get('prd_nm')}%"))
        if request.args.get('mono_syu'):
            query = query.filter(PrdRecordModel.PRR_MONO_SYU == request.args.get('mono_syu'))
        if request.args.get('lot_no'):
            query = query.filter(PrdRecordModel.PRR_LOT_NO.like(f"%{request.args.get('lot_no')}%"))
        # 膜カラーでの絞り込み（部分一致）
        if request.args.get('prd_color'):
            query = query.join(PrdMstModel, PrdRecordModel.PRR_PRD_ID == PrdMstModel.PRD_ID)
            query = query.filter(PrdMstModel.PRD_COLOR.like(f"%{request.args.get('prd_color')}%"))
        # 日付の降順でソート
        query = query.order_by(PrdRecordModel.PRR_R1_IN_DATE.desc())
        
        # 結果の取得
        performances = query.all()
    else:
        # 検索パラメータがない場合は空のリストを返す
        performances = []
    
    # workerの取得
    workers = WorkerModel.query.all()
    
    csv_import_time = SetMst.get_csv_import_time()
    
    return render_template('results2.html', performances=performances, workers=workers, csv_import_time=csv_import_time, mono_list=mono_list)

@main.route('/progress')
@login_required
def progress():
    # モノマー種別マスタを取得
    mono_list = MnoMstModel.get_all()
    
    # 検索条件の取得
    r1_in_date = request.args.get('r1_in_date')
    r1_in_date2 = request.args.get('r1_in_date2')
    mono_syu = request.args.get('mono_syu')
    
    # クエリの構築
    query = db.session.query(
        MnoMstModel.MNO_SYU,
        MnoMstModel.MNO_NM,
        func.count(PrdRecordModel.PRR_LOT_NO).label('total_count'),
        func.sum(case((PrdRecordModel.PRR_R1_INJECT.isnot(None), 1), else_=0)).label('r1_inject_count'),
        func.sum(case((PrdRecordModel.PRR_R2_INJECT.isnot(None), 1), else_=0)).label('r2_inject_count'),
        func.sum(case((PrdRecordModel.PRR_RELEASE_BY.isnot(None), 1), else_=0)).label('release_count'),
        func.sum(case((PrdRecordModel.PRR_ANNEAL_BY.isnot(None), 1), else_=0)).label('anneal_count'),
        func.sum(case((PrdRecordModel.PRR_CHK1_BY.isnot(None), 1), else_=0)).label('chk1_count'),
        func.sum(case((PrdRecordModel.PRR_CHK2_BY.isnot(None), 1), else_=0)).label('chk2_count'),
        func.sum(case((PrdRecordModel.PRR_CHK3_BY.isnot(None), 1), else_=0)).label('chk3_count')
    ).join(
        PrdRecordModel,
        MnoMstModel.MNO_SYU == PrdRecordModel.PRR_MONO_SYU,
        isouter=True
    )
    
    # 検索条件の適用
    if r1_in_date:
        query = query.filter(func.date(PrdRecordModel.PRR_R1_IN_DATE) >= r1_in_date)
    if r1_in_date2:
        query = query.filter(func.date(PrdRecordModel.PRR_R1_IN_DATE) <= r1_in_date2)
    if mono_syu:
        query = query.filter(MnoMstModel.MNO_SYU == mono_syu)
    
    # グループ化
    query = query.group_by(MnoMstModel.MNO_SYU, MnoMstModel.MNO_NM)
    
    # 結果の取得
    results = query.all()
    
    # 進捗データの整形
    progress_data = []
    for result in results:
        total_count = result.total_count or 0
        progress_data.append({
            'mono_nm': result.MNO_NM,
            'total_count': total_count,
            'r1_inject_count': result.r1_inject_count or 0,
            'r2_inject_count': result.r2_inject_count or 0,
            'release_count': result.release_count or 0,
            'anneal_count': result.anneal_count or 0,
            'chk1_count': result.chk1_count or 0,
            'chk2_count': result.chk2_count or 0,
            'chk3_count': result.chk3_count or 0,
            'r1_inject': result.r1_inject_count == total_count and total_count > 0,
            'r2_inject': result.r2_inject_count == total_count and total_count > 0,
            'release': result.release_count == total_count and total_count > 0,
            'anneal': result.anneal_count == total_count and total_count > 0,
            'chk1': result.chk1_count == total_count and total_count > 0,
            'chk2': result.chk2_count == total_count and total_count > 0,
            'chk3': result.chk3_count == total_count and total_count > 0
        })
    
    return render_template('progress.html', mono_list=mono_list, progress_data=progress_data, csv_import_time=SetMst.get_csv_import_time())

@main.route('/api/high_defect_rate_data')
@login_required
def high_defect_rate_data():
    """不良率が高い不良データをJSON形式で返す"""
    # フィルターパラメータ取得
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    date_type = request.args.get('date_type', 'r2')  # r2: R2注入日, chk: 検査日
    
    # 不良項目の定義
    defect_items = {
        'roll_miss': '巻きミス',
        'leak': 'モレ',
        'film_pull': '膜ひっぱり',
        'crack': 'ワレ',
        'tear': 'チギレ',
        'peel': 'ハガレ',
        'chip': 'カケ',
        'poly_crk': '重合ワレ',
        'mold_scr': '型キズ',
        'lens_scr': 'レンズキズ',
        'r1_bubble': 'R1泡',
        'r2_bubble': 'R2泡',
        'defect': 'ブツ',
        'elution': '溶出',
        'haze': 'モヤ',
        'curl': 'カール',
        'film_float': '膜浮き',
        'r1_defect': 'R1不良',
        'film_ng': '膜不良',
        'foreign': 'イブツ',
        'cut_waste': 'カットくず',
        'fiber': 'センイ',
        'mold_dirt': 'モールド汚れ',
        'film_dirt': '膜汚れ',
        'axis_1st': '片軸',
        'stripe_1st': '脈理',
        'edge_defect': 'コバスリ不良',
        'wash_drop': '洗浄落下',
        'unknown': '不明',
        'other_1': 'その他',
        'ecc_defect': '偏心不良',
        'drop': '落下',
        'count_err': '員数違い',
        'suction': '吸い込み',
        'axis_def': '軸不良',
        'color_def': 'カラー不良',
        'trans_def': '透過率不良',
        'curve_def': 'カーブ不良',
        'cen_th_def': '中心厚不良',
        'diam_def': '径不良',
        'r1_th_def': 'R1厚み不良'
    }
    
    # クエリビルド
    base_query = db.session.query(
        PrdRecordModel.PRR_MONO_SYU,
        func.sum(PrdRecordModel.PRR_INJECT_QTY).label('total_inject'),
        # 各不良項目の合計
        func.sum(PrdRecordModel.PRR_ROLL_MISS).label('roll_miss'),
        func.sum(PrdRecordModel.PRR_LEAK).label('leak'),
        func.sum(PrdRecordModel.PRR_FILM_PULL).label('film_pull'),
        func.sum(PrdRecordModel.PRR_CRACK).label('crack'),
        func.sum(PrdRecordModel.PRR_TEAR + PrdRecordModel.PRR_TEAR_RLS).label('tear'),
        func.sum(PrdRecordModel.PRR_PEEL + PrdRecordModel.PRR_PEEL_2ND).label('peel'),
        func.sum(PrdRecordModel.PRR_CHIP).label('chip'),
        func.sum(PrdRecordModel.PRR_POLY_CRK).label('poly_crk'),
        func.sum(PrdRecordModel.PRR_MOLD_SCR + PrdRecordModel.PRR_MOLD_2ND).label('mold_scr'),
        func.sum(PrdRecordModel.PRR_LENS_SCR).label('lens_scr'),
        func.sum(PrdRecordModel.PRR_R1_BUBBLE + PrdRecordModel.PRR_R1_BUB_CHK).label('r1_bubble'),
        func.sum(PrdRecordModel.PRR_R2_BUBBLE + PrdRecordModel.PRR_R2_BUB_REK).label('r2_bubble'),
        func.sum(PrdRecordModel.PRR_DEFECT + PrdRecordModel.PRR_DEFECT_2ND).label('defect'),
        func.sum(PrdRecordModel.PRR_ELUTION).label('elution'),
        func.sum(PrdRecordModel.PRR_HAZE).label('haze'),
        func.sum(PrdRecordModel.PRR_CURL + PrdRecordModel.PRR_CURL_INS).label('curl'),
        func.sum(PrdRecordModel.PRR_FILM_FLOAT + PrdRecordModel.PRR_FILM_FLT_CK + PrdRecordModel.PRR_FILM_3RD).label('film_float'),
        func.sum(PrdRecordModel.PRR_R1_DEFECT).label('r1_defect'),
        func.sum(PrdRecordModel.PRR_FILM_NG + PrdRecordModel.PRR_FILM_NG_CK + PrdRecordModel.PRR_FILM_2ND).label('film_ng'),
        func.sum(PrdRecordModel.PRR_FOREIGN).label('foreign'),
        func.sum(PrdRecordModel.PRR_CUT_WASTE).label('cut_waste'),
        func.sum(PrdRecordModel.PRR_FIBER).label('fiber'),
        func.sum(PrdRecordModel.PRR_MOLD_DIRT).label('mold_dirt'),
        func.sum(PrdRecordModel.PRR_FILM_DIRT + PrdRecordModel.PRR_EDGE_DEF_3).label('film_dirt'),
        func.sum(PrdRecordModel.PRR_AXIS_1ST + PrdRecordModel.PRR_AXIS_3RD).label('axis_1st'),
        func.sum(PrdRecordModel.PRR_STRIPE_1ST + PrdRecordModel.PRR_STRIPE_2ND).label('stripe_1st'),
        func.sum(PrdRecordModel.PRR_EDGE_DEFECT).label('edge_defect'),
        func.sum(PrdRecordModel.PRR_WASH_DROP).label('wash_drop'),
        func.sum(PrdRecordModel.PRR_UNKNOWN).label('unknown'),
        func.sum(PrdRecordModel.PRR_OTHER_1 + PrdRecordModel.PRR_OTHER_2 + PrdRecordModel.PRR_OTHER_3RD + PrdRecordModel.PRR_OTHER_2ND + PrdRecordModel.PRR_OTHER_1ST).label('other_1'),                func.sum(PrdRecordModel.PRR_OTHER_2).label('other_2'),
        func.sum(PrdRecordModel.PRR_ECC_DEFECT + PrdRecordModel.PRR_ECC_3RD + PrdRecordModel.PRR_ECC_1ST).label('ecc_defect'),
        func.sum(PrdRecordModel.PRR_DROP).label('drop'),
        func.sum(PrdRecordModel.PRR_COUNT_ERR).label('count_err'),
        func.sum(PrdRecordModel.PRR_SUCTION).label('suction'),
        func.sum(PrdRecordModel.PRR_AXIS_DEF).label('axis_def'),
        func.sum(PrdRecordModel.PRR_COLOR_DEF).label('color_def'),
        func.sum(PrdRecordModel.PRR_TRANS_DEF).label('trans_def'),
        func.sum(PrdRecordModel.PRR_CURVE_DEF).label('curve_def'),
        func.sum(PrdRecordModel.PRR_CEN_TH_DEF).label('cen_th_def'),
        func.sum(PrdRecordModel.PRR_DIAM_DEF).label('diam_def'),
        func.sum(PrdRecordModel.PRR_R1_TH_DEF).label('r1_th_def')
    )
    
    # フィルター適用
    if date_from:
        if date_type == 'r2':
            base_query = base_query.filter(PrdRecordModel.PRR_R2_DATE >= date_from)
        else:
            base_query = base_query.filter(PrdRecordModel.PRR_CHK_DT >= date_from)
    if date_to:
        if date_type == 'r2':
            base_query = base_query.filter(PrdRecordModel.PRR_R2_DATE <= date_to)
        else:
            base_query = base_query.filter(PrdRecordModel.PRR_CHK_DT <= date_to)
    
    # グループ化して結果取得
    results = base_query.group_by(
        PrdRecordModel.PRR_MONO_SYU
    ).all()
    
    # モノマーマスタの取得
    mono_mst = {m.MNO_SYU: {'name': m.MNO_NM, 'target': float(m.MNO_TARGET) if m.MNO_TARGET else None} 
                for m in MnoMstModel.get_all()}
    
    # 結果の整形
    defect_data = []
    for row in results:
        mono_syu = row.PRR_MONO_SYU or 'Unknown'
        total_inject = float(row.total_inject or 0)
        
        # 各不良項目のデータを格納
        for defect_key, defect_label in defect_items.items():
            defect_count = float(getattr(row, defect_key) or 0)
            if defect_count > 0:  # 不良数が0より大きい場合のみ追加
                defect_rate = round((defect_count / total_inject * 100), 2) if total_inject > 0 else 0
                defect_data.append({
                    'mono_syu': mono_syu,
                    'mono_name': mono_mst.get(mono_syu, {}).get('name', 'Unknown'),
                    'target': mono_mst.get(mono_syu, {}).get('target'),
                    'defect_type': defect_key,
                    'defect_label': defect_label,
                    'total_inject': total_inject,
                    'defect_count': defect_count,
                    'defect_rate': defect_rate
                })
    
    # 不良率でソート（降順）
    defect_data.sort(key=lambda x: x['defect_rate'], reverse=True)
    
    return jsonify({
        'defect_data': defect_data,
        'mono_mst': mono_mst,
        'csv_import_time': SetMst.get_csv_import_time()
    })

@main.route('/api/mono_syu_slide_data')
@login_required
def mono_syu_slide_data():
    """モノマー種別スライド表示用のデータを取得"""
    # パラメータ取得
    five_days_ago = (datetime.now() - timedelta(days=5)).date()
    
    try:
        # モノマーマスタの取得
        mono_mst = {m.MNO_SYU: {'name': m.MNO_NM, 'target': float(m.MNO_TARGET) if m.MNO_TARGET else None} 
                    for m in MnoMstModel.get_all()}
        
        if not mono_mst:
            log_error("モノマーマスタデータがありません")
            return jsonify({'error': 'モノマーマスタデータがありません'}), 404
        
        # モノマー種別ごとの良品率（期間全体）の取得
        good_rate_query = db.session.query(
            PrdRecordModel.PRR_MONO_SYU,
            func.sum(PrdRecordModel.PRR_INJECT_QTY).label('total_inject'),
            func.sum(
            PrdRecordModel.PRR_ROLL_MISS + 
            PrdRecordModel.PRR_R1_BUB_CHK + 
            PrdRecordModel.PRR_CURL_INS + 
            PrdRecordModel.PRR_FILM_FLT_CK + 
            PrdRecordModel.PRR_LEAK + 
            PrdRecordModel.PRR_FILM_PULL + 
            PrdRecordModel.PRR_FILM_NG_CK + 
            PrdRecordModel.PRR_R2_BUB_REK + 
            PrdRecordModel.PRR_CRACK + 
            PrdRecordModel.PRR_TEAR_RLS + 
            PrdRecordModel.PRR_TEAR + 
            PrdRecordModel.PRR_PEEL + 
            PrdRecordModel.PRR_CHIP + 
            PrdRecordModel.PRR_POLY_CRK + 
            PrdRecordModel.PRR_MOLD_SCR + 
            PrdRecordModel.PRR_LENS_SCR + 
            PrdRecordModel.PRR_R1_BUBBLE + 
            PrdRecordModel.PRR_R2_BUBBLE + 
            PrdRecordModel.PRR_DEFECT + 
            PrdRecordModel.PRR_ELUTION + 
            PrdRecordModel.PRR_HAZE + 
            PrdRecordModel.PRR_CURL + 
            PrdRecordModel.PRR_FILM_FLOAT + 
            PrdRecordModel.PRR_R1_DEFECT + 
            PrdRecordModel.PRR_FILM_NG + 
            PrdRecordModel.PRR_FOREIGN + 
            PrdRecordModel.PRR_CUT_WASTE + 
            PrdRecordModel.PRR_FIBER + 
            PrdRecordModel.PRR_MOLD_DIRT + 
            PrdRecordModel.PRR_FILM_DIRT + 
            PrdRecordModel.PRR_AXIS_1ST + 
            PrdRecordModel.PRR_STRIPE_1ST + 
            PrdRecordModel.PRR_EDGE_DEFECT + 
            PrdRecordModel.PRR_ECC_1ST + 
            PrdRecordModel.PRR_WASH_DROP + 
            PrdRecordModel.PRR_UNKNOWN + 
            PrdRecordModel.PRR_OTHER_1 + 
            PrdRecordModel.PRR_OTHER_2 + 
            PrdRecordModel.PRR_ECC_DEFECT + 
            PrdRecordModel.PRR_DROP + 
            PrdRecordModel.PRR_COUNT_ERR + 
            PrdRecordModel.PRR_OTHER_1ST + 
            PrdRecordModel.PRR_PEEL_2ND + 
            PrdRecordModel.PRR_STRIPE_2ND + 
            PrdRecordModel.PRR_SUCTION + 
            PrdRecordModel.PRR_MOLD_2ND + 
            PrdRecordModel.PRR_FILM_2ND + 
            PrdRecordModel.PRR_DEFECT_2ND + 
            PrdRecordModel.PRR_OTHER_2ND + 
            PrdRecordModel.PRR_AXIS_DEF + 
            PrdRecordModel.PRR_FILM_3RD + 
            PrdRecordModel.PRR_COLOR_DEF + 
            PrdRecordModel.PRR_TRANS_DEF + 
            PrdRecordModel.PRR_CURVE_DEF + 
            PrdRecordModel.PRR_CEN_TH_DEF + 
            PrdRecordModel.PRR_DIAM_DEF + 
            PrdRecordModel.PRR_R1_TH_DEF + 
            PrdRecordModel.PRR_ECC_3RD + 
            PrdRecordModel.PRR_EDGE_DEF_3 + 
            PrdRecordModel.PRR_AXIS_3RD + 
            PrdRecordModel.PRR_OTHER_3RD
            ).label('total_defect')
        )
        
        # フィルター適用
        good_rate_query = good_rate_query.filter(PrdRecordModel.PRR_CHK_DT >= five_days_ago)
        
        good_rate_query = good_rate_query.filter(PrdRecordModel.PRR_CHK3_BY.isnot(None))
        
        # クエリをSQL文字列としてログ出力
        sql_str = str(good_rate_query.statement.compile(
            compile_kwargs={"literal_binds": True}))
        log_error(f"実行クエリ: {sql_str}")
        
        # グループ化
        good_rate_results = good_rate_query.group_by(
            PrdRecordModel.PRR_MONO_SYU
        ).all()
        
        if not good_rate_results:
            log_error("指定期間内のデータがありません")
            return jsonify({
                'mono_mst': mono_mst,
                'good_rates': {},
                'defect_items': {}
            })
        
        # 良品率データの整形
        good_rates = {}
        for row in good_rate_results:
            mono_syu = row.PRR_MONO_SYU
            if not mono_syu:
                continue
                
            total_inject = float(row.total_inject or 0)
            total_defect = float(row.total_defect or 0)
            
            if total_inject > 0:
                defect_rate = round((total_defect / total_inject) * 100, 2)
                good_rate = 100 - defect_rate
            else:
                defect_rate = 0
                good_rate = 100
                
            good_rates[mono_syu] = {
                'total_inject': total_inject,
                'total_defect': total_defect,
                'defect_rate': defect_rate,
                'good_rate': good_rate
            }
        
        # 各モノマー種の上位不良項目を取得
        defect_items_data = {}
        
        # モノマー種ごとに処理
        for mono_syu in mono_mst.keys():
            
            # 不良項目の日本語名マッピング
            defect_labels = {
                'roll_miss': '巻きミス',
                'leak': 'モレ',
                'film_pull': '膜ひっぱり',
                'crack': 'ワレ',
                'tear': 'チギレ',
                'peel': 'ハガレ',
                'chip': 'カケ',
                'poly_crk': '重合ワレ',
                'mold_scr': '型キズ',
                'lens_scr': 'レンズキズ',
                'r1_bubble': 'R1泡',
                'r2_bubble': 'R2泡',
                'defect': 'ブツ',
                'elution': '溶出',
                'haze': 'モヤ',
                'curl': 'カール',
                'film_float': '膜浮き',
                'r1_defect': 'R1不良',
                'film_ng': '膜不良',
                'foreign': 'イブツ',
                'cut_waste': 'カットくず',
                'fiber': 'センイ',
                'mold_dirt': 'モールド汚れ',
                'film_dirt': '膜汚れ',
                'axis_1st': '片軸',
                'stripe_1st': '脈理',
                'edge_defect': 'コバスリ不良',
                'wash_drop': '洗浄落下',
                'unknown': '不明',
                'other_1': 'その他',
                'ecc_defect': '偏心不良',
                'drop': '落下',
                'count_err': '員数違い',
                'suction': '吸い込み',
                'axis_def': '軸不良',
                'color_def': 'カラー不良',
                'trans_def': '透過率不良',
                'curve_def': 'カーブ不良',
                'cen_th_def': '中心厚不良',
                'diam_def': '径不良',
                'r1_th_def': 'R1厚み不良'
            }

            # 不良項目集計クエリ
            defect_query = db.session.query(
                # 各不良項目のカラム名と日本語名のマッピング
                func.sum(PrdRecordModel.PRR_ROLL_MISS).label('roll_miss'),
                func.sum(PrdRecordModel.PRR_LEAK).label('leak'),
                func.sum(PrdRecordModel.PRR_FILM_PULL).label('film_pull'),
                func.sum(PrdRecordModel.PRR_CRACK).label('crack'),
                func.sum(PrdRecordModel.PRR_TEAR + PrdRecordModel.PRR_TEAR_RLS).label('tear'),
                func.sum(PrdRecordModel.PRR_PEEL + PrdRecordModel.PRR_PEEL_2ND).label('peel'),
                func.sum(PrdRecordModel.PRR_CHIP).label('chip'),
                func.sum(PrdRecordModel.PRR_POLY_CRK).label('poly_crk'),
                func.sum(PrdRecordModel.PRR_MOLD_SCR + PrdRecordModel.PRR_MOLD_2ND).label('mold_scr'),
                func.sum(PrdRecordModel.PRR_LENS_SCR).label('lens_scr'),
                func.sum(PrdRecordModel.PRR_R1_BUBBLE + PrdRecordModel.PRR_R1_BUB_CHK).label('r1_bubble'),
                func.sum(PrdRecordModel.PRR_R2_BUBBLE + PrdRecordModel.PRR_R2_BUB_REK).label('r2_bubble'),
                func.sum(PrdRecordModel.PRR_DEFECT + PrdRecordModel.PRR_DEFECT_2ND).label('defect'),
                func.sum(PrdRecordModel.PRR_ELUTION).label('elution'),
                func.sum(PrdRecordModel.PRR_HAZE).label('haze'),
                func.sum(PrdRecordModel.PRR_CURL + PrdRecordModel.PRR_CURL_INS).label('curl'),
                func.sum(PrdRecordModel.PRR_FILM_FLOAT + PrdRecordModel.PRR_FILM_FLT_CK + PrdRecordModel.PRR_FILM_3RD).label('film_float'),
                func.sum(PrdRecordModel.PRR_R1_DEFECT).label('r1_defect'),
                func.sum(PrdRecordModel.PRR_FILM_NG + PrdRecordModel.PRR_FILM_NG_CK + PrdRecordModel.PRR_FILM_2ND).label('film_ng'),
                func.sum(PrdRecordModel.PRR_FOREIGN).label('foreign'),
                func.sum(PrdRecordModel.PRR_CUT_WASTE).label('cut_waste'),
                func.sum(PrdRecordModel.PRR_FIBER).label('fiber'),
                func.sum(PrdRecordModel.PRR_MOLD_DIRT).label('mold_dirt'),
                func.sum(PrdRecordModel.PRR_FILM_DIRT + PrdRecordModel.PRR_EDGE_DEF_3).label('film_dirt'),
                func.sum(PrdRecordModel.PRR_AXIS_1ST + PrdRecordModel.PRR_AXIS_3RD).label('axis_1st'),
                func.sum(PrdRecordModel.PRR_STRIPE_1ST + PrdRecordModel.PRR_STRIPE_2ND).label('stripe_1st'),
                func.sum(PrdRecordModel.PRR_EDGE_DEFECT).label('edge_defect'),
                func.sum(PrdRecordModel.PRR_WASH_DROP).label('wash_drop'),
                func.sum(PrdRecordModel.PRR_UNKNOWN).label('unknown'),
                func.sum(PrdRecordModel.PRR_OTHER_1 + PrdRecordModel.PRR_OTHER_2 + PrdRecordModel.PRR_OTHER_3RD + PrdRecordModel.PRR_OTHER_2ND + PrdRecordModel.PRR_OTHER_1ST).label('other_1'),                func.sum(PrdRecordModel.PRR_OTHER_2).label('other_2'),
                func.sum(PrdRecordModel.PRR_ECC_DEFECT + PrdRecordModel.PRR_ECC_3RD + PrdRecordModel.PRR_ECC_1ST).label('ecc_defect'),
                func.sum(PrdRecordModel.PRR_DROP).label('drop'),
                func.sum(PrdRecordModel.PRR_COUNT_ERR).label('count_err'),
                func.sum(PrdRecordModel.PRR_SUCTION).label('suction'),
                func.sum(PrdRecordModel.PRR_AXIS_DEF).label('axis_def'),
                func.sum(PrdRecordModel.PRR_COLOR_DEF).label('color_def'),
                func.sum(PrdRecordModel.PRR_TRANS_DEF).label('trans_def'),
                func.sum(PrdRecordModel.PRR_CURVE_DEF).label('curve_def'),
                func.sum(PrdRecordModel.PRR_CEN_TH_DEF).label('cen_th_def'),
                func.sum(PrdRecordModel.PRR_DIAM_DEF).label('diam_def'),
                func.sum(PrdRecordModel.PRR_R1_TH_DEF).label('r1_th_def')
            ).filter(PrdRecordModel.PRR_MONO_SYU == mono_syu)
            
            # フィルター適用
            defect_query = defect_query.filter(PrdRecordModel.PRR_CHK_DT >= five_days_ago)
            
            defect_query = defect_query.filter(PrdRecordModel.PRR_CHK3_BY.isnot(None))
            
            # 結果取得
            defect_result = defect_query.first()
            
            if defect_result:
                
                # 不良項目データの収集
                defect_items = []
                
                for key in defect_labels.keys():
                    value = getattr(defect_result, key, 0)
                    if value and value > 0:
                        defect_items.append({
                            'code': key,
                            'name': defect_labels[key],
                            'count': int(value)
                        })
                
                # 不良数の降順でソート
                defect_items.sort(key=lambda x: x['count'], reverse=True)
                
                # 上位10項目を保存
                defect_items_data[mono_syu] = defect_items[:10]
        
        # 結果をJSONで返す
        # 直近2週間のモノマー種別ごとの良品率推移を取得
        
        # モノマー種別ごとの日別データを取得
        daily_rates = {}
        mono_dates = {}
        
        for mono_syu in mono_mst.keys():
            # 日別の注入数と不良数を取得
            daily_query = db.session.query(
                func.date(PrdRecordModel.PRR_CHK_DT).label('date'),
                func.sum(PrdRecordModel.PRR_INJECT_QTY).label('total_inject'),
                func.sum(PrdRecordModel.PRR_ROLL_MISS).label('roll_miss'),
                func.sum(PrdRecordModel.PRR_LEAK).label('leak'),
                func.sum(PrdRecordModel.PRR_FILM_PULL).label('film_pull'),
                func.sum(PrdRecordModel.PRR_CRACK).label('crack'),
                func.sum(PrdRecordModel.PRR_TEAR + PrdRecordModel.PRR_TEAR_RLS).label('tear'),
                func.sum(PrdRecordModel.PRR_PEEL + PrdRecordModel.PRR_PEEL_2ND).label('peel'),
                func.sum(PrdRecordModel.PRR_CHIP).label('chip'),
                func.sum(PrdRecordModel.PRR_POLY_CRK).label('poly_crk'),
                func.sum(PrdRecordModel.PRR_MOLD_SCR + PrdRecordModel.PRR_MOLD_2ND).label('mold_scr'),
                func.sum(PrdRecordModel.PRR_LENS_SCR).label('lens_scr'),
                func.sum(PrdRecordModel.PRR_R1_BUBBLE + PrdRecordModel.PRR_R1_BUB_CHK).label('r1_bubble'),
                func.sum(PrdRecordModel.PRR_R2_BUBBLE + PrdRecordModel.PRR_R2_BUB_REK).label('r2_bubble'),
                func.sum(PrdRecordModel.PRR_DEFECT + PrdRecordModel.PRR_DEFECT_2ND).label('defect'),
                func.sum(PrdRecordModel.PRR_ELUTION).label('elution'),
                func.sum(PrdRecordModel.PRR_HAZE).label('haze'),
                func.sum(PrdRecordModel.PRR_CURL + PrdRecordModel.PRR_CURL_INS).label('curl'),
                func.sum(PrdRecordModel.PRR_FILM_FLOAT + PrdRecordModel.PRR_FILM_FLT_CK + PrdRecordModel.PRR_FILM_3RD).label('film_float'),
                func.sum(PrdRecordModel.PRR_R1_DEFECT).label('r1_defect'),
                func.sum(PrdRecordModel.PRR_FILM_NG + PrdRecordModel.PRR_FILM_NG_CK + PrdRecordModel.PRR_FILM_2ND).label('film_ng'),
                func.sum(PrdRecordModel.PRR_FOREIGN).label('foreign'),
                func.sum(PrdRecordModel.PRR_CUT_WASTE).label('cut_waste'),
                func.sum(PrdRecordModel.PRR_FIBER).label('fiber'),
                func.sum(PrdRecordModel.PRR_MOLD_DIRT).label('mold_dirt'),
                func.sum(PrdRecordModel.PRR_FILM_DIRT + PrdRecordModel.PRR_EDGE_DEF_3).label('film_dirt'),
                func.sum(PrdRecordModel.PRR_AXIS_1ST + PrdRecordModel.PRR_AXIS_3RD).label('axis_1st'),
                func.sum(PrdRecordModel.PRR_STRIPE_1ST + PrdRecordModel.PRR_STRIPE_2ND).label('stripe_1st'),
                func.sum(PrdRecordModel.PRR_EDGE_DEFECT).label('edge_defect'),
                func.sum(PrdRecordModel.PRR_WASH_DROP).label('wash_drop'),
                func.sum(PrdRecordModel.PRR_UNKNOWN).label('unknown'),
                func.sum(PrdRecordModel.PRR_OTHER_1 + PrdRecordModel.PRR_OTHER_2 + PrdRecordModel.PRR_OTHER_3RD + PrdRecordModel.PRR_OTHER_2ND + PrdRecordModel.PRR_OTHER_1ST).label('other_1'),                func.sum(PrdRecordModel.PRR_OTHER_2).label('other_2'),
                func.sum(PrdRecordModel.PRR_ECC_DEFECT + PrdRecordModel.PRR_ECC_3RD + PrdRecordModel.PRR_ECC_1ST).label('ecc_defect'),
                func.sum(PrdRecordModel.PRR_DROP).label('drop'),
                func.sum(PrdRecordModel.PRR_COUNT_ERR).label('count_err'),
                func.sum(PrdRecordModel.PRR_SUCTION).label('suction'),
                func.sum(PrdRecordModel.PRR_AXIS_DEF).label('axis_def'),
                func.sum(PrdRecordModel.PRR_COLOR_DEF).label('color_def'),
                func.sum(PrdRecordModel.PRR_TRANS_DEF).label('trans_def'),
                func.sum(PrdRecordModel.PRR_CURVE_DEF).label('curve_def'),
                func.sum(PrdRecordModel.PRR_CEN_TH_DEF).label('cen_th_def'),
                func.sum(PrdRecordModel.PRR_DIAM_DEF).label('diam_def'),
                func.sum(PrdRecordModel.PRR_R1_TH_DEF).label('r1_th_def')
            ).filter(
                PrdRecordModel.PRR_MONO_SYU == mono_syu,
                PrdRecordModel.PRR_CHK_DT >= five_days_ago,
                PrdRecordModel.PRR_CHK3_BY.isnot(None)
            ).group_by(
                func.date(PrdRecordModel.PRR_CHK_DT)
            ).order_by(
                func.date(PrdRecordModel.PRR_CHK_DT)
            ).all()
            
            locale.setlocale(locale.LC_TIME, 'ja_JP.UTF-8')
            # 日別の良品率を計算
            daily_data = []
            dates = []
            for row in daily_query:
                date_str = row.date.strftime('%m月%d日') + row.date.strftime('(%a)')
                if date_str not in dates:
                    dates.append(date_str)

                defect_data = {}
                if row.total_inject > 0:
                    # 各不良項目のデータを格納
                    defect_data[date_str] = {
                        'roll_miss': round((float(row.roll_miss or 0) / float(row.total_inject or 0) * 100), 2),
                        'leak': round((float(row.leak or 0) / float(row.total_inject or 0) * 100), 2),
                        'film_pull': round((float(row.film_pull or 0) / float(row.total_inject or 0) * 100), 2),
                        'crack': round((float(row.crack or 0) / float(row.total_inject or 0) * 100), 2),
                        'tear': round((float(row.tear or 0) / float(row.total_inject or 0) * 100), 2),
                        'peel': round((float(row.peel or 0) / float(row.total_inject or 0) * 100), 2),
                        'chip': round((float(row.chip or 0) / float(row.total_inject or 0) * 100), 2),
                        'poly_crk': round((float(row.poly_crk or 0) / float(row.total_inject or 0) * 100), 2),
                        'mold_scr': round((float(row.mold_scr or 0) / float(row.total_inject or 0) * 100), 2),
                        'lens_scr': round((float(row.lens_scr or 0) / float(row.total_inject or 0) * 100), 2),
                        'r1_bubble': round((float(row.r1_bubble or 0) / float(row.total_inject or 0) * 100), 2),
                        'r2_bubble': round((float(row.r2_bubble or 0) / float(row.total_inject or 0) * 100), 2),
                        'defect': round((float(row.defect or 0) / float(row.total_inject or 0) * 100), 2),
                        'elution': round((float(row.elution or 0) / float(row.total_inject or 0) * 100), 2),
                        'haze': round((float(row.haze or 0) / float(row.total_inject or 0) * 100), 2),
                        'curl': round((float(row.curl or 0) / float(row.total_inject or 0) * 100), 2),
                        'film_float': round((float(row.film_float or 0) / float(row.total_inject or 0) * 100), 2),
                        'r1_defect': round((float(row.r1_defect or 0) / float(row.total_inject or 0) * 100), 2),
                        'film_ng': round((float(row.film_ng or 0) / float(row.total_inject or 0) * 100), 2),
                        'foreign': round((float(row.foreign or 0) / float(row.total_inject or 0) * 100), 2),
                        'cut_waste': round((float(row.cut_waste or 0) / float(row.total_inject or 0) * 100), 2),
                        'fiber': round((float(row.fiber or 0) / float(row.total_inject or 0) * 100), 2),
                        'mold_dirt': round((float(row.mold_dirt or 0) / float(row.total_inject or 0) * 100), 2),
                        'film_dirt': round((float(row.film_dirt or 0) / float(row.total_inject or 0) * 100), 2),
                        'axis_1st': round((float(row.axis_1st or 0) / float(row.total_inject or 0) * 100), 2),
                        'stripe_1st': round((float(row.stripe_1st or 0) / float(row.total_inject or 0) * 100), 2),
                        'edge_defect': round((float(row.edge_defect or 0) / float(row.total_inject or 0) * 100), 2),
                        'wash_drop': round((float(row.wash_drop or 0) / float(row.total_inject or 0) * 100), 2),
                        'unknown': round((float(row.unknown or 0) / float(row.total_inject or 0) * 100), 2),
                        'other_1': round((float(row.other_1 or 0) / float(row.total_inject or 0) * 100), 2),
                        'ecc_defect': round((float(row.ecc_defect or 0) / float(row.total_inject or 0) * 100), 2),
                        'drop': round((float(row.drop or 0) / float(row.total_inject or 0) * 100), 2),
                        'count_err': round((float(row.count_err or 0) / float(row.total_inject or 0) * 100), 2),
                        'suction': round((float(row.suction or 0) / float(row.total_inject or 0) * 100), 2),
                        'axis_def': round((float(row.axis_def or 0) / float(row.total_inject or 0) * 100), 2),
                        'color_def': round((float(row.color_def or 0) / float(row.total_inject or 0) * 100), 2),
                        'trans_def': round((float(row.trans_def or 0) / float(row.total_inject or 0) * 100), 2),
                        'curve_def': round((float(row.curve_def or 0) / float(row.total_inject or 0) * 100), 2),    
                        'cen_th_def': round((float(row.cen_th_def or 0) / float(row.total_inject or 0) * 100), 2),
                        'diam_def': round((float(row.diam_def or 0) / float(row.total_inject or 0) * 100), 2),
                        'r1_th_def': round((float(row.r1_th_def or 0) / float(row.total_inject or 0) * 100), 2)
                    }
                daily_data.append({
                    'date': date_str,
                    'defect_data': defect_data
                })
                
            daily_rates[mono_syu] = daily_data
            mono_dates[mono_syu] = dates
        # お知らせデータを取得
        set_mst = SetMst.query.first()
        
        # NOTE_DATデータを取得
        latest_note_data = None
        recent_notes_count = 0
        
        try:
            # 最新のNOTE_DAT 1件を取得
            latest_note = NoteDatModel.query.order_by(NoteDatModel.NOTE_DATE.desc()).first()
            
            # 過去5日間のNOTE_DAT件数を取得
            five_days_ago_notes = (datetime.now() - timedelta(days=5)).date()
            recent_notes_count = NoteDatModel.query.filter(
                NoteDatModel.NOTE_DATE >= five_days_ago_notes
            ).count()
            
            # 最新NOTE_DATデータの整形
            if latest_note:
                # 作業員情報を取得
                worker_name = '不明'
                if latest_note.worker and latest_note.worker.WRK_NM:
                    worker_name = latest_note.worker.WRK_NM
                elif latest_note.NOTE_USER:
                    # WorkerModelから直接取得を試行
                    worker = WorkerModel.query.filter_by(WRK_ID=latest_note.NOTE_USER).first()
                    if worker:
                        worker_name = worker.WRK_NM
                    else:
                        worker_name = str(latest_note.NOTE_USER)
                
                latest_note_data = {
                    'NOTE_ID': latest_note.NOTE_ID,
                    'NOTE_LOT_NO': latest_note.NOTE_LOT_NO or '',
                    'NOTE_DATE': latest_note.NOTE_DATE.strftime('%Y-%m-%d') if latest_note.NOTE_DATE else '',
                    'NOTE_USER_NAME': worker_name,
                    'NOTE_TITLE': latest_note.NOTE_TITLE or '',
                    'NOTE_CNTNT': latest_note.NOTE_CNTNT or '',
                    'NOTE_PATH': latest_note.NOTE_PATH or ''
                }
                
        except Exception as note_error:
            log_error(f'NOTE_DAT処理でエラー: {note_error}')
            latest_note_data = None
            recent_notes_count = 0
        
        return jsonify({
            'mono_mst': mono_mst,
            'good_rates': good_rates,
            'defect_items': defect_items_data,
            'csv_import_time': SetMst.get_csv_import_time(),
            # お知らせデータを追加
            'SET_INFO_H1': set_mst.SET_INFO_H1 if set_mst else None,
            'SET_INFO_1': set_mst.SET_INFO_1 if set_mst else None,
            'SET_INFO_H2': set_mst.SET_INFO_H2 if set_mst else None,
            'SET_INFO_2': set_mst.SET_INFO_2 if set_mst else None,
            'SET_INFO_H3': set_mst.SET_INFO_H3 if set_mst else None,
            'SET_INFO_3': set_mst.SET_INFO_3 if set_mst else None,
            # NOTE_DATデータを追加
            'latest_note': latest_note_data,
            'recent_notes_count': recent_notes_count,
            'graph_data': daily_rates,
            'defect_labels': defect_labels,
            'mono_dates': mono_dates
        })
        
    except Exception as e:
        log_error(f'モノマー種別スライドデータ取得中にエラーが発生しました: {str(e)}')
        return jsonify({'error': str(e)}), 500

@main.route('/settings', methods=['GET', 'POST'])
@login_required
def settings():
    """設定マスタの更新画面を表示"""
    form = SetMstForm()
    settings = SetMst.query.filter_by(SET_ID=1).first()
    
    if request.method == 'POST':
        if form.validate_on_submit():
            if settings:
                settings.SET_DRW_INT = form.SET_DRW_INT.data
                settings.SET_CHS_TM = form.SET_CHS_TM.data
                settings.SET_INFO_H1 = form.SET_INFO_H1.data
                settings.SET_INFO_1 = form.SET_INFO_1.data
                settings.SET_INFO_H2 = form.SET_INFO_H2.data
                settings.SET_INFO_2 = form.SET_INFO_2.data
                settings.SET_INFO_H3 = form.SET_INFO_H3.data
                settings.SET_INFO_3 = form.SET_INFO_3.data
            else:
                settings = SetMst(
                    SET_ID=1,
                    SET_DRW_INT=form.SET_DRW_INT.data,
                    SET_CHS_TM=form.SET_CHS_TM.data,
                    SET_INFO_H1=form.SET_INFO_H1.data,
                    SET_INFO_1=form.SET_INFO_1.data,
                    SET_INFO_H2=form.SET_INFO_H2.data,
                    SET_INFO_2=form.SET_INFO_2.data,
                    SET_INFO_H3=form.SET_INFO_H3.data,
                    SET_INFO_3=form.SET_INFO_3.data
                )
                db.session.add(settings)
            
            try:
                db.session.commit()
                flash('設定を更新しました。', 'success')
                return redirect(url_for('main.settings'))
            except Exception as e:
                db.session.rollback()
                flash('設定の更新に失敗しました。', 'error')
                log_error(f"設定更新エラー: {str(e)}")
    
    if settings:
        form.SET_DRW_INT.data = settings.SET_DRW_INT
        form.SET_CHS_TM.data = settings.SET_CHS_TM
        form.SET_INFO_H1.data = settings.SET_INFO_H1
        form.SET_INFO_1.data = settings.SET_INFO_1
        form.SET_INFO_H2.data = settings.SET_INFO_H2
        form.SET_INFO_2.data = settings.SET_INFO_2
        form.SET_INFO_H3.data = settings.SET_INFO_H3
        form.SET_INFO_3.data = settings.SET_INFO_3
    
    return render_template('settings.html', form=form, title='設定更新')

@main.route('/inspection_search', methods=['GET'])
@login_required
def inspection_search():
    """
    検査履歴検索画面
    
    機能概要:
    - 離型は完了しているが、三次検査が未完了のデータを一覧表示する
    - 検査工程に滞留しているデータを可視化し、工程管理を支援する
    - 直近2週間に注入され、2日以上前に離型されたデータに限定される
    - R1注入日の昇順で表示されるため、古いものから処理を進めることが可能
    
    表示項目:
    - R1注入日: 製造工程の開始日
    - 離型日: 離型工程の完了日
    - ロットNo.: 製品の一意識別子
    - 製品名: マスタから関連付けられた製品名
    - 膜カラー: 製品の膜カラー情報
    - モノマー種: モノマー種別マスタから関連付けられた種別名
    
    処理フロー:
    1. PrdRecordModelから三次検査未完了データを取得
    2. 設定マスタを取得してCSVインポート時刻を取得
    3. テンプレートにデータを渡してレンダリング
    
    関連機能:
    - get_incomplete_inspections(): モデルメソッドでデータ取得ロジックを実装
    - inspection_search.html: テンプレートでデータ表示を担当
    - csv_import_time: 最終データ更新時刻をフッターに表示
    """
    search_performed = False
    
    
    incomplete_inspections = []
    
    # 未検査品データを取得
    incomplete_inspections = PrdRecordModel.get_incomplete_inspections()
    
    set_mst = SetMst.query.first()
    
    return render_template(
        'inspection_search.html',
        incomplete_inspections=incomplete_inspections,
        csv_import_time=set_mst.get_csv_import_time()
    )

@main.route('/anneal_incomplete')
@login_required
def anneal_incomplete():
    # モノマー種別マスタを取得
    mono_list = MnoMstModel.get_all()
    
    # 検索条件の取得
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    mono_syu = request.args.get('mono_syu')
    
    # クエリの構築
    query = PrdRecordModel.query.join(
        PrdMstModel,
        PrdRecordModel.PRR_PRD_ID == PrdMstModel.PRD_ID,
        isouter=True
    ).join(
        MnoMstModel,
        PrdRecordModel.PRR_MONO_SYU == MnoMstModel.MNO_SYU,
        isouter=True
    ).add_columns(
        PrdRecordModel.PRR_R1_IN_DATE,
        PrdRecordModel.PRR_LOT_NO,
        PrdRecordModel.PRR_RELEASE_DT,
        PrdRecordModel.PRR_ANNEAL_DT,
        PrdMstModel.PRD_NM,
        PrdMstModel.PRD_PLY_DAYS,
        PrdRecordModel.PRR_R1_GOOD_CNT,
        MnoMstModel.MNO_NM,
        func.date_add(
            func.date(PrdRecordModel.PRR_R1_IN_DATE),
            text(f'interval cast(PRD_PLY_DAYS as integer) day')
        ).label('anneal_scheduled_date')
    )
    
    # 検索条件の適用
    if date_from:
        query = query.filter(func.date(PrdRecordModel.PRR_R1_IN_DATE) >= date_from)
    if date_to:
        query = query.filter(func.date(PrdRecordModel.PRR_R1_IN_DATE) <= date_to)
    if mono_syu:
        query = query.filter(PrdRecordModel.PRR_MONO_SYU == mono_syu)
    
    # アニール未完了の条件
    # R1注入日からPRD_PLY_DAYS日後以降のデータで、アニール日が未入力
    query = query.filter(
        PrdRecordModel.PRR_ANNEAL_DT.is_(None),
        func.date(PrdRecordModel.PRR_R1_IN_DATE) + func.cast(PrdMstModel.PRD_PLY_DAYS, Integer) <= func.current_date()
    )
    
    # 直近2週間のデータのみ
    two_weeks_ago = datetime.now() - timedelta(days=14)
    query = query.filter(PrdRecordModel.PRR_R1_IN_DATE >= two_weeks_ago).order_by(PrdRecordModel.PRR_R1_IN_DATE)
    
    # 結果の取得
    records = query.all()
    
    return render_template(
        'anneal_incomplete.html',
        records=records,
        mono_list=mono_list
    )
    
@main.route('/api/info_data')
def api_info_data():
    """お知らせ情報のAPI"""
    try:
        setting = SetMst.query.first()
        if not setting:
            return jsonify({})
            
        return jsonify({
            'SET_INFO_H1': setting.SET_INFO_H1,
            'SET_INFO_1': setting.SET_INFO_1,
            'SET_INFO_H2': setting.SET_INFO_H2,
            'SET_INFO_2': setting.SET_INFO_2,
            'SET_INFO_H3': setting.SET_INFO_H3,
            'SET_INFO_3': setting.SET_INFO_3
        })
    except Exception as e:
        log_error(f'お知らせ情報の取得中にエラーが発生しました: {str(e)}')
        return jsonify({'error': 'データの取得に失敗しました'}), 500



@main.route('/fmc_cross_table')
@login_required
def fmc_cross_table():
    """FMC合格数クロス集計表画面"""
    # 区分マスタ情報の取得
    monomer_list = KbnMst.get_kbn_list('MMNO')  # モノマー
    color_list = KbnMst.get_kbn_list('MCLR')    # 色
    film_curve_list = KbnMst.get_kbn_list('MCRB') # 膜カーブ
    
    # 区分マスタの辞書を作成
    kbn_dict = {
        'MMNO': {str(m.KBN_ID): m.KBN_NM for m in monomer_list},
        'MCLR': {str(m.KBN_ID): m.KBN_NM for m in color_list},
        'MCRB': {str(m.KBN_ID): m.KBN_NM for m in film_curve_list}
    }
    
    return render_template('fmc_cross_table.html', kbn_dict=kbn_dict)

@main.route('/api/fmc_cross_table')
@login_required
def api_fmc_cross_table():
    """FMC合格数クロス集計表APIエンドポイント"""
    try:
        # 検索条件の取得
        cut_date_start = request.args.get('cut_date_start')
        cut_date_end = request.args.get('cut_date_end')
        monomer = request.args.get('monomer')
        cr_film = request.args.get('cr_film')
        
        # 日付の変換
        start_date = None
        end_date = None
        
        if cut_date_start:
            start_date = datetime.strptime(cut_date_start, '%Y-%m-%d').date()
        
        if cut_date_end:
            end_date = datetime.strptime(cut_date_end, '%Y-%m-%d').date()
        
        # モノマーの変換
        monomer_int = None
        if monomer:
            monomer_int = int(monomer)
        
        # CR膜の変換
        cr_film_int = None
        if cr_film:
            cr_film_int = int(cr_film)
        
        # クロス集計データの取得
        cross_data = FmcDat.get_pass_qty_cross_table(start_date, end_date, monomer_int, cr_film_int)
        
        # 区分マスタ情報の取得
        color_list = KbnMst.get_kbn_list('MCLR')
        film_curve_list = KbnMst.get_kbn_list('MCRB')
        
        # 区分マスタの辞書を作成
        color_dict = {int(m.KBN_ID): m.KBN_NM for m in color_list}
        curve_dict = {int(m.KBN_ID): m.KBN_NM for m in film_curve_list}
        
        # 結果に名称情報を追加
        result = {
            'cross_table': cross_data['cross_table'],
            'colors': cross_data['colors'],
            'film_curves': cross_data['film_curves'],
            'color_totals': cross_data['color_totals'],
            'curve_totals': cross_data['curve_totals'],
            'grand_total': cross_data['grand_total'],
            'color_dict': color_dict,
            'curve_dict': curve_dict
        }
        
        return jsonify(result)
    
    except Exception as e:
        log_error(f'FMCクロス集計APIエンドポイントでエラーが発生しました: {str(e)}')
        return jsonify({'error': str(e)}), 500



@main.route('/note_dat_list')
@login_required
def note_dat_list():
    """特記事項一覧画面"""
    # 検索パラメータを取得
    lot_no = request.args.get('lot_no', '')
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    user_id = request.args.get('user_id', '')
    title = request.args.get('title', '')
    
    # ページ番号と1ページあたり件数
    try:
        page = int(request.args.get('page', 1))
    except ValueError:
        page = 1
    per_page = 10
    
    # 日付の変換
    start_dt = None
    end_dt = None
    if start_date:
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        except ValueError:
            pass
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            pass
    
    # 特記事項データを検索（ページネーション対応）
    notes, total = NoteDatModel.search(
        lot_no=lot_no if lot_no else None,
        start_date=start_dt,
        end_date=end_dt,
        user_id=user_id if user_id else None,
        title=title if title else None,
        page=page,
        per_page=per_page
    )
    pages = (total + per_page - 1) // per_page
    has_prev = page > 1
    has_next = page < pages
    prev_num = page - 1 if has_prev else None
    next_num = page + 1 if has_next else None
    
    # 作業員マスタを取得（入力者選択用）
    workers = WorkerModel.query.all()
    
    # 作業員情報を辞書形式で取得（フォールバック用）
    worker_dict = {str(w.WRK_ID): w.WRK_NM for w in workers}
    
    return render_template('note_dat_list.html', 
                         notes=notes, 
                         workers=workers,
                         worker_dict=worker_dict,
                         lot_no=lot_no,
                         start_date=start_date,
                         end_date=end_date,
                         user_id=user_id,
                         title=title,
                         page=page,
                         pages=pages,
                         has_prev=has_prev,
                         has_next=has_next,
                         prev_num=prev_num,
                         next_num=next_num,
                         total=total)

@main.route('/note_dat_input', methods=['GET', 'POST'])
@login_required
def note_dat_input():
    """特記事項入力画面"""
    if request.method == 'POST':
        # フォームデータを取得
        lot_no = request.form.get('lot_no', '').strip()
        note_date = request.form.get('note_date', '').strip()
        user_id = request.form.get('user_id', '').strip()
        title = request.form.get('title', '').strip()
        content = request.form.get('content', '').strip()
        photo_data = request.form.get('photo_data', '')
        
        # バリデーション（入力日のみ必須）
        if not note_date:
            flash('入力日を入力してください')
            return redirect(request.url)
        if not user_id:
            user_id = None
        
        try:
            # 日付の変換
            note_dt = datetime.strptime(note_date, '%Y-%m-%d') if note_date else datetime.now()
            
            # 画像を先に保存
            image_filename = None
            if photo_data:
                image_filename = save_image_from_base64(photo_data)
            
            # 特記事項データを作成
            note = NoteDatModel(
                NOTE_LOT_NO=lot_no if lot_no else None,
                NOTE_DATE=note_dt,
                NOTE_USER=user_id,
                NOTE_TITLE=title if title else None,
                NOTE_CNTNT=content if content else None,
                NOTE_PATH=image_filename
            )
            
            # 保存
            if note.save():
                flash('特記事項を保存しました')
                return redirect(url_for('main.note_dat_list'))
            else:
                flash('特記事項の保存に失敗しました')
                return redirect(request.url)
                
        except ValueError:
            flash('入力日の形式が正しくありません')
            return redirect(request.url)
        except Exception as e:
            flash(f'特記事項の入力中にエラーが発生しました: {str(e)}')
            return redirect(request.url)
    
    # 作業員マスタを取得
    workers = WorkerModel.query.all()
    
    # 当日の日付を設定
    today = datetime.now().strftime('%Y-%m-%d')
    
    return render_template('note_dat_input.html', workers=workers, today=today)

@main.route('/note_dat_detail/<int:note_id>')
@login_required
def note_dat_detail(note_id):
    """特記事項詳細画面（返信も含む）"""
    note = NoteDatModel.get_by_id(note_id)
    if not note:
        flash('指定された特記事項が見つかりません')
        return redirect(url_for('main.note_dat_list'))
    # 作業員情報を辞書形式で取得（フォールバック用）
    workers = WorkerModel.query.all()
    worker_dict = {str(w.WRK_ID): w.WRK_NM for w in workers}
    # 返信一覧を取得
    replies = NansDatModel.get_by_note_id(note_id)
    return render_template('note_dat_detail.html', note=note, worker_dict=worker_dict, replies=replies, workers=workers)

@main.route('/note_reply/<int:note_id>', methods=['POST'])
@login_required
def note_reply(note_id):
    """特記事項への返信投稿"""
    content = request.form.get('reply_content', '').strip()
    reply_user = request.form.get('reply_user', '').strip()
    if not content:
        flash('返信内容を入力してください')
        return redirect(url_for('main.note_dat_detail', note_id=note_id))
    if not reply_user:
        flash('返信者を選択してください')
        return redirect(url_for('main.note_dat_detail', note_id=note_id))
    try:
        reply = NansDatModel(
            NANS_NOTE_ID=note_id,
            NANS_DATE=datetime.now(),
            NANS_USER=reply_user,
            NANS_CNTNT=content
        )
        if reply.save():
            flash('返信を投稿しました')
        else:
            flash('返信の保存に失敗しました', 'danger')
    except Exception as e:
        flash(f'返信の投稿中にエラーが発生しました: {str(e)}', 'danger')
    return redirect(url_for('main.note_dat_detail', note_id=note_id))

@main.route('/note_dat_edit/<int:note_id>', methods=['GET', 'POST'])
@login_required
def note_dat_edit(note_id):
    """特記事項編集画面"""
    note = NoteDatModel.get_by_id(note_id)
    if not note:
        flash('指定された特記事項が見つかりません')
        return redirect(url_for('main.note_dat_list'))
    
    if request.method == 'POST':
        # フォームデータを取得
        lot_no = request.form.get('lot_no', '').strip()
        note_date = request.form.get('note_date', '').strip()
        user_id = request.form.get('user_id', '').strip()
        title = request.form.get('title', '').strip()
        content = request.form.get('content', '').strip()
        photo_data = request.form.get('photo_data', '')
        delete_image = request.form.get('delete_image', '0')
        
        # バリデーション（入力日のみ必須）
        if not note_date:
            flash('入力日を入力してください')
            return redirect(request.url)
        
        try:
            # 日付の変換
            note_dt = datetime.strptime(note_date, '%Y-%m-%d')
            
            # 現在の画像ファイル名を保存
            current_image = note.NOTE_PATH
            
            # データを更新
            note.NOTE_LOT_NO = lot_no if lot_no else None
            note.NOTE_DATE = note_dt
            note.NOTE_USER = user_id if user_id else None
            note.NOTE_TITLE = title if title else None
            note.NOTE_CNTNT = content if content else None
            
            # 画像処理
            if delete_image == '1':
                # 現在の画像を削除
                if current_image:
                    delete_image_file(current_image)
                note.NOTE_PATH = None
            elif photo_data:
                # 新しい画像を保存
                if current_image:
                    delete_image_file(current_image)
                saved_filename = save_image_from_base64(photo_data)
                if saved_filename:
                    note.NOTE_PATH = saved_filename
            
            # 保存
            if note.save():
                flash('特記事項を更新しました')
                return redirect(url_for('main.note_dat_list'))
            else:
                flash('特記事項の更新に失敗しました')
                return redirect(request.url)
                
        except ValueError:
            flash('入力日の形式が正しくありません')
            return redirect(request.url)
        except Exception as e:
            flash(f'特記事項の編集中にエラーが発生しました: {str(e)}')
            return redirect(request.url)
    
    # 作業員マスタを取得
    workers = WorkerModel.query.all()
    
    return render_template('note_dat_edit.html', note=note, workers=workers)

@main.route('/note_dat_delete/<int:note_id>', methods=['POST'])
@login_required
def note_dat_delete(note_id):
    """特記事項削除"""
    note = NoteDatModel.get_by_id(note_id)
    if not note:
        flash('指定された特記事項が見つかりません')
        return redirect(url_for('main.note_dat_list'))

    # 先にNANS_DATの該当レコードを削除
    try:
        session = db.session
        session.query(NansDatModel).filter(NansDatModel.NANS_NOTE_ID == note_id).delete()
        session.commit()
    except Exception as e:
        flash(f'返信データの削除中にエラーが発生しました: {str(e)}', 'danger')
        return redirect(url_for('main.note_dat_list'))

    # 関連する画像ファイルを削除
    if note.NOTE_PATH:
        delete_image_file(note.NOTE_PATH)

    # NOTE_DAT本体を削除
    if note.delete():
        flash('特記事項を削除しました')
    else:
        flash('特記事項の削除に失敗しました')

    return redirect(url_for('main.note_dat_list'))

@main.route('/fmc_dat/create', methods=['GET', 'POST'])
@login_required
def fmc_dat_create():
    """FMC_DATの新規作成"""
    form = FmcDatForm()
    form.FMC_MONOMER.choices = [('', '--- 選択してください ---')] + [(m.KBN_ID, m.KBN_NM) for m in KbnMst.query.filter_by(KBN_TYP='MMNO').all()]
    form.FMC_FILM_CURVE.choices = [('', '--- 選択してください ---')] + [(m.KBN_ID, m.KBN_NM) for m in KbnMst.query.filter_by(KBN_TYP='MCRB').all()]
    form.FMC_COLOR.choices = [('', '--- 選択してください ---')] + [(m.KBN_ID, m.KBN_NM) for m in KbnMst.query.filter_by(KBN_TYP='MCLR').all()]
    form.FMC_CUT_MENU.choices = [('', '--- 選択してください ---')] + [(m.KBN_ID, m.KBN_NM) for m in KbnMst.query.filter_by(KBN_TYP='MCUT').all()]
    form.FMC_ITEM.choices = [('', '--- 選択してください ---')] + [(m.KBN_ID, m.KBN_NM) for m in KbnMst.query.filter_by(KBN_TYP='MITM').all()]
    
    if form.validate_on_submit():
        fmc_dat = FmcDat()
        form.populate_obj(fmc_dat)
        if fmc_dat.save():
            flash('データを保存しました。', 'success')
            return redirect(url_for('main.fmc_dat_list'))
        else:
            flash('データの保存に失敗しました。', 'error')
    
    return render_template('fmc_edit.html', form=form, is_create=True)

@main.route('/fmc_edit/<int:id>', methods=['GET', 'POST'])
@login_required
def fmc_dat_edit(id):
    """FMC_DATの編集"""
    fmc_dat = FmcDat.get_by_id(id)
    if not fmc_dat:
        flash('指定されたレコードが見つかりません。', 'error')
        return redirect(url_for('main.fmc_dat_list'))
    
    form = FmcDatForm(obj=fmc_dat)
    form.FMC_MONOMER.choices = [('', '--- 選択してください ---')] + [(m.KBN_ID, m.KBN_NM) for m in KbnMst.query.filter_by(KBN_TYP='MMNO').all()]
    form.FMC_FILM_CURVE.choices = [('', '--- 選択してください ---')] + [(m.KBN_ID, m.KBN_NM) for m in KbnMst.query.filter_by(KBN_TYP='MCRB').all()]
    form.FMC_COLOR.choices = [('', '--- 選択してください ---')] + [(m.KBN_ID, m.KBN_NM) for m in KbnMst.query.filter_by(KBN_TYP='MCLR').all()]
    form.FMC_CUT_MENU.choices = [('', '--- 選択してください ---')] + [(m.KBN_ID, m.KBN_NM) for m in KbnMst.query.filter_by(KBN_TYP='MCUT').all()]
    form.FMC_ITEM.choices = [('', '--- 選択してください ---')] + [(m.KBN_ID, m.KBN_NM) for m in KbnMst.query.filter_by(KBN_TYP='MITM').all()]
    
    if request.method == 'POST':
        # 削除アクションの処理
        if request.form.get('action') == 'delete':
            if fmc_dat.delete():
                flash('データを削除しました。', 'success')
                return redirect(url_for('main.fmc_dat_list'))
            else:
                flash('データの削除に失敗しました。', 'error')
                return render_template('fmc_edit.html', form=form, is_create=False)
        
        # 通常の更新処理
        if form.validate_on_submit():
            form.populate_obj(fmc_dat)
            if fmc_dat.save():
                flash('データを更新しました。', 'success')
                return redirect(url_for('main.fmc_dat_list'))
            else:
                flash('データの更新に失敗しました。', 'error')
    
    return render_template('fmc_edit.html', form=form, is_create=False)

@main.route('/fmc_dat/delete/<int:id>', methods=['POST'])
@login_required
def fmc_dat_delete(id):
    """FMC_DATの削除"""
    fmc_dat = FmcDat.get_by_id(id)
    if not fmc_dat:
        return jsonify({'success': False, 'message': '指定されたレコードが見つかりません。'})
    
    if fmc_dat.delete():
        return jsonify({'success': True, 'message': 'データを削除しました。'})
    else:
        return jsonify({'success': False, 'message': 'データの削除に失敗しました。'})

@main.route('/fmc_dat_list')
@login_required
def fmc_dat_list():
    """FMC_DATの一覧表示"""
    # 検索パラメータの取得
    cut_date_start = request.args.get('cut_date_start')
    cut_date_end = request.args.get('cut_date_end')
    r1_inj_date = request.args.get('r1_inj_date')
    monomer = request.args.get('monomer')
    cut_menu = request.args.get('cut_menu')
    film_proc_date_start = request.args.get('film_proc_date_start')
    film_proc_date_end = request.args.get('film_proc_date_end')
    color = request.args.get('color')
    film_curve = request.args.get('film_curve')
    
    # 区分マスタ情報の取得
    monomer_list = KbnMst.get_kbn_list('MMNO')  # モノマー
    item_list = KbnMst.get_kbn_list('MITM')     # アイテム
    cut_menu_list = KbnMst.get_kbn_list('MCUT') # カットメニュー
    film_curve_list = KbnMst.get_kbn_list('MCRB') # 膜カーブ
    color_list = KbnMst.get_kbn_list('MCLR')    # 色
    
    # 区分マスタの辞書を作成（キーをDecimalから文字列に変換）
    kbn_dict = {
        'MMNO': {str(m.KBN_ID): m.KBN_NM for m in monomer_list},
        'MITM': {str(m.KBN_ID): m.KBN_NM for m in item_list},
        'MCUT': {str(m.KBN_ID): m.KBN_NM for m in cut_menu_list},
        'MCRB': {str(m.KBN_ID): m.KBN_NM for m in film_curve_list},
        'MCLR': {str(m.KBN_ID): m.KBN_NM for m in color_list}
    }
    records = []
    # 検索実行
    if any([cut_date_start, cut_date_end, r1_inj_date, monomer, cut_menu, film_proc_date_start, film_proc_date_end, color, film_curve]):
        records = FmcDat.search(cut_date_start, cut_date_end, r1_inj_date, monomer, cut_menu, film_proc_date_start, film_proc_date_end, color, film_curve)
    
    return render_template('fmc_list.html', records=records, kbn_dict=kbn_dict)

@main.route('/prd_record_monthly_analysis')
@login_required
def prd_record_monthly_analysis():
    """生産実績不良データ月別分析画面を表示"""
    # モノマー種別マスタを取得
    mono_mst = {m.MNO_SYU: {'name': m.MNO_NM, 'target': float(m.MNO_TARGET) if m.MNO_TARGET else None}
               for m in MnoMstModel.get_all()}
    
    return render_template('prd_record_monthly_analysis.html', mono_mst=mono_mst)

@main.route('/api/prd_record_monthly_data')
@login_required
def api_prd_record_monthly_data():
    """生産実績不良データ月別分析のAPIエンドポイント"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    mono_syu = request.args.get('mono_syu')
    prd_id = request.args.get('prd_id')
    
    try:
        # クエリの構築（R2注入日で月別集計）
        query = db.session.query(
            func.date_format(PrdRecordModel.PRR_R2_DATE, '%Y-%m').label('month'),
            func.sum(PrdRecordModel.PRR_INJECT_QTY).label('total_inject'),
            # 各不良項目の合計（mono_syu_defect_detail_dataと同じ統合ルール）
            func.sum(PrdRecordModel.PRR_ROLL_MISS).label('roll_miss'),
            func.sum(PrdRecordModel.PRR_LEAK).label('leak'),
            func.sum(PrdRecordModel.PRR_FILM_PULL).label('film_pull'),
            func.sum(PrdRecordModel.PRR_CRACK).label('crack'),
            func.sum(PrdRecordModel.PRR_TEAR + PrdRecordModel.PRR_TEAR_RLS).label('tear'),
            func.sum(PrdRecordModel.PRR_PEEL + PrdRecordModel.PRR_PEEL_2ND).label('peel'),
            func.sum(PrdRecordModel.PRR_CHIP).label('chip'),
            func.sum(PrdRecordModel.PRR_POLY_CRK).label('poly_crk'),
            func.sum(PrdRecordModel.PRR_MOLD_SCR + PrdRecordModel.PRR_MOLD_2ND).label('mold_scr'),
            func.sum(PrdRecordModel.PRR_LENS_SCR).label('lens_scr'),
            func.sum(PrdRecordModel.PRR_R1_BUBBLE + PrdRecordModel.PRR_R1_BUB_CHK).label('r1_bubble'),
            func.sum(PrdRecordModel.PRR_R2_BUBBLE + PrdRecordModel.PRR_R2_BUB_REK).label('r2_bubble'),
            func.sum(PrdRecordModel.PRR_DEFECT + PrdRecordModel.PRR_DEFECT_2ND).label('defect'),
            func.sum(PrdRecordModel.PRR_ELUTION).label('elution'),
            func.sum(PrdRecordModel.PRR_HAZE).label('haze'),
            func.sum(PrdRecordModel.PRR_CURL + PrdRecordModel.PRR_CURL_INS).label('curl'),
            func.sum(PrdRecordModel.PRR_FILM_FLOAT + PrdRecordModel.PRR_FILM_FLT_CK + PrdRecordModel.PRR_FILM_3RD).label('film_float'),
            func.sum(PrdRecordModel.PRR_R1_DEFECT).label('r1_defect'),
            func.sum(PrdRecordModel.PRR_FILM_NG + PrdRecordModel.PRR_FILM_NG_CK + PrdRecordModel.PRR_FILM_2ND).label('film_ng'),
            func.sum(PrdRecordModel.PRR_FOREIGN).label('foreign'),
            func.sum(PrdRecordModel.PRR_CUT_WASTE).label('cut_waste'),
            func.sum(PrdRecordModel.PRR_FIBER).label('fiber'),
            func.sum(PrdRecordModel.PRR_MOLD_DIRT).label('mold_dirt'),
            func.sum(PrdRecordModel.PRR_FILM_DIRT + PrdRecordModel.PRR_EDGE_DEF_3).label('film_dirt'),
            func.sum(PrdRecordModel.PRR_AXIS_1ST + PrdRecordModel.PRR_AXIS_3RD).label('axis_1st'),
            func.sum(PrdRecordModel.PRR_STRIPE_1ST + PrdRecordModel.PRR_STRIPE_2ND).label('stripe_1st'),
            func.sum(PrdRecordModel.PRR_EDGE_DEFECT).label('edge_defect'),
            func.sum(PrdRecordModel.PRR_WASH_DROP).label('wash_drop'),
            func.sum(PrdRecordModel.PRR_UNKNOWN).label('unknown'),
            func.sum(PrdRecordModel.PRR_OTHER_1 + PrdRecordModel.PRR_OTHER_2 + PrdRecordModel.PRR_OTHER_3RD + PrdRecordModel.PRR_OTHER_2ND + PrdRecordModel.PRR_OTHER_1ST).label('other_1'),
            func.sum(PrdRecordModel.PRR_ECC_DEFECT + PrdRecordModel.PRR_ECC_3RD + PrdRecordModel.PRR_ECC_1ST).label('ecc_defect'),
            func.sum(PrdRecordModel.PRR_DROP).label('drop'),
            func.sum(PrdRecordModel.PRR_COUNT_ERR).label('count_err'),
            func.sum(PrdRecordModel.PRR_SUCTION).label('suction'),
            func.sum(PrdRecordModel.PRR_AXIS_DEF).label('axis_def'),
            func.sum(PrdRecordModel.PRR_COLOR_DEF).label('color_def'),
            func.sum(PrdRecordModel.PRR_TRANS_DEF).label('trans_def'),
            func.sum(PrdRecordModel.PRR_CURVE_DEF).label('curve_def'),
            func.sum(PrdRecordModel.PRR_CEN_TH_DEF).label('cen_th_def'),
            func.sum(PrdRecordModel.PRR_DIAM_DEF).label('diam_def'),
            func.sum(PrdRecordModel.PRR_R1_TH_DEF).label('r1_th_def')
        )
        
        # フィルター適用
        if start_date:
            query = query.filter(PrdRecordModel.PRR_R2_DATE >= start_date)
        if end_date:
            query = query.filter(PrdRecordModel.PRR_R2_DATE <= end_date)
        if mono_syu:
            query = query.filter(PrdRecordModel.PRR_MONO_SYU == mono_syu)
        if prd_id:
            query = query.filter(PrdRecordModel.PRR_PRD_ID == prd_id)
        
        # NULLでない値のみ対象
        query = query.filter(PrdRecordModel.PRR_R2_DATE.isnot(None))
        query = query.filter(PrdRecordModel.PRR_INJECT_QTY.isnot(None))
        
        # 月別グループ化とソート
        query = query.group_by(func.date_format(PrdRecordModel.PRR_R2_DATE, '%Y-%m'))
        query = query.order_by(func.date_format(PrdRecordModel.PRR_R2_DATE, '%Y-%m'))
        
        results = query.all()
        
        # 不良項目の日本語ラベル
        defect_labels = {
            'roll_miss': '巻きミス',
            'leak': 'モレ',
            'film_pull': '膜ひっぱり',
            'crack': 'ワレ',
            'tear': 'チギレ',
            'peel': 'ハガレ',
            'chip': 'カケ',
            'poly_crk': '重合ワレ',
            'mold_scr': '型キズ',
            'lens_scr': 'レンズキズ',
            'r1_bubble': 'R1泡',
            'r2_bubble': 'R2泡',
            'defect': 'ブツ',
            'elution': '溶出',
            'haze': 'モヤ',
            'curl': 'カール',
            'film_float': '膜浮き',
            'r1_defect': 'R1不良',
            'film_ng': '膜不良',
            'foreign': 'イブツ',
            'cut_waste': 'カットくず',
            'fiber': 'センイ',
            'mold_dirt': 'モールド汚れ',
            'film_dirt': '膜汚れ',
            'axis_1st': '片軸',
            'stripe_1st': '脈理',
            'edge_defect': 'コバスリ不良',
            'wash_drop': '洗浄落下',
            'unknown': '不明',
            'other_1': 'その他',
            'ecc_defect': '偏心不良',
            'drop': '落下',
            'count_err': '員数違い',
            'suction': '吸い込み',
            'axis_def': '軸不良',
            'color_def': 'カラー不良',
            'trans_def': '透過率不良',
            'curve_def': 'カーブ不良',
            'cen_th_def': '中心厚不良',
            'diam_def': '径不良',
            'r1_th_def': 'R1厚み不良'
        }
        
        # データ整形
        months = []
        defect_data = {}
        total_data = {}
        
        for row in results:
            month = row.month
            if month:
                months.append(month)
                total_cnt = float(row.total_inject or 0)
                total_data[month] = {'total_cnt': total_cnt}
                
                defect_data[month] = {}
                for defect_key in defect_labels.keys():
                    value = float(getattr(row, defect_key) or 0)
                    defect_data[month][defect_key] = {
                        'count': value,
                        'rate': round((value / total_cnt * 100) if total_cnt > 0 else 0, 2)
                    }
        
        return jsonify({
            'months': months,
            'defect_labels': defect_labels,
            'defect_data': defect_data,
            'total_data': total_data
        })
        
    except Exception as e:
        log_error(f"Error in api_prd_record_monthly_data: {str(e)}")
        return jsonify({'error': str(e)}), 500





