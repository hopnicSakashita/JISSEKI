from datetime import datetime, timedelta
from flask import Blueprint, render_template, request, current_app as app, jsonify
from .models import SjiDatModel, WorkerModel, db, PrdRecordModel, SetMst
from .master_models import MnoMstModel
from .master_models import PrdMstModel
from .utils import log_error
from .master_models import KbnMst
from app.ishida_models import FmcDat
import tempfile
import os
from flask_login import login_required
from sqlalchemy import case, func, and_

analyse = Blueprint('analyse', __name__)

@analyse.route('/defect_analysis')
@login_required
def defect_analysis():
    """
    不良率分析画面を表示する
    
    機能概要:
    - 全不良項目の不良率を一覧表示し、グラフ化する画面
    - 日付範囲、製品ID、モノマー種、製品名、膜カラーでフィルタリング可能
    - 棒グラフまたはレーダーチャートで不良率を視覚化
    - 詳細なデータを表形式で表示
    
    表示データ:
    - 各不良項目の不良率（パーセント）
    - 各不良項目の不良数
    - 合計注入数
    
    データ処理:
    - モノマーマスタデータを取得し、フィルタリング用ドロップダウンを構築
    - 製品カラーと製品名の一覧を取得し、フィルタリング用ドロップダウンを構築
    - API(/api/defect_data)経由で実際のデータ取得とフィルタリングを実行
    
    関連API:
    - /api/defect_data: 不良率データをJSON形式で提供
    
    テンプレート:
    - defect_analysis.html: 不良率分析の表示用テンプレート
    """
    # モノマーマスタの取得
    mono_mst = {m.MNO_SYU: {'name': m.MNO_NM, 'target': float(m.MNO_TARGET) if m.MNO_TARGET else None} 
                for m in MnoMstModel.get_all()}
    prd_color = PrdMstModel.get_distinct_prd_color()
    prd_nm = PrdMstModel.get_distinct_prd_nm()
    return render_template('defect_analysis.html', title='不良率分析', mono_mst=mono_mst, prd_color=prd_color, prd_nm=prd_nm)

@analyse.route('/api/defect_data')
@login_required
def defect_data():
    """不良率データをJSON形式で返す"""
    # フィルターパラメータ取得
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    date_from2 = request.args.get('date_from2')
    date_to2 = request.args.get('date_to2')
    date_from3 = request.args.get('date_from3')
    date_to3 = request.args.get('date_to3')
    prd_id = request.args.get('prd_id')
    mono_syu = request.args.get('mono_syu')
    prd_nm = request.args.get('prd_nm')
    prd_color = request.args.get('prd_color')
    # クエリビルド
    query = PrdRecordModel.query
    
    # フィルター適用
    if date_from:
        query = query.filter(PrdRecordModel.PRR_R1_IN_DATE >= date_from)
    if date_to:
        query = query.filter(PrdRecordModel.PRR_R1_IN_DATE <= date_to)
    if date_from2:
        query = query.filter(PrdRecordModel.PRR_CHK_DT >= date_from2)
    if date_to2:
        query = query.filter(PrdRecordModel.PRR_CHK_DT <= date_to2)
    if date_from3:
        query = query.filter(PrdRecordModel.PRR_R2_DATE >= date_from3)
    if date_to3:
        query = query.filter(PrdRecordModel.PRR_R2_DATE <= date_to3)
    if prd_id:
        query = query.filter(PrdRecordModel.PRR_PRD_ID == prd_id)
    if mono_syu:
        query = query.filter(PrdRecordModel.PRR_MONO_SYU == mono_syu)
    
    # PrdMstModelとの結合（prd_nmまたはprd_colorが指定されている場合）
    if prd_nm or prd_color:
        query = query.join(PrdMstModel, PrdRecordModel.PRR_PRD_ID == PrdMstModel.PRD_ID)
        if prd_nm:
            query = query.filter(PrdMstModel.PRD_NM == prd_nm)
        if prd_color:
            query = query.filter(PrdMstModel.PRD_COLOR == prd_color)
    # データ取得
    result = query.with_entities(
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
    ).first()
    
    # 結果がNoneの場合の処理
    if not result:
        return jsonify({
            'data': {},
            'labels': {},
            'total_inject': 0
        })
    
    # 割合計算
    total_inject = float(result.total_inject or 0)
    data = {}
    
    if total_inject > 0:
        # 不良率を計算
        data = {
            'roll_miss': round((float(result.roll_miss or 0) / total_inject) * 100, 2),
            'leak': round((float(result.leak or 0) / total_inject) * 100, 2),
            'film_pull': round((float(result.film_pull or 0) / total_inject) * 100, 2),
            'crack': round((float(result.crack or 0) / total_inject) * 100, 2),
            'tear': round((float(result.tear or 0) / total_inject) * 100, 2),
            'peel': round((float(result.peel or 0) / total_inject) * 100, 2),
            'chip': round((float(result.chip or 0) / total_inject) * 100, 2),
            'poly_crk': round((float(result.poly_crk or 0) / total_inject) * 100, 2),
            'mold_scr': round((float(result.mold_scr or 0) / total_inject) * 100, 2),
            'lens_scr': round((float(result.lens_scr or 0) / total_inject) * 100, 2),
            'r1_bubble': round((float(result.r1_bubble or 0) / total_inject) * 100, 2),
            'r2_bubble': round((float(result.r2_bubble or 0) / total_inject) * 100, 2),
            'defect': round((float(result.defect or 0) / total_inject) * 100, 2),
            'elution': round((float(result.elution or 0) / total_inject) * 100, 2),
            'haze': round((float(result.haze or 0) / total_inject) * 100, 2),
            'curl': round((float(result.curl or 0) / total_inject) * 100, 2),
            'film_float': round((float(result.film_float or 0) / total_inject) * 100, 2),
            'r1_defect': round((float(result.r1_defect or 0) / total_inject) * 100, 2),
            'film_ng': round((float(result.film_ng or 0) / total_inject) * 100, 2),
            'foreign': round((float(result.foreign or 0) / total_inject) * 100, 2),
            'cut_waste': round((float(result.cut_waste or 0) / total_inject) * 100, 2),
            'fiber': round((float(result.fiber or 0) / total_inject) * 100, 2),
            'mold_dirt': round((float(result.mold_dirt or 0) / total_inject) * 100, 2),
            'film_dirt': round((float(result.film_dirt or 0) / total_inject) * 100, 2),
            'axis_1st': round((float(result.axis_1st or 0) / total_inject) * 100, 2),
            'stripe_1st': round((float(result.stripe_1st or 0) / total_inject) * 100, 2),
            'edge_defect': round((float(result.edge_defect or 0) / total_inject) * 100, 2),
            'wash_drop': round((float(result.wash_drop or 0) / total_inject) * 100, 2),
            'unknown': round((float(result.unknown or 0) / total_inject) * 100, 2),
            'other_1': round((float(result.other_1 or 0) / total_inject) * 100, 2),
            'ecc_defect': round((float(result.ecc_defect or 0) / total_inject) * 100, 2),
            'drop': round((float(result.drop or 0) / total_inject) * 100, 2),
            'count_err': round((float(result.count_err or 0) / total_inject) * 100, 2),
            'suction': round((float(result.suction or 0) / total_inject) * 100, 2),
            'axis_def': round((float(result.axis_def or 0) / total_inject) * 100, 2),
            'color_def': round((float(result.color_def or 0) / total_inject) * 100, 2),
            'trans_def': round((float(result.trans_def or 0) / total_inject) * 100, 2),
            'curve_def': round((float(result.curve_def or 0) / total_inject) * 100, 2),
            'cen_th_def': round((float(result.cen_th_def or 0) / total_inject) * 100, 2),
            'diam_def': round((float(result.diam_def or 0) / total_inject) * 100, 2),
            'r1_th_def': round((float(result.r1_th_def or 0) / total_inject) * 100, 2)
        }
    
    # 日本語ラベル
    labels = {
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
    
    return jsonify({
        'data': data,
        'labels': labels,
        'total_inject': total_inject,
        'csv_import_time': SetMst.get_csv_import_time()
    })

@analyse.route('/mono_syu_defect')
@login_required
def mono_syu_defect():
    """モノマー種別毎の不良率分析画面を表示"""
    # モノマーマスタの取得
    mono_mst = {m.MNO_SYU: {'name': m.MNO_NM, 'target': float(m.MNO_TARGET) if m.MNO_TARGET else None} 
                for m in MnoMstModel.get_all()}
    return render_template('mono_syu_defect.html', title='モノマー種別毎の不良率分析', mono_mst=mono_mst)

@analyse.route('/mono_syu_slide')
@login_required
def mono_syu_slide():
    """モノマー種別のスライド形式表示"""
    return render_template('mono_syu_slide.html', title='モノマー種別良品率分析（スライド）')

@analyse.route('/api/mono_syu_defect_data')
@login_required
def mono_syu_defect_data():
    """モノマー種別毎の日々の不良率データをJSON形式で返す"""
    # フィルターパラメータ取得
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    
    # クエリビルド
    base_query = db.session.query(
        func.DATE(PrdRecordModel.PRR_R2_DATE).label('date'),
        PrdRecordModel.PRR_MONO_SYU,
        func.sum(PrdRecordModel.PRR_INJECT_QTY).label('total_inject'),
        # 全ての不良項目を合計
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
    if date_from:
        base_query = base_query.filter(PrdRecordModel.PRR_R2_DATE >= date_from)
    if date_to:
        base_query = base_query.filter(PrdRecordModel.PRR_R2_DATE <= date_to)
    base_query = base_query.filter(PrdRecordModel.PRR_CHK3_BY.isnot(None))
    
    # グループ化して結果取得
    results = base_query.group_by(
        func.DATE(PrdRecordModel.PRR_R2_DATE),
        PrdRecordModel.PRR_MONO_SYU
    ).order_by(
        func.DATE(PrdRecordModel.PRR_R2_DATE)
    ).all()
    
    # モノマーマスタの取得
    mono_mst = {m.MNO_SYU: {'name': m.MNO_NM, 'target': float(m.MNO_TARGET) if m.MNO_TARGET else None} 
                for m in MnoMstModel.get_all()}
    
    # 結果の整形
    dates = []
    mono_syu_types = set()
    mono_syu_data = {}
    total_data = {}  # 合計データ用
    period_totals = {}  # 期間合計用
    
    for row in results:
        date_str = row.date.strftime('%Y-%m-%d') if row.date else 'Unknown'
        mono_syu = row.PRR_MONO_SYU or 'Unknown'
        
        # 日付リストに追加
        if date_str not in dates:
            dates.append(date_str)
        
        # モノマー種類に追加
        mono_syu_types.add(mono_syu)
        
        # データセット作成
        if mono_syu not in mono_syu_data:
            mono_syu_data[mono_syu] = {}
            period_totals[mono_syu] = {'total_inject': 0, 'total_defect': 0}
        
        # 不良率計算（パーセント）
        defect_rate = 0
        if row.total_inject and row.total_inject > 0:
            defect_rate = round((row.total_defect / row.total_inject) * 100, 2)
        
        # モノマー種別データ
        mono_syu_data[mono_syu][date_str] = {
            'total_inject': float(row.total_inject or 0),
            'total_defect': float(row.total_defect or 0),
            'defect_rate': defect_rate
        }
        
        # 期間合計の更新
        period_totals[mono_syu]['total_inject'] += float(row.total_inject or 0)
        period_totals[mono_syu]['total_defect'] += float(row.total_defect or 0)
        
        # 合計データの更新
        if date_str not in total_data:
            total_data[date_str] = {'total_inject': 0, 'total_defect': 0}
        total_data[date_str]['total_inject'] += float(row.total_inject or 0)
        total_data[date_str]['total_defect'] += float(row.total_defect or 0)
    
    # 合計の不良率計算
    total_rates = {}
    for date_str in dates:
        if date_str in total_data and total_data[date_str]['total_inject'] > 0:
            total_rates[date_str] = round(
                (total_data[date_str]['total_defect'] / total_data[date_str]['total_inject']) * 100, 2
            )
        else:
            total_rates[date_str] = 0
    
    # 期間合計の不良率計算
    period_total_rates = {}
    for mono_syu in period_totals:
        if period_totals[mono_syu]['total_inject'] > 0:
            period_total_rates[mono_syu] = round(
                (period_totals[mono_syu]['total_defect'] / period_totals[mono_syu]['total_inject']) * 100, 2
            )
        else:
            period_total_rates[mono_syu] = 0
    
    # 全体の期間合計
    total_period_inject = sum(total['total_inject'] for total in total_data.values())
    total_period_defect = sum(total['total_defect'] for total in total_data.values())
    total_period_rate = round((total_period_defect / total_period_inject * 100), 2) if total_period_inject > 0 else 0
    
    # Chart.js用データセット作成
    datasets = []
    colors = ['#4dc9f6', '#f67019', '#f53794', '#537bc4', '#acc236', '#166a8f', '#00a950', '#58595b', '#8549ba']
    
    # 合計データセットを追加
    datasets.append({
        'label': '合計',
        'data': [total_rates.get(date, 0) for date in dates],
        'borderColor': '#000000',
        'backgroundColor': '#00000020',
        'fill': False,
        'tension': 0.4,
        'borderWidth': 2
    })
    
    # モノマー種別データセット
    for i, mono_syu in enumerate(sorted(mono_syu_types)):
        color_index = i % len(colors)
        data_points = []
        
        for date in dates:
            if date in mono_syu_data.get(mono_syu, {}):
                data_points.append(mono_syu_data[mono_syu][date]['defect_rate'])
            else:
                data_points.append(None)
        
        datasets.append({
            'label': f'モノマー種: {mono_syu}',
            'data': data_points,
            'borderColor': colors[color_index],
            'backgroundColor': colors[color_index] + '20',
            'fill': False,
            'tension': 0.4
        })
    
    return jsonify({
        'labels': dates,
        'datasets': datasets,
        'mono_syu_data': mono_syu_data,
        'mono_syu_types': list(sorted(mono_syu_types)),
        'mono_mst': mono_mst,
        'total_rates': total_rates,
        'period_total_rates': period_total_rates,
        'total_period_rate': total_period_rate,
        'period_totals': period_totals,
        'total_period_inject': total_period_inject,
        'total_period_defect': total_period_defect,
        'csv_import_time': SetMst.get_csv_import_time()
    })

@analyse.route('/mono_syu_defect_detail')
@login_required
def mono_syu_defect_detail():
    """モノマー種別の不良詳細分析画面を表示"""
    # モノマーマスタの取得
    mono_mst = {m.MNO_SYU: {'name': m.MNO_NM, 'target': float(m.MNO_TARGET) if m.MNO_TARGET else None} 
                for m in MnoMstModel.get_all()}
    return render_template('mono_syu_defect_detail.html', title='モノマー種別不良詳細分析', mono_mst=mono_mst)

@analyse.route('/api/mono_syu_defect_detail_data')
@login_required
def mono_syu_defect_detail_data():
    """モノマー種別の不良詳細データをJSON形式で返す"""
    # フィルターパラメータ取得
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    prd_id = request.args.get('prd_id')
    mono_syu = request.args.get('mono_syu')
    
    # クエリビルド
    base_query = db.session.query(
        func.DATE(PrdRecordModel.PRR_R2_DATE).label('date'),
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
        base_query = base_query.filter(PrdRecordModel.PRR_R2_DATE >= date_from)
    if date_to:
        base_query = base_query.filter(PrdRecordModel.PRR_R2_DATE <= date_to)
    if prd_id:
        base_query = base_query.filter(PrdRecordModel.PRR_PRD_ID == prd_id)
    if mono_syu:
        base_query = base_query.filter(PrdRecordModel.PRR_MONO_SYU == mono_syu)
    base_query = base_query.filter(PrdRecordModel.PRR_CHK3_BY.isnot(None))
    
    # グループ化して結果取得
    results = base_query.group_by(
        func.DATE(PrdRecordModel.PRR_R2_DATE)
    ).order_by(
        func.DATE(PrdRecordModel.PRR_R2_DATE)
    ).all()
    
    # モノマーマスタの取得
    mono_mst = {m.MNO_SYU: {'name': m.MNO_NM, 'target': float(m.MNO_TARGET) if m.MNO_TARGET else None} 
                for m in MnoMstModel.get_all()}
    
    # 結果の整形
    dates = []
    defect_data = {}
    total_data = {}
    total_rates = {}  # total_ratesを初期化
    
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
    
    for row in results:
        date_str = row.date.strftime('%Y-%m-%d') if row.date else 'Unknown'
        mono_syu = row.PRR_MONO_SYU or 'Unknown'
        
        # 日付リストに追加
        if date_str not in dates:
            dates.append(date_str)
        
        # 不良データの初期化
        if date_str not in defect_data:
            defect_data[date_str] = {}
        
        # 各不良項目のデータを格納
        defect_data[date_str] = {
            'total_inject': float(row.total_inject or 0),
            'roll_miss': float(row.roll_miss or 0),
            'leak': float(row.leak or 0),
            'film_pull': float(row.film_pull or 0),
            'crack': float(row.crack or 0),
            'tear': float(row.tear or 0),
            'peel': float(row.peel or 0),
            'chip': float(row.chip or 0),
            'poly_crk': float(row.poly_crk or 0),
            'mold_scr': float(row.mold_scr or 0),
            'lens_scr': float(row.lens_scr or 0),
            'r1_bubble': float(row.r1_bubble or 0),
            'r2_bubble': float(row.r2_bubble or 0),
            'defect': float(row.defect or 0),
            'elution': float(row.elution or 0),
            'haze': float(row.haze or 0),
            'curl': float(row.curl or 0),
            'film_float': float(row.film_float or 0),
            'r1_defect': float(row.r1_defect or 0),
            'film_ng': float(row.film_ng or 0),
            'foreign': float(row.foreign or 0),
            'cut_waste': float(row.cut_waste or 0),
            'fiber': float(row.fiber or 0),
            'mold_dirt': float(row.mold_dirt or 0),
            'film_dirt': float(row.film_dirt or 0),
            'axis_1st': float(row.axis_1st or 0),
            'stripe_1st': float(row.stripe_1st or 0),
            'edge_defect': float(row.edge_defect or 0),
            'wash_drop': float(row.wash_drop or 0),
            'unknown': float(row.unknown or 0),
            'other_1': float(row.other_1 or 0),
            'ecc_defect': float(row.ecc_defect or 0),
            'drop': float(row.drop or 0),
            'count_err': float(row.count_err or 0),
            'suction': float(row.suction or 0),
            'axis_def': float(row.axis_def or 0),
            'color_def': float(row.color_def or 0),
            'trans_def': float(row.trans_def or 0),
            'curve_def': float(row.curve_def or 0),
            'cen_th_def': float(row.cen_th_def or 0),
            'diam_def': float(row.diam_def or 0),
            'r1_th_def': float(row.r1_th_def or 0)
        }
        
        # 合計データの更新
        if date_str not in total_data:
            total_data[date_str] = {
                'total_inject': 0,
                'total_defect': 0
            }
        total_data[date_str]['total_inject'] += float(row.total_inject or 0)
        total_data[date_str]['total_defect'] += sum([
            float(row.roll_miss or 0),
            float(row.leak or 0),
            float(row.film_pull or 0),
            float(row.crack or 0),
            float(row.tear or 0),
            float(row.peel or 0),
            float(row.chip or 0),
            float(row.poly_crk or 0),
            float(row.mold_scr or 0),
            float(row.lens_scr or 0),
            float(row.r1_bubble or 0),
            float(row.r2_bubble or 0),
            float(row.defect or 0),
            float(row.elution or 0),
            float(row.haze or 0),
            float(row.curl or 0),
            float(row.film_float or 0),
            float(row.r1_defect or 0),
            float(row.film_ng or 0),
            float(row.foreign or 0),
            float(row.cut_waste or 0),
            float(row.fiber or 0),
            float(row.mold_dirt or 0),
            float(row.film_dirt or 0),
            float(row.axis_1st or 0),
            float(row.stripe_1st or 0),
            float(row.edge_defect or 0),
            float(row.wash_drop or 0),
            float(row.unknown or 0),
            float(row.other_1 or 0),
            float(row.ecc_defect or 0),
            float(row.drop or 0),
            float(row.count_err or 0),
            float(row.suction or 0),
            float(row.axis_def or 0),
            float(row.color_def or 0),
            float(row.trans_def or 0),
            float(row.curve_def or 0),
            float(row.cen_th_def or 0),
            float(row.diam_def or 0),
            float(row.r1_th_def or 0)
        ])
    
    # 不良率の計算
    defect_rates = {}
    for date_str in dates:
        if date_str in defect_data and defect_data[date_str]['total_inject'] > 0:
            defect_rates[date_str] = {}
            total_defect = 0  # 全不良数の合計
            
            # 各不良項目の不良率を計算
            for defect_type, count in defect_data[date_str].items():
                if defect_type != 'total_inject':
                    rate = round(
                        (count / defect_data[date_str]['total_inject']) * 100, 2
                    ) if defect_data[date_str]['total_inject'] > 0 else 0
                    defect_rates[date_str][defect_type] = rate
                    total_defect += count  # 全不良数を合計
            
            # 合計不良率を計算（全不良数/注入数）
            total_rates[date_str] = round(
                (total_defect / defect_data[date_str]['total_inject']) * 100, 2
            ) if defect_data[date_str]['total_inject'] > 0 else 0
        else:
            # 注入数が0の場合、全ての不良率を0に設定
            defect_rates[date_str] = {defect_type: 0 for defect_type in defect_labels.keys()}
            total_rates[date_str] = 0

    # 合計データの更新
    for date_str in dates:
        if date_str in defect_data:
            total_data[date_str] = {
                'total_inject': defect_data[date_str]['total_inject'],
                'total_defect': sum(
                    count for defect_type, count in defect_data[date_str].items()
                    if defect_type != 'total_inject'
                )
            }

    return jsonify({
        'dates': dates,
        'defect_data': defect_data,
        'defect_rates': defect_rates,
        'defect_labels': defect_labels,
        'mono_mst': mono_mst,
        'total_data': total_data,
        'total_rates': total_rates,
        'csv_import_time': SetMst.get_csv_import_time()
    })

@analyse.route('/high_defect_rate')
@login_required
def high_defect_rate():
    """不良率が高い不良をリストアップする画面を表示"""
    # モノマーマスタの取得
    mono_mst = {m.MNO_SYU: {'name': m.MNO_NM, 'target': float(m.MNO_TARGET) if m.MNO_TARGET else None} 
                for m in MnoMstModel.get_all()}
    return render_template('high_defect_rate.html', title='不良率分析', mono_mst=mono_mst)

@analyse.route('/defect_by_item')
@login_required
def defect_by_item():
    """不良項目別のモノマー種毎の不良率分析画面を表示"""
    # モノマーマスタの取得
    mono_mst = {m.MNO_SYU: {'name': m.MNO_NM, 'target': float(m.MNO_TARGET) if m.MNO_TARGET else None} 
                for m in MnoMstModel.get_all()}
    workers = WorkerModel.query.all()
    return render_template('defect_by_item.html', title='不良項目別モノマー種毎の不良率分析', mono_mst=mono_mst, workers=workers)

@analyse.route('/api/defect_by_item_data')
@login_required
def defect_by_item_data():
    """特定の不良項目における各モノマー種別の不良率データをJSON形式で返す"""
    # フィルターパラメータ取得
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    date_from2 = request.args.get('date_from2')
    date_to2 = request.args.get('date_to2')
    defect_item = request.args.get('defect_item')
    injector = request.args.get('injector')
    injector2 = request.args.get('injector2')
    
    # 不良項目が指定されていない場合はエラー
    if not defect_item:
        return jsonify({
            'error': '不良項目が指定されていません',
            'mono_syu_data': []
        }), 400
    
    try:
        # カラム名の作成 - 全て大文字にして統一
        column_name = f'PRR_{defect_item.upper()}'
        
        # カラム名のマッピング確認（必要な場合）
        # 例：defect_itemが「r1_bubble」の場合、「PRR_R1_BUBBLE」のようになるかを確認
        log_error(f'検索カラム: {column_name}')
        
        # カラム名が存在するか確認
        if not hasattr(PrdRecordModel, column_name):
            # 大文字・小文字の区別が問題かもしれないので、すべてのカラムをチェック
            all_columns = [c.key for c in PrdRecordModel.__table__.columns]
            log_error(f'利用可能なカラム: {all_columns}')
            
            return jsonify({
                'error': f'不良項目 {defect_item} に対応するカラム {column_name} は存在しません',
                'mono_syu_data': []
            }), 400
        
        # SQLAlchemyでカラム名を動的に扱うためにカラムオブジェクトを取得
        defect_column = getattr(PrdRecordModel, column_name)
        
        # クエリビルド
        base_query = db.session.query(
            PrdRecordModel.PRR_MONO_SYU,
            func.sum(PrdRecordModel.PRR_INJECT_QTY).label('total_inject'),
            func.sum(defect_column).label('defect_count')
        )
        
        # フィルター適用
        if date_from:
            base_query = base_query.filter(PrdRecordModel.PRR_R1_IN_DATE >= date_from)
        if date_to:
            base_query = base_query.filter(PrdRecordModel.PRR_R1_IN_DATE <= date_to)
        if date_from2:
            base_query = base_query.filter(PrdRecordModel.PRR_R2_DATE >= date_from2)
        if date_to2:
            base_query = base_query.filter(PrdRecordModel.PRR_R2_DATE <= date_to2) 
        if injector:
            base_query = base_query.filter(PrdRecordModel.PRR_R1_INJECT == injector)
        if injector2:
            base_query = base_query.filter(PrdRecordModel.PRR_R2_INJECT == injector2)
            
        # グループ化して結果取得
        results = base_query.group_by(
            PrdRecordModel.PRR_MONO_SYU
        ).all()
        
        # モノマーマスタの取得
        mono_mst = {m.MNO_SYU: {'name': m.MNO_NM, 'target': float(m.MNO_TARGET) if m.MNO_TARGET else None} 
                    for m in MnoMstModel.get_all()}
        
        # 結果の整形
        mono_syu_data = []
        
        for row in results:
            mono_syu = row.PRR_MONO_SYU or 'Unknown'
            total_inject = float(row.total_inject or 0)
            defect_count = float(row.defect_count or 0)
            
            # 不良率を計算
            defect_rate = 0
            if total_inject > 0:
                defect_rate = round((defect_count / total_inject) * 100, 2)
            
            # モノマー情報を取得
            mono_name = mono_mst.get(mono_syu, {}).get('name', 'Unknown')
            target = mono_mst.get(mono_syu, {}).get('target')
            
            # すべての場合を追加（不良数が0でも表示）
            mono_syu_data.append({
                'mono_syu': mono_syu,
                'mono_name': mono_name,
                'target': target,
                'total_inject': total_inject,
                'defect_count': defect_count,
                'defect_rate': defect_rate
            })
        
        # 不良率でソート（降順）
        mono_syu_data.sort(key=lambda x: x['defect_rate'], reverse=True)
        
        return jsonify({
            'mono_syu_data': mono_syu_data,
            'mono_mst': mono_mst,
            'csv_import_time': SetMst.get_csv_import_time()
        })
    except Exception as e:
        log_error(f'不良項目別データ取得中にエラーが発生しました: {str(e)}')
        return jsonify({
            'error': f'データ取得中にエラーが発生しました: {str(e)}',
            'mono_syu_data': []
        }), 500

@analyse.route('/progress')
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

@analyse.route('/api/high_defect_rate_data')
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

@analyse.route('/mono_syu_achievement', methods=['GET'])
@login_required
def mono_syu_achievement():
    """
    モノマー種別の指示数と実績を表示する画面
    
    機能概要:
    - モノマー種別ごとの指示数、目標数、注入数、良品数を表示
    - 指示数に対する良品率と注入数に対する良品率を計算して表示
    - R1注入日で期間を指定可能
    - グループ内の全レコードで検査が完了しているものだけを表示
    
    表示項目:
    - モノマー種名: モノマー種別の名称
    - 指示数: 期間内の合計指示数
    - 目標数: 指示数 × 目標率
    - 注入数: 期間内の合計注入数
    - 良品数: 期間内の合計良品数（A品 + B品）
    - 指示良品率: 良品数 ÷ 指示数 × 100
    - 注入良品率: 良品数 ÷ 注入数 × 100
    """
    # 検索条件の取得
    r1_in_date = request.args.get('r1_in_date')
    r1_in_date2 = request.args.get('r1_in_date2')
    
    # サブクエリで検査完了の確認を行う
    subquery = db.session.query(
        SjiDatModel.SJI_PRD_ID,
        SjiDatModel.SJI_DATE,
        PrdRecordModel.PRR_MONO_SYU,
        func.sum(PrdRecordModel.PRR_INJECT_QTY).label('inj_qty'),
        func.sum(PrdRecordModel.PRR_A_GRADE + PrdRecordModel.PRR_B_GRADE).label('gd_qty'),
        func.max(SjiDatModel.SJI_QTY).label('sji_qty'),
        func.count(PrdRecordModel.PRR_CHK_DT).label('chk_count'),
        func.count().label('total_count')
    ).select_from(
        SjiDatModel
    ).join(
        PrdRecordModel,
        and_(
            SjiDatModel.SJI_PRD_ID == PrdRecordModel.PRR_PRD_ID,
            SjiDatModel.SJI_DATE == PrdRecordModel.PRR_R1_IN_DATE
        )
    ).group_by(
        SjiDatModel.SJI_PRD_ID,
        SjiDatModel.SJI_DATE,
        PrdRecordModel.PRR_MONO_SYU
    ).having(
        func.count(PrdRecordModel.PRR_CHK_DT) == func.count()  # 全レコードで検査完了
    ).subquery()
    
    # メインクエリの構築
    query = db.session.query(
        MnoMstModel.MNO_NM,
        MnoMstModel.MNO_TARGET,
        func.sum(subquery.c.sji_qty).label('total_sji_qty'),
        (func.sum(subquery.c.sji_qty) * MnoMstModel.MNO_TARGET / 100).label('target_qty'),
        func.sum(subquery.c.inj_qty).label('total_inj_qty'),
        func.sum(subquery.c.gd_qty).label('total_gd_qty')
    ).select_from(
        subquery
    ).join(
        MnoMstModel,
        MnoMstModel.MNO_SYU == subquery.c.PRR_MONO_SYU
    )
    
    # 検索条件の適用
    if r1_in_date:
        query = query.filter(subquery.c.SJI_DATE >= r1_in_date)
    if r1_in_date2:
        query = query.filter(subquery.c.SJI_DATE <= r1_in_date2)
    
    # グループ化
    query = query.group_by(
        MnoMstModel.MNO_NM,
        MnoMstModel.MNO_TARGET
    )
    
    # 結果の取得
    results = query.all()
    
    # 結果の整形
    achievement_data = []
    for result in results:
        total_sji_qty = float(result.total_sji_qty or 0)
        total_inj_qty = float(result.total_inj_qty or 0)
        total_gd_qty = float(result.total_gd_qty or 0)
        
        achievement_data.append({
            'mono_nm': result.MNO_NM,
            'mono_target': result.MNO_TARGET,
            'total_sji_qty': total_sji_qty,
            'target_qty': float(result.target_qty or 0),
            'total_inj_qty': total_inj_qty,
            'total_gd_qty': total_gd_qty,
            'gd_per_sji': round((total_gd_qty / total_sji_qty * 100), 2) if total_sji_qty > 0 else 0,
            'gd_per_inj': round((total_gd_qty / total_inj_qty * 100), 2) if total_inj_qty > 0 else 0
        })
    
    set_mst = SetMst.query.first()
    
    return render_template(
        'mono_syu_achievement.html',
        achievement_data=achievement_data,
        csv_import_time=set_mst.get_csv_import_time()
    )

@analyse.route('/mono_syu_inspection')
@login_required
def mono_syu_inspection():
    return render_template('mono_syu_inspection.html')

@analyse.route('/api/mono_syu_inspection')
@login_required
def mono_syu_inspection_data():
    try:
        # 検索期間の取得
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({'error': '検索期間が指定されていません'}), 400
        
        # 日付文字列をdatetimeオブジェクトに変換
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)  # 終了日の23:59:59まで含める
        
        # モノマー種別マスタを取得
        mono_mst = MnoMstModel.get_all()
        
        result_data = []
        
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
        
        for mono in mono_mst:
            # 期間内の実績データを取得
            query = db.session.query(
                PrdRecordModel.PRR_MONO_SYU,
                func.sum(PrdRecordModel.PRR_INJECT_QTY).label('total_inject'),
                func.sum(PrdRecordModel.PRR_A_GRADE + PrdRecordModel.PRR_B_GRADE).label('good_count'),
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
            ).filter(
                PrdRecordModel.PRR_CHK_DT >= start_date,
                PrdRecordModel.PRR_CHK_DT < end_date,
                PrdRecordModel.PRR_MONO_SYU == mono.MNO_SYU,
                PrdRecordModel.PRR_CHK3_BY.isnot(None)  # 三次検査完了データのみ
            )
            
            # 結果取得
            result = query.first()
            
            if not result or not result.total_inject:
                continue
            
            total_shots = float(result.total_inject or 0)
            good_count = float(result.good_count or 0)
            
            # 良品率の計算
            good_rate = (good_count / total_shots * 100) if total_shots > 0 else 0
            
            # 目標値との差分（decimal.Decimalをfloatに変換）
            target = float(mono.MNO_TARGET) if mono.MNO_TARGET is not None else 0
            difference = good_rate - target
            
            # 不良項目の集計
            defect_counts = {}
            for key in defect_labels.keys():
                count = float(getattr(result, key) or 0)
                if count > 0:
                    defect_counts[key] = {
                        'name': defect_labels[key],
                        'count': int(count),
                        'rate': (count / total_shots * 100)
                    }
            
            # 上位不良項目の取得（上位5件）
            top_defects = []
            for key, data in sorted(defect_counts.items(), key=lambda x: x[1]['count'], reverse=True)[:5]:
                top_defects.append({
                    'name': data['name'],
                    'count': data['count'],
                    'rate': data['rate']
                })
                
            csv_import_time = SetMst.query.first().get_csv_import_time()
            
            # 結果データの追加
            result_data.append({
                'mono_name': mono.MNO_NM,
                'target': target,
                'good_rate': good_rate,
                'difference': difference,
                'total_shots': int(total_shots),
                'good_count': int(good_count),
                'defect_count': int(total_shots - good_count),
                'top_defects': top_defects,
                'csv_import_time': csv_import_time
            })
        
        # 良品率の降順でソート
        result_data.sort(key=lambda x: x['good_rate'], reverse=True)
        
        return jsonify(result_data)
    
    except Exception as e:
        log_error(f'モノマー種別良品率分析データの取得中にエラーが発生しました: {str(e)}')
        return jsonify({'error': 'データの取得中にエラーが発生しました'}), 500

@analyse.route('/fmc_defect_analysis')
@login_required
def fmc_defect_analysis():
    """膜カット不良率分析画面を表示"""
    # 各種マスタデータを取得
    monomers = KbnMst.get_kbn_list('MMNO')  # モノマー
    items = KbnMst.get_kbn_list('MITM')     # アイテム
    cut_menus = KbnMst.get_kbn_list('MCUT') # カットメニュー
    film_curves = KbnMst.get_kbn_list('MCRB') # 膜カーブ
    colors = KbnMst.get_kbn_list('MCLR')    # 色
    
    return render_template('fmc_defect_analysis.html',
                         monomers=monomers,
                         items=items,
                         cut_menus=cut_menus,
                         film_curves=film_curves,
                         colors=colors)

@analyse.route('/api/fmc_defect_analysis')
@login_required
def api_fmc_defect_analysis():
    """膜カット不良率分析のAPIエンドポイント"""
    try:
        # パラメータの取得と変換
        start_date = request.args.get('start_date')
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
            
        end_date = request.args.get('end_date')
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
            
        month = request.args.get('month')
        if month:
            month = int(month)
            
        r1_inj_date = request.args.get('r1_inj_date')
        if r1_inj_date:
            r1_inj_date = datetime.strptime(r1_inj_date, '%Y-%m-%d')
            
        monomer = request.args.get('monomer')
        if monomer:
            monomer = int(monomer)
            
        anneal_no = request.args.get('anneal_no')
        if anneal_no:
            anneal_no = int(anneal_no)
            
        cut_mach_no = request.args.get('cut_mach_no')
        if cut_mach_no:
            cut_mach_no = int(cut_mach_no)
            
        item = request.args.get('item')
        if item:
            item = int(item)
            
        cut_menu = request.args.get('cut_menu')
        if cut_menu:
            cut_menu = int(cut_menu)
            
        film_proc_dt = request.args.get('film_proc_dt')
        if film_proc_dt:
            film_proc_dt = datetime.strptime(film_proc_dt, '%Y-%m-%d')
            
        cr_film = request.args.get('cr_film')
        if cr_film:
            cr_film = int(cr_film)
            
        heat_proc_dt = request.args.get('heat_proc_dt')
        if heat_proc_dt:
            heat_proc_dt = datetime.strptime(heat_proc_dt, '%Y-%m-%d')
            
        film_curve = request.args.get('film_curve')
        if film_curve:
            film_curve = int(film_curve)
            
        color = request.args.get('color')
        if color:
            color = int(color)
            
        # 不良率分析の実行
        result = FmcDat.get_defect_analysis(
            start_date=start_date,
            end_date=end_date,
            month=month,
            r1_inj_date=r1_inj_date,
            monomer=monomer,
            anneal_no=anneal_no,
            cut_mach_no=cut_mach_no,
            item=item,
            cut_menu=cut_menu,
            film_proc_dt=film_proc_dt,
            cr_film=cr_film,
            heat_proc_dt=heat_proc_dt,
            film_curve=film_curve,
            color=color
        )
        
        if result is None:
            return jsonify({'error': '分析中にエラーが発生しました'}), 500
            
        return jsonify(result)
        
    except Exception as e:
        log_error(f'不良率分析APIでエラーが発生しました: {str(e)}')
        return jsonify({'error': str(e)}), 500

@analyse.route('/fmc_defect_detail_analysis', methods=['GET'])
@login_required
def fmc_defect_detail_analysis():
    """膜カット不良詳細分析画面を表示"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    # 区分マスタ情報の取得
    monomers = KbnMst.get_kbn_list('MMNO')  # モノマー
    items = KbnMst.get_kbn_list('MITM')     # アイテム
    colors = KbnMst.get_kbn_list('MCLR')    # 色
    film_curves = KbnMst.get_kbn_list('MCRB')  # 膜カーブ
    
    return render_template('fmc_defect_detail_analysis.html', 
                         start_date=start_date, 
                         end_date=end_date,
                         monomers=monomers,
                         items=items,
                         colors=colors,
                         film_curves=film_curves)

@analyse.route('/api/fmc_defect_detail_data')
@login_required
def api_fmc_defect_detail_data():
    """膜カット不良詳細データの日付×不良項目クロス集計データをJSONで返す"""
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        monomer = request.args.get('monomer')
        item = request.args.get('item')
        color = request.args.get('color')
        film_curve = request.args.get('film_curve')
        
        # カット工程不良項目リスト
        cut_keys = [
            'FMC_CUT_FOREIGN', 'FMC_CUT_WRINKLE', 'FMC_CUT_WAVE', 'FMC_CUT_ERR', 
            'FMC_CUT_CRACK', 'FMC_CUT_SCRATCH', 'FMC_CUT_OTHERS'
        ]
        
        # 洗浄工程不良項目リスト
        wash_keys = [
            'FMC_WASH_WRINKLE', 'FMC_WASH_SCRATCH', 'FMC_WASH_FOREIGN', 'FMC_WASH_ACETONE', 
            'FMC_WASH_ERR', 'FMC_WASH_CUT_ERR', 'FMC_WASH_OTHERS'
        ]
        
        # 不良項目ラベル
        cut_defect_labels = {
            'FMC_CUT_FOREIGN': 'カットブツ',
            'FMC_CUT_WRINKLE': 'カットシワ',
            'FMC_CUT_WAVE': 'カットウエーブ',
            'FMC_CUT_ERR': 'カットミス',
            'FMC_CUT_CRACK': 'カットサケ',
            'FMC_CUT_SCRATCH': 'カットキズ',
            'FMC_CUT_OTHERS': 'カットその他'
        }
        
        wash_defect_labels = {
            'FMC_WASH_WRINKLE': '洗浄シワ',
            'FMC_WASH_SCRATCH': '洗浄キズ',
            'FMC_WASH_FOREIGN': '洗浄イブツ',
            'FMC_WASH_ACETONE': '洗浄アセトン',
            'FMC_WASH_ERR': '洗浄ミス',
            'FMC_WASH_CUT_ERR': '洗浄カットミス',
            'FMC_WASH_OTHERS': '洗浄その他'
        }
        
        # クエリビルド
        query = db.session.query(
            func.DATE(FmcDat.FMC_CUT_DATE).label('date'),
            *[func.sum(getattr(FmcDat, k)).label(k) for k in cut_keys + wash_keys],
            func.sum(FmcDat.FMC_INPUT_QTY).label('total_input'),
            func.sum(FmcDat.FMC_GOOD_QTY).label('total_good')
        )
        
        if start_date:
            query = query.filter(FmcDat.FMC_CUT_DATE >= start_date)
        if end_date:
            query = query.filter(FmcDat.FMC_CUT_DATE <= end_date)
        if monomer:
            query = query.filter(FmcDat.FMC_MONOMER == monomer)
        if item:
            query = query.filter(FmcDat.FMC_ITEM == item)
        if color:
            query = query.filter(FmcDat.FMC_COLOR == color)
        if film_curve:
            query = query.filter(FmcDat.FMC_FILM_CURVE == film_curve)
            
        query = query.group_by(func.DATE(FmcDat.FMC_CUT_DATE))
        query = query.order_by(func.DATE(FmcDat.FMC_CUT_DATE))
        
        results = query.all()

        # データ整形
        dates = []
        total_data = {}
        cut_defect_rates = {}
        wash_defect_rates = {}
        cut_total_rates = {}
        wash_total_rates = {}

        for row in results:
            date_str = row.date.strftime('%Y-%m-%d') if row.date else 'Unknown'
            dates.append(date_str)
            
            total_input = float(row.total_input or 0)
            total_good = float(row.total_good or 0)
            
            # カット工程不良データの集計
            cut_defect_sum = 0
            cut_defect_rates[date_str] = {}
            
            for defect in cut_keys:
                value = float(getattr(row, defect) or 0)
                cut_defect_sum += value
                cut_defect_rates[date_str][defect] = round((value / total_input * 100) if total_input else 0, 2)
            
            # 洗浄工程不良データの集計
            wash_defect_sum = 0
            wash_defect_rates[date_str] = {}
            
            for defect in wash_keys:
                value = float(getattr(row, defect) or 0)
                wash_defect_sum += value
                wash_defect_rates[date_str][defect] = round((value / total_good * 100) if total_good else 0, 2)
            
            total_data[date_str] = {
                'total_input': total_input,
                'total_good': total_good
            }
            
            # 合計不良率の計算
            cut_total_rates[date_str] = round((cut_defect_sum / total_input * 100) if total_input else 0, 2)
            wash_total_rates[date_str] = round((wash_defect_sum / total_good * 100) if total_good else 0, 2)

        return jsonify({
            'dates': dates,
            'cut_defect_labels': cut_defect_labels,
            'wash_defect_labels': wash_defect_labels,
            'cut_defect_rates': cut_defect_rates,
            'wash_defect_rates': wash_defect_rates,
            'cut_total_rates': cut_total_rates,
            'wash_total_rates': wash_total_rates,
            'total_data': total_data
        })
        
    except Exception as e:
        log_error(f'膜カット不良詳細データのAPIエンドポイントでエラーが発生しました: {str(e)}')
        return jsonify({'error': str(e)}), 500

@analyse.route('/fmc_monomer_summary')
@login_required
def fmc_monomer_summary():
    """FMCモノマー別集計画面"""
    return render_template('fmc_monomer_summary.html')

@analyse.route('/api/fmc_monomer_summary')
@login_required
def api_fmc_monomer_summary():
    """FMCモノマー別集計APIエンドポイント"""
    try:
        # 検索条件の取得
        cut_date_start = request.args.get('cut_date_start')
        cut_date_end = request.args.get('cut_date_end')
        
        # 日付の変換
        start_date = None
        end_date = None
        
        if cut_date_start:
            start_date = datetime.strptime(cut_date_start, '%Y-%m-%d').date()
        
        if cut_date_end:
            end_date = datetime.strptime(cut_date_end, '%Y-%m-%d').date()
        
        # モノマー別集計データの取得
        summary_data = FmcDat.get_monomer_summary(start_date, end_date)
        
        # 合計値の計算
        total_input = sum(item['input_qty'] for item in summary_data)
        total_good = sum(item['good_qty'] for item in summary_data)
        total_pass = sum(item['pass_qty'] for item in summary_data)
        
        # 全体の歩留率計算
        overall_good_rate = (total_good / total_input * 100) if total_input > 0 else 0
        overall_pass_rate = (total_pass / total_good * 100) if total_good > 0 else 0
        overall_total_rate = (total_pass / total_input * 100) if total_input > 0 else 0
        
        result = {
            'summary_data': summary_data,
            'totals': {
                'input_qty': total_input,
                'good_qty': total_good,
                'pass_qty': total_pass,
                'good_rate': round(overall_good_rate, 2),
                'pass_rate': round(overall_pass_rate, 2),
                'total_rate': round(overall_total_rate, 2)
            }
        }
        
        return jsonify(result)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@analyse.route('/fmc_defect_monthly_analysis')
@login_required
def fmc_defect_monthly_analysis():
    """膜カット不良データ月別分析画面を表示"""
    # 区分マスタ情報の取得
    monomers = KbnMst.get_kbn_list('MMNO')  # モノマー
    items = KbnMst.get_kbn_list('MITM')     # アイテム
    colors = KbnMst.get_kbn_list('MCLR')    # 色
    film_curves = KbnMst.get_kbn_list('MCRB')  # 膜カーブ
    
    return render_template('fmc_defect_monthly_analysis.html',
                         monomers=monomers,
                         items=items,
                         colors=colors,
                         film_curves=film_curves)

@analyse.route('/api/fmc_defect_monthly_data')
@login_required
def api_fmc_defect_monthly_data():
    """膜カット不良データの月別集計データをJSONで返す"""
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        monomer = request.args.get('monomer')
        item = request.args.get('item')
        color = request.args.get('color')
        film_curve = request.args.get('film_curve')
        
        # カット工程不良項目リスト
        cut_keys = [
            'FMC_CUT_FOREIGN', 'FMC_CUT_WRINKLE', 'FMC_CUT_WAVE', 'FMC_CUT_ERR', 
            'FMC_CUT_CRACK', 'FMC_CUT_SCRATCH', 'FMC_CUT_OTHERS'
        ]
        
        # 洗浄工程不良項目リスト
        wash_keys = [
            'FMC_WASH_WRINKLE', 'FMC_WASH_SCRATCH', 'FMC_WASH_FOREIGN', 'FMC_WASH_ACETONE', 
            'FMC_WASH_ERR', 'FMC_WASH_CUT_ERR', 'FMC_WASH_OTHERS'
        ]
        
        # 不良項目ラベル
        cut_defect_labels = {
            'FMC_CUT_FOREIGN': 'カットブツ',
            'FMC_CUT_WRINKLE': 'カットシワ',
            'FMC_CUT_WAVE': 'カットウエーブ',
            'FMC_CUT_ERR': 'カットミス',
            'FMC_CUT_CRACK': 'カットサケ',
            'FMC_CUT_SCRATCH': 'カットキズ',
            'FMC_CUT_OTHERS': 'カットその他'
        }
        
        wash_defect_labels = {
            'FMC_WASH_WRINKLE': '洗浄シワ',
            'FMC_WASH_SCRATCH': '洗浄キズ',
            'FMC_WASH_FOREIGN': '洗浄イブツ',
            'FMC_WASH_ACETONE': '洗浄アセトン',
            'FMC_WASH_ERR': '洗浄ミス',
            'FMC_WASH_CUT_ERR': '洗浄カットミス',
            'FMC_WASH_OTHERS': '洗浄その他'
        }
        
        # クエリビルド（カット日基準で月別集計）
        query = db.session.query(
            func.date_format(FmcDat.FMC_CUT_DATE, '%Y-%m').label('month'),
            *[func.sum(getattr(FmcDat, k)).label(k) for k in cut_keys + wash_keys],
            func.sum(FmcDat.FMC_INPUT_QTY).label('total_input'),
            func.sum(FmcDat.FMC_GOOD_QTY).label('total_good')
        )
        
        # フィルター適用
        if start_date:
            query = query.filter(FmcDat.FMC_CUT_DATE >= start_date)
        if end_date:
            query = query.filter(FmcDat.FMC_CUT_DATE <= end_date)
        if monomer:
            query = query.filter(FmcDat.FMC_MONOMER == monomer)
        if item:
            query = query.filter(FmcDat.FMC_ITEM == item)
        if color:
            query = query.filter(FmcDat.FMC_COLOR == color)
        if film_curve:
            query = query.filter(FmcDat.FMC_FILM_CURVE == film_curve)
            
        # NULLでない値のみ対象、色目を除く
        query = query.filter(FmcDat.FMC_CUT_DATE.isnot(None))
        query = query.filter(FmcDat.FMC_INPUT_QTY.isnot(None))
        query = query.filter(FmcDat.FMC_MONOMER != 8)  # 色目を除く
        query = query.filter(FmcDat.FMC_CUT_MENU != 62)  # 色目を除く
        
        # 月別グループ化とソート
        query = query.group_by(func.date_format(FmcDat.FMC_CUT_DATE, '%Y-%m'))
        query = query.order_by(func.date_format(FmcDat.FMC_CUT_DATE, '%Y-%m'))
        
        results = query.all()

        # データ整形
        months = []
        total_data = {}
        cut_defect_data = {}
        wash_defect_data = {}
        cut_total_data = {}
        wash_total_data = {}

        for row in results:
            month = row.month
            if month:
                months.append(month)
                
                total_input = float(row.total_input or 0)
                total_good = float(row.total_good or 0)
                
                total_data[month] = {
                    'total_input': total_input,
                    'total_good': total_good
                }
                
                # カット工程不良データの集計
                cut_defect_sum = 0
                cut_defect_data[month] = {}
                
                for defect in cut_keys:
                    value = float(getattr(row, defect) or 0)
                    cut_defect_sum += value
                    cut_defect_data[month][defect] = {
                        'count': value,
                        'rate': round((value / total_input * 100) if total_input > 0 else 0, 2)
                    }
                
                # 洗浄工程不良データの集計
                wash_defect_sum = 0
                wash_defect_data[month] = {}
                
                for defect in wash_keys:
                    value = float(getattr(row, defect) or 0)
                    wash_defect_sum += value
                    wash_defect_data[month][defect] = {
                        'count': value,
                        'rate': round((value / total_good * 100) if total_good > 0 else 0, 2)
                    }
                
                # 合計不良率の計算
                cut_total_data[month] = round((cut_defect_sum / total_input * 100) if total_input > 0 else 0, 2)
                wash_total_data[month] = round((wash_defect_sum / total_good * 100) if total_good > 0 else 0, 2)

        return jsonify({
            'months': months,
            'cut_defect_labels': cut_defect_labels,
            'wash_defect_labels': wash_defect_labels,
            'cut_defect_data': cut_defect_data,
            'wash_defect_data': wash_defect_data,
            'cut_total_data': cut_total_data,
            'wash_total_data': wash_total_data,
            'total_data': total_data
        })
        
    except Exception as e:
        log_error(f'膜カット不良月別データAPIでエラーが発生しました: {str(e)}')
        return jsonify({'error': str(e)}), 500
