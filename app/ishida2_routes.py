import csv
import io
from datetime import datetime, timedelta
import locale
from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app as app, jsonify

from .models import db, SetMst

from .utils import log_error
from app.ishida_models import HdcDat, SpcDat
import tempfile
import os
from flask_login import login_required
from sqlalchemy import func

ishida2 = Blueprint('ishida2', __name__)

@ishida2.route('/import_spc', methods=['GET', 'POST'])
@login_required
def import_spc():
    if request.method == 'POST':
        if 'csv_file' not in request.files:
            flash('ファイルがありません')
            return redirect(request.url)
        file = request.files['csv_file']
        if file.filename == '':
            flash('ファイルが選択されていません')
            return redirect(request.url)
        if not file.filename.endswith('.csv'):
            flash('CSVファイルのみアップロード可能です')
            return redirect(request.url)
        if file:
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
                    temp_path = temp_file.name
                    file.save(temp_path)
                try:
                    success, message = SpcDat.import_from_csv(temp_path, encoding='shift_jis')
                    if success:
                        flash(message)
                        return redirect(request.url)
                    else:
                        flash(f'インポート中にエラーが発生しました: {message}')
                        return redirect(request.url)
                finally:
                    try:
                        os.unlink(temp_path)
                    except Exception as e:
                        log_error(f'一時ファイルの削除中にエラーが発生しました: {str(e)}')
            except Exception as e:
                log_error(f'CSVファイル処理中にエラーが発生しました: {str(e)}')
                flash('CSVファイルの処理中にエラーが発生しました')
                return redirect(request.url)
    return render_template('upload_spc.html')

@ishida2.route('/import_hdc', methods=['GET', 'POST'])
@login_required
def import_hdc():
    if request.method == 'POST':
        if 'csv_file' not in request.files:
            flash('ファイルがありません')
            return redirect(request.url)
        file = request.files['csv_file']
        if file.filename == '':
            flash('ファイルが選択されていません')
            return redirect(request.url)
        if not file.filename.endswith('.csv'):
            flash('CSVファイルのみアップロード可能です')
            return redirect(request.url)
        if file:
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
                    temp_path = temp_file.name
                    file.save(temp_path)
                try:
                    success, message = HdcDat.import_from_csv(temp_path, encoding='shift_jis')
                    if success:
                        flash(message)
                        return redirect(request.url)
                    else:
                        flash(f'インポート中にエラーが発生しました: {message}')
                        return redirect(request.url)
                finally:
                    try:
                        os.unlink(temp_path)
                    except Exception as e:
                        log_error(f'一時ファイルの削除中にエラーが発生しました: {str(e)}')
            except Exception as e:
                log_error(f'CSVファイル処理中にエラーが発生しました: {str(e)}')
                flash('CSVファイルの処理中にエラーが発生しました')
                return redirect(request.url)
    return render_template('upload_hdc.html')

@ishida2.route('/hdc_defect_analysis', methods=['GET', 'POST'])
def hdc_defect_analysis():
    from datetime import datetime
    start_date = end_date = None
    ct_type = None
    color = None
    results = None
    error = None
    # 不良項目リスト
    defect_items = [
        ('HDC_PRE_FOREIGN', '硬化前ブツ'),
        ('HDC_PRE_DROP', '硬化前タレ'),
        ('HDC_PRE_CHIP', '硬化前カケ'),
        ('HDC_PRE_STREAK', '硬化前スジ'),
        ('HDC_PRE_OTHERS', '硬化前その他'),
        ('HDC_TRS_BASE_FAIL', '透過基材不良'),
        ('HDC_TRS_FOREIGN', '透過ブツ'),
        ('HDC_TRS_INCL', '透過イブツ'),
        ('HDC_TRS_SCRATCH', '透過キズ'),
        ('HDC_TRS_COAT_FAIL', '透過コート不良'),
        ('HDC_TRS_DROP', '透過タレ'),
        ('HDC_TRS_STREAK', '透過スジ'),
        ('HDC_TRS_DIRT', '透過汚れ'),
        ('HDC_TRS_CHIP', '透過カケ'),
        ('HDC_PRJ_BASE', '投影基材'),
        ('HDC_PRJ_FOREIGN', '投影ブツ'),
        ('HDC_PRJ_DUST', '投影ごみ'),
        ('HDC_PRJ_SCRATCH', '投影キズ'),
        ('HDC_PRJ_DROP', '投影タレ'),
        ('HDC_PRJ_CHIP', '投影カケ'),
        ('HDC_PRJ_STREAK', '投影スジ'),
    ]
    # デフォルト不良項目
    selected_item = request.form.get('defect_item') if request.method == 'POST' else 'HDC_PRE_FOREIGN'
    if request.method == 'POST':
        try:
            start_date_str = request.form.get('start_date')
            end_date_str = request.form.get('end_date')
            ct_type = request.form.get('ct_type')
            color = request.form.get('color')
            if start_date_str:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            if end_date_str:
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
            results = HdcDat.get_defect_analysis(start_date, end_date, ct_type, color)
        except Exception as e:
            error = f'日付の形式が正しくありません: {str(e)}'
    # 日ごと合計不良率・各不良項目不良率
    daily_summary = HdcDat.get_daily_defect_summary(start_date, end_date, ct_type, color)
    # HDC_TIMESごと日別集計
    daily_by_times = HdcDat.get_daily_defect_summary_by_times(start_date, end_date, ct_type, color)
    return render_template('hdc_defect_analysis.html', results=results, error=error, start_date=start_date, end_date=end_date, defect_items=defect_items, selected_item=selected_item, daily_summary=daily_summary, daily_by_times=daily_by_times)

@ishida2.route('/spc_defect_analysis', methods=['GET', 'POST'])
def spc_defect_analysis():
    from datetime import datetime
    start_date = end_date = None
    ct_type = None
    color = None
    results = None
    error = None
    daily_by_times = None
    if request.method == 'POST':
        try:
            start_date_str = request.form.get('start_date')
            end_date_str = request.form.get('end_date')
            ct_type = request.form.get('ct_type')
            color = request.form.get('color')
            if start_date_str:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            if end_date_str:
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
            results = SpcDat.get_defect_analysis(start_date, end_date, ct_type, color)
        except Exception as e:
            error = f'日付の形式が正しくありません: {str(e)}'
    # SPC_TIMESごと日別集計
    daily_by_times = SpcDat.get_daily_defect_summary_by_times(start_date, end_date, ct_type, color)
    return render_template('spc_defect_analysis.html',
                           start_date=start_date,
                           end_date=end_date,
                           results=results,
                           error=error,
                           daily_by_times=daily_by_times)

@ishida2.route('/api/spc_defect_detail_data')
def api_spc_defect_detail_data():
    """日付×不良項目クロス集計データをJSONで返す（routes.pyで直接集計）"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    ct_type = request.args.get('ct_type')
    color = request.args.get('color')
    # 不良項目リスト
    pre_keys = [
        'SPC_PRE_BLK_DUST', 'SPC_PRE_WHT_DUST', 'SPC_PRE_EDGE_FAIL', 'SPC_PRE_COAT_FAIL',
        'SPC_PRE_DARK_SPOT', 'SPC_PRE_SNAIL', 'SPC_PRE_MIST', 'SPC_PRE_WRINKLE',
        'SPC_PRE_BRRL_BUB', 'SPC_PRE_STICK', 'SPC_PRE_TRBL_FIL', 'SPC_PRE_BASE_FIL'
    ]
    pst_keys = [
        'SPC_PST_SCRATCH', 'SPC_PST_COAT_FIL', 'SPC_PST_SNAIL', 'SPC_PST_DARK_SPOT',
        'SPC_PST_WRINKLE', 'SPC_PST_BUBBLE', 'SPC_PST_EDGE_FAIL', 'SPC_PST_WHT_DUST',
        'SPC_PST_BLK_DUST', 'SPC_PST_STICK', 'SPC_PST_PRM_STICK', 'SPC_PST_BASE_FAIL', 'SPC_PST_OTHERS'
    ]
    defect_labels = {
        'pre': {
            'SPC_PRE_BLK_DUST': '硬化前黒ブツ', 'SPC_PRE_WHT_DUST': '硬化前白ブツ', 'SPC_PRE_EDGE_FAIL': '硬化前外周不良', 'SPC_PRE_COAT_FAIL': '硬化前コート不良',
            'SPC_PRE_DARK_SPOT': '硬化前ダークスポット', 'SPC_PRE_SNAIL': '硬化前スネイル', 'SPC_PRE_MIST': '硬化前ミスト', 'SPC_PRE_WRINKLE': '硬化前シワ',
            'SPC_PRE_BRRL_BUB': '硬化前バレル泡', 'SPC_PRE_STICK': '硬化前付着物', 'SPC_PRE_TRBL_FIL': '硬化前トラブル不', 'SPC_PRE_BASE_FIL': '硬化前基材不良'
        },
        'pst': {
            'SPC_PST_SCRATCH': '硬化後キズ', 'SPC_PST_COAT_FIL': '硬化後コート不良', 'SPC_PST_SNAIL': '硬化後スネイル', 'SPC_PST_DARK_SPOT': '硬化後ダークスポット',
            'SPC_PST_WRINKLE': '硬化後シワ', 'SPC_PST_BUBBLE': '硬化後泡', 'SPC_PST_EDGE_FAIL': '硬化後外周不良', 'SPC_PST_WHT_DUST': '硬化後白ブツ',
            'SPC_PST_BLK_DUST': '硬化後黒ブツ', 'SPC_PST_STICK': '硬化後付着物', 'SPC_PST_PRM_STICK': '硬化後プライマー付着跡', 'SPC_PST_BASE_FAIL': '硬化後基材不良', 'SPC_PST_OTHERS': '硬化後その他'
        }
    }
    # クエリビルド
    query = db.session.query(
        func.DATE(SpcDat.SPC_COAT_DATE).label('date'),
        *[func.sum(getattr(SpcDat, k)).label(k) for k in pre_keys + pst_keys],
        func.sum(SpcDat.SPC_SHEETS).label('total_sheets'),
        func.sum(SpcDat.SPC_PRE_GOOD_QTY).label('total_pre_good')
    )
    if start_date:
        query = query.filter(SpcDat.SPC_COAT_DATE >= start_date)
    if end_date:
        query = query.filter(SpcDat.SPC_COAT_DATE <= end_date)
    if ct_type:
        query = query.filter(SpcDat.SPC_TYPE == ct_type)
    if color:
        query = query.filter(SpcDat.SPC_COAT_COLOR == color)
    query = query.group_by(func.DATE(SpcDat.SPC_COAT_DATE))
    query = query.order_by(func.DATE(SpcDat.SPC_COAT_DATE))
    query = query.filter(SpcDat.SPC_TYPE != 4)
    results = query.all()

    # データ整形
    dates = []
    total_data = {}
    defect_rates = {}
    total_rates = {}

    # defect_labelsをpre+pstで平坦化
    defect_labels = {**defect_labels['pre'], **defect_labels['pst']}

    for row in results:
        date_str = row.date.strftime('%Y-%m-%d') if row.date else 'Unknown'
        dates.append(date_str)
        
        total_sheets = float(row.total_sheets or 0)
        total_pre_good = float(row.total_pre_good or 0)
        
        # 不良データの集計
        defect_sum = 0
        defect_rates[date_str] = {}
        
        for defect in pre_keys + pst_keys:
            value = float(getattr(row, defect) or 0)
            defect_sum += value
            defect_rates[date_str][defect] = round((value / total_sheets * 100) if total_sheets else 0, 2)
        
        total_data[date_str] = {
            'total_sheets': total_sheets,
            'total_pre_good': total_pre_good
        }
        
        # 合計不良率の計算
        total_rates[date_str] = round((defect_sum / total_sheets * 100) if total_sheets else 0, 2)

    return jsonify({
        'dates': dates,
        'defect_labels': defect_labels,
        'defect_rates': defect_rates,
        'total_rates': total_rates,
        'total_data': total_data
    })

@ishida2.route('/api/hdc_defect_detail_data')
def api_hdc_defect_detail_data():
    """HDC_TIMESごとの日付×不良項目クロス集計データをJSONで返す（routes.pyで直接集計）"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    ct_type = request.args.get('ct_type')
    color = request.args.get('color')
    # 不良項目リスト
    defect_keys = [
        'HDC_PRE_FOREIGN', 'HDC_PRE_DROP', 'HDC_PRE_CHIP', 'HDC_PRE_STREAK', 'HDC_PRE_OTHERS',
        'HDC_TRS_BASE_FAIL', 'HDC_TRS_FOREIGN', 'HDC_TRS_INCL', 'HDC_TRS_SCRATCH', 'HDC_TRS_COAT_FAIL',
        'HDC_TRS_DROP', 'HDC_TRS_STREAK', 'HDC_TRS_DIRT', 'HDC_TRS_CHIP', 'HDC_PRJ_BASE',
        'HDC_PRJ_FOREIGN', 'HDC_PRJ_DUST', 'HDC_PRJ_SCRATCH', 'HDC_PRJ_DROP', 'HDC_PRJ_CHIP', 'HDC_PRJ_STREAK'
    ]
    defect_labels = {
        'HDC_PRE_FOREIGN': '硬化前ブツ', 'HDC_PRE_DROP': '硬化前タレ', 'HDC_PRE_CHIP': '硬化前カケ', 'HDC_PRE_STREAK': '硬化前スジ', 'HDC_PRE_OTHERS': '硬化前その他',
        'HDC_TRS_BASE_FAIL': '透過基材不良', 'HDC_TRS_FOREIGN': '透過ブツ', 'HDC_TRS_INCL': '透過イブツ', 'HDC_TRS_SCRATCH': '透過キズ', 'HDC_TRS_COAT_FAIL': '透過コート不良',
        'HDC_TRS_DROP': '透過タレ', 'HDC_TRS_STREAK': '透過スジ', 'HDC_TRS_DIRT': '透過汚れ', 'HDC_TRS_CHIP': '透過カケ', 'HDC_PRJ_BASE': '投影基材',
        'HDC_PRJ_FOREIGN': '投影ブツ', 'HDC_PRJ_DUST': '投影ごみ', 'HDC_PRJ_SCRATCH': '投影キズ', 'HDC_PRJ_DROP': '投影タレ', 'HDC_PRJ_CHIP': '投影カケ', 'HDC_PRJ_STREAK': '投影スジ'
    }
    # クエリビルド
    query = db.session.query(
        func.DATE(HdcDat.HDC_COAT_DATE).label('date'),
        HdcDat.HDC_TIMES,
        *[func.sum(getattr(HdcDat, k)).label(k) for k in defect_keys],
        func.sum(HdcDat.HDC_COAT_CNT).label('total_cnt')
    )
    if start_date:
        query = query.filter(HdcDat.HDC_COAT_DATE >= start_date)
    if end_date:
        query = query.filter(HdcDat.HDC_COAT_DATE <= end_date)
    if ct_type:
        query = query.filter(HdcDat.HDC_TYPE == ct_type)
    if color:
        query = query.filter(HdcDat.HDC_COLOR == color)
    query = query.group_by(func.DATE(HdcDat.HDC_COAT_DATE), HdcDat.HDC_TIMES)
    query = query.order_by(func.DATE(HdcDat.HDC_COAT_DATE), HdcDat.HDC_TIMES)
    query = query.filter(HdcDat.HDC_TYPE != 5)
    results = query.all()
    # データ整形
    dates = set()
    times_list = set()
    summary = {}
    for row in results:
        date_str = row.date.strftime('%Y-%m-%d') if row.date else 'Unknown'
        times = str(row.HDC_TIMES)
        dates.add(date_str)
        times_list.add(times)
        if times not in summary:
            summary[times] = {}
        summary[times][date_str] = {
            'items': {k: float(getattr(row, k) or 0) for k in defect_keys},
            'total_cnt': float(row.total_cnt or 0)
        }
    date_list = sorted(dates)
    times_list = sorted(times_list)
    # 合計値（日付ごと）
    total_data = {}
    for date in date_list:
        total_cnt = 0
        for times in times_list:
            v = summary.get(times, {}).get(date, {})
            total_cnt += v.get('total_cnt', 0)
        total_data[date] = {'total_cnt': total_cnt}
    return jsonify({
        'dates': date_list,
        'times_list': times_list,
        'summary': summary,
        'defect_labels': defect_labels,
        'total_data': total_data
    })

@ishida2.route('/spc_type_analysis')
@login_required
def spc_type_analysis():
    """スピンコート種類別分析画面を表示"""
    return render_template('spc_type_analysis.html')

@ishida2.route('/api/spc_type_analysis_data')
@login_required
def api_spc_type_analysis_data():
    """スピンコート種類別分析データを取得するAPI"""
    # リクエストパラメータの取得
    date_from = request.args.get('date_from', None)
    date_to = request.args.get('date_to', None)

    # 日付の変換
    if date_from:
        date_from = datetime.strptime(date_from, '%Y-%m-%d')
    if date_to:
        date_to = datetime.strptime(date_to, '%Y-%m-%d') + timedelta(days=1)


    try:
        # クエリの構築
        query = db.session.query(
            SpcDat.SPC_COAT_DATE,
            SpcDat.SPC_TYPE,
            SpcDat.SPC_SHEETS,
            SpcDat.SPC_FNL_GD_QTY
        )

        # 日付フィルターの適用
        if date_from:
            query = query.filter(SpcDat.SPC_COAT_DATE >= date_from)
        if date_to:
            query = query.filter(SpcDat.SPC_COAT_DATE < date_to)
        query = query.filter(SpcDat.SPC_TYPE != 4)

        # データの取得と集計
        results = query.all()

        # 日付ごとのデータを格納する辞書
        spc_type_data = {}
        total_rates = {}
        period_total_rates = {}
        dates = set()
        spc_types = set()

        for result in results:
            date = result.SPC_COAT_DATE.strftime('%Y-%m-%d')
            spc_type = str(int(result.SPC_TYPE)) if result.SPC_TYPE is not None else 'Unknown'
            sheets = float(result.SPC_SHEETS) if result.SPC_SHEETS is not None else 0
            good_qty = float(result.SPC_FNL_GD_QTY) if result.SPC_FNL_GD_QTY is not None else 0

            dates.add(date)
            spc_types.add(spc_type)

            # 日付ごとのデータ集計
            if spc_type not in spc_type_data:
                spc_type_data[spc_type] = {}
            if date not in spc_type_data[spc_type]:
                spc_type_data[spc_type][date] = {'total': 0, 'good': 0, 'defect_rate': 0}
            
            spc_type_data[spc_type][date]['total'] += sheets
            spc_type_data[spc_type][date]['good'] += good_qty

            # 期間合計の集計
            if spc_type not in period_total_rates:
                period_total_rates[spc_type] = {'total': 0, 'good': 0, 'defect_rate': 0}
            period_total_rates[spc_type]['total'] += sheets
            period_total_rates[spc_type]['good'] += good_qty

        # 日付ごとの不良率計算
        for spc_type in spc_type_data:
            for date in spc_type_data[spc_type]:
                data = spc_type_data[spc_type][date]
                if data['total'] > 0:
                    defect_rate = ((data['total'] - data['good']) / data['total']) * 100
                    spc_type_data[spc_type][date]['defect_rate'] = defect_rate

        # 期間合計の不良率計算
        for spc_type in period_total_rates:
            data = period_total_rates[spc_type]
            if data['total'] > 0:
                period_total_rates[spc_type]['defect_rate'] = ((data['total'] - data['good']) / data['total']) * 100

        # 日付ごとの合計不良率計算
        for date in dates:
            total_sheets = 0
            total_good = 0
            for spc_type in spc_types:
                if spc_type in spc_type_data and date in spc_type_data[spc_type]:
                    total_sheets += spc_type_data[spc_type][date]['total']
                    total_good += spc_type_data[spc_type][date]['good']
            if total_sheets > 0:
                total_rates[date] = ((total_sheets - total_good) / total_sheets) * 100

        # データセットの作成
        datasets = []
        for spc_type in sorted(spc_types):
            data = []
            for date in sorted(dates):
                if spc_type in spc_type_data and date in spc_type_data[spc_type]:
                    data.append(spc_type_data[spc_type][date]['defect_rate'])
                else:
                    data.append(None)
            datasets.append({
                'label': f'種類: {spc_type}',
                'data': data
            })

        # 合計のデータセット追加
        total_data = []
        for date in sorted(dates):
            total_data.append(total_rates.get(date))
        datasets.append({
            'label': '合計',
            'data': total_data
        })

        return jsonify({
            'labels': sorted(list(dates)),
            'datasets': datasets,
            'spc_types': sorted(list(spc_types)),
            'spc_type_data': spc_type_data,
            'total_rates': total_rates,
            'period_total_rates': period_total_rates
        })

    except Exception as e:
        log_error(f"Error in api_spc_type_analysis_data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@ishida2.route('/hdc_type_analysis')
@login_required
def hdc_type_analysis():
    """ハードコート種類別分析画面を表示"""
    return render_template('hdc_type_analysis.html')

@ishida2.route('/api/hdc_type_analysis_data')
@login_required
def api_hdc_type_analysis_data():
    """ハードコート種類別分析データを取得するAPI"""
    # リクエストパラメータの取得
    date_from = request.args.get('date_from', None)
    date_to = request.args.get('date_to', None)

    # 日付の変換
    if date_from:
        date_from = datetime.strptime(date_from, '%Y-%m-%d')
    if date_to:
        date_to = datetime.strptime(date_to, '%Y-%m-%d') + timedelta(days=1)

    try:
        # クエリの構築
        query = db.session.query(
            HdcDat.HDC_COAT_DATE,
            HdcDat.HDC_TYPE,
            func.sum(HdcDat.HDC_COAT_CNT).label('total_count'),
            func.sum(
                HdcDat.HDC_PRE_FOREIGN +
                HdcDat.HDC_PRE_DROP +
                HdcDat.HDC_PRE_CHIP +
                HdcDat.HDC_PRE_STREAK +
                HdcDat.HDC_PRE_OTHERS +
                HdcDat.HDC_TRS_BASE_FAIL +
                HdcDat.HDC_TRS_FOREIGN +
                HdcDat.HDC_TRS_INCL +
                HdcDat.HDC_TRS_SCRATCH +
                HdcDat.HDC_TRS_COAT_FAIL +
                HdcDat.HDC_TRS_DROP +
                HdcDat.HDC_TRS_STREAK +
                HdcDat.HDC_TRS_DIRT +
                HdcDat.HDC_TRS_CHIP +
                HdcDat.HDC_PRJ_BASE +
                HdcDat.HDC_PRJ_FOREIGN +
                HdcDat.HDC_PRJ_DUST +
                HdcDat.HDC_PRJ_SCRATCH +
                HdcDat.HDC_PRJ_DROP +
                HdcDat.HDC_PRJ_CHIP +
                HdcDat.HDC_PRJ_STREAK
            ).label('defect_count')
        ).group_by(
            HdcDat.HDC_COAT_DATE,
            HdcDat.HDC_TYPE
        )

        # 日付フィルターの適用
        if date_from:
            query = query.filter(HdcDat.HDC_COAT_DATE >= date_from)
        if date_to:
            query = query.filter(HdcDat.HDC_COAT_DATE < date_to)
        query = query.filter(HdcDat.HDC_TYPE != 5)

        # データの取得
        results = query.all()

        # 日付のリストを作成（ソート済み）
        dates = sorted(list(set(r.HDC_COAT_DATE.strftime('%Y-%m-%d') for r in results)))
        
        # 種類のリストを作成
        hdc_types = sorted(list(set(r.HDC_TYPE for r in results)))

        # データの整形
        hdc_type_data = {}
        for hdc_type in hdc_types:
            hdc_type_data[hdc_type] = {}
            for date in dates:
                hdc_type_data[hdc_type][date] = {
                    'defect_rate': 0,
                    'total_count': 0,
                    'defect_count': 0
                }

        # 合計データの初期化
        total_rates = {date: 0 for date in dates}
        total_counts = {date: 0 for date in dates}
        total_defects = {date: 0 for date in dates}

        # データの集計
        for r in results:
            date = r.HDC_COAT_DATE.strftime('%Y-%m-%d')
            hdc_type = r.HDC_TYPE
            
            if r.total_count > 0:
                defect_rate = (r.defect_count / r.total_count * 100) if r.total_count > 0 else None
            else:
                defect_rate = None
            
            hdc_type_data[hdc_type][date] = {
                'defect_rate': defect_rate,
                'total_count': r.total_count,
                'defect_count': r.defect_count
            }
            
            total_counts[date] += r.total_count
            total_defects[date] += r.defect_count

        # 合計の不良率を計算
        for date in dates:
            if total_counts[date] > 0:
                total_rates[date] = (total_defects[date] / total_counts[date] * 100)
            else:
                total_rates[date] = None

        # 期間合計の計算
        period_total_counts = {hdc_type: 0 for hdc_type in hdc_types}
        period_total_defects = {hdc_type: 0 for hdc_type in hdc_types}
        period_total_rates = {}

        for hdc_type in hdc_types:
            for date in dates:
                data = hdc_type_data[hdc_type][date]
                period_total_counts[hdc_type] += data['total_count']
                period_total_defects[hdc_type] += data['defect_count']

        for hdc_type in hdc_types:
            if period_total_counts[hdc_type] > 0:
                period_total_rates[hdc_type] = (period_total_defects[hdc_type] / period_total_counts[hdc_type] * 100)
            else:
                period_total_rates[hdc_type] = None

        # データセットの作成
        datasets = []
        for hdc_type in hdc_types:
            data_points = []
            for date in dates:
                if (hdc_type in hdc_type_data and 
                    date in hdc_type_data[hdc_type] and 
                    hdc_type_data[hdc_type][date]['total_count'] > 0):
                    data_points.append(hdc_type_data[hdc_type][date]['defect_rate'])
                else:
                    data_points.append(None)
            
            dataset = {
                'label': f'種類: {hdc_type}',
                'data': data_points,
                'borderWidth': 2,
                'tension': 0.1
            }
            datasets.append(dataset)

        # 合計のデータセット
        total_data_points = []
        for date in dates:
            if total_counts[date] > 0:
                total_data_points.append(total_rates[date])
            else:
                total_data_points.append(None)
        
        total_dataset = {
            'label': '合計',
            'data': total_data_points,
            'borderWidth': 2,
            'tension': 0.1
        }
        datasets.append(total_dataset)

        return jsonify({
            'labels': dates,
            'datasets': datasets,
            'hdc_types': hdc_types,
            'hdc_type_data': hdc_type_data,
            'total_rates': total_rates,
            'period_total_rates': period_total_rates,
            'total_counts': total_counts,
            'period_total_counts': period_total_counts
        })

    except Exception as e:
        print(f"Error in api_hdc_type_analysis_data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@ishida2.route('/hdc_spn_slide')
@login_required
def hdc_spn_slide():
    """ハードコートスピンコートスライダー画面を表示"""
    return render_template('hdc_spn_slide.html')

@ishida2.route('/hdc_defect_monthly_analysis')
@login_required
def hdc_defect_monthly_analysis():
    """ハードコート不良データ月別分析画面を表示"""
    return render_template('hdc_defect_monthly_analysis.html')

@ishida2.route('/api/hdc_defect_monthly_data')
@login_required
def api_hdc_defect_monthly_data():
    """ハードコート不良データ月別分析のAPIエンドポイント"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    ct_type = request.args.get('ct_type')
    color = request.args.get('color')
    
    # 不良項目リスト
    defect_keys = [
        'HDC_PRE_FOREIGN', 'HDC_PRE_DROP', 'HDC_PRE_CHIP', 'HDC_PRE_STREAK', 'HDC_PRE_OTHERS',
        'HDC_TRS_BASE_FAIL', 'HDC_TRS_FOREIGN', 'HDC_TRS_INCL', 'HDC_TRS_SCRATCH', 'HDC_TRS_COAT_FAIL',
        'HDC_TRS_DROP', 'HDC_TRS_STREAK', 'HDC_TRS_DIRT', 'HDC_TRS_CHIP', 'HDC_PRJ_BASE',
        'HDC_PRJ_FOREIGN', 'HDC_PRJ_DUST', 'HDC_PRJ_SCRATCH', 'HDC_PRJ_DROP', 'HDC_PRJ_CHIP', 'HDC_PRJ_STREAK'
    ]
    
    defect_labels = {
        'HDC_PRE_FOREIGN': '硬化前ブツ', 'HDC_PRE_DROP': '硬化前タレ', 'HDC_PRE_CHIP': '硬化前カケ', 'HDC_PRE_STREAK': '硬化前スジ', 'HDC_PRE_OTHERS': '硬化前その他',
        'HDC_TRS_BASE_FAIL': '透過基材不良', 'HDC_TRS_FOREIGN': '透過ブツ', 'HDC_TRS_INCL': '透過イブツ', 'HDC_TRS_SCRATCH': '透過キズ', 'HDC_TRS_COAT_FAIL': '透過コート不良',
        'HDC_TRS_DROP': '透過タレ', 'HDC_TRS_STREAK': '透過スジ', 'HDC_TRS_DIRT': '透過汚れ', 'HDC_TRS_CHIP': '透過カケ', 'HDC_PRJ_BASE': '投影基材',
        'HDC_PRJ_FOREIGN': '投影ブツ', 'HDC_PRJ_DUST': '投影ごみ', 'HDC_PRJ_SCRATCH': '投影キズ', 'HDC_PRJ_DROP': '投影タレ', 'HDC_PRJ_CHIP': '投影カケ', 'HDC_PRJ_STREAK': '投影スジ'
    }
    
    try:
        # クエリの構築
        query = db.session.query(
            func.date_format(HdcDat.HDC_COAT_DATE, '%Y-%m').label('month'),
            *[func.sum(getattr(HdcDat, k)).label(k) for k in defect_keys],
            func.sum(HdcDat.HDC_COAT_CNT).label('total_cnt')
        )
        
        # フィルター適用
        if start_date:
            query = query.filter(HdcDat.HDC_COAT_DATE >= start_date)
        if end_date:
            query = query.filter(HdcDat.HDC_COAT_DATE <= end_date)
        if ct_type:
            query = query.filter(HdcDat.HDC_TYPE == ct_type)
        if color:
            query = query.filter(HdcDat.HDC_COLOR == color)
        
        # 種類5を除外
        query = query.filter(HdcDat.HDC_TYPE != 5)
        
        # 月別グループ化とソート
        query = query.group_by(func.date_format(HdcDat.HDC_COAT_DATE, '%Y-%m'))
        query = query.order_by(func.date_format(HdcDat.HDC_COAT_DATE, '%Y-%m'))
        
        results = query.all()
        
        # データ整形
        months = []
        defect_data = {}
        total_data = {}
        
        for row in results:
            month = row.month
            if month:
                months.append(month)
                total_cnt = float(row.total_cnt or 0)
                total_data[month] = {'total_cnt': total_cnt}
                
                defect_data[month] = {}
                for defect in defect_keys:
                    value = float(getattr(row, defect) or 0)
                    defect_data[month][defect] = {
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
        log_error(f"Error in api_hdc_defect_monthly_data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@ishida2.route('/spc_defect_monthly_analysis')
@login_required
def spc_defect_monthly_analysis():
    """スピンコート不良データ月別分析画面を表示"""
    return render_template('spc_defect_monthly_analysis.html')

@ishida2.route('/api/spc_defect_monthly_data')
@login_required
def api_spc_defect_monthly_data():
    """スピンコート不良データ月別分析のAPIエンドポイント"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    ct_type = request.args.get('ct_type')
    color = request.args.get('color')
    
    # 不良項目リスト
    pre_keys = [
        'SPC_PRE_BLK_DUST', 'SPC_PRE_WHT_DUST', 'SPC_PRE_EDGE_FAIL', 'SPC_PRE_COAT_FAIL',
        'SPC_PRE_DARK_SPOT', 'SPC_PRE_SNAIL', 'SPC_PRE_MIST', 'SPC_PRE_WRINKLE',
        'SPC_PRE_BRRL_BUB', 'SPC_PRE_STICK', 'SPC_PRE_TRBL_FIL', 'SPC_PRE_BASE_FIL'
    ]
    pst_keys = [
        'SPC_PST_SCRATCH', 'SPC_PST_COAT_FIL', 'SPC_PST_SNAIL', 'SPC_PST_DARK_SPOT',
        'SPC_PST_WRINKLE', 'SPC_PST_BUBBLE', 'SPC_PST_EDGE_FAIL', 'SPC_PST_WHT_DUST',
        'SPC_PST_BLK_DUST', 'SPC_PST_STICK', 'SPC_PST_PRM_STICK', 'SPC_PST_BASE_FAIL', 'SPC_PST_OTHERS'
    ]
    
    defect_labels = {
        'SPC_PRE_BLK_DUST': '硬化前黒ブツ', 'SPC_PRE_WHT_DUST': '硬化前白ブツ', 'SPC_PRE_EDGE_FAIL': '硬化前外周不良', 'SPC_PRE_COAT_FAIL': '硬化前コート不良',
        'SPC_PRE_DARK_SPOT': '硬化前ダークスポット', 'SPC_PRE_SNAIL': '硬化前スネイル', 'SPC_PRE_MIST': '硬化前ミスト', 'SPC_PRE_WRINKLE': '硬化前シワ',
        'SPC_PRE_BRRL_BUB': '硬化前バレル泡', 'SPC_PRE_STICK': '硬化前付着物', 'SPC_PRE_TRBL_FIL': '硬化前トラブル不', 'SPC_PRE_BASE_FIL': '硬化前基材不良',
        'SPC_PST_SCRATCH': '硬化後キズ', 'SPC_PST_COAT_FIL': '硬化後コート不良', 'SPC_PST_SNAIL': '硬化後スネイル', 'SPC_PST_DARK_SPOT': '硬化後ダークスポット',
        'SPC_PST_WRINKLE': '硬化後シワ', 'SPC_PST_BUBBLE': '硬化後泡', 'SPC_PST_EDGE_FAIL': '硬化後外周不良', 'SPC_PST_WHT_DUST': '硬化後白ブツ',
        'SPC_PST_BLK_DUST': '硬化後黒ブツ', 'SPC_PST_STICK': '硬化後付着物', 'SPC_PST_PRM_STICK': '硬化後プライマー付着跡', 'SPC_PST_BASE_FAIL': '硬化後基材不良', 'SPC_PST_OTHERS': '硬化後その他'
    }
    
    try:
        # クエリの構築
        query = db.session.query(
            func.date_format(SpcDat.SPC_COAT_DATE, '%Y-%m').label('month'),
            *[func.sum(getattr(SpcDat, k)).label(k) for k in pre_keys + pst_keys],
            func.sum(SpcDat.SPC_SHEETS).label('total_sheets'),
            func.sum(SpcDat.SPC_PRE_GOOD_QTY).label('total_pre_good')
        )
        
        # フィルター適用
        if start_date:
            query = query.filter(SpcDat.SPC_COAT_DATE >= start_date)
        if end_date:
            query = query.filter(SpcDat.SPC_COAT_DATE <= end_date)
        if ct_type:
            query = query.filter(SpcDat.SPC_TYPE == ct_type)
        if color:
            query = query.filter(SpcDat.SPC_COAT_COLOR == color)
        
        # 種類4を除外
        query = query.filter(SpcDat.SPC_TYPE != 4)
        
        # 月別グループ化とソート
        query = query.group_by(func.date_format(SpcDat.SPC_COAT_DATE, '%Y-%m'))
        query = query.order_by(func.date_format(SpcDat.SPC_COAT_DATE, '%Y-%m'))
        
        results = query.all()
        
        # データ整形
        months = []
        pre_defect_data = {}
        pst_defect_data = {}
        total_data = {}
        
        for row in results:
            month = row.month
            if month:
                months.append(month)
                total_sheets = float(row.total_sheets or 0)
                total_pre_good = float(row.total_pre_good or 0)
                total_data[month] = {
                    'total_sheets': total_sheets,
                    'total_pre_good': total_pre_good
                }
                
                # 硬化前不良データ（分母：total_sheets）
                pre_defect_data[month] = {}
                for defect in pre_keys:
                    value = float(getattr(row, defect) or 0)
                    pre_defect_data[month][defect] = {
                        'count': value,
                        'rate': round((value / total_sheets * 100) if total_sheets > 0 else 0, 2)
                    }
                
                # 硬化後不良データ（分母：total_pre_good）
                pst_defect_data[month] = {}
                for defect in pst_keys:
                    value = float(getattr(row, defect) or 0)
                    pst_defect_data[month][defect] = {
                        'count': value,
                        'rate': round((value / total_pre_good * 100) if total_pre_good > 0 else 0, 2)
                    }
        
        return jsonify({
            'months': months,
            'defect_labels': defect_labels,
            'pre_defect_data': pre_defect_data,
            'pst_defect_data': pst_defect_data,
            'total_data': total_data
        })
        
    except Exception as e:
        log_error(f"Error in api_spc_defect_monthly_data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@ishida2.route('/api/hdc_spc_slide')
@login_required
def api_hdc_spc_slide():
    """ハードコートスピンコートスライダーのデータを取得するAPI"""
    try:
        
        setmst = SetMst.query.first()
        infoData = {
            'SET_INFO_H1': setmst.SET_INFO_H1 or '',
            'SET_INFO_1': setmst.SET_INFO_1 or '',
            'SET_INFO_H2': setmst.SET_INFO_H2 or '',
            'SET_INFO_2': setmst.SET_INFO_2 or '',
            'SET_INFO_H3': setmst.SET_INFO_H3 or '',
            'SET_INFO_3': setmst.SET_INFO_3 or ''
        };
        
        # データの取得
        # 日付の範囲を取得
        date_from = datetime.now() - timedelta(days=7)
        # 日付の変換
        date_from = date_from.strftime('%Y-%m-%d')
        
        # 今月の日付を取得
        today = datetime.now()
        first_day_of_month = today.replace(day=1)
        date_from_month = first_day_of_month.strftime('%Y-%m-%d')

        
        # データの取得
        query = db.session.query(
            func.sum(SpcDat.SPC_SHEETS).label('total_sheets'),
            func.sum(SpcDat.SPC_FNL_GD_QTY).label('total_qty'),
            SpcDat.SPC_TYPE
        ).filter(
            SpcDat.SPC_COAT_DATE >= date_from
        ).group_by(
            SpcDat.SPC_TYPE
        ) 
        query = query.filter(SpcDat.SPC_TYPE != 4)

        results = query.all()
        spc_types = []
        spc_type_rate = {}
        for result in results:
            spc_type = str(int(result.SPC_TYPE))
            spc_types.append(spc_type)
            if spc_type not in spc_type_rate:
                spc_type_rate[spc_type] = 0
            rate = 0    
            if result.total_sheets:
                rate = result.total_qty / result.total_sheets * 100
            spc_type_rate[spc_type] = rate
            
        # データの取得
        query = db.session.query(
            func.sum(SpcDat.SPC_SHEETS).label('total_sheets'),
            func.sum(SpcDat.SPC_FNL_GD_QTY).label('total_qty'),
            SpcDat.SPC_TYPE
        ).filter(
            SpcDat.SPC_COAT_DATE >= date_from_month
        ).group_by(
            SpcDat.SPC_TYPE
        ) 
        query = query.filter(SpcDat.SPC_TYPE != 4)

        results = query.all()
        spc_types_month = []
        spc_type_rate_month = {}
        for result in results:
            spc_type = str(int(result.SPC_TYPE))
            spc_types_month.append(spc_type)
            if spc_type not in spc_type_rate_month:
                spc_type_rate_month[spc_type] = 0
            rate = 0    
            if result.total_sheets:
                rate = result.total_qty / result.total_sheets * 100
            spc_type_rate_month[spc_type] = rate
            
        # 不良項目リスト
        spc_keys = [
            'SPC_PRE_BLK_DUST', 'SPC_PRE_WHT_DUST', 'SPC_PRE_EDGE_FAIL', 'SPC_PRE_COAT_FAIL',
            'SPC_PRE_DARK_SPOT', 'SPC_PRE_SNAIL', 'SPC_PRE_MIST', 'SPC_PRE_WRINKLE',
            'SPC_PRE_BRRL_BUB', 'SPC_PRE_STICK', 'SPC_PRE_TRBL_FIL', 'SPC_PRE_BASE_FIL',
            'SPC_PST_SCRATCH', 'SPC_PST_COAT_FIL', 'SPC_PST_SNAIL', 'SPC_PST_DARK_SPOT',
            'SPC_PST_WRINKLE', 'SPC_PST_BUBBLE', 'SPC_PST_EDGE_FAIL', 'SPC_PST_WHT_DUST',
            'SPC_PST_BLK_DUST', 'SPC_PST_STICK', 'SPC_PST_PRM_STICK', 'SPC_PST_BASE_FAIL', 'SPC_PST_OTHERS'
        ]
        spc_defect_labels = {
            'SPC_PRE_BLK_DUST': '硬化前黒ブツ', 'SPC_PRE_WHT_DUST': '硬化前白ブツ', 'SPC_PRE_EDGE_FAIL': '硬化前外周不良', 'SPC_PRE_COAT_FAIL': '硬化前コート不良',
            'SPC_PRE_DARK_SPOT': '硬化前ダークスポット', 'SPC_PRE_SNAIL': '硬化前スネイル', 'SPC_PRE_MIST': '硬化前ミスト', 'SPC_PRE_WRINKLE': '硬化前シワ',
            'SPC_PRE_BRRL_BUB': '硬化前バレル泡', 'SPC_PRE_STICK': '硬化前付着物', 'SPC_PRE_TRBL_FIL': '硬化前トラブル不', 'SPC_PRE_BASE_FIL': '硬化前基材不良',
            'SPC_PST_SCRATCH': '硬化後キズ', 'SPC_PST_COAT_FIL': '硬化後コート不良', 'SPC_PST_SNAIL': '硬化後スネイル', 'SPC_PST_DARK_SPOT': '硬化後ダークスポット',
            'SPC_PST_WRINKLE': '硬化後シワ', 'SPC_PST_BUBBLE': '硬化後泡', 'SPC_PST_EDGE_FAIL': '硬化後外周不良', 'SPC_PST_WHT_DUST': '硬化後白ブツ',
            'SPC_PST_BLK_DUST': '硬化後黒ブツ', 'SPC_PST_STICK': '硬化後付着物', 'SPC_PST_PRM_STICK': '硬化後プライマー付着跡', 'SPC_PST_BASE_FAIL': '硬化後基材不良', 'SPC_PST_OTHERS': '硬化後その他'
        }
        # クエリビルド
        query = db.session.query(
            func.DATE(SpcDat.SPC_COAT_DATE).label('date'),
            *[func.sum(getattr(SpcDat, k)).label(k) for k in spc_keys],
            func.sum(SpcDat.SPC_SHEETS).label('total_sheets'),
            func.sum(SpcDat.SPC_PRE_GOOD_QTY).label('total_pre_good'),
            SpcDat.SPC_TYPE
            )
        query = query.filter(SpcDat.SPC_COAT_DATE >= date_from)
        query = query.filter(SpcDat.SPC_TYPE != 4)
        query = query.group_by(func.DATE(SpcDat.SPC_COAT_DATE), SpcDat.SPC_TYPE)
        query = query.order_by(func.DATE(SpcDat.SPC_COAT_DATE), SpcDat.SPC_TYPE)
        results = query.all()

        # データ整形
        spc_type_dates = {}
        spc_type_data = {}

        for row in results:
            spc_type = str(int(row.SPC_TYPE))
            if spc_type not in spc_type_data:
                spc_type_data[spc_type] = {}
                spc_type_dates[spc_type] = []
            date_str = row.date.strftime('%Y-%m-%d') if row.date else 'Unknown'
            if date_str not in spc_type_dates[spc_type]:
                spc_type_dates[spc_type].append(date_str)
           
            if date_str not in spc_type_data[spc_type]:
                spc_type_data[spc_type][date_str] = {
                    'total_sheets': 0,
                    'total_pre_good': 0,
                    'defect_rates': {}
                }
            
            for defect in spc_keys:
                value = float(getattr(row, defect) or 0)
                spc_type_data[spc_type][date_str]['defect_rates'][defect] = round((float(value) / float(row.total_sheets) * 100) if float(row.total_sheets) else 0, 2)
            
        # データの取得
        query = db.session.query(
            func.sum(HdcDat.HDC_COAT_CNT).label('total_cnt'),
            func.sum(HdcDat.HDC_PASS_QTY).label('total_pass'),
            HdcDat.HDC_TYPE
        ).filter(
            HdcDat.HDC_COAT_DATE >= date_from
        ).group_by(
            HdcDat.HDC_TYPE
        )   
        query = query.filter(HdcDat.HDC_TYPE != 5)
        results = query.all()
        hdc_types = []
        hdc_type_rate = {}
        for result in results:
            hdc_type = str(int(result.HDC_TYPE))
            hdc_types.append(hdc_type)
            if hdc_type not in hdc_type_rate:
                hdc_type_rate[hdc_type] = 0
            rate = 0    
            if result.total_cnt:
                rate = result.total_pass / result.total_cnt * 100
            hdc_type_rate[hdc_type] = rate
            
        # データの取得
        query = db.session.query(
            func.sum(HdcDat.HDC_COAT_CNT).label('total_cnt'),
            func.sum(HdcDat.HDC_PASS_QTY).label('total_pass'),
            HdcDat.HDC_TYPE
        ).filter(
            HdcDat.HDC_COAT_DATE >= date_from_month
        ).group_by(
            HdcDat.HDC_TYPE
        )   
        query = query.filter(HdcDat.HDC_TYPE != 5)
        results = query.all()
        hdc_types_month = []
        hdc_type_rate_month = {}
        for result in results:
            hdc_type = str(int(result.HDC_TYPE))
            hdc_types_month.append(hdc_type)
            if hdc_type not in hdc_type_rate_month:
                hdc_type_rate_month[hdc_type] = 0
            rate = 0    
            if result.total_cnt:
                rate = result.total_pass / result.total_cnt * 100
            hdc_type_rate_month[hdc_type] = rate
            
        # 不良項目リスト
        hdc_defect_keys = [
            'HDC_PRE_FOREIGN', 'HDC_PRE_DROP', 'HDC_PRE_CHIP', 'HDC_PRE_STREAK', 'HDC_PRE_OTHERS',
            'HDC_TRS_BASE_FAIL', 'HDC_TRS_FOREIGN', 'HDC_TRS_INCL', 'HDC_TRS_SCRATCH', 'HDC_TRS_COAT_FAIL',
            'HDC_TRS_DROP', 'HDC_TRS_STREAK', 'HDC_TRS_DIRT', 'HDC_TRS_CHIP', 'HDC_PRJ_BASE',
            'HDC_PRJ_FOREIGN', 'HDC_PRJ_DUST', 'HDC_PRJ_SCRATCH', 'HDC_PRJ_DROP', 'HDC_PRJ_CHIP', 'HDC_PRJ_STREAK'
        ]
        hdc_defect_labels = {
            'HDC_PRE_FOREIGN': '硬化前ブツ', 'HDC_PRE_DROP': '硬化前タレ', 'HDC_PRE_CHIP': '硬化前カケ', 'HDC_PRE_STREAK': '硬化前スジ', 'HDC_PRE_OTHERS': '硬化前その他',
            'HDC_TRS_BASE_FAIL': '透過基材不良', 'HDC_TRS_FOREIGN': '透過ブツ', 'HDC_TRS_INCL': '透過イブツ', 'HDC_TRS_SCRATCH': '透過キズ', 'HDC_TRS_COAT_FAIL': '透過コート不良',
            'HDC_TRS_DROP': '透過タレ', 'HDC_TRS_STREAK': '透過スジ', 'HDC_TRS_DIRT': '透過汚れ', 'HDC_TRS_CHIP': '透過カケ', 'HDC_PRJ_BASE': '投影基材',
            'HDC_PRJ_FOREIGN': '投影ブツ', 'HDC_PRJ_DUST': '投影ごみ', 'HDC_PRJ_SCRATCH': '投影キズ', 'HDC_PRJ_DROP': '投影タレ', 'HDC_PRJ_CHIP': '投影カケ', 'HDC_PRJ_STREAK': '投影スジ'
        }
        # クエリビルド
        query = db.session.query(
            func.DATE(HdcDat.HDC_COAT_DATE).label('date'),
            HdcDat.HDC_TYPE,
            *[func.sum(getattr(HdcDat, k)).label(k) for k in hdc_defect_keys],
            func.sum(HdcDat.HDC_COAT_CNT).label('total_cnt')
        )
        query = query.filter(HdcDat.HDC_COAT_DATE >= date_from)
        query = query.filter(HdcDat.HDC_TYPE != 5)
        query = query.group_by(func.DATE(HdcDat.HDC_COAT_DATE), HdcDat.HDC_TYPE)
        query = query.order_by(func.DATE(HdcDat.HDC_COAT_DATE), HdcDat.HDC_TYPE)
        results = query.all()
        
        # データ整形
        hdc_type_dates = {}
        hdc_type_data = {}

        for row in results:
            hdc_type = str(int(row.HDC_TYPE))
            if hdc_type not in hdc_type_data:
                hdc_type_data[hdc_type] = {}
                hdc_type_dates[hdc_type] = []
            date_str = row.date.strftime('%Y-%m-%d') if row.date else 'Unknown'
            if date_str not in hdc_type_dates[hdc_type]:
                hdc_type_dates[hdc_type].append(date_str)
           
            if date_str not in hdc_type_data[hdc_type]:
                hdc_type_data[hdc_type][date_str] = {
                    'total_cnt': 0,
                    'defect_rates': {}
                }
            
            for defect in hdc_defect_keys:
                value = float(getattr(row, defect) or 0)
                hdc_type_data[hdc_type][date_str]['defect_rates'][defect] = round((float(value) / float(row.total_cnt) * 100) if float(row.total_cnt) else 0, 2)

            
        return jsonify({
            'spc_defect_labels': spc_defect_labels,
            'spc_types': spc_types,
            'spc_type_dates': spc_type_dates,
            'spc_type_data': spc_type_data,
            'spc_type_rate': spc_type_rate,
            'hdc_defect_labels': hdc_defect_labels,
            'hdc_types': hdc_types,
            'hdc_type_dates': hdc_type_dates,
            'hdc_type_data': hdc_type_data,
            'hdc_type_rate': hdc_type_rate,
            'infoData': infoData,
            'spc_types_month': spc_types_month,
            'spc_type_rate_month': spc_type_rate_month,
            'hdc_types_month': hdc_types_month,
            'hdc_type_rate_month': hdc_type_rate_month
        })

    except Exception as e:
        log_error(f"Error in api_hdc_spc_slide: {str(e)}")
        return jsonify({'error': str(e)}), 500

