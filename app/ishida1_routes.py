import csv
import io
from datetime import datetime, timedelta
import locale
from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app as app, jsonify

from app.ishida_models import FmpDat, FngDat
from .master_models import KbnMst
from .models import db
from .utils import log_error
from flask_login import login_required
from sqlalchemy import   func

ishida1 = Blueprint('ishida1', __name__)

@ishida1.route('/fmp_defect_analysis')
@login_required
def fmp_defect_analysis():
    """膜加工データ画面を表示"""
    # パラメータの取得と変換
    film_curves = KbnMst.get_kbn_list('MCRB') # 膜カーブ
    colors = KbnMst.get_kbn_list('MCLR')    # 色
    
    return render_template('fmp_defect_analysis.html',
                         film_curves=film_curves,
                         colors=colors)

@ishida1.route('/api/fmp_defect_analysis')
@login_required
def api_fmp_defect_analysis():
    """膜加工データのAPIエンドポイント"""
    try:
        # パラメータの取得と変換
        start_date = request.args.get('start_date')
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
            
        end_date = request.args.get('end_date')
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d')  
    
        proc_date = request.args.get('proc_date')
        if proc_date:
            proc_date = datetime.strptime(proc_date, '%Y-%m-%d')
            
        color = request.args.get('color')
        if color:
            color = int(color)
            
        pva_lot_no = request.args.get('pva_lot_no')
        if pva_lot_no:
            pva_lot_no = int(pva_lot_no)
            
        film_curve = request.args.get('film_curve')
        if film_curve:
            film_curve = int(film_curve)
            
        # 不良率分析の実行
        result = FmpDat.get_defect_analysis(
            start_date=start_date,
            end_date=end_date,
            proc_date=proc_date,
            color=color,
            pva_lot_no=pva_lot_no,
            film_curve=film_curve
        )
        
        if result is None:
            return jsonify({'error': '分析中にエラーが発生しました'}), 500
            
        return jsonify(result)
        
    except Exception as e:
        log_error(f'膜加工データのAPIエンドポイントでエラーが発生しました: {str(e)}')
        return jsonify({'error': str(e)}), 500

@ishida1.route('/color_trans_defects')
@login_required
def color_trans_defects():
    """カラー不良と透過率不良の一覧画面"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    return render_template('color_trans_defects.html', start_date=start_date, end_date=end_date)

@ishida1.route('/api/color_trans_defects')
@login_required
def api_color_trans_defects():
    """カラー不良と透過率不良のデータを取得するAPI"""
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        defects = FngDat.get_color_trans_defects(start_date, end_date)
        return jsonify(defects)
    except Exception as e:
        log_error(f'不良データの取得中にエラーが発生しました: {str(e)}')
        return jsonify({'error': 'データの取得に失敗しました'}), 500

@ishida1.route('/color_trans_defects/input', methods=['GET', 'POST'])
@login_required
def color_trans_defects_input():
    """不良データ入力画面"""
    if request.method == 'POST':
        try:
            lot_no = request.form.get('lot_no')
            ng_id = request.form.get('ng_id')
            ins_qty = request.form.get('ins_qty')
            ng_qty = request.form.get('ng_qty')
            biko = request.form.get('biko')

            # 既存のデータを検索
            fng_dat = FngDat.get_by_lot_and_ng(lot_no, ng_id)
            
            if fng_dat:
                # 既存データの更新
                fng_dat.FNG_INS_QTY = ins_qty
                fng_dat.FNG_NG_QTY = ng_qty
                fng_dat.FNG_BIKO = biko
            else:
                # 新規データの作成
                fng_dat = FngDat(
                    FNG_LOT_NO=lot_no,
                    FNG_NG_ID=ng_id,
                    FNG_INS_QTY=ins_qty,
                    FNG_NG_QTY=ng_qty,
                    FNG_BIKO=biko
                )
                db.session.add(fng_dat)

            db.session.commit()
            flash('データを保存しました')
            return redirect(url_for('ishida1.color_trans_defects'))

        except Exception as e:
            db.session.rollback()
            log_error(f'不良データの保存中にエラーが発生しました: {str(e)}')
            flash('データの保存に失敗しました')
            return redirect(url_for('ishida1.color_trans_defects'))

    lot_no = request.args.get('lot_no')
    ng_id = request.args.get('ng_id')
    
    # 既存のデータを取得
    fng_dat = None
    if lot_no and ng_id:
        fng_dat = FngDat.get_by_lot_and_ng(lot_no, ng_id)
    
    return render_template('color_trans_defects_input.html', 
                         lot_no=lot_no, 
                         ng_id=ng_id, 
                         fng_dat=fng_dat)

@ishida1.route('/color_trans_defects_slide')
def color_trans_defects_slide():
    """カラー不良・透過率不良スライド画面"""
    return render_template('color_trans_defects_slide.html')

@ishida1.route('/api/color_trans_defects_slide')
def api_color_trans_defects_slide():
    """カラー不良・透過率不良データのAPI"""
    try:
        two_weeks_ago = datetime.now() - timedelta(days=14)
        defects = FngDat.get_color_trans_defects(start_date=two_weeks_ago, end_date=None)
        return jsonify({
            'defects': defects
        })
    except Exception as e:
        log_error(f'カラー不良・透過率不良データの取得中にエラーが発生しました: {str(e)}')
        return jsonify({'error': 'データの取得に失敗しました'}), 500

@ishida1.route('/api/fmp_defect_analysis_slide')
def api_fmp_defect_analysis_slide():
    """膜加工不良率分析データのAPI"""
    try:
        start_date = datetime.now() - timedelta(days=10)
        # 基本の分析データを取得
        analysis_data = FmpDat.get_defect_analysis(start_date=start_date)
        
        # 直近5日間の推移データを取得
        trend_data = FmpDat.get_recent_defect_trend()
        
        # 分析データに推移データを追加
        if analysis_data:
            analysis_data['trend_data'] = trend_data
        
        return jsonify(analysis_data or {})
    except Exception as e:
        log_error(f'膜加工不良率分析データの取得中にエラーが発生しました: {str(e)}')
        return jsonify({'error': 'データの取得に失敗しました'}), 500

@ishida1.route('/fmp_defect_detail_analysis', methods=['GET'])
@login_required
def fmp_defect_detail_analysis():
    """膜加工不良詳細分析画面を表示"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    # 区分マスタ情報の取得
    colors = KbnMst.get_kbn_list('MCLR')  # 色
    film_curves = KbnMst.get_kbn_list('MCRB')  # 膜カーブ
    
    return render_template('fmp_defect_detail_analysis.html', 
                         start_date=start_date, 
                         end_date=end_date,
                         colors=colors,
                         film_curves=film_curves)

@ishida1.route('/fmp_defect_monthly_analysis', methods=['GET'])
@login_required
def fmp_defect_monthly_analysis():
    """膜加工不良月別分析画面を表示"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    # 区分マスタ情報の取得
    colors = KbnMst.get_kbn_list('MCLR')  # 色
    film_curves = KbnMst.get_kbn_list('MCRB')  # 膜カーブ
    
    return render_template('fmp_defect_monthly_analysis.html', 
                         start_date=start_date, 
                         end_date=end_date,
                         colors=colors,
                         film_curves=film_curves)

@ishida1.route('/api/fmp_defect_detail_data')
@login_required
def api_fmp_defect_detail_data():
    """膜加工不良詳細データの日付×不良項目クロス集計データをJSONで返す"""
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        color = request.args.get('color')
        film_curve = request.args.get('film_curve')
        pva_lot_no = request.args.get('pva_lot_no')
        
        # 一次検査不良項目リスト
        primary_keys = [
            'FMP_WRINKLE_A', 'FMP_WRINKLE_B', 'FMP_TEAR', 'FMP_FOREIGN', 
            'FMP_FIBER', 'FMP_SCRATCH', 'FMP_HOLE', 'FMP_PRM_OTHERS'
        ]
        
        # 二次検査不良項目リスト
        secondary_keys = [
            'FMP_CLR_FADE', 'FMP_CLR_IRREG', 'FMP_DYE_STREAK', 'FMP_DIRT', 'FMP_OTHERS'
        ]
        
        # 不良項目ラベル
        primary_defect_labels = {
            'FMP_WRINKLE_A': 'シワA',
            'FMP_WRINKLE_B': 'シワB',
            'FMP_TEAR': '裂け',
            'FMP_FOREIGN': 'ブツ',
            'FMP_FIBER': '繊維',
            'FMP_SCRATCH': 'キズ',
            'FMP_HOLE': '穴',
            'FMP_PRM_OTHERS': 'その他'
        }
        
        secondary_defect_labels = {
            'FMP_CLR_FADE': '色抜け',
            'FMP_CLR_IRREG': '色ムラ',
            'FMP_DYE_STREAK': '染スジ',
            'FMP_DIRT': '汚れ',
            'FMP_OTHERS': 'その他'
        }
        
        # クエリビルド
        query = db.session.query(
            func.DATE(FmpDat.FMP_INSP_DATE).label('date'),
            *[func.sum(getattr(FmpDat, k)).label(k) for k in primary_keys + secondary_keys],
            func.sum(FmpDat.FMP_PROC_SHTS).label('total_sheets'),
            func.sum(FmpDat.FMP_PRM_GOOD_QTY).label('total_primary_good'),
            func.sum(FmpDat.FMP_GRADE_A).label('total_grade_a'),
            func.sum(FmpDat.FMP_GRADE_B).label('total_grade_b'),
            func.sum(FmpDat.FMP_GRADE_C).label('total_grade_c')
        )
        
        if start_date:
            query = query.filter(FmpDat.FMP_INSP_DATE >= start_date)
        if end_date:
            query = query.filter(FmpDat.FMP_INSP_DATE <= end_date)
        if color:
            query = query.filter(FmpDat.FMP_COLOR == color)
        if film_curve:
            query = query.filter(FmpDat.FMP_FILM_CURVE == film_curve)
        if pva_lot_no:
            query = query.filter(FmpDat.FMP_PVA_LOT_NO == pva_lot_no)
            
        query = query.group_by(func.DATE(FmpDat.FMP_INSP_DATE))
        query = query.order_by(func.DATE(FmpDat.FMP_INSP_DATE))
        
        results = query.all()

        # データ整形
        dates = []
        total_data = {}
        primary_defect_rates = {}
        secondary_defect_rates = {}
        primary_total_rates = {}
        secondary_total_rates = {}

        for row in results:
            date_str = row.date.strftime('%Y-%m-%d') if row.date else 'Unknown'
            dates.append(date_str)
            
            total_sheets = float(row.total_sheets or 0)
            total_primary_good = float(row.total_primary_good or 0)
            
            # 一次検査不良データの集計
            primary_defect_sum = 0
            primary_defect_rates[date_str] = {}
            
            for defect in primary_keys:
                value = float(getattr(row, defect) or 0)
                primary_defect_sum += value
                primary_defect_rates[date_str][defect] = round((value / total_sheets * 100) if total_sheets else 0, 2)
            
            # 二次検査不良データの集計
            secondary_defect_sum = 0
            secondary_defect_rates[date_str] = {}
            
            for defect in secondary_keys:
                value = float(getattr(row, defect) or 0)
                secondary_defect_sum += value
                secondary_defect_rates[date_str][defect] = round((value / total_primary_good * 100) if total_primary_good else 0, 2)
            
            # 収率計算用データ
            total_grade_a = float(row.total_grade_a or 0)
            total_grade_b = float(row.total_grade_b or 0)
            total_grade_c = float(row.total_grade_c or 0)
            
            total_data[date_str] = {
                'total_sheets': total_sheets,
                'total_primary_good': total_primary_good,
                'total_grade_a': total_grade_a,
                'total_grade_b': total_grade_b,
                'total_grade_c': total_grade_c
            }
            
            # 合計不良率の計算
            primary_total_rates[date_str] = round((primary_defect_sum / total_sheets * 100) if total_sheets else 0, 2)
            secondary_total_rates[date_str] = round((secondary_defect_sum / total_primary_good * 100) if total_primary_good else 0, 2)

        return jsonify({
            'dates': dates,
            'primary_defect_labels': primary_defect_labels,
            'secondary_defect_labels': secondary_defect_labels,
            'primary_defect_rates': primary_defect_rates,
            'secondary_defect_rates': secondary_defect_rates,
            'primary_total_rates': primary_total_rates,
            'secondary_total_rates': secondary_total_rates,
            'total_data': total_data
        })
        
    except Exception as e:
        log_error(f'膜加工不良詳細データのAPIエンドポイントでエラーが発生しました: {str(e)}')
        return jsonify({'error': str(e)}), 500

@ishida1.route('/api/fmp_defect_monthly_data')
@login_required
def api_fmp_defect_monthly_data():
    """膜加工不良詳細データの月別×不良項目クロス集計データをJSONで返す"""
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        color = request.args.get('color')
        film_curve = request.args.get('film_curve')
        pva_lot_no = request.args.get('pva_lot_no')
        
        # 一次検査不良項目リスト
        primary_keys = [
            'FMP_WRINKLE_A', 'FMP_WRINKLE_B', 'FMP_TEAR', 'FMP_FOREIGN', 
            'FMP_FIBER', 'FMP_SCRATCH', 'FMP_HOLE', 'FMP_PRM_OTHERS'
        ]
        
        # 二次検査不良項目リスト
        secondary_keys = [
            'FMP_CLR_FADE', 'FMP_CLR_IRREG', 'FMP_DYE_STREAK', 'FMP_DIRT', 'FMP_OTHERS'
        ]
        
        # 不良項目ラベル
        primary_defect_labels = {
            'FMP_WRINKLE_A': 'シワA',
            'FMP_WRINKLE_B': 'シワB',
            'FMP_TEAR': '裂け',
            'FMP_FOREIGN': 'ブツ',
            'FMP_FIBER': '繊維',
            'FMP_SCRATCH': 'キズ',
            'FMP_HOLE': '穴',
            'FMP_PRM_OTHERS': 'その他'
        }
        
        secondary_defect_labels = {
            'FMP_CLR_FADE': '色抜け',
            'FMP_CLR_IRREG': '色ムラ',
            'FMP_DYE_STREAK': '染スジ',
            'FMP_DIRT': '汚れ',
            'FMP_OTHERS': 'その他'
        }
        
        # 月別集計のクエリビルド
        query = db.session.query(
            func.date_format(FmpDat.FMP_INSP_DATE, '%Y-%m').label('month'),
            *[func.sum(getattr(FmpDat, k)).label(k) for k in primary_keys + secondary_keys],
            func.sum(FmpDat.FMP_PROC_SHTS).label('total_sheets'),
            func.sum(FmpDat.FMP_PRM_GOOD_QTY).label('total_primary_good')
        )
        
        if start_date:
            query = query.filter(FmpDat.FMP_INSP_DATE >= start_date)
        if end_date:
            query = query.filter(FmpDat.FMP_INSP_DATE <= end_date)
        if color:
            query = query.filter(FmpDat.FMP_COLOR == color)
        if film_curve:
            query = query.filter(FmpDat.FMP_FILM_CURVE == film_curve)
        if pva_lot_no:
            query = query.filter(FmpDat.FMP_PVA_LOT_NO == pva_lot_no)
            
        query = query.group_by(func.date_format(FmpDat.FMP_INSP_DATE, '%Y-%m'))
        query = query.order_by(func.date_format(FmpDat.FMP_INSP_DATE, '%Y-%m'))
        
        results = query.all()

        # データ整形
        months = []
        total_data = {}
        primary_defect_rates = {}
        secondary_defect_rates = {}
        primary_total_rates = {}
        secondary_total_rates = {}

        for row in results:
            month_str = row.month if row.month else 'Unknown'
            months.append(month_str)
            
            total_sheets = float(row.total_sheets or 0)
            total_primary_good = float(row.total_primary_good or 0)
            
            # 一次検査不良データの集計
            primary_defect_sum = 0
            primary_defect_rates[month_str] = {}
            
            for defect in primary_keys:
                value = float(getattr(row, defect) or 0)
                primary_defect_sum += value
                primary_defect_rates[month_str][defect] = round((value / total_sheets * 100) if total_sheets else 0, 2)
            
            # 二次検査不良データの集計
            secondary_defect_sum = 0
            secondary_defect_rates[month_str] = {}
            
            for defect in secondary_keys:
                value = float(getattr(row, defect) or 0)
                secondary_defect_sum += value
                secondary_defect_rates[month_str][defect] = round((value / total_primary_good * 100) if total_primary_good else 0, 2)
            
            total_data[month_str] = {
                'total_sheets': total_sheets,
                'total_primary_good': total_primary_good
            }
            
            # 合計不良率の計算
            primary_total_rates[month_str] = round((primary_defect_sum / total_sheets * 100) if total_sheets else 0, 2)
            secondary_total_rates[month_str] = round((secondary_defect_sum / total_primary_good * 100) if total_primary_good else 0, 2)

        return jsonify({
            'months': months,
            'primary_defect_labels': primary_defect_labels,
            'secondary_defect_labels': secondary_defect_labels,
            'primary_defect_rates': primary_defect_rates,
            'secondary_defect_rates': secondary_defect_rates,
            'primary_total_rates': primary_total_rates,
            'secondary_total_rates': secondary_total_rates,
            'total_data': total_data
        })
        
    except Exception as e:
        log_error(f'膜加工不良月別データのAPIエンドポイントでエラーが発生しました: {str(e)}')
        return jsonify({'error': str(e)}), 500

