from flask import Blueprint, render_template, request, redirect, flash
from .models import SjiDatModel, PrdRecord
from .master_models import PrdMstModel
from .utils import log_error
from app.ishida_models import FmcDat, FmpDat
import tempfile
import os
from flask_login import login_required

upload_bp   = Blueprint('upload', __name__)

@upload_bp.route('/upload', methods=['GET', 'POST'])
@login_required
def upload():
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
                # 一時ファイルの作成
                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
                    temp_path = temp_file.name
                    # アップロードされたファイルを一時ファイルに保存
                    file.save(temp_path)
                
                try:
                    # import_from_csv関数を使用してCSVデータをインポート
                    success, message = PrdRecord.import_from_csv(
                        temp_path,
                        encoding='shift_jis'  # Shift-JISエンコーディングを指定
                    )
                    
                    if success:
                        flash(message)
                        return redirect(request.url)
                    else:
                        flash(f'インポート中にエラーが発生しました: {message}')
                        return redirect(request.url)
                
                finally:
                    # 一時ファイルの削除
                    try:
                        os.unlink(temp_path)
                    except Exception as e:
                        log_error(f'一時ファイルの削除中にエラーが発生しました: {str(e)}')
            
            except Exception as e:
                log_error(f'CSVファイル処理中にエラーが発生しました: {str(e)}')
                flash('CSVファイルの処理中にエラーが発生しました')
                return redirect(request.url)
    
    return render_template('upload.html')

@upload_bp.route('/upload2', methods=['GET', 'POST'])
@login_required
def upload2():
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
        
        data_type = request.form.get('data_type')
        
        if file:
            try:
                # 一時ファイルの作成
                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
                    temp_path = temp_file.name
                    # アップロードされたファイルを一時ファイルに保存
                    file.save(temp_path)
                
                try:
                    # import_from_csv関数を使用してCSVデータをインポート
                    success, message = PrdRecord.import_from_csv2(
                        temp_path,
                        encoding='shift_jis'  # Shift-JISエンコーディングを指定
                    )
                    
                    if success:
                        flash(message)
                        return redirect(request.url)
                    else:
                        flash(f'インポート中にエラーが発生しました: {message}')
                        return redirect(request.url)
                
                finally:
                    # 一時ファイルの削除
                    try:
                        os.unlink(temp_path)
                    except Exception as e:
                        log_error(f'一時ファイルの削除中にエラーが発生しました: {str(e)}')
            
            except Exception as e:
                log_error(f'CSVファイル処理中にエラーが発生しました: {str(e)}')
                flash('CSVファイルの処理中にエラーが発生しました')
                return redirect(request.url)
    
    return render_template('upload2.html')

@upload_bp.route('/upload4', methods=['GET', 'POST'])
@login_required
def upload4():
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
                # 一時ファイルの作成
                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
                    temp_path = temp_file.name
                    # アップロードされたファイルを一時ファイルに保存
                    file.save(temp_path)
                    
                try:
                    # import_from_csv関数を使用してCSVデータをインポート
                    success, message = SjiDatModel.import_from_csv(
                        temp_path,
                        encoding='shift_jis'
                    )
                    
                    if success:
                        flash(message)
                        return redirect(request.url)
                    else:
                        flash(f'インポート中にエラーが発生しました: {message}')
                        return redirect(request.url)
                    
                finally:
                    # 一時ファイルの削除
                    try:
                        os.unlink(temp_path)
                    except Exception as e:
                        log_error(f'一時ファイルの削除中にエラーが発生しました: {str(e)}')
                        
            except Exception as e:
                log_error(f'CSVファイル処理中にエラーが発生しました: {str(e)}')
                flash('CSVファイルの処理中にエラーが発生しました')
                return redirect(request.url)
            
    return render_template('upload4.html')

@upload_bp.route('/upload5', methods=['GET', 'POST'])
@login_required
def upload5():
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
                # 一時ファイルの作成
                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
                    temp_path = temp_file.name
                    # アップロードされたファイルを一時ファイルに保存
                    file.save(temp_path)
                    
                try:
                    # import_from_csv関数を使用してCSVデータをインポート
                    success, message = FmcDat.import_from_csv(
                        temp_path,
                        encoding='shift_jis'
                    )
                    
                    if success:
                        flash(message)
                        return redirect(request.url)
                    else:
                        flash(f'インポート中にエラーが発生しました: {message}')
                        return redirect(request.url)
                    
                finally:
                    # 一時ファイルの削除
                    try:
                        os.unlink(temp_path)
                    except Exception as e:
                        log_error(f'一時ファイルの削除中にエラーが発生しました: {str(e)}')
                        
            except Exception as e:
                log_error(f'CSVファイル処理中にエラーが発生しました: {str(e)}')
                flash('CSVファイルの処理中にエラーが発生しました')
                return redirect(request.url)
            
    return render_template('upload5.html')

@upload_bp.route('/upload6', methods=['GET', 'POST'])
@login_required
def upload6():
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
                # 一時ファイルの作成
                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
                    temp_path = temp_file.name
                    # アップロードされたファイルを一時ファイルに保存
                    file.save(temp_path)
                    
                try:
                    # import_from_csv関数を使用してCSVデータをインポート
                    success, message = FmpDat.import_from_csv(
                        temp_path,
                        encoding='shift_jis'
                    )
                    
                    if success:
                        flash(message)
                        return redirect(request.url)
                    else:
                        flash(f'インポート中にエラーが発生しました: {message}')
                        return redirect(request.url)
                    
                finally:
                    # 一時ファイルの削除
                    try:
                        os.unlink(temp_path)
                    except Exception as e:
                        log_error(f'一時ファイルの削除中にエラーが発生しました: {str(e)}')
                        
            except Exception as e:
                log_error(f'CSVファイル処理中にエラーが発生しました: {str(e)}')
                flash('CSVファイルの処理中にエラーが発生しました')
                return redirect(request.url)
            
    return render_template('upload6.html')

@upload_bp.route('/upload_prd_mst', methods=['GET', 'POST'])
@login_required
def upload_prd_mst():
    """PRD_MST（製品マスタ）用CSV取り込み"""
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
                # 一時ファイルの作成
                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
                    temp_path = temp_file.name
                    # アップロードされたファイルを一時ファイルに保存
                    file.save(temp_path)
                
                try:
                    # import_from_csv関数を使用してCSVデータをインポート
                    success, message = PrdMstModel.import_from_csv(
                        temp_path,
                        encoding='shift_jis'  # Shift-JISエンコーディングを指定
                    )
                    
                    if success:
                        flash(message)
                        return redirect(request.url)
                    else:
                        flash(f'インポート中にエラーが発生しました: {message}')
                        return redirect(request.url)
                
                finally:
                    # 一時ファイルの削除
                    try:
                        os.unlink(temp_path)
                    except Exception as e:
                        log_error(f'一時ファイルの削除中にエラーが発生しました: {str(e)}')
            
            except Exception as e:
                log_error(f'CSVファイル処理中にエラーが発生しました: {str(e)}')
                flash('CSVファイルの処理中にエラーが発生しました')
                return redirect(request.url)
    
    return render_template('upload_prd_mst.html')


