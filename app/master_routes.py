from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required
from .master_models import PrdMstModel, KbnMst, MnoMstModel
from .master_models import MnoMstModel
from .forms import PrdMstForm, KbnMstForm, MnoMstForm

# マスタ関連のBlueprintを作成
master = Blueprint('master', __name__)

# ==================== PRD_MST (製品マスタ) ルート ====================

@master.route('/prd_mst_list')
@login_required
def prd_mst_list():
    """PRD_MSTの一覧表示"""
    # 検索パラメータの取得
    prd_id = request.args.get('prd_id')
    prd_nm = request.args.get('prd_nm')
    prd_color = request.args.get('prd_color')
    prd_typ = request.args.get('prd_typ')
    
    # 検索実行
    if any([prd_id, prd_nm, prd_color, prd_typ]):
        records = PrdMstModel.search(prd_id, prd_nm, prd_color, prd_typ)
    else:
        records = PrdMstModel.get_all()
    
    # MnoMstModelから識別IDのマッピングを作成
    mono_mst_list = MnoMstModel.get_all()
    mono_mst_dict = {m.MNO_SYU: m.MNO_NM for m in mono_mst_list}
    
    return render_template('prd_mst_list.html', records=records, mono_mst_dict=mono_mst_dict, mono_mst_list=mono_mst_list)

@master.route('/prd_mst/create', methods=['GET', 'POST'])
@login_required
def prd_mst_create():
    """PRD_MSTの新規作成"""
    form = PrdMstForm()
    
    # MnoMstModelから識別IDの選択肢を取得
    mono_mst_list = MnoMstModel.get_all()
    form.PRD_TYP.choices = [('', '--- 選択してください ---')] + [(m.MNO_SYU, f'{m.MNO_SYU} - {m.MNO_NM}') for m in mono_mst_list]
    
    if form.validate_on_submit():
        prd_mst = PrdMstModel()
        form.populate_obj(prd_mst)
        if prd_mst.save():
            flash('製品マスタを保存しました。', 'success')
            return redirect(url_for('master.prd_mst_list'))
        else:
            flash('製品マスタの保存に失敗しました。', 'error')
    
    return render_template('prd_mst_edit.html', form=form, is_create=True)

@master.route('/prd_mst/edit/<string:prd_id>', methods=['GET', 'POST'])
@login_required
def prd_mst_edit(prd_id):
    """PRD_MSTの編集"""
    prd_mst = PrdMstModel.get_by_id(prd_id)
    if not prd_mst:
        flash('指定された製品マスタが見つかりません。', 'error')
        return redirect(url_for('master.prd_mst_list'))
    
    form = PrdMstForm(obj=prd_mst)
    # 編集時は製品IDを変更不可にする
    form.PRD_ID.render_kw = {'readonly': True}
    
    # MnoMstModelから識別IDの選択肢を取得
    mono_mst_list = MnoMstModel.get_all()
    form.PRD_TYP.choices = [('', '--- 選択してください ---')] + [(m.MNO_SYU, f'{m.MNO_SYU} - {m.MNO_NM}') for m in mono_mst_list]
    
    if request.method == 'POST':
        # 削除処理
        if request.form.get('action') == 'delete':
            if prd_mst.delete():
                flash('製品マスタを削除しました。', 'success')
            else:
                flash('製品マスタの削除に失敗しました。', 'error')
            return redirect(url_for('master.prd_mst_list'))
        
        # 更新処理
        if form.validate_on_submit():
            form.populate_obj(prd_mst)
            if prd_mst.save():
                flash('製品マスタを更新しました。', 'success')
                return redirect(url_for('master.prd_mst_list'))
            else:
                flash('製品マスタの更新に失敗しました。', 'error')
    
    return render_template('prd_mst_edit.html', form=form, is_create=False)

@master.route('/prd_mst/delete/<string:prd_id>', methods=['POST'])
@login_required
def prd_mst_delete(prd_id):
    """PRD_MSTの削除（AJAX用）"""
    prd_mst = PrdMstModel.get_by_id(prd_id)
    if not prd_mst:
        return jsonify({'success': False, 'message': '指定された製品マスタが見つかりません。'})
    
    if prd_mst.delete():
        return jsonify({'success': True, 'message': '製品マスタを削除しました。'})
    else:
        return jsonify({'success': False, 'message': '製品マスタの削除に失敗しました。'})

# ==================== KBN_MST (区分マスタ) ルート ====================

@master.route('/kbn_mst_list')
@login_required
def kbn_mst_list():
    """KBN_MSTの一覧表示"""
    # 検索パラメータの取得
    kbn_typ = request.args.get('kbn_typ')
    
    records = []
    if kbn_typ:
        records = KbnMst.get_kbn_list(kbn_typ)
    else:
        records = KbnMst.get_all()
    
    # 区分種別の一覧を取得
    kbn_types = KbnMst.get_distinct_types()
    
    return render_template('kbn_mst_list.html', records=records, kbn_types=kbn_types, selected_typ=kbn_typ)

@master.route('/kbn_mst/create', methods=['GET', 'POST'])
@login_required
def kbn_mst_create():
    """KBN_MSTの新規作成"""
    form = KbnMstForm()
    
    if form.validate_on_submit():
        # 既存チェック
        if KbnMst.exists(form.KBN_TYP.data, form.KBN_ID.data):
            flash('同じ区分種別・区分IDの組み合わせが既に存在します。', 'error')
        else:
            kbn_mst = KbnMst(
                KBN_TYP=form.KBN_TYP.data,
                KBN_ID=form.KBN_ID.data,
                KBN_NM=form.KBN_NM.data
            )
            if kbn_mst.save():
                flash('区分マスタを保存しました。', 'success')
                return redirect(url_for('master.kbn_mst_list'))
            else:
                flash('区分マスタの保存に失敗しました。', 'error')
    
    return render_template('kbn_mst_edit.html', form=form, is_create=True)

@master.route('/kbn_mst/edit/<string:kbn_typ>/<kbn_id>', methods=['GET', 'POST'])
@login_required
def kbn_mst_edit(kbn_typ, kbn_id):
    """KBN_MSTの編集"""
    # kbn_idを数値に変換
    try:
        kbn_id = int(kbn_id)
    except ValueError:
        flash('無効な区分IDです。', 'error')
        return redirect(url_for('master.kbn_mst_list'))
    
    kbn_mst = KbnMst.get_by_keys(kbn_typ, kbn_id)
    if not kbn_mst:
        flash('指定された区分マスタが見つかりません。', 'error')
        return redirect(url_for('master.kbn_mst_list'))
    
    form = KbnMstForm(obj=kbn_mst)
    # 編集時は主キーを変更不可にする
    form.KBN_TYP.render_kw = {'readonly': True}
    form.KBN_ID.render_kw = {'readonly': True}
    
    if form.validate_on_submit():
        # 区分名のみ更新可能
        kbn_mst.KBN_NM = form.KBN_NM.data
        if kbn_mst.save():
            flash('区分マスタを更新しました。', 'success')
            return redirect(url_for('master.kbn_mst_list'))
        else:
            flash('区分マスタの更新に失敗しました。', 'error')
    
    return render_template('kbn_mst_edit.html', form=form, is_create=False, kbn_mst=kbn_mst)

@master.route('/kbn_mst/delete/<string:kbn_typ>/<kbn_id>', methods=['POST'])
@login_required
def kbn_mst_delete(kbn_typ, kbn_id):
    """KBN_MSTの削除"""
    try:
        # kbn_idを数値に変換
        kbn_id = int(kbn_id)
    except ValueError:
        return jsonify({'success': False, 'message': '無効な区分IDです。'})
    
    try:
        kbn_mst = KbnMst.get_by_keys(kbn_typ, kbn_id)
        if not kbn_mst:
            return jsonify({'success': False, 'message': '指定された区分マスタが見つかりません。'})
        
        if kbn_mst.delete():
            return jsonify({'success': True, 'message': '区分マスタを削除しました。'})
        else:
            return jsonify({'success': False, 'message': '区分マスタの削除に失敗しました。'})
    
    except Exception as e:
        from .utils import log_error
        log_error(f'区分マスタ削除エラー: {str(e)}')
        return jsonify({'success': False, 'message': 'データの削除に失敗しました。'})

# ==================== MNO_MST (モノマーマスタ) ルート ====================

@master.route('/mno_mst_list')
@login_required
def mno_mst_list():
    """MNO_MSTの一覧表示"""
    # 検索パラメータの取得
    mno_syu = request.args.get('mno_syu')
    mno_nm = request.args.get('mno_nm')
    
    # 検索実行
    if any([mno_syu, mno_nm]):
        records = MnoMstModel.search(mno_syu, mno_nm)
    else:
        records = MnoMstModel.get_all()
    
    return render_template('mno_mst_list.html', records=records)

@master.route('/mno_mst/create', methods=['GET', 'POST'])
@login_required
def mno_mst_create():
    """MNO_MSTの新規作成"""
    form = MnoMstForm()
    
    if form.validate_on_submit():
        mno_mst = MnoMstModel()
        form.populate_obj(mno_mst)
        if mno_mst.save():
            flash('モノマーマスタを保存しました。', 'success')
            return redirect(url_for('master.mno_mst_list'))
        else:
            flash('モノマーマスタの保存に失敗しました。', 'error')
    
    return render_template('mno_mst_edit.html', form=form, is_create=True)

@master.route('/mno_mst/edit/<string:mno_syu>', methods=['GET', 'POST'])
@login_required
def mno_mst_edit(mno_syu):
    """MNO_MSTの編集"""
    mno_mst = MnoMstModel.get_by_id(mno_syu)
    if not mno_mst:
        flash('指定されたモノマーマスタが見つかりません。', 'error')
        return redirect(url_for('master.mno_mst_list'))
    
    form = MnoMstForm(obj=mno_mst)
    # 編集時はモノマー種別を変更不可にする
    form.MNO_SYU.render_kw = {'readonly': True}
    
    if request.method == 'POST':
        # 削除処理
        if request.form.get('action') == 'delete':
            if mno_mst.delete():
                flash('モノマーマスタを削除しました。', 'success')
            else:
                flash('モノマーマスタの削除に失敗しました。', 'error')
            return redirect(url_for('master.mno_mst_list'))
        
        # 更新処理
        if form.validate_on_submit():
            form.populate_obj(mno_mst)
            if mno_mst.save():
                flash('モノマーマスタを更新しました。', 'success')
                return redirect(url_for('master.mno_mst_list'))
            else:
                flash('モノマーマスタの更新に失敗しました。', 'error')
    
    return render_template('mno_mst_edit.html', form=form, is_create=False)

@master.route('/mno_mst/delete/<string:mno_syu>', methods=['POST'])
@login_required
def mno_mst_delete(mno_syu):
    """MNO_MSTの削除（AJAX用）"""
    mno_mst = MnoMstModel.get_by_id(mno_syu)
    if not mno_mst:
        return jsonify({'success': False, 'message': '指定されたモノマーマスタが見つかりません。'})
    
    if mno_mst.delete():
        return jsonify({'success': True, 'message': 'モノマーマスタを削除しました。'})
    else:
        return jsonify({'success': False, 'message': 'モノマーマスタの削除に失敗しました。'}) 