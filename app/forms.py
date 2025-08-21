from flask_wtf import FlaskForm
from wtforms import DateField, SelectField, StringField, IntegerField, TextAreaField, DecimalField, SubmitField
from wtforms.validators import DataRequired, Optional, Length

class SetMstForm(FlaskForm):
    SET_DRW_INT = IntegerField('描画間隔', validators=[Optional()])
    SET_CHS_TM = IntegerField('抽出期間', validators=[Optional()])
    SET_INFO_H1 = StringField('お知らせヘッダ１', validators=[Optional()])
    SET_INFO_1 = TextAreaField('お知らせ１', validators=[Optional()])
    SET_INFO_H2 = StringField('お知らせヘッダ２', validators=[Optional()])
    SET_INFO_2 = TextAreaField('お知らせ２', validators=[Optional()])
    SET_INFO_H3 = StringField('お知らせヘッダ３', validators=[Optional()])
    SET_INFO_3 = TextAreaField('お知らせ３', validators=[Optional()])

class FmcDatForm(FlaskForm):
    """FMC_DAT入力用フォーム"""
    cr_choices = [('0', 'なし'), ('1', 'あり'),('2','出荷CTあり'),('3','出荷CTなし')]
    FMC_CUT_DATE = DateField('カット日', validators=[DataRequired()])
    FMC_R1_INJ_DATE = DateField('R1注入日', validators=[Optional()])
    FMC_MONOMER = SelectField('モノマー', choices=[], validators=[DataRequired()])
    FMC_ANNEAL_NO = DecimalField('アニール№', validators=[DataRequired()])
    FMC_CUT_MACH_NO = DecimalField('カット機№', validators=[DataRequired()])
    FMC_ITEM = SelectField('アイテム', choices=[], validators=[DataRequired()])
    FMC_CUT_MENU = SelectField('カットメニュー', choices=[], validators=[DataRequired()])
    FMC_FILM_PROC_DT = DateField('膜加工日', validators=[DataRequired()])
    FMC_CR_FILM = SelectField('CR膜', choices=cr_choices, validators=[Optional()])
    FMC_HEAT_PROC_DT = DateField('熱処理日', validators=[Optional()])
    FMC_FILM_CURVE = SelectField('膜カーブ', choices=[], validators=[DataRequired()])
    FMC_COLOR = SelectField('色', choices=[], validators=[DataRequired()])
    FMC_AMPM = SelectField('AM/PM', choices=[('0', '未設定'),('1', 'AM'), ('2', 'PM')], validators=[Optional()])
    FMC_INPUT_QTY = DecimalField('投入数', validators=[DataRequired()])
    FMC_CUT_FOREIGN = DecimalField('カットブツ', validators=[Optional()])
    FMC_CUT_WRINKLE = DecimalField('カットシワ', validators=[Optional()])
    FMC_CUT_WAVE = DecimalField('カットウエーブ', validators=[Optional()])
    FMC_CUT_ERR = DecimalField('カットミス', validators=[Optional()])
    FMC_CUT_CRACK = DecimalField('カットサケ', validators=[Optional()])
    FMC_CUT_SCRATCH = DecimalField('カットキズ', validators=[Optional()])
    FMC_CUT_OTHERS = DecimalField('カットその他', validators=[Optional()])
    FMC_GOOD_QTY = DecimalField('良品数', validators=[Optional()])
    FMC_WASH_WRINKLE = DecimalField('洗浄シワ', validators=[Optional()])
    FMC_WASH_SCRATCH = DecimalField('洗浄キズ', validators=[Optional()])
    FMC_WASH_FOREIGN = DecimalField('洗浄イブツ', validators=[Optional()])
    FMC_WASH_ACETONE = DecimalField('洗浄アセトン', validators=[Optional()])
    FMC_WASH_ERR = DecimalField('洗浄ミス', validators=[Optional()])
    FMC_WASH_CUT_ERR = DecimalField('洗浄カットミス', validators=[Optional()])
    FMC_WASH_OTHERS = DecimalField('洗浄その他', validators=[Optional()])
    FMC_PASS_QTY = DecimalField('合格数', validators=[Optional()])
    
    submit = SubmitField('保存')

class KbnMstForm(FlaskForm):
    """KBN_MST(区分マスタ)入力用フォーム"""
    KBN_TYP = StringField('区分種別', validators=[
        DataRequired(message='区分種別を入力してください'),
        Length(max=4, message='区分種別は4文字以内で入力してください')
    ])
    KBN_ID = DecimalField('区分ID', validators=[
        DataRequired(message='区分IDを入力してください')
    ])
    KBN_NM = StringField('区分名', validators=[
        Optional(),
        Length(max=30, message='区分名は30文字以内で入力してください')
    ])
    
    submit = SubmitField('保存')

class PrdMstForm(FlaskForm):
    """PRD_MST(製品マスタ)入力用フォーム"""
    PRD_ID = StringField('製品ID', validators=[
        DataRequired(message='製品IDを入力してください'),
        Length(max=5, message='製品IDは5文字以内で入力してください')
    ])
    PRD_KBN = IntegerField('商品分類', validators=[Optional()])
    PRD_TYP = SelectField('識別ID', choices=[], validators=[Optional()])
    PRD_NM = StringField('製品名', validators=[
        Optional(),
        Length(max=60, message='製品名は60文字以内で入力してください')
    ])
    PRD_COLOR = StringField('膜カラー', validators=[
        Optional(),
        Length(max=20, message='膜カラーは20文字以内で入力してください')
    ])
    PRD_PLY_DAYS = IntegerField('重合日数', validators=[Optional()])
    
    submit = SubmitField('保存')


class MnoMstForm(FlaskForm):
    """MNO_MST(モノマーマスタ)入力用フォーム"""
    MNO_SYU = StringField('モノマー種別', validators=[
        DataRequired(message='モノマー種別を入力してください'),
        Length(max=1, message='モノマー種別は1文字以内で入力してください')
    ])
    MNO_NM = StringField('モノマー名', validators=[
        Optional(),
        Length(max=30, message='モノマー名は30文字以内で入力してください')
    ])
    MNO_TARGET = DecimalField('目標値', validators=[Optional()])
    
    submit = SubmitField('保存')

