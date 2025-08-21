from flask import Blueprint, render_template, redirect, url_for, request, flash, current_app
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.security import check_password_hash
from app import db
from app.models import User
from datetime import datetime

auth = Blueprint('auth', __name__)

@auth.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('main.index'))
        
    if request.method == 'POST':
        user_id = request.form.get('user_id')
        password = request.form.get('password')
        
        user = User.query.filter_by(USER_ID=user_id).first()
        
        if user and user.check_password(password):
            login_user(user)
            user.update_last_login()
            next_page = request.args.get('next')
            return redirect(next_page or url_for('main.index'))
        
        flash('ユーザーIDまたはパスワードが正しくありません。')
    
    return render_template('login.html')

@auth.route('/logout')
@login_required
def logout():
    """ユーザーをログアウトさせる"""
    logout_user()
    flash('ログアウトしました。')
    return redirect(url_for('auth.login')) 