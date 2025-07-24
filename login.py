from flask import Blueprint, render_template, request, redirect, url_for, flash, session
import logging
from auth import Auth

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

login_bp = Blueprint('login', __name__, template_folder='templates')

@login_bp.route('/login', methods=['GET', 'POST'])
def login():
    """Handle user login"""
    # Initialize database
    try:
        Auth.init_db()
    except Exception as e:
        flash("Database initialization failed. Please try again later.", "error")
        logger.error(f"Database initialization failed: {e}")
        return render_template('login.html')

    # Redirect authenticated users
    if 'authenticated' in session and session['authenticated']:
        logger.debug(f"User already authenticated, redirecting based on role: {session.get('role')}")
        if session.get('role') == 'admin':
            return redirect(url_for('dashboard.dashboard_route'))
        elif session.get('role') == 'user':
            return redirect(url_for('dashboard.dashboard_route'))
        else:
            session.clear()
            flash("Invalid role detected, please log in again", "error")

    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')

        logger.debug(f"Login attempt - Email: '{email}'")
        if not Auth.is_valid_email(email):
            logger.warning(f"Invalid email format or domain: {email}")
            flash("Email must be a valid @megaserve.tech address", "error")
            return render_template('login.html')

        user = Auth.authenticate(email, password)
        if user:
            session.update(user)
            flash(f"Welcome, {email}! Logged in successfully", "success")
            redirect_url = url_for('dashboard.dashboard_route') if user['role'] == 'admin' else url_for('dashboard.dashboard_route')
            logger.debug(f"Redirecting to: {redirect_url}, Session: {session}")
            return redirect(redirect_url)
        else:
            flash("Invalid email or password", "error")

    logger.debug(f"Rendering login page, Session: {session}")
    return render_template('login.html')

@login_bp.route('/logout', methods=['POST'])
def logout():
    """Log out user"""
    Auth.logout()
    flash("Logged out successfully", "success")
    return redirect(url_for('login.login'))

@login_bp.route('/forgot_password', methods=['GET', 'POST'])
def forgot_password():
    """Handle password reset"""
    try:
        Auth.init_db()
    except Exception as e:
        flash("Database initialization failed. Please try again later.", "error")
        logger.error(f"Database initialization failed: {e}")
        return render_template('forgot_password.html', step='verify_code', show_popup=False)

    if request.method == 'POST':
        step = request.form.get('step')

        if step == 'verify_code':
            email = request.form.get('email')
            code = request.form.get('code')

            if not Auth.is_valid_email(email):
                flash("Email must be a valid @megaserve.tech address", "error")
                return render_template('forgot_password.html', step='verify_code', show_popup=False)

            # Check user role
            user_role = Auth.get_user_role(email)  # Assumes Auth.get_user_role exists
            if user_role == 'user':
                flash("User accounts must contact the admin for password reset.", "error")
                return render_template('forgot_password.html', step='verify_code', show_popup=True)

            if Auth.verify_code(email, code):
                logger.info(f"Code verified for {email}")
                session['reset_email'] = email
                return render_template('forgot_password.html', step='reset_password', email=email, show_popup=False)
            else:
                flash("Invalid email or code", "error")
                return render_template('forgot_password.html', step='verify_code', show_popup=False)

        elif step == 'reset_password':
            email = session.get('reset_email')
            new_password = request.form.get('new_password')
            confirm_password = request.form.get('confirm_password')

            if not email or not Auth.is_valid_email(email):
                flash("Invalid session. Please start over.", "error")
                return render_template('forgot_password.html', step='verify_code', show_popup=False)

            # Check user role again to ensure consistency
            user_role = Auth.get_user_role(email)
            if user_role == 'user':
                flash("User accounts must contact the admin for password reset.", "error")
                return render_template('forgot_password.html', step='verify_code', show_popup=True)

            if new_password != confirm_password:
                flash("Passwords do not match", "error")
                return render_template('forgot_password.html', step='reset_password', email=email, show_popup=False)

            if len(new_password) < 6:
                flash("Password must be at least 6 characters long", "error")
                return render_template('forgot_password.html', step='reset_password', email=email, show_popup=False)

            if Auth.reset_password(email, new_password):
                session.clear()
                flash("Password reset successfully. Please log in.", "success")
                logger.info(f"Password reset successful for {email}")
                return redirect(url_for('login.login'))
            else:
                flash("Error resetting password. Please try again.", "error")
                return render_template('forgot_password.html', step='reset_password', email=email, show_popup=False)

    return render_template('forgot_password.html', step='verify_code', show_popup=False)
