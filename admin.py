from flask import Blueprint, render_template, request, redirect, url_for, flash, session, Response, jsonify
from functools import wraps
from utils import get_db_connection, get_tables
from mapping import table_mappings, normalize_column_name
from auth import Auth
import threading
import logging
import os
import tempfile
import shutil
import pandas as pd
from sqlalchemy.sql import text
import csv
import io
from datetime import datetime, timedelta
from passlib.hash import bcrypt

admin_bp = Blueprint('admin', __name__, template_folder='templates/admin')

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# List of allowed email IDs for general admin access
ALLOWED_EMAILS = [
    'admin1@megaserve.tech',
    'user1@megaserve.tech',
    'avinash@megaserve.tech'
]

# Decorator to restrict access based on email
def restrict_email(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'email' not in session or session['email'] not in ALLOWED_EMAILS:
            flash('Access denied: Unauthorized email.', 'danger')
            logger.warning(f"Unauthorized access attempt: {session.get('email', 'None')}")
            return render_template('admin_users.html', is_authorized=False)
        return f(*args, **kwargs)
    return decorated_function

# Decorator to restrict user management to avinash@megaserve.tech with admin role
def restrict_admin_user_management(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'email' not in session or session['email'] != 'avinash@megaserve.tech' or session['role'] != 'admin':
            flash('Access denied: Only avinash@megaserve.tech with admin role can manage users.', 'danger')
            logger.warning(f"Access denied for user management: {session.get('email', 'None')}, role: {session.get('role', 'None')}")
            return redirect(url_for('dashboard.dashboard_route'))
        return f(*args, **kwargs)
    return decorated_function

def find_column(df, possible_names):
    """Find the first matching column name from a list of possible names."""
    for name in possible_names:
        if name in df.columns:
            return name
    return None

def process_aggregate_data(excluded_users, engine, export=False, use_latest_date=False, selected_date=None):
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SHOW TABLES LIKE 'users'"))
            if not result.fetchall():
                logger.warning("Table 'users' does not exist")
                return None, None, None, None, [], [], 0, [], [], None, "Table 'users' does not exist"

            summary = pd.read_sql_table('users', con=engine, coerce_float=True, parse_dates=['date'])
            logger.info(f"Read {len(summary)} rows from users table")

            def clean_user_id(uid):
                try:
                    if uid is None or pd.isna(uid):
                        return ''
                    uid = str(uid).strip()
                    if uid and uid.startswith('0'):
                        uid = uid[1:]
                    return uid
                except Exception as e:
                    logger.error(f"Error cleaning user_id {uid} (type: {type(uid)}): {str(e)}")
                    return str(uid) if uid is not None else ''

            required_columns = {'user_id', 'date', 'allocation', 'mtm_all', 'server', 'algo'}
            if not required_columns.issubset(summary.columns):
                missing_cols = required_columns - set(summary.columns)
                logger.warning(f"Missing columns: {missing_cols}")
                return None, None, None, None, [], [], 0, [], [], None, f"Missing columns: {missing_cols}"

            summary['user_id'] = summary['user_id'].apply(clean_user_id)
            summary['date'] = pd.to_datetime(summary['date'], errors='coerce').dt.date

            numeric_columns = ['allocation', 'mtm_all']
            optional_columns = ['max_loss', 'available_margin', 'total_orders', 'total_lots']
            for col in numeric_columns + optional_columns:
                if col in summary.columns:
                    summary[col] = pd.to_numeric(summary[col], errors='coerce').fillna(0)

            all_user_ids = sorted(summary['user_id'].unique().tolist())
            logger.info(f"All user IDs: {len(all_user_ids)}")

            latest_date = summary['date'].max()
            if pd.isna(latest_date):
                logger.warning("No valid dates found in users table")
                return None, None, None, None, all_user_ids, [], 0, [], [], None, "No valid dates found"

            if use_latest_date:
                selected_date = latest_date
                logger.info(f"Using latest date: {selected_date}")
            elif selected_date:
                try:
                    selected_date = pd.to_datetime(selected_date).date()
                except ValueError:
                    logger.warning(f"Invalid date format: {selected_date}")
                    return None, None, None, None, all_user_ids, [], 0, [], [], latest_date, "Invalid date format"
            else:
                selected_date = None

            if selected_date:
                filtered_summary = summary[summary['date'] == selected_date]
                logger.info(f"Filtered users for date={selected_date}: {len(filtered_summary)} rows")
            else:
                filtered_summary = summary

            if filtered_summary.empty:
                logger.warning(f"No data found for date={selected_date}")
                return None, None, None, None, all_user_ids, [], 0, [], [], latest_date, f"No data found for date {selected_date}"

            filtered_summary = filtered_summary[filtered_summary['server'] != '5 Total']
            filtered_summary = filtered_summary[
                (filtered_summary['allocation'].notnull()) & 
                (filtered_summary['allocation'] != 0) & 
                (filtered_summary['mtm_all'].notnull()) & 
                (filtered_summary['mtm_all'] != 0)
            ]
            filtered_summary = filtered_summary[~filtered_summary['alias'].str.contains('DEAL', case=False, na=False)]
            filtered_summary = filtered_summary[
                ~(
                    (filtered_summary['max_loss'].isnull() | (filtered_summary['max_loss'] == 0)) &
                    (filtered_summary['mtm_all'].isnull() | (filtered_summary['mtm_all'] == 0)) &
                    (filtered_summary['algo'] != 5)
                )
            ]
            filtered_summary = filtered_summary[filtered_summary['user_id'] != '92176368']
            logger.info(f"After all filters: {len(filtered_summary)} rows")

            if filtered_summary.empty:
                logger.warning(f"No data remains after filtering for date={selected_date}")
                return None, None, None, None, all_user_ids, [], 0, [], [], latest_date, "No users meet the criteria"

            if excluded_users:
                filtered_summary = filtered_summary[~filtered_summary['user_id'].isin(excluded_users)]
                if filtered_summary.empty:
                    logger.warning("All users excluded")
                    return None, None, None, None, all_user_ids, [], 0, [], [], latest_date, "All users excluded"

            num_algos = filtered_summary['algo'].nunique()
            unique_server_count = filtered_summary['server'].nunique()
            logger.info(f"Number of algos: {num_algos}, Unique server count: {unique_server_count}")

            filtered_summary['Return Ratio'] = (filtered_summary['mtm_all'] / filtered_summary['allocation']).round(2)
            user_ratios = filtered_summary.groupby(['user_id', 'algo'])['Return Ratio'].mean().reset_index()
            user_count = len(user_ratios)
            top_count = max(1, int(user_count * 0.2))
            top_users = user_ratios.sort_values(by='Return Ratio', ascending=False).head(top_count).to_dict('records')
            least_users = user_ratios.sort_values(by='Return Ratio').head(top_count).to_dict('records')
            logger.info(f"Top users: {len(top_users)}, Least users: {len(least_users)}")

            grouped = filtered_summary.groupby(['algo', 'server']).agg(
                **{
                    'No. of Users': pd.NamedAgg(column='user_id', aggfunc='count'),
                    'Sum of ALLOCATION': pd.NamedAgg(column='allocation', aggfunc='sum'),
                    'Sum of MTM (All)': pd.NamedAgg(column='mtm_all', aggfunc='sum')
                }
            ).reset_index()

            grouped['Return Ratio'] = (grouped['Sum of MTM (All)'] / grouped['Sum of ALLOCATION']).round(2)

            final_df = grouped.sort_values(by=['algo', 'server'])
            final_df = final_df.rename(columns={'algo': 'ALGO', 'server': 'SERVER'})
            final_df = final_df[['ALGO', 'SERVER', 'No. of Users', 'Sum of ALLOCATION', 'Sum of MTM (All)', 'Return Ratio']]

            grand_total = {
                'ALGO': 'GRAND TOTAL',
                'SERVER': '',
                'No. of Users': final_df['No. of Users'].sum(),
                'Sum of ALLOCATION': final_df['Sum of ALLOCATION'].sum(),
                'Sum of MTM (All)': final_df['Sum of MTM (All)'].sum(),
                'Return Ratio': round(final_df['Sum of MTM (All)'].sum() / final_df['Sum of ALLOCATION'].sum(), 2)
            }

            data = final_df.to_dict('records')
            data.append(grand_total)
            logger.info(f"Processed {len(data)} records, including Grand Total")

            total_mtm = filtered_summary['mtm_all'].sum()
            num_users = filtered_summary['user_id'].nunique()
            servers = filtered_summary['server'].unique().tolist()
            logger.info(f"Total MTM: {total_mtm}, Users: {num_users}, Servers: {servers}")

            if export == 'csv':
                output = io.StringIO()
                writer = csv.DictWriter(output, fieldnames=['ALGO', 'SERVER', 'No. of Users', 'Sum of ALLOCATION', 'Sum of MTM (All)', 'Return Ratio'])
                writer.writeheader()
                for row in data:
                    writer.writerow(row)
                output.seek(0)
                return output, total_mtm, num_users, num_algos, servers, all_user_ids, top_users, least_users, latest_date, None

            return data, total_mtm, num_users, num_algos, servers, all_user_ids, top_users, least_users, latest_date, None

    except Exception as e:
        logger.error(f"Error in process_aggregate_data: {str(e)}")
        return None, None, None, None, [], [], 0, [], [], None, f"Error processing data: {str(e)}"

@admin_bp.route('/home', methods=['GET', 'POST'])
def admin_home():
    logger.info(f"Accessing admin_home, Session: {session}")
    if 'role' not in session or session.get('role', '') not in ['admin'] or not session['authenticated']:
        logger.info("Redirecting to login due to failed session check")
        flash("Please log in as admin to access this page", "error")
        return redirect(url_for('login.login'))
    
    tables = admin_bp.get_tables()
    logger.info(f"Passing tables to admin_home dropdown: {tables}")
    return render_template('admin_home.html', tables=tables)

@admin_bp.route('/create_table', methods=['POST'])
def create_table_route():
    logger.info(f"Accessing create_table_route, Session: {session}")
    if 'role' not in session or session.get('role', '') not in ['admin'] or not session['authenticated']:
        logger.info("Redirecting to login due to failed session check")
        flash("Please log in as admin to access this page", "error")
        return redirect(url_for('login.login'))
    
    table_name = request.form.get('table_name', '').strip()
    if not table_name:
        flash("Table name cannot be empty", "error")
    else:
        success, msg, category = admin_bp.create_table(table_name)
        flash(msg, category)
    
    return redirect(url_for('admin.admin_home'))

@admin_bp.route('/upload_files', methods=['POST'])
def upload_files():
    logger.info(f"Accessing upload_files, Session: {session}")
    if 'role' not in session or session.get('role', '') not in ['admin'] or not session['authenticated']:
        logger.info("Redirecting to login due to failed session check")
        flash("Please log in as admin to access this page", "error")
        return redirect(url_for('login.login'))
    
    table_name = request.form.get('table', '').strip()
    files = request.files.getlist('file_upload')
    has_header = request.form.get('has_header', 'true') == 'true'
    
    if not table_name:
        flash("Please select a table", "error")
        return redirect(url_for('admin.admin_home'))
    
    if not files or all(file.filename == '' for file in files):
        flash("No files selected for upload", "error")
        return redirect(url_for('admin.admin_home'))
    
    temp_dir = tempfile.mkdtemp()
    uploaded_files = []
    
    try:
        logger.info(f"Starting upload from local files to table: {table_name}")
        for file in files:
            if file and file.filename.endswith(('.csv', '.xlsx', '.xls')):
                file_path = os.path.join(temp_dir, file.filename)
                file.save(file_path)
                uploaded_files.append(file_path)
        
        if not uploaded_files:
            flash("No valid files (CSV, XLSX, or XLS) uploaded", "error")
            return redirect(url_for('admin.admin_home'))
        
        result_list = []
        event = threading.Event()
        uploaded_by = session.get('role', 'unknown')
        thread, event = admin_bp.upload_files_to_table(
            temp_dir, table_name, result_list, event, uploaded_by, has_header=has_header
        )
        thread.join()
        
        for category, message in result_list:
            flash(message, category)
        logger.info(f"Upload completed for table: {table_name}, results: {result_list}")
    
    except Exception as e:
        logger.error(f"Error uploading files: {str(e)}")
        flash(f"Error uploading files: {str(e)}", "error")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    return redirect(url_for('admin.admin_home'))

@admin_bp.route('/summary')
def admin_summary():
    logger.info(f"Accessing admin_summary, Session: {session}")
    if 'role' not in session or session.get('role', '') not in ['admin'] or not session['authenticated']:
        logger.info("Redirecting to login due to failed session check")
        flash("Please log in as admin to access this page", "error")
        return redirect(url_for('login.login'))
    predefined_tables = ['users', 'strategytags', 'legs', 'multilegorders', 'positions', 'portfolios', 'orderbook']
    logger.info(f"Passing tables to admin_summary: {predefined_tables}")
    return render_template('admin_summary.html', tables=predefined_tables)

@admin_bp.route('/orderbook')
def admin_orderbook():
    logger.info(f"Accessing admin_orderbook, Session: {session}")
    if 'role' not in session or session.get('role', '') not in ['admin'] or not session['authenticated']:
        logger.info("Redirecting to login due to failed session check")
        flash("Please log in as admin to access this page", "error")
        return redirect(url_for('login.login'))
    predefined_tables = ['ob']
    logger.info(f"Passing tables to admin_orderbook: {predefined_tables}")
    return render_template('admin_orderbook.html', tables=predefined_tables)

@admin_bp.route('/gridlog')
def admin_gridlog():
    logger.info(f"Accessing admin_gridlog, Session: {session}")
    if 'role' not in session or session.get('role', '') not in ['admin'] or not session['authenticated']:
        logger.info("Redirecting to login due to failed session check")
        flash("Please log in as admin to access this page", "error")
        return redirect(url_for('login.login'))
    predefined_tables = ['gridlog']
    logger.info(f"Passing tables to admin_gridlog: {predefined_tables}")
    return render_template('admin_gridlog.html', tables=predefined_tables)

@admin_bp.route('/other')
def admin_other():
    logger.info(f"Accessing admin_other, Session: {session}")
    if 'role' not in session or session.get('role', '') not in ['admin'] or not session['authenticated']:
        logger.info("Redirecting to login due to failed session check")
        flash("Please log in as admin to access this page", "error")
        return redirect(url_for('login.login'))
    predefined_tables = ['orderbook', 'users', 'strategytags', 'legs', 'multilegorders', 'positions', 'portfolios', 'ob', 'gridlog']
    all_tables = admin_bp.get_tables()
    other_tables = [table for table in all_tables if table not in predefined_tables]
    logger.info(f"Passing tables to admin_other: {other_tables}")
    return render_template('admin_other.html', tables=other_tables)

@admin_bp.route('/users', methods=['GET', 'POST'])
@restrict_email
@restrict_admin_user_management
def admin_users():
    logger.info(f"Accessing admin_users, Session: {session}")
    try:
        Auth.init_db()
    except Exception as e:
        flash("Database initialization failed. Please try again later.", "danger")
        logger.error(f"Database initialization failed: {e}")
        return render_template('admin_users.html', users=[], is_authorized=True)

    try:
        users = Auth.get_all_users()
        logger.debug(f"Fetched users: {[user['email'] for user in users]}")
        if not users:
            logger.info("No users found in the database.")
    except Exception as e:
        flash("Error fetching users. Please try again.", "danger")
        logger.error(f"Error fetching users: {e}")
        users = []

    if request.method == 'POST':
        logger.debug(f"Received POST request: {request.form}")
        action = request.form.get('action')
        email = request.form.get('email')
        logger.debug(f"POST action: {action}, email: {email}")

        if not email or not Auth.is_valid_email(email):
            flash("Invalid or missing email address.", "danger")
            logger.warning(f"Invalid email for action {action}: {email}")
            return redirect(url_for('admin.admin_users'))

        if action == 'add_user':
            password = request.form.get('password')
            confirm_password = request.form.get('confirm_password')
            code = request.form.get('code')
            role = request.form.get('role')
            logger.debug(f"Add user: email={email}, role={role}, code={code}")

            if password != confirm_password:
                flash('Passwords do not match.', 'danger')
            elif len(password) < 6:
                flash("Password must be at least 6 characters long.", "danger")
            elif not code:
                flash("Security code is required.", "danger")
            elif role not in ['user', 'admin']:
                flash("Invalid role selected.", "danger")
            else:
                try:
                    if Auth.user_exists(email):
                        flash(f"User {email} already exists.", "danger")
                        logger.warning(f"Cannot add user {email}: Email already exists")
                    elif Auth.add_user(email, password, code, role):
                        flash(f'User {email} added successfully.', 'success')
                        logger.info(f"User {email} added with role {role} by {session['email']}")
                    else:
                        flash('Error adding user.', 'danger')
                        logger.warning(f"Failed to add user {email}: Unknown error")
                except Exception as e:
                    flash("Error adding user. Please try again.", "danger")
                    logger.error(f"Error adding user {email}: {str(e)}")

        elif action == 'delete_user':
            if email == session['email']:
                flash("You cannot delete your own account.", "danger")
            else:
                try:
                    if not Auth.user_exists(email):
                        flash(f"User {email} does not exist in the database.", "danger")
                        logger.warning(f"User not found for deletion: {email}")
                    elif Auth.delete_user(email):
                        flash(f'User {email} deleted successfully.', 'success')
                        logger.info(f"User {email} deleted by {session['email']}")
                    else:
                        flash('Error deleting user.', 'danger')
                        logger.warning(f"Failed to delete user {email}")
                except Exception as e:
                    flash("Error deleting user. Please try again.", "danger")
                    logger.error(f"Error deleting user {email}: {str(e)}")

        elif action == 'update_role':
            new_role = request.form.get('new_role')
            logger.debug(f"Update role: email={email}, new_role={new_role}")
            if email == session['email']:
                flash("You cannot change your own role.", "danger")
            elif new_role not in ['user', 'admin']:
                flash("Invalid role selected.", "danger")
            else:
                try:
                    if not Auth.user_exists(email):
                        flash(f"User {email} does not exist in the database.", "danger")
                        logger.warning(f"User not found for role update: {email}")
                    elif Auth.update_role(email, new_role):
                        flash(f"Role updated to {new_role.capitalize()} for {email}.", 'success')
                        logger.info(f"Role updated to {new_role} for {email} by {session['email']}")
                    else:
                        flash('Error updating role.', 'danger')
                        logger.warning(f"Failed to update role for {email}")
                except Exception as e:
                    flash("Error updating role. Please try again.", "danger")
                    logger.error(f"Error updating role for {email}: {str(e)}")

        elif action == 'change_password':
            new_password = request.form.get('new_password')
            confirm_new_password = request.form.get('confirm_new_password')
            new_code = request.form.get('new_code')
            logger.debug(f"Change password: email={email}, new_code={new_code}")

            if new_password != confirm_new_password:
                flash("New passwords do not match.", "danger")
                logger.warning(f"Password change failed for {email}: Passwords do not match")
            elif len(new_password) < 6:
                flash("New password must be at least 6 characters long.", "danger")
                logger.warning(f"Password change failed for {email}: Password too short")
            elif not new_code:
                flash("New security code is required.", "danger")
                logger.warning(f"Password change failed for {email}: Missing code")
            else:
                try:
                    if not Auth.user_exists(email):
                        flash(f"User {email} does not exist in the database.", "danger")
                        logger.warning(f"User not found for password/code update: {email}")
                    elif Auth.update_password_and_code(email, new_password, new_code):
                        flash(f"Password and code updated successfully for {email}.", "success")
                        logger.info(f"Password and code updated for {email} by {session['email']}")
                    else:
                        flash("Error updating password/code.", "danger")
                        logger.warning(f"Failed to update password/code for {email}")
                except Exception as e:
                    flash("Error updating password/code. Please try again.", "danger")
                    logger.error(f"Error updating password/code for {email}: {str(e)}")

        return redirect(url_for('admin.admin_users'))

    return render_template('admin_users.html', users=users, is_authorized=True)

def create_table(table_name):
    try:
        engine = get_db_connection()
        with engine.connect() as conn:
            conn.execute(text(f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY)"))
            conn.commit()
        return True, f"Table '{table_name}' created successfully", "success"
    except Exception as e:
        return False, f"Error creating table: {str(e)}", "error"

def upload_files_to_table(file_source, table_name, result_list, event, uploaded_by, has_header=True):
    def upload_task():
        engine = get_db_connection()
        if not engine:
            result_list.append(("error", "Database connection failed"))
            event.set()
            return
        
        try:
            column_mapping = table_mappings.get(table_name, {})
            if not column_mapping:
                result_list.append(("error", f"No column mapping found for table {table_name}"))
                event.set()
                return
            
            with engine.connect() as conn:
                result = conn.execute(text(f"SHOW COLUMNS FROM {table_name}"))
                table_columns = [row['Field'] for row in result.mappings()]
                logger.debug(f"Table {table_name} columns: {table_columns}")

            uploaded_files = [os.path.join(file_source, f) for f in os.listdir(file_source) if f.endswith(('.csv', '.xlsx', '.xls'))]
            
            for file_path in uploaded_files:
                try:
                    if file_path.endswith('.csv'):
                        df = pd.read_csv(file_path, header=0 if has_header else None)
                    elif file_path.endswith(('.xlsx', '.xls')):
                        df = pd.read_excel(file_path, header=0 if has_header else None)
                    
                    original_columns = df.columns.tolist()
                    normalized_columns = [normalize_column_name(col, column_mapping) for col in original_columns]
                    df.columns = normalized_columns
                    logger.debug(f"File {os.path.basename(file_path)} columns normalized: {original_columns} -> {normalized_columns}")
                    
                    valid_columns = [col for col in df.columns if col in column_mapping and col in table_columns]
                    if not valid_columns:
                        result_list.append(("error", f"No valid columns found in {os.path.basename(file_path)} for table {table_name}"))
                        continue
                    df = df[valid_columns]
                    
                    df.to_sql(table_name, engine, if_exists='append', index=False)
                    result_list.append(("success", f"Uploaded {os.path.basename(file_path)} to {table_name} ({len(df)} rows)"))
                    logger.info(f"Uploaded {file_path} to {table_name} with {len(df)} rows")
                
                except Exception as e:
                    result_list.append(("error", f"Failed to upload {os.path.basename(file_path)}: {str(e)}"))
                    logger.error(f"Error uploading {file_path}: {str(e)}")
        
        except Exception as e:
            result_list.append(("error", f"Database error: {str(e)}"))
            logger.error(f"Database error in upload_files_to_table: {str(e)}")
        
        finally:
            event.set()
    
    thread = threading.Thread(target=upload_task)
    thread.start()
    return thread, event

admin_bp.create_table = create_table
admin_bp.upload_files_to_table = upload_files_to_table
admin_bp.get_tables = get_tables