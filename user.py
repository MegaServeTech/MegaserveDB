from flask import Blueprint, render_template, request, redirect, url_for, flash, session, Response
from utils import get_db_connection, get_tables, get_table_columns
from pymysql.cursors import DictCursor
from mapping import table_mappings, normalize_column_name
import logging
from datetime import datetime
import threading
import os
import tempfile
import shutil
import pandas as pd
from sqlalchemy.sql import text
import csv
import io


user_bp = Blueprint('user', __name__, template_folder='templates/user')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@user_bp.route('/home')
def user_home():
    logger.info(f"Accessing user_home, Session: {session}")
    if 'role' not in session or session['role'] != 'user' or not session['authenticated']:
        logger.info("Redirecting to login due to failed session check")
        flash("Please log in as user to access this page", "error")
        return redirect(url_for('login.login'))
    tables = get_tables()
    logger.info(f"Passing tables to user_home: {tables}")
    return render_template('user_home.html', tables=tables)

@user_bp.route('/upload_files', methods=['POST'])
def upload_files():
    logger.info(f"Accessing upload_files, Session: {session}")
    if 'role' not in session or session['role'] != 'user' or not session['authenticated']:
        logger.info("Redirecting to login due to failed session check")
        flash("Please log in as user to access this page", "error")
        return redirect(url_for('login.login'))
    
    table_name = request.form.get('table', '').strip()
    if not table_name:
        flash("Please select a table", "error")
        return redirect(url_for('user.user_home'))
    
    files = request.files.getlist('file_upload')
    has_header = request.form.get('has_header', 'true') == 'true'
    
    if not files or all(file.filename == '' for file in files):
        flash("No files selected for upload", "error")
        return redirect(url_for('user.user_home'))
    
    # Create a temporary directory to store uploaded files
    temp_dir = tempfile.mkdtemp()
    uploaded_files = []
    
    try:
        logger.info(f"Starting upload from local files to table: {table_name}")
        for file in files:
            if file and file.filename.endswith(('.csv', '.xlsx', '.xls', '.xlsb')):
                file_path = os.path.join(temp_dir, file.filename)
                file.save(file_path)
                uploaded_files.append(file_path)
        
        if not uploaded_files:
            flash("No valid files (CSV, XLSX, XLS, XLSB) uploaded", "error")
            return redirect(url_for('user.user_home'))
        
        result_list = []
        event = threading.Event()
        uploaded_by = session.get('role', 'unknown')  # Capture uploaded_by from session
        thread, event = user_bp.upload_files_to_table(
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
    
    return redirect(url_for('user.user_home'))

@user_bp.route('/summary')
def user_summary():
    logger.info(f"Accessing user_summary, Session: {session}")
    if 'role' not in session or session['role'] != 'user' or not session['authenticated']:
        logger.info("Redirecting to login due to failed session check")
        flash("Please log in as user to access this page", "error")
        return redirect(url_for('login.login'))
    predefined_tables = ['users', 'strategytags', 'legs', 'multilegorders', 'positions', 'portfolios', 'orderbook']
    logger.info(f"Passing tables to user_summary: {predefined_tables}")
    return render_template('summary.html', tables=predefined_tables)

@user_bp.route('/orderbook')
def user_orderbook():
    logger.info(f"Accessing user_orderbook, Session: {session}")
    if 'role' not in session or session['role'] != 'user' or not session['authenticated']:
        logger.info("Redirecting to login due to failed session check")
        flash("Please log in as user to access this page", "error")
        return redirect(url_for('login.login'))
    predefined_tables = ['ob']
    logger.info(f"Passing tables to user_orderbook: {predefined_tables}")
    return render_template('orderbook.html', tables=predefined_tables)

@user_bp.route('/gridlog')
def user_gridlog():
    logger.info(f"Accessing user_gridlog, Session: {session}")
    if 'role' not in session or session['role'] != 'user' or not session['authenticated']:
        logger.info("Redirecting to login due to failed session check")
        flash("Please log in as user to access this page", "error")
        return redirect(url_for('login.login'))
    predefined_tables = ['gridlog']
    logger.info(f"Passing tables to user_gridlog: {predefined_tables}")
    return render_template('gridlog.html', tables=predefined_tables)

@user_bp.route('/other')
def user_other():
    logger.info(f"Accessing user_other, Session: {session}")
    if 'role' not in session or session['role'] != 'user' or not session['authenticated']:
        logger.info("Redirecting to login due to failed session check")
        flash("Please log in as user to access this page", "error")
        return redirect(url_for('login.login'))
    predefined_tables = ['orderbook', 'users', 'strategytags', 'legs', 'multilegorders', 'positions', 'portfolios', 'ob', 'gridlog']
    all_tables = get_tables()
    other_tables = [table for table in all_tables if table not in predefined_tables]
    logger.info(f"Passing tables to user_other: {other_tables}")
    return render_template('other.html', tables=other_tables)

@user_bp.route('/aggregate', methods=['GET', 'POST'])
def user_aggregate():
    logger.info(f"Accessing user_aggregate, Session: {session}")
    if 'role' not in session or session.get('role', '') not in ['user', 'user'] or not session['authenticated']:
        logger.info("Redirecting to login due to failed session check")
        flash("Please log in to access this page", "error")
        return redirect(url_for('login.login'))

    # Get parameters
    selected_date = request.form.get('selected_date') or request.args.get('selected_date', '')
    export = request.args.get('export')

    # Get excluded user IDs from session
    excluded_users = session.get('excluded_users', [])

    logger.info(f"Parameters - selected_date: {selected_date}, export: {export}, excluded_users: {excluded_users}")

    engine = get_db_connection()
    if not engine:
        flash("Database connection failed", "error")
        return render_template('user_aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date=selected_date, excluded_users=excluded_users, all_user_ids=[])

    try:
        with engine.connect() as conn:
            # Verify users table exists
            result = conn.execute(text("SHOW TABLES LIKE 'users'"))
            if not result.fetchall():
                flash("Table 'users' does not exist. Please create it and upload data.", "error")
                return render_template('user_aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date=selected_date, excluded_users=excluded_users, all_user_ids=[])

            # Read users table
            summary = pd.read_sql_table('users', con=engine, coerce_float=False, parse_dates=['date'])
            logger.info(f"Read {len(summary)} rows from users")

            # Log data types and sample values for debugging
            logger.info(f"users dtypes: {summary.dtypes.to_dict()}")
            logger.info(f"users user_id sample: {summary['user_id'].head().tolist()}")

            # Cleaning function for user_id
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

            # Clean summary
            required_summary_columns = {'user_id', 'date', 'allocation', 'mtm_all', 'server', 'algo'}
            if not required_summary_columns.issubset(summary.columns):
                missing_cols = required_summary_columns - set(summary.columns)
                logger.warning(f"Required columns missing in users table: {missing_cols}")
                flash(f"Required columns {missing_cols} missing in users table", "error")
                return render_template('user_aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date=selected_date, excluded_users=excluded_users, all_user_ids=[])

            summary['user_id'] = summary['user_id'].apply(clean_user_id)
            summary['date'] = pd.to_datetime(summary['date'], errors='coerce').dt.date

            # Convert numeric columns in summary
            numeric_columns = ['allocation', 'mtm_all']
            optional_columns = ['max_loss', 'available_margin', 'total_orders', 'total_lots']
            for col in numeric_columns + optional_columns:
                if col in summary.columns:
                    summary[col] = pd.to_numeric(summary[col], errors='coerce').fillna(0)

            # Get all user IDs for the dropdown
            all_user_ids = sorted(summary['user_id'].unique().tolist())
            logger.info(f"All user IDs: {len(all_user_ids)}")

            # Apply date filter if provided
            total_mtm = None
            num_users = None
            servers = None
            if selected_date:
                try:
                    selected_date = pd.to_datetime(selected_date).date()
                    filtered_summary = summary[summary['date'] == selected_date]
                    logger.info(f"Filtered users for date={selected_date}: {len(filtered_summary)} rows")
                except ValueError:
                    logger.warning(f"Invalid date format: {selected_date}")
                    flash("Invalid date format", "error")
                    return render_template('user_aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date=selected_date, excluded_users=excluded_users, all_user_ids=all_user_ids)

                # Check if data exists for the selected date
                if filtered_summary.empty:
                    logger.warning(f"No data found for date={selected_date}")
                    flash(f"No data found for the selected date {selected_date}", "warning")
                    return render_template('user_aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date=selected_date, excluded_users=excluded_users, all_user_ids=all_user_ids)

                # Filter out rows where SERVER is "5 Total"
                filtered_summary = filtered_summary[filtered_summary['server'] != '5 Total']

                # Exclude users with zero or null in allocation or mtm_all
                filtered_summary = filtered_summary[
                    (filtered_summary['allocation'].notnull()) & 
                    (filtered_summary['allocation'] != 0) & 
                    (filtered_summary['mtm_all'].notnull()) & 
                    (filtered_summary['mtm_all'] != 0)
                ]
                logger.info(f"After filtering out zero/null allocation and mtm_all: {len(filtered_summary)} rows")
                # Exclude users with 'DEAL' in alias (case-insensitive)
                filtered_summary = filtered_summary[
                    ~filtered_summary['alias'].str.contains('DEAL', case=False, na=False)
                ]
                logger.info(f"After filtering out users with 'DEAL' in alias : {len(filtered_summary)} rows")

                # Exclude users with zero or null in max_loss and mtm_all, except for algo == 5
                filtered_summary = filtered_summary[
                    ~(
                        (filtered_summary['max_loss'].isnull() | (filtered_summary['max_loss'] == 0)) &
                        (filtered_summary['mtm_all'].isnull() | (filtered_summary['mtm_all'] == 0)) &
                        (filtered_summary['algo'] != 5)
                    )
                ]
                logger.info(f"After filtering out zero/null max_loss and mtm_all (except algo 5): {len(filtered_summary)} rows")

                # Exclude user_id '92176368'
                filtered_summary = filtered_summary[
                    filtered_summary['user_id'] != '92176368'
                ]
                logger.info(f"After filtering out user_id '92176368': {len(filtered_summary)} rows")

                # Check if any data remains after filtering
                if filtered_summary.empty:
                    logger.warning(f"No data remains after filtering for date={selected_date}")
                    flash("No users | meet the criteria (non-zero and non-null Allocation and MTM (All), no 'DEAL' in algo, and not 'VIVEK_THEBARIA')", "warning")
                    return redirect(url_for('user.user_aggregate')) 
                # Check if any data remains after filtering
                if filtered_summary.empty:
                    logger.warning(f"No data remains after filtering for date={selected_date}")
                    flash("No users meet the criteria (non-zero and non-null values required in Allocation and MTM (All))", "warning")
                    return render_template('user_aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date=selected_date, excluded_users=excluded_users, all_user_ids=all_user_ids)

                # Exclude selected user IDs
                if excluded_users:
                    filtered_summary = filtered_summary[~filtered_summary['user_id'].isin(excluded_users)]
                    if filtered_summary.empty:
                        flash("All users have been excluded from the calculation", "warning")
                        return render_template('user_aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date=selected_date, excluded_users=excluded_users, all_user_ids=all_user_ids)

                # Group by ALGO and SERVER
                grouped = filtered_summary.groupby(['algo', 'server']).agg(
                    **{
                        'No. of Users': pd.NamedAgg(column='user_id', aggfunc='count'),
                        'Sum of ALLOCATION': pd.NamedAgg(column='allocation', aggfunc='sum'),
                        'Sum of MTM (All)': pd.NamedAgg(column='mtm_all', aggfunc='sum')
                    }
                ).reset_index()

                # Calculate Return Ratio
                grouped['Return Ratio'] = (grouped['Sum of MTM (All)'] / grouped['Sum of ALLOCATION'])

                # Sort by ALGO and SERVER
                final_df = grouped.sort_values(by=['algo', 'server'])

                # Rename columns for display
                final_df = final_df.rename(columns={'algo': 'ALGO', 'server': 'SERVER'})

                # Select and order columns
                final_df = final_df[['ALGO', 'SERVER', 'No. of Users', 'Sum of ALLOCATION', 'Sum of MTM (All)', 'Return Ratio']]

                # Round Return Ratio
                final_df['Return Ratio'] = final_df['Return Ratio'].round(2)

                # Compute Grand Total row
                grand_total = {
                    'ALGO': 'GRAND TOTAL',
                    'SERVER': '',
                    'No. of Users': final_df['No. of Users'].sum(),
                    'Sum of ALLOCATION': final_df['Sum of ALLOCATION'].sum(),
                    'Sum of MTM (All)': final_df['Sum of MTM (All)'].sum(),
                    'Return Ratio': round(final_df['Sum of MTM (All)'].sum() / final_df['Sum of ALLOCATION'].sum(), 2)
                }

                # Convert to dictionary for rendering
                data = final_df.to_dict('records')
                # Append Grand Total row
                data.append(grand_total)
                logger.info(f"Processed {len(data)} records for display, including Grand Total")

                # Calculate total MTM (All), number of users, and servers for the selected date
                total_mtm = filtered_summary['mtm_all'].sum()
                num_users = filtered_summary['user_id'].nunique()
                servers = filtered_summary['server'].unique().tolist()
                logger.info(f"Total MTM (All) for date={selected_date}: {total_mtm}, No. of Users: {num_users}, Servers: {servers}")
            else:
                data = None

        # Handle CSV export
        if export == 'csv' and data:
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=['ALGO', 'SERVER', 'No. of Users', '-hyphenSum of ALLOCATION', 'Sum of MTM (All)', 'Return Ratio'])
            writer.writeheader()
            for row in data:
                writer.writerow(row)
            output.seek(0)
            return Response(
                output,
                mimetype='text/csv',
                headers={'Content-Disposition': f'attachment;filename=aggregate_report.csv'}
            )

        # Render template
        if data:
            flash(f"Aggregation completed: {len(data)} records processed", "success")
        return render_template('user_aggregate.html', role=session.get('role'), data=data, total_mtm=total_mtm, num_users=num_users, servers=servers, selected_date=selected_date, excluded_users=excluded_users, all_user_ids=all_user_ids)

    except Exception as e:
        logger.error(f"Error executing aggregation: {str(e)}")
        flash(f"Error processing aggregation: {str(e)}", "error")
        return render_template('user_aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date=selected_date, excluded_users=excluded_users, all_user_ids=[])

@user_bp.route('/aggregate/total_mtm', methods=['POST'])
def user_total_mtm():
    logger.info(f"Accessing user_total_mtm, Session: {session}")
    if 'role' not in session or session.get('role', '') not in ['user', 'user'] or not session['authenticated']:
        logger.info("Redirecting to login due to failed session check")
        flash("Please log in to access this page", "error")
        return redirect(url_for('login.login'))

    # Get parameters
    selected_date = request.form.get('selected_date', '')
    logger.info(f"Parameters - selected_date: {selected_date}")

    if not selected_date:
        flash("Please select a date", "error")
        return redirect(url_for('user.user_aggregate'))

    engine = get_db_connection()
    if not engine:
        flash("Database connection failed", "error")
        return redirect(url_for('user.user_aggregate'))

    try:
        with engine.connect() as conn:
            # Verify users table exists
            result = conn.execute(text("SHOW TABLES LIKE 'users'"))
            if not result.fetchall():
                flash("Table 'users' does not exist. Please create it and upload data.", "error")
                return redirect(url_for('user.user_aggregate'))

            # Read users table
            summary = pd.read_sql_table('users', con=engine, coerce_float=False, parse_dates=['date'])
            logger.info(f"Read {len(summary)} rows from users")

            # Clean date column
            summary['date'] = pd.to_datetime(summary['date'], errors='coerce').dt.date

            # Convert numeric columns
            numeric_columns = ['mtm_all', 'allocation']
            optional_columns = ['max_loss', 'available_margin', 'total_orders', 'total_lots']
            for col in numeric_columns + optional_columns:
                if col in summary.columns:
                    summary[col] = pd.to_numeric(summary[col], errors='coerce').fillna(0)

            # Apply date filter
            try:
                selected_date = pd.to_datetime(selected_date).date()
                filtered_summary = summary[summary['date'] == selected_date]
                logger.info(f"Filtered users for date={selected_date}: {len(filtered_summary)} rows")
            except ValueError:
                logger.warning(f"Invalid date format: {selected_date}")
                flash("Invalid date format", "error")
                return redirect(url_for('user.user_aggregate'))

            # Check if data exists for the selected date
            if filtered_summary.empty:
                logger.warning(f"No data found for date={selected_date}")
                flash(f"No data found for the selected date {selected_date}", "warning")
                return redirect(url_for('user.user_aggregate'))

            # Exclude users with zero or null in allocation or mtm_all
            filtered_summary = filtered_summary[
                (filtered_summary['allocation'].notnull()) & 
                (filtered_summary['allocation'] != 0) & 
                (filtered_summary['mtm_all'].notnull()) & 
                (filtered_summary['mtm_all'] != 0)
            ]
            logger.info(f"After filtering out zero/null allocation and mtm_all: {len(filtered_summary)} rows")

            # Check if any data remains after filtering
            if filtered_summary.empty:
                logger.warning(f"No data remains after filtering for date={selected_date}")
                flash("No users meet the criteria (non-zero and non-null values required in Allocation and MTM (All))", "warning")
                return redirect(url_for('user.user_aggregate'))

            # Exclude selected user IDs from session
            excluded_users = session.get('excluded_users', [])
            if excluded_users:
                filtered_summary = filtered_summary[~filtered_summary['user_id'].isin(excluded_users)]
                if filtered_summary.empty:
                    logger.warning(f"No data remains after excluding users for date={selected_date}")
                    flash("All users have been excluded from the calculation", "warning")
                    return redirect(url_for('user.user_aggregate'))

            # Calculate total MTM (All), number of users, and servers
            total_mtm = filtered_summary['mtm_all'].sum()
            num_users = filtered_summary['user_id'].nunique()
            servers = filtered_summary['server'].unique().tolist()
            logger.info(f"Total MTM (All) for date={selected_date}: {total_mtm}, No. of Users: {num_users}, Servers: {servers}")

        # Redirect back to aggregate page with total_mtm, num_users, servers, and selected_date
        return redirect(url_for('user.user_aggregate', selected_date=selected_date))

    except Exception as e:
        logger.error(f"Error calculating total MTM: {str(e)}")
        flash(f"Error calculating total MTM: {str(e)}", "error")
        return redirect(url_for('user.user_aggregate'))

@user_bp.route('/aggregate/include_users', methods=['POST'])
def include_users():
    logger.info(f"Accessing include_users, Session: {session}")
    if 'role' not in session or session.get('role', '') not in ['user', 'user'] or not session['authenticated']:
        logger.info("Redirecting to login due to failed session check")
        flash("Please log in to access this page", "error")
        return redirect(url_for('login.login'))

    # Get selected (included) user IDs
    included_users = request.form.getlist('included_users')
    selected_date = request.form.get('selected_date', '')

    # Get all user IDs to determine excluded ones
    engine = get_db_connection()
    if not engine:
        flash("Database connection failed", "error")
        return redirect(url_for('user.user_aggregate', selected_date=selected_date))

    try:
        with engine.connect() as conn:
            summary = pd.read_sql_table('users', con=engine, coerce_float=False)
            all_user_ids = summary['user_id'].unique().tolist()
            excluded_users = [uid for uid in all_user_ids if uid not in included_users]

        # Update session with excluded user IDs
        session['excluded_users'] = excluded_users
        session.modified = True
        logger.info(f"Updated excluded users in session: {session['excluded_users']}")

        flash(f"Updated user inclusions: {len(excluded_users)} user(s) excluded", "success")
    except Exception as e:
        logger.error(f"Error updating user inclusions: {str(e)}")
        flash(f"Error updating user inclusions: {str(e)}", "error")

    return redirect(url_for('user.user_aggregate', selected_date=selected_date))

@user_bp.route('/aggregate/reset_exclusions', methods=['POST'])
def reset_exclusions():
    logger.info(f"Accessing reset_exclusions, Session: {session}")
    if 'role' not in session or session.get('role', '') not in ['user', 'user'] or not session['authenticated']:
        logger.info("Redirecting to login due to failed session check")
        flash("Please log in to access this page", "error")
        return redirect(url_for('login.login'))

    # Clear excluded users from session
    selected_date = request.form.get('selected_date', '')
    session.pop('excluded_users', None)
    session.modified = True
    logger.info("Reset excluded users in session")

    flash("Reset user exclusions", "success")
    return redirect(url_for('user.user_aggregate', selected_date=selected_date))



@user_bp.route('/analysis', methods=['GET', 'POST'])
def user_analysis():
    logger.info(f"Accessing admin_analysis_page, Session: {session}, Method: {request.method}")
    if 'role' not in session or session.get('role', '') not in ['admin', 'user'] or not session['authenticated']:
        logger.info("Redirecting to login due to failed session check")
        flash("Please log in to access this page", "error")
        return redirect(url_for('login.login'))

    # Initialize variables
    data = None
    summary_stats = None
    ce_cat_counts = None
    pe_cat_counts = None

    try:
        if request.method == 'POST':
            logger.info(f"POST request received, Form data: {request.form}")

            # Handle Excel export
            if request.form.get('export') == 'csv' and 'processed_data_path' in session:
                try:
                    data_path = session['processed_data_path']
                    if not os.path.exists(data_path):
                        flash("Processed data not found. Please upload the file again.", "error")
                        return redirect(url_for('user.user_analysis_page'))

                    df = pd.read_csv(data_path)
                    summary_stats = df[['CE_HEDGE_RATIO', 'PE_HEDGE_RATIO']].describe().reset_index()
                    ce_cat_counts = df['CE_HEDGE_STATUS'].value_counts().reset_index()
                    ce_cat_counts.columns = ['CE_HEDGE_STATUS', 'Count']
                    pe_cat_counts = df['PE_HEDGE_STATUS'].value_counts().reset_index()
                    pe_cat_counts.columns = ['PE_HEDGE_STATUS', 'Count']

                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        df.to_excel(writer, sheet_name='Hedge Data', index=False)
                        summary_stats.to_excel(writer, sheet_name='Summary', startrow=0, index=False)
                        ce_cat_counts.to_excel(writer, sheet_name='Summary', startrow=len(summary_stats) + 2, index=False)
                        pe_cat_counts.to_excel(writer, sheet_name='Summary', startrow=len(summary_stats) + len(ce_cat_counts) + 4, index=False)

                    output.seek(0)
                    original_filename = session.get('processed_filename', 'hedge_calculations')
                    download_name = f"{original_filename.rsplit('.', 1)[0]}_processed.xlsx"
                    return Response(
                        output,
                        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        headers={'Content-Disposition': f'attachment;filename={download_name}'}
                    )
                except Exception as e:
                    flash(f"Error generating Excel: {str(e)}", "error")
                    return redirect(url_for('user.user_analysis_page'))

            # Handle clear session
            if request.form.get('clear_session') == 'true':
                if 'processed_data_path' in session:
                    data_path = session.pop('processed_data_path', None)
                    if data_path and os.path.exists(data_path):
                        try:
                            os.remove(data_path)
                        except Exception as e:
                            logger.error(f"Error deleting temp file: {str(e)}")
                    session.pop('processed_filename', None)
                    session.modified = True
                flash("Session data cleared", "success")
                return redirect(url_for('user.user_analysis_page'))

            # Handle file upload
            if 'file_upload' not in request.files:
                flash("No file selected", "error")
                return render_template('user_analysis.html', role=session.get('role'))

            file = request.files['file_upload']
            if file.filename == '':
                flash("No file selected", "error")
                return render_template('user_analysis.html', role=session.get('role'))

            if file and file.filename.endswith('.csv'):
                try:
                    df = pd.read_csv(file)

                    # ✅ EARLY FILTER: Keep only rows with Status == COMPLETE
                    if 'Status' not in df.columns:
                        flash("Missing 'Status' column in uploaded file.", "error")
                        return render_template('user_analysis.html', role=session.get('role'))

                    df = df[df['Status'] == 'COMPLETE']
                    if df.empty:
                        flash("No valid data with Status == 'COMPLETE'.", "error")
                        return render_template('user_analysis.html', role=session.get('role'))

                    # Verify required columns
                    required_columns = {'Symbol', 'Transaction', 'Quantity', 'User ID'}
                    if not required_columns.issubset(df.columns):
                        missing = required_columns - set(df.columns)
                        flash(f"Missing required columns: {missing}", "error")
                        return render_template('user_analysis.html', role=session.get('role'))

                    df['Order Time'] = pd.to_datetime(df['Order Time'], errors='coerce')
                    df.sort_values(by=['User ID', 'Order Time'], inplace=True)
                    df.reset_index(drop=True, inplace=True)

                    df['CE/PE'] = df['Symbol'].str[-2:]

                    df['CE_B'] = 0
                    df['CE_S'] = 0
                    df['PE_B'] = 0
                    df['PE_S'] = 0

                    df.loc[(df['Transaction'] == 'BUY') & (df['CE/PE'] == 'CE'), 'CE_B'] = df['Quantity']
                    df.loc[(df['Transaction'] == 'SELL') & (df['CE/PE'] == 'CE'), 'CE_S'] = df['Quantity']
                    df.loc[(df['Transaction'] == 'BUY') & (df['CE/PE'] == 'PE'), 'PE_B'] = df['Quantity']
                    df.loc[(df['Transaction'] == 'SELL') & (df['CE/PE'] == 'PE'), 'PE_S'] = df['Quantity']

                    df['CUM_CE_B'] = df.groupby('User ID')['CE_B'].cumsum()
                    df['CUM_CE_S'] = df.groupby('User ID')['CE_S'].cumsum()
                    df['CUM_PE_B'] = df.groupby('User ID')['PE_B'].cumsum()
                    df['CUM_PE_S'] = df.groupby('User ID')['PE_S'].cumsum()

                    def calculate_hedge_ratio(buy, sell):
                        return 0 if sell == 0 else round(abs(buy) / abs(sell), 2)

                    df['CE_HEDGE_RATIO'] = df.apply(lambda r: calculate_hedge_ratio(r['CUM_CE_B'], r['CUM_CE_S']), axis=1)
                    df['PE_HEDGE_RATIO'] = df.apply(lambda r: calculate_hedge_ratio(r['CUM_PE_B'], r['CUM_PE_S']), axis=1)

                    def categorize_hedge_ratio(r):
                        if r < 0.90:
                            return "CRITICAL-NOT MAINTAINED"
                        elif 0.90 <= r <= 1.20:
                            return "MAINTAINED"
                        else:
                            return "CRITICAL-EXTRA BUY"

                    df['CE_HEDGE_STATUS'] = df['CE_HEDGE_RATIO'].apply(categorize_hedge_ratio)
                    df['PE_HEDGE_STATUS'] = df['PE_HEDGE_RATIO'].apply(categorize_hedge_ratio)

                    # Save temp file
                    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
                    df.to_csv(temp_file.name, index=False)

                    if 'processed_data_path' in session:
                        old = session['processed_data_path']
                        if old and os.path.exists(old):
                            try:
                                os.remove(old)
                            except Exception as e:
                                logger.error(f"Error deleting old temp file: {str(e)}")

                    session['processed_data_path'] = temp_file.name
                    session['processed_filename'] = file.filename
                    session.modified = True

                    data = df.head(100).to_dict('records')
                    summary_stats = df[['CE_HEDGE_RATIO', 'PE_HEDGE_RATIO']].describe().to_dict()
                    ce_cat_counts = df['CE_HEDGE_STATUS'].value_counts().to_dict()
                    pe_cat_counts = df['PE_HEDGE_STATUS'].value_counts().to_dict()

                    flash("Hedge calculations completed successfully", "success")

                except Exception as e:
                    logger.error(f"Error processing file: {str(e)}")
                    flash(f"Error processing file: {str(e)}", "error")
                    return render_template('user_analysis.html', role=session.get('role'))

            else:
                flash("Invalid file format. Please upload a CSV file.", "error")

        return render_template('user_analysis.html', role=session.get('role'), data=data,
                               summary_stats=summary_stats, ce_cat_counts=ce_cat_counts, pe_cat_counts=pe_cat_counts)

    except Exception as e:
        logger.error(f"Unexpected error in user_analysis: {str(e)}")
        flash(f"Unexpected error: {str(e)}", "error")
        return redirect(url_for('user.user_home'))

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

            uploaded_files = [os.path.join(file_source, f) for f in os.listdir(file_source) if f.endswith(('.csv', '.xlsx', '.xls', '.xlsb'))]
            
            for file_path in uploaded_files:
                try:
                    if file_path.endswith('.csv'):
                        df = pd.read_csv(file_path, header=0 if has_header else None)
                    elif file_path.endswith(('.xlsx', '.xls', '.xlsb')):
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

user_bp.upload_files_to_table = upload_files_to_table