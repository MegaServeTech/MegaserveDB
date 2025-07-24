from flask import Blueprint, render_template, request, redirect, url_for, flash, session, Response
from functools import wraps
from utils import get_db_connection, logger
import pandas as pd
import numpy as np
import tempfile
import os
import io
from sqlalchemy import text
from datetime import datetime

analysis_bp = Blueprint('analysis', __name__, template_folder='templates')

def require_role(roles):
    """Decorator to check session authentication and role."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'role' not in session or session.get('role', '') not in roles or not session['authenticated']:
                logger.info(f"Redirecting to login due to failed session check: {session}")
                flash("Please log in with appropriate role to access this page", "error")
                return redirect(url_for('login.login'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def handle_error(e, context="Operation"):
    """Standardize error handling with logging and flashing."""
    error_msg = f"{context}: {str(e)}"
    logger.error(error_msg)
    flash(error_msg, "error")
    return error_msg

@analysis_bp.route('/analysis', methods=['GET', 'POST'])
@require_role(['admin', 'user'])
def analysis_page():
    logger.info(f"Accessing analysis_page, Session: {session}, Method: {request.method}")
    data = None
    summary_stats = None
    ce_cat_counts = None
    pe_cat_counts = None

    try:
        if request.method == 'POST':
            if request.form.get('export') == 'csv' and 'processed_data_path' in session:
                try:
                    data_path = session['processed_data_path']
                    if not os.path.exists(data_path):
                        flash("Processed data not found. Please upload the file again.", "error")
                        return redirect(url_for('analysis.analysis_page'))

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
                    handle_error(e, "Generating Excel")
                    return redirect(url_for('analysis.analysis_page'))

            if request.form.get('clear_session') == 'true':
                if 'processed_data_path' in session:
                    data_path = session.pop('processed_data_path', None)
                    if data_path and os.path.exists(data_path):
                        os.remove(data_path)
                    session.pop('processed_filename', None)
                    session.modified = True
                flash("Session data cleared", "success")
                return redirect(url_for('analysis.analysis_page'))

            if 'file_upload' not in request.files:
                flash("No file selected", "error")
                return render_template('analysis.html', role=session.get('role'))

            file = request.files['file_upload']
            if file.filename == '':
                flash("No file selected", "error")
                return render_template('analysis.html', role=session.get('role'))

            if file and file.filename.endswith('.csv'):
                try:
                    df = pd.read_csv(file)
                    if 'Status' not in df.columns:
                        flash("Missing 'Status' column in uploaded file.", "error")
                        return render_template('analysis.html', role=session.get('role'))

                    df = df[df['Status'] == 'COMPLETE']
                    if df.empty:
                        flash("No valid data with Status == 'COMPLETE'.", "error")
                        return render_template('analysis.html', role=session.get('role'))

                    required_columns = {'Symbol', 'Transaction', 'Quantity', 'User ID'}
                    if not required_columns.issubset(df.columns):
                        missing = required_columns - set(df.columns)
                        flash(f"Missing required columns: {missing}", "error")
                        return render_template('analysis.html', role=session.get('role'))

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

                    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
                    df.to_csv(temp_file.name, index=False)

                    if 'processed_data_path' in session:
                        old = session['processed_data_path']
                        if old and os.path.exists(old):
                            os.remove(old)

                    session['processed_data_path'] = temp_file.name
                    session['processed_filename'] = file.filename
                    session.modified = True

                    data = df.head(100).to_dict('records')
                    summary_stats = df[['CE_HEDGE_RATIO', 'PE_HEDGE_RATIO']].describe().to_dict()
                    ce_cat_counts = df['CE_HEDGE_STATUS'].value_counts().to_dict()
                    pe_cat_counts = df['PE_HEDGE_STATUS'].value_counts().to_dict()

                    flash("Hedge calculations completed successfully", "success")

                except Exception as e:
                    handle_error(e, "Processing file")
                    return render_template('analysis.html', role=session.get('role'))

            else:
                flash("Invalid file format. Please upload a CSV file.", "error")

        return render_template('analysis.html', role=session.get('role'), data=data,
                               summary_stats=summary_stats, ce_cat_counts=ce_cat_counts, pe_cat_counts=pe_cat_counts,
                               hedge_cost_data=None, selected_date=None)

    except Exception as e:
        handle_error(e, "Unexpected error")
        return redirect(url_for('admin.admin_home'))

@analysis_bp.route('/hedge_cost', methods=['GET', 'POST'])
@require_role(['admin', 'user'])
def hedge_cost_page():
    logger.info(f"Accessing hedge_cost_page, Session: {session}, Method: {request.method}")
    hedge_cost_data = None
    selected_date = None
    date_based_data = None
    date_based_summary_stats = None
    date_based_ce_cat_counts = None
    date_based_pe_cat_counts = None

    try:
        if request.method == 'POST':
            selected_date = request.form.get('selected_date')
            if not selected_date:
                flash("Please select a date.", "error")
                return render_template('analysis.html', role=session.get('role'), hedge_cost_data=hedge_cost_data,
                                       selected_date=selected_date, data=None, summary_stats=None,
                                       ce_cat_counts=None, pe_cat_counts=None,
                                       date_based_data=None, date_based_summary_stats=None,
                                       date_based_ce_cat_counts=None, date_based_pe_cat_counts=None)

            try:
                engine = get_db_connection()
                if not engine:
                    raise ValueError("Failed to establish database connection")

                ob_query = """
                    SELECT user_id, order_id, symbol, order_time, transaction, quantity, avg_price, status, date, tag
                    FROM ob
                    WHERE DATE(date) = :selected_date AND UPPER(status) = 'COMPLETE'
                """
                with engine.connect() as connection:
                    df = pd.read_sql(text(ob_query), connection, params={"selected_date": selected_date})

                if df.empty:
                    flash("No valid data found for the selected date with status 'COMPLETE'.", "error")
                    return render_template('analysis.html', role=session.get('role'), hedge_cost_data=hedge_cost_data,
                                           selected_date=selected_date, data=None, summary_stats=None,
                                           ce_cat_counts=None, pe_cat_counts=None,
                                           date_based_data=None, date_based_summary_stats=None,
                                           date_based_ce_cat_counts=None, date_based_pe_cat_counts=None)

                # Hedge Cost Analysis
                df['Net Cost'] = df['quantity'] * df['avg_price']
                pivot_df = df[df['tag'].str.contains('h', case=False, na=False)].pivot_table(
                    values='Net Cost',
                    index=['user_id', 'date'],
                    columns='transaction',
                    aggfunc='sum',
                    fill_value=0
                )
                pivot_df['Hedge Cost'] = pivot_df.get('SELL', 0) - pivot_df.get('BUY', 0)
                pivot_df = pivot_df.reset_index()
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
                pivot_df.to_csv(temp_file.name, index=False)

                if 'hedge_cost_data_path' in session:
                    old = session['hedge_cost_data_path']
                    if old and os.path.exists(old):
                        os.remove(old)

                session['hedge_cost_data_path'] = temp_file.name
                session['hedge_cost_filename'] = f'hedge_cost_summary_{selected_date}.csv'
                session.modified = True
                hedge_cost_data = pivot_df[['user_id', 'BUY', 'SELL', 'Hedge Cost', 'date']].to_dict('records')

                # Hedge Ratio Analysis (Date-Based)
                required_columns = {'symbol', 'transaction', 'quantity', 'user_id'}
                if not required_columns.issubset(df.columns):
                    missing = required_columns - set(df.columns)
                    flash(f"Missing required columns for hedge ratio: {missing}", "error")
                    return render_template('analysis.html', role=session.get('role'), hedge_cost_data=hedge_cost_data,
                                           selected_date=selected_date, data=None, summary_stats=None,
                                           ce_cat_counts=None, pe_cat_counts=None,
                                           date_based_data=None, date_based_summary_stats=None,
                                           date_based_ce_cat_counts=None, date_based_pe_cat_counts=None)

                df['order_time'] = pd.to_datetime(df['order_time'], errors='coerce')
                df.sort_values(by=['user_id', 'order_time'], inplace=True)
                df.reset_index(drop=True, inplace=True)

                df['CE/PE'] = df['symbol'].str[-2:]
                df['CE_B'] = 0
                df['CE_S'] = 0
                df['PE_B'] = 0
                df['PE_S'] = 0

                df.loc[(df['transaction'] == 'BUY') & (df['CE/PE'] == 'CE'), 'CE_B'] = df['quantity']
                df.loc[(df['transaction'] == 'SELL') & (df['CE/PE'] == 'CE'), 'CE_S'] = df['quantity']
                df.loc[(df['transaction'] == 'BUY') & (df['CE/PE'] == 'PE'), 'PE_B'] = df['quantity']
                df.loc[(df['transaction'] == 'SELL') & (df['CE/PE'] == 'PE'), 'PE_S'] = df['quantity']

                df['CUM_CE_B'] = df.groupby('user_id')['CE_B'].cumsum()
                df['CUM_CE_S'] = df.groupby('user_id')['CE_S'].cumsum()
                df['CUM_PE_B'] = df.groupby('user_id')['PE_B'].cumsum()
                df['CUM_PE_S'] = df.groupby('user_id')['PE_S'].cumsum()

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

                temp_file_ratio = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
                df.to_csv(temp_file_ratio.name, index=False)

                if 'date_based_data_path' in session:
                    old = session['date_based_data_path']
                    if old and os.path.exists(old):
                        os.remove(old)

                session['date_based_data_path'] = temp_file_ratio.name
                session['date_based_filename'] = f'hedge_ratio_{selected_date}.csv'
                session.modified = True

                date_based_data = df.head(100).to_dict('records')
                date_based_summary_stats = df[['CE_HEDGE_RATIO', 'PE_HEDGE_RATIO']].describe().to_dict()
                date_based_ce_cat_counts = df['CE_HEDGE_STATUS'].value_counts().to_dict()
                date_based_pe_cat_counts = df['PE_HEDGE_STATUS'].value_counts().to_dict()

                flash("Hedge cost and ratio calculations completed successfully", "success")

                if request.form.get('export') == 'csv' and 'hedge_cost_data_path' in session:
                    try:
                        data_path = session['hedge_cost_data_path']
                        if not os.path.exists(data_path):
                            flash("Processed hedge cost data not found.", "error")
                            return redirect(url_for('analysis.hedge_cost_page'))

                        df_export = pd.read_csv(data_path)
                        output = io.BytesIO()
                        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                            df_export.to_excel(writer, sheet_name='Hedge Cost Summary', index=False)
                            if 'date_based_data_path' in session:
                                df_ratio_export = pd.read_csv(session['date_based_data_path'])
                                df_ratio_export.to_excel(writer, sheet_name='Hedge Ratio Summary', index=False)

                        output.seek(0)
                        download_name = session.get('hedge_cost_filename', f'hedge_cost_summary_{selected_date}.xlsx')
                        return Response(
                            output,
                            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                            headers={'Content-Disposition': f'attachment;filename={download_name}'}
                        )

                    except Exception as e:
                        handle_error(e, "Generating Hedge Cost Excel")
                        return redirect(url_for('analysis.hedge_cost_page'))

                return render_template('analysis.html', role=session.get('role'), hedge_cost_data=hedge_cost_data,
                                       selected_date=selected_date, data=None, summary_stats=None,
                                       ce_cat_counts=None, pe_cat_counts=None,
                                       date_based_data=date_based_data,
                                       date_based_summary_stats=date_based_summary_stats,
                                       date_based_ce_cat_counts=date_based_ce_cat_counts,
                                       date_based_pe_cat_counts=date_based_pe_cat_counts)

            except Exception as e:
                handle_error(e, "Processing hedge cost and ratio calculations")
                return render_template('analysis.html', role=session.get('role'), hedge_cost_data=hedge_cost_data,
                                       selected_date=selected_date, data=None, summary_stats=None,
                                       ce_cat_counts=None, pe_cat_counts=None,
                                       date_based_data=None, date_based_summary_stats=None,
                                       date_based_ce_cat_counts=None, date_based_pe_cat_counts=None)

        return render_template('analysis.html', role=session.get('role'), hedge_cost_data=hedge_cost_data,
                               selected_date=selected_date, data=None, summary_stats=None,
                               ce_cat_counts=None, pe_cat_counts=None,
                               date_based_data=None, date_based_summary_stats=None,
                               date_based_ce_cat_counts=None, date_based_pe_cat_counts=None)

    except Exception as e:
        handle_error(e, "Unexpected error in hedge cost calculation")
        return redirect(url_for('admin.admin_home'))