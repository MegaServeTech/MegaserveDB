from flask import Blueprint, render_template, request, redirect, url_for, flash, session, Response
from functools import wraps
from utils import get_db_connection, logger
import pandas as pd
from sqlalchemy.sql import text
import csv
import io
import tempfile
import os
import shutil

aggregate_bp = Blueprint('aggregate', __name__, template_folder='templates')

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

def find_column(df, possible_names):
    """Find the first matching column name from a list of possible names."""
    for name in possible_names:
        if name in df.columns:
            return name
    return None

@aggregate_bp.route('/aggregate', methods=['GET', 'POST'])
@require_role(['admin', 'user'])
def aggregate_page():
    logger.info(f"Accessing aggregate_page, Session: {session}")
    selected_date = request.form.get('selected_date') or request.args.get('selected_date', '')
    export = request.args.get('export')
    excluded_users = session.get('excluded_users', [])
    logger.info(f"Parameters - selected_date: {selected_date}, export: {export}, excluded_users: {excluded_users}")

    engine = get_db_connection()
    if not engine:
        flash("Database connection failed", "error")
        return render_template('aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date=selected_date, excluded_users=excluded_users, all_user_ids=[])

    try:
        with engine.connect() as conn:
            result = conn.execute(text("SHOW TABLES LIKE 'users'"))
            if not result.fetchall():
                flash("Table 'users' does not exist. Please create it and upload data.", "error")
                return render_template('aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date=selected_date, excluded_users=excluded_users, all_user_ids=[])

            summary = pd.read_sql_table('users', con=engine, coerce_float=False, parse_dates=['date'])
            logger.info(f"Read {len(summary)} rows from users")

            def clean_user_id(uid):
                try:
                    if uid is None or pd.isna(uid):
                        return ''
                    uid = str(uid).strip()
                    if uid and uid.startswith('0'):
                        uid = uid[1:]
                    return uid
                except Exception as e:
                    logger.error(f"Error cleaning user_id {uid}: {str(e)}")
                    return str(uid) if uid is not None else ''

            required_summary_columns = {'user_id', 'date', 'allocation', 'mtm_all', 'server', 'algo'}
            if not required_summary_columns.issubset(summary.columns):
                missing_cols = required_summary_columns - set(summary.columns)
                flash(f"Required columns {missing_cols} missing in users table", "error")
                return render_template('aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date=selected_date, excluded_users=excluded_users, all_user_ids=[])

            summary['user_id'] = summary['user_id'].apply(clean_user_id)
            summary['date'] = pd.to_datetime(summary['date'], errors='coerce').dt.date
            numeric_columns = ['allocation', 'mtm_all']
            optional_columns = ['max_loss', 'available_margin', 'total_orders', 'total_lots']
            for col in numeric_columns + optional_columns:
                if col in summary.columns:
                    summary[col] = pd.to_numeric(summary[col], errors='coerce').fillna(0)

            all_user_ids = sorted(summary['user_id'].unique().tolist())
            logger.info(f"All user IDs: {len(all_user_ids)}")

            total_mtm = None
            num_users = None
            servers = None
            if selected_date:
                try:
                    selected_date = pd.to_datetime(selected_date).date()
                    filtered_summary = summary[summary['date'] == selected_date]
                    logger.info(f"Filtered users for date={selected_date}: {len(filtered_summary)} rows")
                except ValueError:
                    flash("Invalid date format", "error")
                    return render_template('aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date=selected_date, excluded_users=excluded_users, all_user_ids=all_user_ids)

                if filtered_summary.empty:
                    flash(f"No data found for the selected date {selected_date}", "warning")
                    return render_template('aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date=selected_date, excluded_users=excluded_users, all_user_ids=all_user_ids)

                filtered_summary = filtered_summary[
                    (filtered_summary['allocation'].notnull()) & 
                    (filtered_summary['allocation'] != 0) & 
                    (filtered_summary['mtm_all'].notnull()) & 
                    (filtered_summary['mtm_all'] != 0)
                ]
                logger.info(f"After filtering out zero/null allocation and mtm_all: {len(filtered_summary)} rows")

                if filtered_summary.empty:
                    flash("No users meet the criteria (non-zero and non-null values required in Allocation and MTM (All))", "warning")
                    return render_template('aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date=selected_date, excluded_users=excluded_users, all_user_ids=all_user_ids)

                if excluded_users:
                    filtered_summary = filtered_summary[~filtered_summary['user_id'].isin(excluded_users)]
                    if filtered_summary.empty:
                        flash("All users have been excluded from the calculation", "warning")
                        return render_template('aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date=selected_date, excluded_users=excluded_users, all_user_ids=all_user_ids)

                grouped = filtered_summary.groupby(['algo', 'server']).agg(
                    **{
                        'No. of Users': pd.NamedAgg(column='user_id', aggfunc='count'),
                        'Sum of ALLOCATION': pd.NamedAgg(column='allocation', aggfunc='sum'),
                        'Sum of MTM (All)': pd.NamedAgg(column='mtm_all', aggfunc='sum')
                    }
                ).reset_index()

                grouped['Return Ratio'] = (grouped['Sum of MTM (All)'] / grouped['Sum of ALLOCATION'])
                final_df = grouped.sort_values(by=['algo', 'server'])
                final_df = final_df.rename(columns={'algo': 'ALGO', 'server': 'SERVER'})
                final_df = final_df[['ALGO', 'SERVER', 'No. of Users', 'Sum of ALLOCATION', 'Sum of MTM (All)', 'Return Ratio']]
                final_df['Return Ratio'] = final_df['Return Ratio'].round(2)

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
                logger.info(f"Processed {len(data)} records for display, including Grand Total")

                total_mtm = filtered_summary['mtm_all'].sum()
                num_users = filtered_summary['user_id'].nunique()
                servers = filtered_summary['server'].unique().tolist()
                logger.info(f"Total MTM (All) for date={selected_date}: {total_mtm}, No. of Users: {num_users}, Servers: {servers}")
            else:
                data = None

            if export == 'csv' and data:
                output = io.StringIO()
                writer = csv.DictWriter(output, fieldnames=['ALGO', 'SERVER', 'No. of Users', 'Sum of ALLOCATION', 'Sum of MTM (All)', 'Return Ratio'])
                writer.writeheader()
                for row in data:
                    writer.writerow(row)
                output.seek(0)
                return Response(
                    output,
                    mimetype='text/csv',
                    headers={'Content-Disposition': 'attachment;filename=aggregate_report.csv'}
                )

            if data:
                flash(f"Aggregation completed: {len(data)} records processed", "success")
            return render_template('aggregate.html', role=session.get('role'), data=data, total_mtm=total_mtm, num_users=num_users, servers=servers, selected_date=selected_date, excluded_users=excluded_users, all_user_ids=all_user_ids)

    except Exception as e:
        handle_error(e, "Aggregation")
        return render_template('aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date=selected_date, excluded_users=excluded_users, all_user_ids=[])

@aggregate_bp.route('/aggregate/total_mtm', methods=['POST'])
@require_role(['admin', 'user'])
def total_mtm():
    logger.info(f"Accessing total_mtm, Session: {session}")
    selected_date = request.form.get('selected_date', '')
    logger.info(f"Parameters - selected_date: {selected_date}")

    if not selected_date:
        flash("Please select a date", "error")
        return redirect(url_for('aggregate.aggregate_page'))

    engine = get_db_connection()
    if not engine:
        flash("Database connection failed", "error")
        return redirect(url_for('aggregate.aggregate_page'))

    try:
        with engine.connect() as conn:
            result = conn.execute(text("SHOW TABLES LIKE 'users'"))
            if not result.fetchall():
                flash("Table 'users' does not exist. Please create it and upload data.", "error")
                return redirect(url_for('aggregate.aggregate_page'))

            summary = pd.read_sql_table('users', con=engine, coerce_float=False, parse_dates=['date'])
            logger.info(f"Read {len(summary)} rows from users")

            summary['date'] = pd.to_datetime(summary['date'], errors='coerce').dt.date
            numeric_columns = ['mtm_all', 'allocation']
            optional_columns = ['max_loss', 'available_margin', 'total_orders', 'total_lots']
            for col in numeric_columns + optional_columns:
                if col in summary.columns:
                    summary[col] = pd.to_numeric(summary[col], errors='coerce').fillna(0)

            try:
                selected_date = pd.to_datetime(selected_date).date()
                filtered_summary = summary[summary['date'] == selected_date]
                logger.info(f"Filtered users for date={selected_date}: {len(filtered_summary)} rows")
            except ValueError:
                flash("Invalid date format", "error")
                return redirect(url_for('aggregate.aggregate_page'))

            if filtered_summary.empty:
                flash(f"No data found for the selected date {selected_date}", "warning")
                return redirect(url_for('aggregate.aggregate_page'))

            filtered_summary = filtered_summary[
                (filtered_summary['allocation'].notnull()) & 
                (filtered_summary['allocation'] != 0) & 
                (filtered_summary['mtm_all'].notnull()) & 
                (filtered_summary['mtm_all'] != 0)
            ]
            logger.info(f"After filtering out zero/null allocation and mtm_all: {len(filtered_summary)} rows")

            if filtered_summary.empty:
                flash("No users meet the criteria (non-zero and non-null values required in Allocation and MTM (All))", "warning")
                return redirect(url_for('aggregate.aggregate_page'))

            excluded_users = session.get('excluded_users', [])
            if excluded_users:
                filtered_summary = filtered_summary[~filtered_summary['user_id'].isin(excluded_users)]
                if filtered_summary.empty:
                    flash("All users have been excluded from the calculation", "warning")
                    return redirect(url_for('aggregate.aggregate_page'))

            total_mtm = filtered_summary['mtm_all'].sum()
            num_users = filtered_summary['user_id'].nunique()
            servers = filtered_summary['server'].unique().tolist()
            logger.info(f"Total MTM (All) for date={selected_date}: {total_mtm}, No. of Users: {num_users}, Servers: {servers}")

        return redirect(url_for('aggregate.aggregate_page', selected_date=selected_date))

    except Exception as e:
        handle_error(e, "Total MTM calculation")
        return redirect(url_for('aggregate.aggregate_page'))

@aggregate_bp.route('/aggregate/include_users', methods=['POST'])
@require_role(['admin', 'user'])
def include_users():
    logger.info(f"Accessing include_users, Session: {session}")
    included_users = request.form.getlist('included_users')
    selected_date = request.form.get('selected_date', '')

    engine = get_db_connection()
    if not engine:
        flash("Database connection failed", "error")
        return redirect(url_for('aggregate.aggregate_page', selected_date=selected_date))

    try:
        with engine.connect() as conn:
            summary = pd.read_sql_table('users', con=engine, coerce_float=False)
            all_user_ids = summary['user_id'].unique().tolist()
            excluded_users = [uid for uid in all_user_ids if uid not in included_users]

        session['excluded_users'] = excluded_users
        session.modified = True
        logger.info(f"Updated excluded users in session: {session['excluded_users']}")

        flash(f"Updated user inclusions: {len(excluded_users)} user(s) excluded", "success")
        return redirect(url_for('aggregate.aggregate_page', selected_date=selected_date))

    except Exception as e:
        handle_error(e, "Updating user inclusions")
        return redirect(url_for('aggregate.aggregate_page', selected_date=selected_date))

@aggregate_bp.route('/aggregate/reset_exclusions', methods=['POST'])
@require_role(['admin', 'user'])
def reset_exclusions():
    logger.info(f"Accessing reset_exclusions, Session: {session}")
    selected_date = request.form.get('selected_date', '')
    session.pop('excluded_users', None)
    session.modified = True
    logger.info("Reset excluded users in session")

    flash("Reset user exclusions", "success")
    return redirect(url_for('aggregate.aggregate_page', selected_date=selected_date))

@aggregate_bp.route('/aggregate/realised_profit', methods=['GET', 'POST'])
@require_role(['admin', 'user'])
def realised_profit():
    logger.info(f"Accessing realised_profit, Session: {session}, Method: {request.method}")
    user_ids = session.get('realised_profit_user_ids', [])
    profit_data = None
    selected_user_id = request.form.get('user_id') or session.get('selected_user_id', '')
    export = request.args.get('export')

    engine = get_db_connection()
    all_user_ids = []
    if engine:
        try:
            with engine.connect() as conn:
                summary = pd.read_sql_table('users', con=engine, coerce_float=False)
                all_user_ids = sorted(summary['user_id'].unique().tolist())
                logger.info(f"All user IDs for Total MTM: {len(all_user_ids)}")
        except Exception as e:
            handle_error(e, "Fetching user IDs")

    if request.method == 'POST':
        if 'file_upload' not in request.files:
            flash("No file selected", "error")
            return render_template('aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date='', excluded_users=session.get('excluded_users', []), all_user_ids=all_user_ids, user_ids=user_ids, profit_data=None, selected_user_id=selected_user_id)

        file = request.files['file_upload']
        if file.filename == '':
            flash("No file selected", "error")
            return render_template('aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date='', excluded_users=session.get('excluded_users', []), all_user_ids=all_user_ids, user_ids=user_ids, profit_data=None, selected_user_id=selected_user_id)

        if not file.filename.endswith(('.xlsx', '.xls')):
            flash("Invalid file format. Please upload an Excel file (.xlsx or .xls).", "error")
            return render_template('aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date='', excluded_users=session.get('excluded_users', []), all_user_ids=all_user_ids, user_ids=user_ids, profit_data=None, selected_user_id=selected_user_id)

        try:
            temp_dir = tempfile.mkdtemp()
            file_path = os.path.join(temp_dir, file.filename)
            file.save(file_path)
            logger.info(f"Saved uploaded file: {file_path}")

            df = pd.read_excel(file_path)
            if 'OrderStatus' not in df.columns:
                df['OrderStatus'] = 'COMPLETE'
            if 'LegID' not in df.columns:
                df['LegID'] = 0

            column_mappings = {
                'TradingSymbol': ['Exchange Symbol', 'Symbol', 'Trading Symbol', 'TradingSymbol'],
                'quantity': ['Filled Qty', 'Filled Quantity', 'Qty Filled', 'Quantity'],
                'OrderSide': ['Txn', 'Transaction', 'Order Side', 'Side'],
                'OrderAverageTradedPrice': ['Avg Price', 'Average Price', 'Traded Price', 'AvgPrice'],
                'ExchangeTransactTime': ['Exchg Time', 'Order Time', 'Transaction Time', 'Time'],
                'OrderStatus': ['Status', 'Order Status', 'OrderStatus'],
                'LegID': ['Leg ID', 'LegID', 'Leg'],
                'UserID': ['User ID', 'UserID', 'User']
            }

            columns = {}
            missing_cols = []
            for internal_name, possible_names in column_mappings.items():
                found_col = find_column(df, possible_names)
                if found_col:
                    columns[found_col] = internal_name
                else:
                    missing_cols.append(internal_name)

            if missing_cols:
                expected_names = ", ".join([f"{', '.join(column_mappings[col])} (or {col})" for col in missing_cols])
                flash(f"Missing required columns. Please ensure the Excel file contains: {expected_names}", "error")
                shutil.rmtree(temp_dir, ignore_errors=True)
                return render_template('aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date='', excluded_users=session.get('excluded_users', []), all_user_ids=all_user_ids, user_ids=user_ids, profit_data=None, selected_user_id=selected_user_id)

            df = df.rename(columns=columns)
            required_columns = {'TradingSymbol', 'OrderSide', 'quantity', 'OrderStatus', 'OrderAverageTradedPrice', 'LegID', 'UserID'}
            if not required_columns.issubset(df.columns):
                missing_cols = required_columns - set(df.columns)
                flash(f"Unexpected error: Missing columns {missing_cols}", "error")
                shutil.rmtree(temp_dir, ignore_errors=True)
                return render_template('aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date='', excluded_users=session.get('excluded_users', []), all_user_ids=all_user_ids, user_ids=user_ids, profit_data=None, selected_user_id=selected_user_id)

            user_ids = sorted(df['UserID'].unique().tolist())
            logger.info(f"Extracted {len(user_ids)} unique User IDs: {user_ids}")

            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
            df.to_csv(temp_file.name, index=False)
            logger.info(f"Saved processed data to temporary file: {temp_file.name}")

            if 'realised_profit_data_path' in session:
                old_path = session['realised_profit_data_path']
                if old_path and os.path.exists(old_path):
                    try:
                        os.remove(old_path)
                        logger.info(f"Deleted previous temporary file: {old_path}")
                    except Exception as e:
                        logger.error(f"Error deleting previous temporary file {old_path}: {str(e)}")

            session['realised_profit_data_path'] = temp_file.name
            session['realised_profit_user_ids'] = user_ids
            session.modified = True
            logger.info(f"Stored in session: data_path={temp_file.name}, user_ids={user_ids}")

            shutil.rmtree(temp_dir, ignore_errors=True)

            if selected_user_id and selected_user_id in user_ids:
                df = pd.read_csv(temp_file.name)
                profit_df = calculate_profit(df, selected_user_id)
                if profit_df is not None:
                    profit_data = profit_df.to_dict('records')
                    flash(f"Realised profit calculated for User {selected_user_id}", "success")
                    session['selected_user_id'] = selected_user_id
                    session.modified = True
                else:
                    flash("Error calculating realised profit", "error")

        except Exception as e:
            handle_error(e, "Processing uploaded file")
            shutil.rmtree(temp_dir, ignore_errors=True)
            return render_template('aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date='', excluded_users=session.get('excluded_users', []), all_user_ids=all_user_ids, user_ids=user_ids, profit_data=None, selected_user_id=selected_user_id)

    if export == 'csv' and profit_data:
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=['TradingSymbol', 'BuyQuantity', 'SellQuantity', 'which_side', 'total_sell_value', 'total_buy_value', 'RealizedProfit'])
        writer.writeheader()
        for row in profit_data:
            writer.writerow(row)
        output.seek(0)
        return Response(
            output,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment;filename=realised_profit_{selected_user_id}.csv'}
        )

    return render_template('aggregate.html', role=session.get('role'), data=None, total_mtm=None, num_users=None, servers=None, selected_date='', excluded_users=session.get('excluded_users', []), all_user_ids=all_user_ids, user_ids=user_ids, profit_data=profit_data, selected_user_id=selected_user_id)

def get_total_value(row, df, order_side, consider_multi_leg=False):
    tidf = df[(df['TradingSymbol'] == row['TradingSymbol']) & (df['OrderSide'] == order_side)]
    if consider_multi_leg:
        tdf = tidf[tidf['MultiLeg'] == True].reset_index()
    else:
        tdf = tidf.reset_index()

    required_quantity = row['rqty']
    total_value = 0

    for _, trow in tdf.iterrows():
        if required_quantity < 1:
            break
        tqty = min(trow['quantity'], required_quantity)
        required_quantity -= tqty
        total_value += tqty * trow['OrderAverageTradedPrice']

    if consider_multi_leg or required_quantity < 1:
        return total_value

    for _, trow in tidf[tidf['MultiLeg'] == False].iterrows():
        if required_quantity < 1:
            break
        tqty = min(trow['quantity'], required_quantity)
        required_quantity -= tqty
        total_value += tqty * trow['OrderAverageTradedPrice']

    return total_value

def calculate_profit(df, selected_user_id):
    try:
        df_filtered = df[df['UserID'] == selected_user_id]
        df_filtered = df_filtered[df_filtered['OrderStatus'] == 'COMPLETE']
        df_filtered['OrderSide'] = df_filtered['OrderSide'].str.upper()

        if df_filtered.empty:
            flash(f"No completed orders found for User {selected_user_id}", "warning")
            return None

        leg_counts = df_filtered.groupby(['TradingSymbol', 'LegID']).size().reset_index().groupby('TradingSymbol')['LegID'].nunique()
        multi_leg_symbols = leg_counts[leg_counts > 1].index.tolist()
        df_filtered['MultiLeg'] = df_filtered.apply(lambda x: x['TradingSymbol'] in multi_leg_symbols and x['LegID'] in df_filtered[df_filtered['TradingSymbol'] == x['TradingSymbol']]['LegID'].unique(), axis=1)

        pivot_df = df_filtered.pivot_table(
            index='TradingSymbol',
            columns='OrderSide',
            values='quantity',
            aggfunc='sum',
            fill_value=0
        ).reset_index()

        pivot_df.columns.name = None
        pivot_df = pivot_df.rename(columns={'BUY': 'BuyQuantity', 'SELL': 'SellQuantity'})

        if 'BuyQuantity' not in pivot_df.columns:
            pivot_df['BuyQuantity'] = 0
        if 'SellQuantity' not in pivot_df.columns:
            pivot_df['SellQuantity'] = 0

        pivot_df['rqty'] = pivot_df[['BuyQuantity', 'SellQuantity']].min(axis=1)
        pivot_df['which_side'] = pivot_df['BuyQuantity'] >= pivot_df['SellQuantity']
        pivot_df['which_side'] = pivot_df['which_side'].replace({True: 'BUY', False: 'SELL'})

        for i, row in pivot_df.iterrows():
            if row['which_side'] == 'SELL':
                total_sell_value = get_total_value(row, df_filtered, 'SELL')
                total_buy_value = get_total_value(row, df_filtered, 'BUY', consider_multi_leg=True)
            else:
                total_buy_value = get_total_value(row, df_filtered, 'BUY')
                total_sell_value = get_total_value(row, df_filtered, 'SELL', consider_multi_leg=True)

            pivot_df.at[i, 'total_sell_value'] = total_sell_value
            pivot_df.at[i, 'total_buy_value'] = total_buy_value
            pivot_df.at[i, 'RealizedProfit'] = total_sell_value - total_buy_value

        pivot_df = pivot_df[['TradingSymbol', 'BuyQuantity', 'SellQuantity', 'which_side', 'total_sell_value', 'total_buy_value', 'RealizedProfit']]
        pivot_df = pivot_df.sort_values(by='TradingSymbol')
        return pivot_df
    except Exception as e:
        handle_error(e, f"Calculating profit for UserID={selected_user_id}")
        return None