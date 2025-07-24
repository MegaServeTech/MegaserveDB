from flask import Blueprint, render_template, request, redirect, url_for, flash, session, Response
from functools import wraps
from utils import get_db_connection, get_table_columns, logger
import pandas as pd
import numpy as np
import io
import re
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import uuid
import json

margin_bp = Blueprint('margin', __name__, template_folder='templates')

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

def extract_shortfall(msg):
    if pd.isna(msg):
        return None
    try:
        if "Margin Shortfall[" in msg:
            return round(float(re.search(r"Margin Shortfall\[([\d.]+)\]", msg).group(1)), 2)
        elif "Shortfall:INR " in msg:
            return round(float(re.search(r"Shortfall:INR ([\d.]+)", msg).group(1)), 2)
        elif "Insufficient Funds; Required Amount" in msg:
            required = float(re.search(r"Required Amount ([\d.]+); Available Amount", msg).group(1))
            available = float(re.search(r"Available Amount ([\d.]+)", msg).group(1))
            return round(required - available, 2)
        elif ";Required:" in msg and "; Available:" in msg:
            required_amount = float(re.search(r";Required:([\d.]+)", msg).group(1))
            available_amount = float(re.search(r"; Available:([\d.]+)", msg).group(1))
            return round(required_amount - available_amount, 2)
    except Exception:
        return None
    return None

def analyze_margin_shortfalls(trade_date):
    try:
        # Validate trade_date format
        try:
            datetime.strptime(trade_date, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Invalid date format. Please use YYYY-MM-DD.")

        engine = get_db_connection()
        if not engine:
            raise ValueError("Failed to establish database connection")

        # Check if required tables exist
        available_tables = get_table_columns('users') or get_table_columns('ob')
        if not available_tables:
            with engine.connect() as connection:
                result = connection.execute(text("SHOW TABLES"))
                available_tables = [row[0] for row in result.fetchall()]
                logger.error(f"Tables 'users' or 'ob' not found. Available tables: {available_tables}")
                flash(f"Required tables 'users' or 'ob' not found in database. Available tables: {available_tables}", "error")
                return pd.DataFrame(), pd.DataFrame()

        # Debug: Check available dates in ob table
        with engine.connect() as connection:
            result = connection.execute(text("SELECT DISTINCT DATE(date) AS trade_date FROM ob"))
            available_dates = [row.trade_date.strftime('%Y-%m-%d') for row in result.fetchall()]
            logger.debug(f"Available dates in ob table: {available_dates}")
            if trade_date not in available_dates:
                logger.warning(f"No data found for trade_date: {trade_date}")
                flash(f"No data found for selected date: {trade_date}", "error")
                return pd.DataFrame(), pd.DataFrame()

        # Raw SQL queries
        user_query = """
            SELECT user_id, alias, broker, mtm_all, allocation, max_loss, available_margin, algo, server
            FROM users
        """
        orderbook_query = """
            SELECT user_id, user_alias, exchange, date AS order_date, order_time, status_message, status
            FROM ob
            WHERE DATE(date) = :trade_date
        """

        # Execute queries
        logger.debug("Executing users query")
        try:
            with engine.connect() as connection:
                result = connection.execute(text(user_query))
                users_df = pd.DataFrame(result.fetchall(), columns=result.keys())
        except SQLAlchemyError as e:
            logger.error(f"Users query failed: {str(e)}")
            flash(f"Users query failed: {str(e)}", "error")
            return pd.DataFrame(), pd.DataFrame()

        logger.debug(f"Executing orderbook query with trade_date: {trade_date}")
        try:
            with engine.connect() as connection:
                result = connection.execute(text(orderbook_query), {"trade_date": trade_date})
                orderbook_df = pd.DataFrame(result.fetchall(), columns=result.keys())
                logger.debug(f"Orderbook query returned {len(orderbook_df)} rows for trade_date: {trade_date}")
        except SQLAlchemyError as e:
            logger.error(f"Orderbook query failed: {str(e)}")
            flash(f"Orderbook query failed: {str(e)}", "error")
            return pd.DataFrame(), pd.DataFrame()

        # Check if dataframes are empty
        if users_df.empty:
            flash("No user data found in the database.", "error")
            logger.info("Users DataFrame is empty")
            return pd.DataFrame(), pd.DataFrame()
        if orderbook_df.empty:
            flash(f"No orderbook data found for date {trade_date}.", "error")
            logger.info(f"Orderbook DataFrame is empty for trade_date: {trade_date}")
            return pd.DataFrame(), pd.DataFrame()

        # Verify required columns
        required_ob_columns = ['user_id', 'user_alias', 'exchange', 'order_date', 'order_time', 'status_message', 'status']
        required_user_columns = ['user_id', 'alias', 'broker', 'mtm_all', 'allocation', 'max_loss', 'available_margin']
        missing_ob_columns = [col for col in required_ob_columns if col not in orderbook_df.columns]
        missing_user_columns = [col for col in required_user_columns if col not in users_df.columns]
        if missing_ob_columns or missing_user_columns:
            logger.error(f"Missing columns: ob: {missing_ob_columns}, users: {missing_user_columns}")
            flash(f"Missing columns in database: ob: {missing_ob_columns}, users: {missing_user_columns}", "error")
            return pd.DataFrame(), pd.DataFrame()

        # Multiply allocation by 100
        if 'allocation' in users_df.columns:
            users_df['allocation'] = pd.to_numeric(users_df['allocation'], errors='coerce') * 100
            users_df['allocation'] = users_df['allocation'].where(pd.notna(users_df['allocation']), None)
        else:
            logger.warning("Allocation column not found in users_df")
            flash("Allocation column not found in users table", "error")
            return pd.DataFrame(), pd.DataFrame()

        # Rename columns
        users_df = users_df.rename(columns={
            'mtm_all': 'MTM (All)',
            'max_loss': 'MAXLOSS',
            'allocation': 'ALLOCATION'
        })
        orderbook_df = orderbook_df.rename(columns={
            'user_alias': 'User Alias',
            'order_date': 'Order Date',
            'order_time': 'Order Time'
        })

        # Convert and clean User ID
        users_df['user_id'] = users_df['user_id'].astype(str).str.strip().str.upper()
        orderbook_df['user_id'] = orderbook_df['user_id'].astype(str).str.strip().str.upper()

        # Remove duplicates in users_df
        users_df = users_df.drop_duplicates(subset=['user_id'])

        # Exclude specific users
        excluded_users = ["CC_SISL_GS_DEALER", "GSPLDEAL", "GSPLDEALER"]
        orderbook_df = orderbook_df[~orderbook_df["User Alias"].isin(excluded_users)]

        # Convert datetime fields
        orderbook_df['Order Date'] = pd.to_datetime(orderbook_df['Order Date'], errors='coerce').dt.date
        orderbook_df['Order Time'] = pd.to_datetime(orderbook_df['Order Time'], errors='coerce')
        if orderbook_df['Order Time'].isna().any():
            min_valid_time = orderbook_df['Order Time'].min()
            if pd.notna(min_valid_time):
                orderbook_df['Order Time'] = orderbook_df['Order Time'].fillna(min_valid_time)
            else:
                orderbook_df['Order Time'] = orderbook_df['Order Time'].fillna(pd.Timestamp.now())

        # Sort orderbook by user_id and Order Time
        orderbook_df = orderbook_df.sort_values(['user_id', 'Order Time'])

        # Extract shortfall
        orderbook_df['Margin Shortfall'] = orderbook_df['status_message'].apply(extract_shortfall)

        # Filter for orders with shortfall
        shortfall_orders = orderbook_df[orderbook_df['Margin Shortfall'].notna()]

        if len(shortfall_orders) == 0:
            flash("No margin shortfall orders found.", "info")
            logger.info("No margin shortfall orders found")
            return pd.DataFrame(), pd.DataFrame()

        # Get unique User IDs with shortfall
        shortfall_users = shortfall_orders[['user_id']].drop_duplicates()

        # Calculate total shortfall per user
        total_shortfall = shortfall_orders.groupby('user_id')['Margin Shortfall'].sum().reset_index(name='Margin Shortfall_Total')

        # Merge with shortfall orders
        shortfall_users = shortfall_users.merge(
            shortfall_orders[['user_id', 'Margin Shortfall', 'exchange', 'Order Date', 'Order Time']],
            on='user_id',
            how='left'
        ).merge(
            total_shortfall,
            on='user_id',
            how='left'
        )

        # Merge with user data
        user_columns = ['user_id', 'alias', 'broker', 'MTM (All)', 'ALLOCATION', 'MAXLOSS', 'available_margin']
        if 'algo' in users_df.columns:
            user_columns.append('algo')
        if 'server' in users_df.columns:
            user_columns.append('server')
        existing_columns = [col for col in user_columns if col in users_df.columns]
        result_df = shortfall_users.merge(
            users_df[existing_columns],
            on='user_id',
            how='left'
        )

        # Rename columns
        result_df = result_df.rename(columns={
            'user_id': 'User ID',
            'alias': 'Alias',
            'broker': 'Broker',
            'available_margin': 'Available Margin',
            'exchange': 'Exchange'
        })

        # Filter for specific exchanges
        result_df = result_df[result_df['Exchange'].isin(['NFO', 'BFO'])]

        # Calculate status counts
        group_cols = ['user_id']
        if 'algo' in users_df.columns:
            group_cols.append('algo')
        if 'server' in users_df.columns:
            group_cols.append('server')
        status_count = orderbook_df.merge(
            users_df[['user_id'] + [col for col in ['algo', 'server'] if col in users_df.columns]],
            on='user_id',
            how='left'
        ).groupby(group_cols)['status'].value_counts().unstack(fill_value=0).reset_index()

        # Calculate margin shortfall rejections
        margin_shortfall = shortfall_orders[shortfall_orders['Margin Shortfall'] > 0]
        rejections = margin_shortfall.merge(
            users_df[['user_id'] + [col for col in ['algo', 'server'] if col in users_df.columns]],
            on='user_id',
            how='left'
        ).groupby(group_cols).size().reset_index(name='Margin Shortfall Rejections')

        # Calculate total shortfall for pivot
        shortfall_total_pivot = shortfall_orders.merge(
            users_df[['user_id'] + [col for col in ['algo', 'server'] if col in users_df.columns]],
            on='user_id',
            how='left'
        ).groupby(group_cols)['Margin Shortfall'].sum().reset_index(name='Margin Shortfall_Total')

        # Merge status counts with rejections and total shortfall
        pivot_df = pd.merge(status_count, rejections, on=group_cols, how='left')
        pivot_df = pd.merge(pivot_df, shortfall_total_pivot, on=group_cols, how='left')
        pivot_df['Margin Shortfall Rejections'] = pivot_df['Margin Shortfall Rejections'].fillna(0).astype(int)
        pivot_df['Margin Shortfall_Total'] = pivot_df['Margin Shortfall_Total'].fillna(0).round(2)

        # Rename pivot_df columns
        pivot_df = pivot_df.rename(columns={'user_id': 'User ID', 'algo': 'ALGO', 'server': 'SERVER'})

        # Convert non-serializable types for JSON
        result_df['Order Date'] = result_df['Order Date'].apply(
            lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else ''
        )
        result_df['Order Time'] = result_df['Order Time'].apply(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else ''
        )
        # Convert numeric columns to Python float for JSON serialization
        for col in result_df.select_dtypes(include=[np.integer, np.floating]).columns:
            result_df[col] = result_df[col].astype(float).where(pd.notna(result_df[col]), None)
        for col in pivot_df.select_dtypes(include=[np.integer, np.floating]).columns:
            pivot_df[col] = pivot_df[col].astype(float).where(pd.notna(pivot_df[col]), None)

        logger.info(f"Margin shortfall analysis completed for trade_date: {trade_date}")
        logger.debug(f"Result_df rows: {len(result_df)}, Pivot_df rows: {len(pivot_df)}")
        return result_df, pivot_df

    except Exception as e:
        handle_error(e, "Margin Shortfall Calculation")
        return pd.DataFrame(), pd.DataFrame()

@margin_bp.route('/margin', methods=['GET', 'POST'])
@require_role(['admin', 'user'])
def margin_shortfall_page():
    try:
        # Initialize session_id if not present
        if 'session_id' not in session:
            session['session_id'] = str(uuid.uuid4())
            session.modified = True
        session_id = session['session_id']
        trade_date = session.get('margin_trade_date', None)
        
        # Retrieve existing data from session
        result_data = session.get('margin_result_data', None)
        pivot_data = session.get('margin_pivot_data', None)

        if request.method == 'POST':
            trade_date = request.form.get('trade_date')
            if not trade_date:
                flash("Please select a date", "error")
                logger.warning("No trade date provided in POST request")
                return render_template('margin_shortfall.html', role=session.get('role'),
                                      result_data=result_data, pivot_data=pivot_data,
                                      trade_date=trade_date)

            # Validate trade_date format
            try:
                datetime.strptime(trade_date, '%Y-%m-%d')
            except ValueError:
                flash("Invalid date format. Please use YYYY-MM-DD.", "error")
                return render_template('margin_shortfall.html', role=session.get('role'),
                                      result_data=result_data, pivot_data=pivot_data,
                                      trade_date=trade_date)

            if request.form.get('export') == 'xlsx':
                try:
                    session_id = request.form.get('session_id')
                    if not session_id:
                        logger.warning("No session_id provided in export request")
                        flash("Session ID missing. Please analyze data first.", "error")
                        return render_template('margin_shortfall.html', role=session.get('role'),
                                              result_data=result_data, pivot_data=pivot_data,
                                              trade_date=trade_date)

                    result_data = session.get('margin_result_data', None)
                    pivot_data = session.get('margin_pivot_data', None)
                    if not result_data or not pivot_data:
                        logger.warning(f"No data found for export. session_id: {session_id}, trade_date: {trade_date}")
                        flash("No margin shortfall data available to export.", "error")
                        return render_template('margin_shortfall.html', role=session.get('role'),
                                              result_data=result_data, pivot_data=pivot_data,
                                              trade_date=trade_date)

                    result_df = pd.DataFrame(result_data)
                    pivot_df = pd.DataFrame(pivot_data)

                    if result_df.empty or pivot_df.empty:
                        logger.warning("Empty result_df or pivot_df during export. Result_df rows: %d, Pivot_df rows: %d",
                                      len(result_df), len(pivot_df))
                        flash("No margin shortfall data available to export.", "error")
                        return render_template('margin_shortfall.html', role=session.get('role'),
                                              result_data=result_data, pivot_data=pivot_data,
                                              trade_date=trade_date)

                    # Ensure numeric columns are floats
                    for col in result_df.select_dtypes(include=['object']).columns:
                        if col in ['Margin Shortfall', 'Margin Shortfall_Total', 'MTM (All)', 'ALLOCATION', 'MAXLOSS', 'Available Margin']:
                            result_df[col] = pd.to_numeric(result_df[col], errors='coerce')
                    for col in pivot_df.select_dtypes(include=['object']).columns:
                        if col in ['Margin Shortfall_Total', 'Margin Shortfall Rejections']:
                            pivot_df[col] = pd.to_numeric(pivot_df[col], errors='coerce')

                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        pivot_df.to_excel(writer, sheet_name='Summary', index=False)
                        result_df.to_excel(writer, sheet_name='Details', index=False)

                    output.seek(0)
                    logger.info(f"Exporting margin shortfall data for trade_date: {trade_date}")
                    return Response(
                        output,
                        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        headers={'Content-Disposition': f'attachment;filename=margin_shortfall_{trade_date.replace("-", "")}.xlsx'}
                    )
                except Exception as e:
                    logger.error(f"Export failed: {str(e)}")
                    flash(f"Failed to export data: {str(e)}", "error")
                    return render_template('margin_shortfall.html', role=session.get('role'),
                                          result_data=result_data, pivot_data=pivot_data,
                                          trade_date=trade_date)

            logger.debug(f"Calling analyze_margin_shortfalls with trade_date: {trade_date}")
            result_df, pivot_df = analyze_margin_shortfalls(trade_date)
            if result_df.empty or pivot_df.empty:
                logger.info("No data returned from analyze_margin_shortfalls")
                flash("No margin shortfall data found for the selected date.", "error")
                return render_template('margin_shortfall.html', role=session.get('role'),
                                      result_data=result_data, pivot_data=pivot_data,
                                      trade_date=trade_date)

            result_data = result_df.to_dict('records')
            pivot_data = pivot_df.to_dict('records')
            logger.debug(f"Storing result_data: {len(result_data)} records, pivot_data: {len(pivot_data)} records")
            session['margin_result_data'] = result_data
            session['margin_pivot_data'] = pivot_data
            session['margin_trade_date'] = trade_date
            session.modified = True
            flash(f"Margin shortfall calculated for {trade_date}", "success")
            logger.info(f"Margin shortfall data stored in session for session_id: {session_id}, trade_date: {trade_date}")

        return render_template('margin_shortfall.html', role=session.get('role'),
                              result_data=result_data, pivot_data=pivot_data,
                              trade_date=trade_date)

    except Exception as e:
        handle_error(e, "Margin Shortfall Page")
        return render_template('margin_shortfall.html', role=session.get('role'),
                              result_data=result_data, pivot_data=pivot_data,
                              trade_date=trade_date)