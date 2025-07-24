from flask import Blueprint, redirect, render_template, request, session, url_for
import pandas as pd
from sqlalchemy import text
from datetime import datetime, timedelta
from utils import get_db_connection, logger

# Define the dashboard Blueprint
dashboard = Blueprint('dashboard', __name__, template_folder='templates')

def fetch_data():
    """Fetch data from the users table using SQLAlchemy connection from utils."""
    engine = get_db_connection()
    if not engine:
        logger.error("Database connection failed in fetch_data")
        return None
    try:
        with engine.connect() as connection:
            query = "SELECT * FROM users"
            df = pd.read_sql(query, connection)
            return df
    except Exception as e:
        logger.error(f"Database error in fetch_data: {str(e)}")
        return None

def format_indian_number(number):
    """Format a number in Indian numbering system with commas (e.g., 12,34,56,789.00)."""
    try:
        number = round(float(number), 2)
        integer_part, decimal_part = str(number).split('.')
        is_negative = integer_part.startswith('-')
        if is_negative:
            integer_part = integer_part[1:]
        integer_part = integer_part[::-1]
        formatted = integer_part[:3]
        for i in range(3, len(integer_part), 2):
            formatted += ',' + integer_part[i:i+2]
        formatted = formatted[::-1]
        if is_negative:
            formatted = '-' + formatted
        return f"{formatted}.{decimal_part:0>2}"
    except (ValueError, AttributeError):
        return "0.00"

def process_data(df, time_period='Last Day', total_mtm_algo=None, total_mtm_servers=None, top_least_algo=None, top_least_servers=None, selected_user_id=None, dte_filter=None, start_date=None, end_date=None):
    """Process data for the dashboard with filtering and aggregation for each card."""
    # Initialize all return variables to avoid UnboundLocalError
    final_df = None
    grand_total = None
    num_users = 0
    num_algos = 0
    all_user_ids = []
    unique_server_count = 0
    top_users = []
    least_users = []
    unique_algos = []
    unique_servers = []
    selected_date = None
    dte_message = None
    date_display = "N/A"
    top_users_count = 0
    least_users_count = 0
    total_return_percent = 0.0
    mtm_dates = []
    mtm_values = []
    worst_trading_day = None
    best_trading_day = None
    win_rate = 0.0
    total_mtm_value = "0.00"
    profit_factor = 0.0
    algo_to_servers = {}
    total_aum_summary = "0.00"
    total_trading_days = 0
    win_day = 0
    loss_day = 0

    if df is None or df.empty:
        logger.warning("No data fetched or empty DataFrame in process_data")
        return (
            final_df, grand_total, num_users, num_algos, all_user_ids, unique_server_count,
            top_users, least_users, unique_algos, unique_servers, time_period, selected_date,
            "No data available", date_display, top_users_count, least_users_count, total_return_percent,
            mtm_dates, mtm_values, worst_trading_day, best_trading_day, win_rate, total_mtm_value,
            profit_factor, algo_to_servers, total_aum_summary, total_trading_days, win_day, loss_day
        )

    try:
        # Ensure date column is in datetime format
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

        # Clean and deduplicate unique values for dropdowns
        df['algo'] = df['algo'].astype(str).str.strip().str.lower().replace('nan', None)
        df['server'] = df['server'].astype(str).str.strip().str.lower().replace('nan', None)
        df['user_id'] = df['user_id'].astype(str).str.strip().replace('nan', None)
        
        # Get unique algos, servers, user IDs, and DTE values for dropdowns
        unique_algos = sorted(df['algo'].dropna().drop_duplicates().tolist())
        unique_servers = sorted(df['server'].dropna().drop_duplicates().tolist())
        all_user_ids = sorted(df['user_id'].dropna().drop_duplicates().tolist())
        unique_dtes = sorted(df['dte'].dropna().drop_duplicates().tolist()) if 'dte' in df.columns else []

        # Get current date and latest date in data
        current_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_date = current_date - timedelta(days=1)
        latest_date = df['date'].max()
        if pd.isna(latest_date):
            logger.warning("No valid dates in data")
            return (
                final_df, grand_total, num_users, num_algos, all_user_ids, unique_server_count,
                top_users, least_users, unique_algos, unique_servers, time_period, selected_date,
                "No valid dates in data", date_display, top_users_count, least_users_count,
                total_return_percent, mtm_dates, mtm_values, worst_trading_day, best_trading_day,
                win_rate, total_mtm_value, profit_factor, algo_to_servers, total_aum_summary,
                total_trading_days, win_day, loss_day
            )

        # Define time period ranges
        time_periods = {
            'Today': current_date,
            'Yesterday': yesterday_date,
            'Last Week': (current_date - timedelta(days=6), current_date),
            'Last Month': (current_date - timedelta(days=29), current_date),
            '6 Months': (current_date - timedelta(days=181), current_date),
            '1 Year': (current_date - timedelta(days=365), current_date),
            '2 Years': (current_date - timedelta(days=730), current_date),
            'Last Day': latest_date
        }

        # Apply date and DTE filter
        if time_period == 'Custom':
            if not start_date or not end_date:
                logger.warning("Custom time period selected but start_date or end_date missing")
                return (
                    final_df, grand_total, num_users, num_algos, all_user_ids, unique_server_count,
                    top_users, least_users, unique_algos, unique_servers, time_period, selected_date,
                    "Please select a valid date range for Custom period", date_display, top_users_count,
                    least_users_count, total_return_percent, mtm_dates, mtm_values, worst_trading_day,
                    best_trading_day, win_rate, total_mtm_value, profit_factor, algo_to_servers,
                    total_aum_summary, total_trading_days, win_day, loss_day
                )
            try:
                start_date = pd.to_datetime(start_date)
                end_date = pd.to_datetime(end_date)
                if start_date > end_date:
                    logger.warning("start_date is after end_date")
                    return (
                        final_df, grand_total, num_users, num_algos, all_user_ids, unique_server_count,
                        top_users, least_users, unique_algos, unique_servers, time_period, selected_date,
                        "Start date cannot be after end date", date_display, top_users_count,
                        least_users_count, total_return_percent, mtm_dates, mtm_values, worst_trading_day,
                        best_trading_day, win_rate, total_mtm_value, profit_factor, algo_to_servers,
                        total_aum_summary, total_trading_days, win_day, loss_day
                    )
                if start_date > current_date or end_date > current_date:
                    logger.warning("Selected dates are in the future")
                    return (
                        final_df, grand_total, num_users, num_algos, all_user_ids, unique_server_count,
                        top_users, least_users, unique_algos, unique_servers, time_period, selected_date,
                        "Selected dates cannot be in the future", date_display, top_users_count,
                        least_users_count, total_return_percent, mtm_dates, mtm_values, worst_trading_day,
                        best_trading_day, win_rate, total_mtm_value, profit_factor, algo_to_servers,
                        total_aum_summary, total_trading_days, win_day, loss_day
                    )
                filtered_df = df[(df['date'] >= start_date) & (df['date'] <= end_date)].copy()
                selected_date = end_date
                date_display = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
                if filtered_df.empty:
                    logger.warning(f"No data for Custom period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
                    return (
                        final_df, grand_total, num_users, num_algos, all_user_ids, unique_server_count,
                        top_users, least_users, unique_algos, unique_servers, time_period, selected_date,
                        f"No data available for {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                        date_display, top_users_count, least_users_count, total_return_percent, mtm_dates,
                        mtm_values, worst_trading_day, best_trading_day, win_rate, total_mtm_value,
                        profit_factor, algo_to_servers, total_aum_summary, total_trading_days, win_day, loss_day
                    )
            except ValueError as e:
                logger.error(f"Invalid date format for Custom period: {str(e)}")
                return (
                    final_df, grand_total, num_users, num_algos, all_user_ids, unique_server_count,
                    top_users, least_users, unique_algos, unique_servers, time_period, selected_date,
                    f"Invalid date format: {str(e)}", date_display, top_users_count, least_users_count,
                    total_return_percent, mtm_dates, mtm_values, worst_trading_day, best_trading_day,
                    win_rate, total_mtm_value, profit_factor, algo_to_servers, total_aum_summary,
                    total_trading_days, win_day, loss_day
                )
        elif dte_filter and dte_filter != 'Overall':
            if dte_filter not in unique_dtes:
                logger.warning(f"Invalid DTE filter: {dte_filter}")
                return (
                    final_df, grand_total, num_users, num_algos, all_user_ids, unique_server_count,
                    top_users, least_users, unique_algos, unique_servers, time_period, selected_date,
                    f"Invalid DTE: {dte_filter}", f"{dte_filter.upper()}", top_users_count,
                    least_users_count, total_return_percent, mtm_dates, mtm_values, worst_trading_day,
                    best_trading_day, win_rate, total_mtm_value, profit_factor, algo_to_servers,
                    total_aum_summary, total_trading_days, win_day, loss_day
                )
            filtered_df = df[df['dte'] == dte_filter].copy()
            if filtered_df.empty:
                logger.warning(f"No data for DTE={dte_filter}")
                return (
                    final_df, grand_total, num_users, num_algos, all_user_ids, unique_server_count,
                    top_users, least_users, unique_algos, unique_servers, time_period, selected_date,
                    f"No data available for DTE={dte_filter}", f"{dte_filter.upper()}", top_users_count,
                    least_users_count, total_return_percent, mtm_dates, mtm_values, worst_trading_day,
                    best_trading_day, win_rate, total_mtm_value, profit_factor, algo_to_servers,
                    total_aum_summary, total_trading_days, win_day, loss_day
                )
            if time_period in ['Today', 'Last Day', 'Yesterday']:
                selected_date = current_date if time_period == 'Today' else (yesterday_date if time_period == 'Yesterday' else latest_date)
                filtered_df = filtered_df[filtered_df['date'] == selected_date]
                date_display = f"{dte_filter.upper()} ({selected_date.strftime('%Y-%m-%d')})"
                if filtered_df.empty and time_period == 'Yesterday':
                    filtered_df = df[(df['dte'] == dte_filter) & (df['date'] == current_date)].copy()
                    if filtered_df.empty:
                        logger.warning(f"No data for DTE={dte_filter} on Yesterday ({yesterday_date}) or Today ({current_date})")
                        return (
                            final_df, grand_total, num_users, num_algos, all_user_ids, unique_server_count,
                            top_users, least_users, unique_algos, unique_servers, time_period, yesterday_date,
                            f"No data available for {yesterday_date.strftime('%Y-%m-%d')}", date_display, top_users_count,
                            least_users_count, total_return_percent, mtm_dates, mtm_values, worst_trading_day,
                            best_trading_day, win_rate, total_mtm_value, profit_factor, algo_to_servers,
                            total_aum_summary, total_trading_days, win_day, loss_day
                        )
                    else:
                        selected_date = current_date
                        date_display = f"{dte_filter.upper()} ({current_date.strftime('%Y-%m-%d')})"
                elif filtered_df.empty:
                    logger.warning(f"No data for DTE={dte_filter} on {time_period} ({selected_date})")
                    return (
                        final_df, grand_total, num_users, num_algos, all_user_ids, unique_server_count,
                        top_users, least_users, unique_algos, unique_servers, time_period, selected_date,
                        f"No data available for DTE={dte_filter} on {time_period}", date_display, top_users_count,
                        least_users_count, total_return_percent, mtm_dates, mtm_values, worst_trading_day,
                        best_trading_day, win_rate, total_mtm_value, profit_factor, algo_to_servers,
                        total_aum_summary, total_trading_days, win_day, loss_day
                    )
            elif time_period in ['Last Week', 'Last Month', '6 Months', '1 Year', '2 Years']:
                start_date, end_date = time_periods[time_period]
                filtered_df = filtered_df[(filtered_df['date'] >= start_date) & (filtered_df['date'] <= end_date)]
                selected_date = filtered_df['date'].max() if not filtered_df.empty else None
                if filtered_df.empty:
                    logger.warning(f"No data for DTE={dte_filter} in {time_period}")
                    return (
                        final_df, grand_total, num_users, num_algos, all_user_ids, unique_server_count,
                        top_users, least_users, unique_algos, unique_servers, time_period, selected_date,
                        f"No data available for DTE={dte_filter} in {time_period}", f"{dte_filter.upper()}", top_users_count,
                        least_users_count, total_return_percent, mtm_dates, mtm_values, worst_trading_day,
                        best_trading_day, win_rate, total_mtm_value, profit_factor, algo_to_servers,
                        total_aum_summary, total_trading_days, win_day, loss_day
                    )
                first_date = filtered_df['date'].min().strftime('%Y-%m-%d')
                last_date = filtered_df['date'].max().strftime('%Y-%m-%d')
                date_display = f"{dte_filter.upper()} ({first_date} to {last_date})"
        else:
            if time_period == 'Today':
                filtered_df = df[df['date'] == current_date].copy()
                selected_date = current_date
                date_display = current_date.strftime('%Y-%m-%d')
            elif time_period == 'Yesterday':
                filtered_df = df[df['date'] == yesterday_date].copy()
                selected_date = yesterday_date
                date_display = yesterday_date.strftime('%Y-%m-%d')
                if filtered_df.empty:
                    filtered_df = df[df['date'] == current_date].copy()
                    if filtered_df.empty:
                        logger.warning(f"No data for Yesterday ({yesterday_date}) or Today ({current_date})")
                        return (
                            final_df, grand_total, num_users, num_algos, all_user_ids, unique_server_count,
                            top_users, least_users, unique_algos, unique_servers, time_period, yesterday_date,
                            f"No data available for {yesterday_date.strftime('%Y-%m-%d')}", date_display, top_users_count,
                            least_users_count, total_return_percent, mtm_dates, mtm_values, worst_trading_day,
                            best_trading_day, win_rate, total_mtm_value, profit_factor, algo_to_servers,
                            total_aum_summary, total_trading_days, win_day, loss_day
                        )
                    else:
                        selected_date = current_date
                        date_display = current_date.strftime('%Y-%m-%d')
            elif time_period == 'Last Day':
                filtered_df = df[df['date'] == latest_date].copy()
                selected_date = latest_date
                date_display = latest_date.strftime('%Y-%m-%d')
            else:
                start_date, end_date = time_periods[time_period]
                filtered_df = df[(df['date'] >= start_date) & (df['date'] <= end_date)].copy()
                selected_date = filtered_df['date'].max() if not filtered_df.empty else end_date
                if filtered_df.empty:
                    logger.warning(f"No data for time period={time_period} (Overall)")
                    return (
                        final_df, grand_total, num_users, num_algos, all_user_ids, unique_server_count,
                        top_users, least_users, unique_algos, unique_servers, time_period, selected_date,
                        f"No data available for {time_period} (Overall)", date_display, top_users_count,
                        least_users_count, total_return_percent, mtm_dates, mtm_values, worst_trading_day,
                        best_trading_day, win_rate, total_mtm_value, profit_factor, algo_to_servers,
                        total_aum_summary, total_trading_days, win_day, loss_day
                    )
                first_date = filtered_df['date'].min().strftime('%Y-%m-%d')
                last_date = filtered_df['date'].max().strftime('%Y-%m-%d')
                date_display = f"{first_date} to {last_date}"

        # Calculate Total Trading Days for Summary
        total_trading_days_summary = filtered_df['date'].nunique()

        # Compute algo-to-servers mapping for dropdowns
        filtered_df_for_mapping = filtered_df.copy()
        filtered_df_for_mapping = filtered_df_for_mapping[filtered_df_for_mapping['server'] != '5 total']
        filtered_df_for_mapping = filtered_df_for_mapping[
            (filtered_df_for_mapping['allocation'].notnull()) & 
            (filtered_df_for_mapping['allocation'] != 0) & 
            (filtered_df_for_mapping['mtm_all'].notnull()) & 
            (filtered_df_for_mapping['mtm_all'] != 0)
        ]
        filtered_df_for_mapping = filtered_df_for_mapping[~filtered_df_for_mapping['alias'].str.contains('DEAL', case=False, na=False)]
        filtered_df_for_mapping = filtered_df_for_mapping[
            ~(
                (filtered_df_for_mapping['max_loss'].isnull() | (filtered_df_for_mapping['max_loss'] == 0)) &
                (filtered_df_for_mapping['mtm_all'].isnull() | (filtered_df_for_mapping['mtm_all'] == 0)) &
                (filtered_df_for_mapping['algo'] != '5')
            )
        ]

        algo_to_servers = {}
        for algo in filtered_df_for_mapping['algo'].dropna().drop_duplicates():
            servers = sorted(filtered_df_for_mapping[filtered_df_for_mapping['algo'] == algo]['server'].dropna().drop_duplicates().tolist())
            algo_to_servers[algo] = servers
        algo_to_servers['All Algos'] = unique_servers

        # Apply base filters for all cards
        base_df = filtered_df.copy()
        base_df = base_df[
            (base_df['allocation'].notnull()) & 
            (base_df['allocation'] != 0) & 
            (base_df['mtm_all'].notnull()) & 
            (base_df['mtm_all'] != 0)
        ]
        base_df = base_df[~base_df['alias'].str.contains('DEAL', case=False, na=False)]
        base_df = base_df[
            ~(
                (base_df['max_loss'].isnull() | (base_df['max_loss'] == 0)) &
                (base_df['mtm_all'].isnull() | (base_df['mtm_all'] == 0)) &
                (base_df['algo'] != '5')
            )
        ]

        # --- Summary Card ---
        summary_df = base_df.copy()
        if selected_user_id and selected_user_id != "All Users":
            summary_df = summary_df[summary_df['user_id'] == selected_user_id]
        if total_mtm_algo and total_mtm_algo != "All Algos":
            summary_df = summary_df[summary_df['algo'] == total_mtm_algo]
        if total_mtm_servers and "All Servers" not in total_mtm_servers:
            summary_df = summary_df[summary_df['server'].isin(total_mtm_servers)]

        num_users = summary_df['user_id'].nunique()
        num_algos = summary_df['algo'].nunique()
        unique_server_count = summary_df['server'].nunique()
        
        user_aggregation = summary_df.groupby('user_id').agg({
            'mtm_all': 'sum',
            'allocation': 'mean'
        }).reset_index()
        total_mtm = user_aggregation['mtm_all'].sum()
        total_allocation = user_aggregation['allocation'].sum()
        total_return_percent = round((total_mtm / total_allocation) , 2) if total_allocation != 0 else 0.0

        total_aum_summary = round(total_allocation * 100, 2) if total_allocation else 0.00
        if total_aum_summary >= 1e7:
            total_aum_summary = f"{round(total_aum_summary / 1e7, 2)} Cr"
        else:
            total_aum_summary = format_indian_number(total_aum_summary)

        # --- Total MTM and Trading Statistics Card ---
        total_mtm_df = base_df.copy()
        if selected_user_id and selected_user_id != "All Users":
            total_mtm_df = total_mtm_df[total_mtm_df['user_id'] == selected_user_id]
        if total_mtm_algo and total_mtm_algo != "All Algos":
            total_mtm_df = total_mtm_df[total_mtm_df['algo'] == total_mtm_algo]
        if total_mtm_servers and "All Servers" not in total_mtm_servers:
            total_mtm_df = total_mtm_df[total_mtm_df['server'].isin(total_mtm_servers)]
            
        total_mtm_value = round(total_mtm_df['mtm_all'].sum(), 2) if not total_mtm_df.empty else 0.00
        total_mtm_value = format_indian_number(total_mtm_value)

        user_avg_alloc = total_mtm_df.groupby('user_id')['allocation'].mean()
        total_allocation = user_avg_alloc.sum()
        total_aum = round(total_allocation * 100, 2) if total_allocation else 0.00
        if total_aum >= 1e7:
            total_aum = f"{round(total_aum / 1e7, 2)} Cr"
        else:
            total_aum = format_indian_number(total_aum)

        all_user_ids = sorted(total_mtm_df['user_id'].dropna().drop_duplicates().tolist())

        # --- Performance Overview and Trading Insights Cards ---
        mtm_vs_day_df = total_mtm_df.copy()
        if not mtm_vs_day_df.empty:
            daily_mtm = mtm_vs_day_df.groupby(mtm_vs_day_df['date'].dt.strftime('%Y-%m-%d'))['mtm_all'].sum().reset_index()
            mtm_dates = daily_mtm['date'].tolist()
            mtm_values = daily_mtm['mtm_all'].tolist()

            total_trading_days = mtm_vs_day_df['date'].nunique()

            negative_mtm = daily_mtm[daily_mtm['mtm_all'] < 0]
            worst_day = negative_mtm.loc[negative_mtm['mtm_all'].idxmin()] if not negative_mtm.empty else None
            worst_trading_day = {
                'date': worst_day['date'],
                'mtm': format_indian_number(round(worst_day['mtm_all'], 2))
            } if worst_day is not None else None

            positive_mtm = daily_mtm[daily_mtm['mtm_all'] > 0]
            best_day = positive_mtm.loc[positive_mtm['mtm_all'].idxmax()] if not positive_mtm.empty else None
            best_trading_day = {
                'date': best_day['date'],
                'mtm': format_indian_number(round(best_day['mtm_all'], 2))
            } if best_day is not None else None

            positive_days = daily_mtm[daily_mtm['mtm_all'] > 0].shape[0]
            total_days = daily_mtm.shape[0]
            win_rate = round((positive_days / total_days) * 100, 2) if total_days > 0 else 0.0

            gross_profits = daily_mtm[daily_mtm['mtm_all'] > 0]['mtm_all'].sum()
            gross_losses = abs(daily_mtm[daily_mtm['mtm_all'] < 0]['mtm_all'].sum())
            profit_factor = round(gross_profits / gross_losses, 2) if gross_losses != 0 else 0.0

            win_day = daily_mtm[daily_mtm['mtm_all'] > 0].shape[0]
            loss_day = daily_mtm[daily_mtm['mtm_all'] < 0].shape[0]
        else:
            mtm_dates = []
            mtm_values = []
            total_aum = "0.00"
            total_trading_days = 0
            worst_trading_day = None
            best_trading_day = None
            win_rate = 0.0
            profit_factor = 0.0
            win_day = 0
            loss_day = 0

        # --- Top/Least Performing Users Card ---
        top_least_df = base_df.copy()
        if top_least_algo and top_least_algo != "All Algos":
            top_least_df = top_least_df[top_least_df['algo'] == top_least_algo]
        if top_least_servers and "All Servers" not in top_least_servers:
            top_least_df = top_least_df[top_least_df['server'].isin(top_least_servers)]

        if top_least_df.empty:
            logger.warning(f"Top/Least & Detailed Report - No data after filters")
            return (
                final_df, grand_total, num_users, num_algos, all_user_ids, unique_server_count,
                top_users, least_users, unique_algos, unique_servers, time_period, selected_date,
                dte_message or f"No data available after filters", date_display, top_users_count,
                least_users_count, total_return_percent, mtm_dates, mtm_values, worst_trading_day,
                best_trading_day, win_rate, total_mtm_value, profit_factor, algo_to_servers,
                total_aum_summary, total_trading_days, win_day, loss_day
            )

        user_aggregation = top_least_df.groupby(['user_id', 'algo', 'server']).agg({
            'mtm_all': 'sum',
            'allocation': 'mean'
        }).reset_index()
        
        user_aggregation['Return Ratio'] = (user_aggregation['mtm_all'] / user_aggregation['allocation']).round(2)
        user_aggregation['Return Ratio'] = user_aggregation['Return Ratio'].replace([float('inf'), -float('inf')], 0)
        
        user_count = len(user_aggregation)
        top_count = max(1, int(user_count * 0.2))
        top_users = user_aggregation.sort_values(by='Return Ratio', ascending=False).head(top_count).to_dict('records')
        least_users = user_aggregation.sort_values(by='Return Ratio').head(top_count).to_dict('records')
        top_users_count = len(top_users)
        least_users_count = len(least_users)

        # --- Detailed Report Card ---
        grouped = top_least_df.groupby(['algo', 'server']).agg({
            'user_id': 'nunique',
            'allocation': lambda x: top_least_df[top_least_df['user_id'].isin(x.index)]['allocation'].mean(),
            'mtm_all': 'sum'
        }).reset_index()

        grouped['allocation'] = grouped.apply(
            lambda row: top_least_df[
                (top_least_df['algo'] == row['algo']) & 
                (top_least_df['server'] == row['server'])
            ].groupby('user_id')['allocation'].mean().sum(),
            axis=1
        )

        grouped['Return Ratio'] = (grouped['mtm_all'] / grouped['allocation']).round(2)
        grouped['Return Ratio'] = grouped['Return Ratio'].replace([float('inf'), -float('inf')], 0)
        
        final_df = grouped.sort_values(by=['algo', 'server'])
        final_df = final_df.rename(columns={
            'algo': 'ALGO',
            'server': 'SERVER',
            'user_id': 'No. of Users',
            'allocation': 'Sum of ALLOCATION',
            'mtm_all': 'Sum of MTM (All)'
        })
        final_df = final_df[['ALGO', 'SERVER', 'No. of Users', 'Sum of ALLOCATION', 'Sum of MTM (All)', 'Return Ratio']]

        final_df['Sum of ALLOCATION'] = final_df['Sum of ALLOCATION'].apply(format_indian_number)
        final_df['Sum of MTM (All)'] = final_df['Sum of MTM (All)'].apply(format_indian_number)

        grand_total = {
            'ALGO': 'GRAND TOTAL',
            'SERVER': '',
            'No. of Users': final_df['No. of Users'].sum(),
            'Sum of ALLOCATION': format_indian_number(top_least_df.groupby('user_id')['allocation'].mean().sum()),
            'Sum of MTM (All)': format_indian_number(grouped['mtm_all'].sum()),
            'Return Ratio': round(grouped['mtm_all'].sum() / top_least_df.groupby('user_id')['allocation'].mean().sum(), 2) if top_least_df.groupby('user_id')['allocation'].mean().sum() != 0 else 0.0
        }

    except Exception as e:
        logger.error(f"Error in process_data: {str(e)}")
        return (
            final_df, grand_total, num_users, num_algos, all_user_ids, unique_server_count,
            top_users, least_users, unique_algos, unique_servers, time_period, selected_date,
            f"Error processing data: {str(e)}", date_display, top_users_count, least_users_count,
            total_return_percent, mtm_dates, mtm_values, worst_trading_day, best_trading_day,
            win_rate, total_mtm_value, profit_factor, algo_to_servers, total_aum_summary,
            total_trading_days, win_day, loss_day
        )

    return (
        final_df, grand_total, num_users, num_algos, all_user_ids, unique_server_count,
        top_users, least_users, unique_algos, unique_servers, time_period, selected_date,
        dte_message, date_display, top_users_count, least_users_count, total_return_percent,
        mtm_dates, mtm_values, worst_trading_day, best_trading_day, win_rate, total_mtm_value,
        profit_factor, algo_to_servers, total_aum_summary, total_trading_days, win_day, loss_day
    )

@dashboard.route('/dashboard')
def dashboard_route():
    """Render the dashboard with filtered data."""
    if 'authenticated' not in session or not session['authenticated']:
        logger.warning("Unauthenticated access attempt to dashboard")
        return redirect(url_for('login.login'))

    time_period = request.args.get('time_period', 'Last Day')
    total_mtm_algo = request.args.get('total_mtm_algo', 'All Algos')
    total_mtm_servers = request.args.getlist('total_mtm_servers')
    top_least_algo = request.args.get('top_least_algo', 'All Algos')
    top_least_servers = request.args.getlist('top_least_servers')
    selected_user_id = request.args.get('user_id', 'All Users')
    dte_filter = request.args.get('dte_filter', 'Overall')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    df = fetch_data()
    if df is not None and not df.empty and 'dte' in df.columns:
        unique_dtes = sorted(df['dte'].dropna().drop_duplicates().tolist())
    else:
        unique_dtes = []

    result = process_data(df, time_period, total_mtm_algo, total_mtm_servers, top_least_algo, top_least_servers, selected_user_id, dte_filter, start_date, end_date)
    
    if len(result) != 29:
        logger.error(f"process_data returned {len(result)} values, expected 29")
        return render_template(
            'dashboard.html',
            error="Internal server error: Invalid data processing",
            selected_date=None,
            unique_algos=[],
            unique_servers=[],
            unique_dtes=unique_dtes,
            total_mtm_algo=total_mtm_algo,
            total_mtm_servers=total_mtm_servers,
            top_least_algo=top_least_algo,
            top_least_servers=top_least_servers,
            time_period=time_period,
            date_display="N/A",
            top_users=[],
            top_users_count=0,
            least_users=[],
            least_users_count=0,
            total_return_percent=0.0,
            mtm_dates=[],
            mtm_values=[],
            worst_trading_day=None,
            best_trading_day=None,
            win_rate=0.0,
            total_mtm_value="0.00",
            all_user_ids=[],
            selected_user_id=selected_user_id,
            profit_factor=0.0,
            algo_to_servers={},
            dte_filter=dte_filter,
            dte_message=None,
            total_aum="0.00",
            total_trading_days=0,
            win_day=0,
            loss_day=0
        )
    
    final_df, grand_total, num_users, num_algos, all_user_ids, unique_server_count, top_users, least_users, unique_algos, unique_servers, time_period, selected_date, dte_message, date_display, top_users_count, least_users_count, total_return_percent, mtm_dates, mtm_values, worst_trading_day, best_trading_day, win_rate, total_mtm_value, profit_factor, algo_to_servers, total_aum_summary, total_trading_days, win_day, loss_day = result
    
    if dte_message:
        logger.warning(f"Dashboard message/error: {dte_message}")
        return render_template(
            'dashboard.html',
            error=dte_message,
            selected_date=selected_date,
            unique_algos=unique_algos,
            unique_servers=unique_servers,
            unique_dtes=unique_dtes,
            total_mtm_algo=total_mtm_algo,
            total_mtm_servers=total_mtm_servers,
            top_least_algo=top_least_algo,
            top_least_servers=top_least_servers,
            time_period=time_period,
            date_display=date_display,
            top_users=[],
            top_users_count=0,
            least_users=[],
            least_users_count=0,
            total_return_percent=0.0,
            mtm_dates=[],
            mtm_values=[],
            worst_trading_day=None,
            best_trading_day=None,
            win_rate=0.0,
            total_mtm_value="0.00",
            all_user_ids=[],
            selected_user_id=selected_user_id,
            profit_factor=0.0,
            algo_to_servers=algo_to_servers,
            dte_filter=dte_filter,
            dte_message=dte_message,
            total_aum="0.00",
            total_trading_days=0,
            win_day=0,
            loss_day=0
        )
    
    return render_template(
        'dashboard.html',
        final_df=final_df.to_dict('records') if final_df is not None else [],
        grand_total=grand_total,
        num_users=num_users,
        num_algos=num_algos,
        unique_server_count=unique_server_count,
        top_users=top_users,
        least_users=least_users,
        unique_algos=unique_algos,
        unique_servers=unique_servers,
        unique_dtes=unique_dtes,
        time_period=time_period,
        selected_date=selected_date,
        total_mtm_algo=total_mtm_algo,
        total_mtm_servers=total_mtm_servers,
        top_least_algo=top_least_algo,
        top_least_servers=top_least_servers,
        date_display=date_display,
        top_users_count=top_users_count,
        least_users_count=least_users_count,
        total_return_percent=total_return_percent,
        mtm_dates=mtm_dates,
        mtm_values=mtm_values,
        worst_trading_day=worst_trading_day,
        best_trading_day=best_trading_day,
        win_rate=win_rate,
        total_mtm_value=total_mtm_value,
        all_user_ids=all_user_ids,
        selected_user_id=selected_user_id,
        profit_factor=profit_factor,
        algo_to_servers=algo_to_servers,
        dte_filter=dte_filter,
        dte_message=dte_message,
        total_aum=total_aum_summary,
        total_trading_days=total_trading_days,
        win_day=win_day,
        loss_day=loss_day
    )
