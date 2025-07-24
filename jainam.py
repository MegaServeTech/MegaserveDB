from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, Response, session
from functools import wraps
import pandas as pd
from datetime import date, datetime, timedelta
import os
import logging
from io import StringIO
import csv
import math
from utils import get_db_connection, logger
from auth import Auth
from sqlalchemy import text
from dotenv import load_dotenv
import os

# Load variables from .env
load_dotenv()


# Configure logging
logger = logging.getLogger(__name__)

# Initialize Blueprint
jainam_bp = Blueprint('jainam', __name__, template_folder='templates/jainam')

# Prevent re-registration
if not hasattr(jainam_bp, 'registered'):
    jainam_bp.registered = True

    # Database connection
    db_engine = get_db_connection()
    if not db_engine:
        logger.error("Failed to initialize database connection. Exiting...")
        raise RuntimeError("Database connection failed")

    # Admin access decorator
    def admin_required(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'authenticated' not in session or not session['authenticated']:
                flash("Please log in to access this page.", "error")
                logger.warning("Unauthorized access attempt: User not authenticated")
                return redirect(url_for('login.login'))
            if session.get('role') != 'admin':
                flash("You must be an admin to access this page.", "error")
                logger.warning(f"Unauthorized access attempt by {session.get('email')}: Not an admin")
                return redirect(url_for('login.login'))
            return f(*args, **kwargs)
        return decorated_function

    # Check and update schema
    def check_and_update_schema():
        connection = None
        try:
            connection = db_engine.connect()
            trans = connection.begin()
            
            # Step 1: Create jainam table first (required for foreign key references)
            result = connection.execute(text("SHOW TABLES LIKE 'jainam'")).fetchone()
            if not result:
                logger.info("Creating jainam table")
                connection.execute(text("""
                    CREATE TABLE jainam (
                        row_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        user_id VARCHAR(255) NOT NULL,
                        alias VARCHAR(50),
                        MTM DECIMAL(15,2),
                        allocation DECIMAL(15,2),
                        max_loss DECIMAL(15,2),
                        server VARCHAR(255),
                        date DATE,
                        broker VARCHAR(50),
                        algo VARCHAR(255),
                        INDEX idx_jainam_user_id (user_id)
                    ) ENGINE=InnoDB
                """))
                trans.commit()
                logger.info("Jainam table created successfully")
                trans = connection.begin()

            # Step 2: Create partner_distributions table
            result = connection.execute(text("SHOW TABLES LIKE 'partner_distributions'")).fetchone()
            if not result:
                logger.info("Creating partner_distributions table")
                connection.execute(text("""
                    CREATE TABLE partner_distributions (
                        row_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        user_id VARCHAR(255),
                        alias VARCHAR(50),
                        allocation DECIMAL(15,2),
                        calculated_mtm DECIMAL(15,2),
                        max_loss DECIMAL(15,2),
                        FOREIGN KEY fk_user_id (user_id) REFERENCES jainam(user_id)
                    ) ENGINE=InnoDB
                """))
                trans.commit()
                logger.info("Partner_distributions table created successfully")
                trans = connection.begin()

            # Step 3: Create user_partner_data table
            result = connection.execute(text("SHOW TABLES LIKE 'user_partner_data'")).fetchone()
            if not result:
                logger.info("Creating user_partner_data table")
                connection.execute(text("""
                    CREATE TABLE user_partner_data (
                        row_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        user_id VARCHAR(255),
                        alias VARCHAR(50),
                        allocation DECIMAL(15,2),
                        mtm DECIMAL(15,2),
                        max_loss DECIMAL(15,2),
                        is_main BOOLEAN NOT NULL DEFAULT FALSE,
                        date DATE,
                        broker VARCHAR(50),
                        algo VARCHAR(255),
                        FOREIGN KEY fk_user_id (user_id) REFERENCES jainam(user_id)
                    ) ENGINE=InnoDB
                """))
                trans.commit()
                logger.info("User_partner_data table created successfully")
                trans = connection.begin()

            # Step 4: Check and update columns for all tables
            tables = ['jainam', 'partner_distributions', 'user_partner_data']
            for table_name in tables:
                columns_result = connection.execute(text(f"SHOW COLUMNS FROM {table_name}")).fetchall()
                columns = {col[0]: col for col in columns_result}
                
                if 'row_id' not in columns:
                    logger.info(f"Adding row_id column to {table_name}")
                    try:
                        connection.execute(text(f"ALTER TABLE {table_name} DROP PRIMARY KEY"))
                    except Exception as e:
                        logger.warning(f"No primary key to drop in {table_name}: {e}")
                    connection.execute(text(f"ALTER TABLE {table_name} ADD COLUMN row_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST"))
                    trans.commit()
                    trans = connection.begin()

                elif 'id' in columns:
                    logger.info(f"Renaming id to row_id in {table_name}")
                    connection.execute(text(f"ALTER TABLE {table_name} CHANGE id row_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY"))
                    trans.commit()
                    trans = connection.begin()

                connection.execute(text(f"ALTER TABLE {table_name} AUTO_INCREMENT = 1"))
                trans.commit()
                trans = connection.begin()

                if table_name == 'jainam':
                    # Drop old unique constraint if it exists
                    result = connection.execute(text("SHOW INDEX FROM jainam WHERE Key_name = 'uq_user_id_date'")).fetchone()
                    if result:
                        logger.info("Dropping unique constraint uq_user_id_date from jainam")
                        connection.execute(text("ALTER TABLE jainam DROP INDEX uq_user_id_date"))
                        trans.commit()
                        trans = connection.begin()

                    # Ensure non-unique index on user_id
                    result = connection.execute(text("SHOW INDEX FROM jainam WHERE Key_name = 'idx_jainam_user_id'")).fetchone()
                    if not result:
                        logger.info("Adding non-unique index on user_id in jainam")
                        connection.execute(text("CREATE INDEX idx_jainam_user_id ON jainam (user_id)"))
                        trans.commit()
                        trans = connection.begin()

                    # Add algo column if missing
                    if 'algo' not in columns:
                        logger.info("Adding algo column to jainam")
                        connection.execute(text("ALTER TABLE jainam ADD COLUMN algo VARCHAR(255)"))
                        trans.commit()
                        trans = connection.begin()

                    # Add max_loss column if missing
                    if 'max_loss' not in columns:
                        logger.info("Adding max_loss column to jainam")
                        connection.execute(text("ALTER TABLE jainam ADD COLUMN max_loss DECIMAL(15,2)"))
                        trans.commit()
                        trans = connection.begin()

                if table_name == 'partner_distributions':
                    # Add max_loss column if missing
                    if 'max_loss' not in columns:
                        logger.info("Adding max_loss column to partner_distributions")
                        connection.execute(text("ALTER TABLE partner_distributions ADD COLUMN max_loss DECIMAL(15,2)"))
                        trans.commit()
                        trans = connection.begin()

                    # Ensure foreign key constraint
                    create_table = connection.execute(text(f"SHOW CREATE TABLE {table_name}")).fetchone()[1]
                    has_user_id_fk = any('FOREIGN KEY' in line and 'user_id' in line for line in create_table.split('\n'))
                    if not has_user_id_fk:
                        logger.info(f"Adding foreign key on user_id in {table_name}")
                        connection.execute(text(f"ALTER TABLE {table_name} ADD CONSTRAINT fk_{table_name}_user_id FOREIGN KEY (user_id) REFERENCES jainam(user_id)"))
                        trans.commit()
                        trans = connection.begin()

                if table_name == 'user_partner_data':
                    # Rename algorithm to algo if it exists
                    if 'algorithm' in columns and 'algo' not in columns:
                        logger.info("Renaming algorithm to algo in user_partner_data")
                        connection.execute(text("ALTER TABLE user_partner_data CHANGE algorithm algo VARCHAR(255)"))
                        trans.commit()
                        trans = connection.begin()

                    # Add algo column if missing
                    if 'algo' not in columns:
                        logger.info("Adding algo column to user_partner_data")
                        connection.execute(text("ALTER TABLE user_partner_data ADD COLUMN algo VARCHAR(255)"))
                        trans.commit()
                        trans = connection.begin()

                    # Ensure foreign key constraint
                    create_table = connection.execute(text(f"SHOW CREATE TABLE {table_name}")).fetchone()[1]
                    has_user_id_fk = any('FOREIGN KEY' in line and 'user_id' in line for line in create_table.split('\n'))
                    if not has_user_id_fk:
                        logger.info(f"Adding foreign key on user_id in {table_name}")
                        connection.execute(text(f"ALTER TABLE {table_name} ADD CONSTRAINT fk_{table_name}_user_id FOREIGN KEY (user_id) REFERENCES jainam(user_id)"))
                        trans.commit()
                        trans = connection.begin()

            logger.info("Schema update completed successfully")
            trans.commit()
        except Exception as e:
            logger.error(f"Error in check_and_update_schema: {e}")
            if connection:
                trans.rollback()
            raise
        finally:
            if connection:
                connection.close()

    # Error handler for 405
    @jainam_bp.errorhandler(405)
    def method_not_allowed(e):
        logger.error(f"Method Not Allowed: {request.method} on {request.url}")
        flash(f"Method {request.method} not allowed.", 'error')
        return render_template('jainam/index.html', jainam=(), default_start_date='', default_end_date='', page=1, total_pages=1, rows_per_page=50), 405

    def get_latest_date_range():
        connection = None
        try:
            connection = db_engine.connect()
            query = text("SELECT MIN(date), MAX(date) FROM jainam WHERE broker IN (:b1, :b2, :b3, :b4, :b5) OR user_id LIKE :u")
            params = {'b1': 'JAINAM_CTRADE_DL', 'b2': 'SREDJAINAM_CTRADE', 'b3': 'SREDJAINAM_103', 'b4': 'SREDJAINAM2_P', 'b5': 'ACHINTYA', 'u': '%MEGASERV%'}
            date_range = connection.execute(query, params).fetchone()
            min_date, max_date = date_range[0], date_range[1]
            if max_date:
                logger.info(f"Date range found: min={min_date}, max={max_date}")
                return min_date or date.today(), max_date
            logger.warning("No valid dates in jainam table for selected brokers or MEGASERV. Defaulting to today.")
            return date.today(), date.today()
        except Exception as e:
            logger.error(f"Error getting date range: {e}")
            return date.today(), date.today()
        finally:
            if connection:
                connection.close()

    @jainam_bp.route('/user_ids', methods=['GET'])
    @admin_required
    def get_user_ids():
        connection = None
        try:
            connection = db_engine.connect()
            query = text("SELECT DISTINCT user_id FROM jainam WHERE broker IN (:b1, :b2, :b3, :b4, :b5) OR user_id LIKE :u ORDER BY user_id")
            params = {'b1': 'JAINAM_CTRADE_DL', 'b2': 'SREDJAINAM_CTRADE', 'b3': 'SREDJAINAM_103', 'b4': 'SREDJAINAM2_P', 'b5': 'ACHINTYA', 'u': '%MEGASERV%'}
            result = connection.execute(query, params).fetchall()
            user_ids = [row._mapping['user_id'] for row in result]
            return jsonify({'user_ids': user_ids})
        except Exception as e:
            logger.error(f"Error fetching user IDs: {e}")
            return jsonify({'error': str(e)}), 500
        finally:
            if connection:
                connection.close()

    @jainam_bp.route('/', methods=['GET', 'POST'])
    @admin_required
    def index():
        logger.info(f"Request: {request.method} {request.url}")
        jainam_brokers = ['JAINAM_CTRADE_DL', 'SREDJAINAM_CTRADE', 'SREDJAINAM_103', 'SREDJAINAM2_P', 'ACHINTYA']
        jainam_records = []
        start_date = request.args.get('start_date', '')
        end_date = request.args.get('end_date', '')
        date_filter = request.args.get('date', '')
        search = request.args.get('search', '')
        user_id = request.args.get('user_id', '')
        page = int(request.args.get('page', 1))
        total_records = 0
        total_pages = 1
        try:
            rows_per_page = int(request.args.get('rows_per_page', 50))
            if rows_per_page not in [50, 100, 500, 1000]:
                rows_per_page = 50
        except ValueError:
            rows_per_page = 50

        # Set default date to the latest date if no filters are provided
        default_start_date, default_end_date = get_latest_date_range()
        if not start_date and not end_date and not date_filter:
            date_filter = default_end_date.strftime('%Y-%m-%d') if default_end_date else date.today().strftime('%Y-%m-%d')
        default_start_date = default_start_date.strftime('%Y-%m-%d') if default_start_date else ''
        default_end_date = default_end_date.strftime('%Y-%m-%d') if default_end_date else ''

        if request.method == 'POST':
            file = request.files.get('file')
            if not file:
                flash('No file uploaded', 'error')
                return redirect(url_for('jainam.index', start_date=start_date or default_start_date, end_date=end_date or default_end_date, date=date_filter, rows_per_page=rows_per_page, page=1))
            if not file.filename.endswith(('.xlsx', '.xls')):
                flash('Please upload an Excel file (.xlsx or .xls)', 'error')
                return redirect(url_for('jainam.index', start_date=start_date or default_start_date, end_date=end_date or default_end_date, date=date_filter, rows_per_page=rows_per_page, page=1))
            connection = None
            try:
                df = pd.read_excel(file)
                allowed_columns = ['user_id', 'alias', 'MTM', 'allocation', 'max_loss', 'server', 'date', 'broker', 'algo']
                existing_columns = [col for col in allowed_columns if col in df.columns]
                if not existing_columns:
                    flash('No valid columns found in the uploaded file', 'error')
                    return redirect(url_for('jainam.index', start_date=start_date or default_start_date, end_date=end_date or default_end_date, date=date_filter, rows_per_page=rows_per_page, page=1))
                df = df[existing_columns]
                
                connection = db_engine.connect()
                trans = connection.begin()
                for _, row in df.iterrows():
                    date_val = None
                    if 'date' in row and pd.notna(row['date']):
                        try:
                            date_val = pd.to_datetime(row['date'], errors='coerce').date()
                            if date_val is None:
                                logger.warning(f"Invalid date format in row: {row.to_dict()}")
                                continue
                        except Exception as e:
                            logger.warning(f"Error parsing date {row['date']}: {e}")
                            continue
                    user_id_val = str(row.get('user_id', '')).strip()
                    if not user_id_val:
                        logger.warning(f"Skipping row with empty user_id: {row.to_dict()}")
                        continue

                    insert_query = text("""
                        INSERT INTO jainam (user_id, alias, MTM, allocation, max_loss, server, date, broker, algo)
                        VALUES (:user_id, :alias, :MTM, :allocation, :max_loss, :server, :date, :broker, :algo)
                    """)
                    values = {
                        'user_id': user_id_val,
                        'alias': row.get('alias', '') or None,
                        'MTM': float(row.get('MTM', 0)) if pd.notna(row.get('MTM')) else None,
                        'allocation': float(row.get('allocation', 0)) if pd.notna(row.get('allocation')) else None,
                        'max_loss': float(row.get('max_loss', 0)) if pd.notna(row.get('max_loss')) else None,
                        'server': row.get('server', '') or None,
                        'date': date_val,
                        'broker': row.get('broker', '') or None,
                        'algo': str(row.get('algo', '')) or None
                    }
                    connection.execute(insert_query, values)
                
                trans.commit()
                flash('File uploaded successfully, data appended', 'success')
                return redirect(url_for('jainam.index', start_date=start_date or default_start_date, end_date=end_date or default_end_date, date=date_filter, rows_per_page=rows_per_page, page=1))
            except Exception as e:
                if connection:
                    trans.rollback()
                logger.error(f"Error saving to database: {e}")
                flash(f'Error saving to database: {str(e)}', 'error')
                return redirect(url_for('jainam.index', start_date=start_date or default_start_date, end_date=end_date or default_end_date, date=date_filter, rows_per_page=rows_per_page, page=1))
            finally:
                if connection:
                    connection.close()
        
        connection = None
        try:
            connection = db_engine.connect()
            query = text("SELECT * FROM jainam WHERE (broker IN (:b1, :b2, :b3, :b4, :b5) OR user_id LIKE :u)")
            params = {'b1': 'JAINAM_CTRADE_DL', 'b2': 'SREDJAINAM_CTRADE', 'b3': 'SREDJAINAM_103', 'b4': 'SREDJAINAM2_P', 'b5': 'ACHINTYA', 'u': '%MEGASERV%'}
            
            if search:
                query = text(str(query) + " AND (user_id LIKE :s1 OR alias LIKE :s2)")
                params['s1'] = f"%{search}%"
                params['s2'] = f"%{search}%"
            if user_id:
                query = text(str(query) + " AND user_id = :user_id")
                params['user_id'] = user_id
            if date_filter:
                try:
                    datetime.strptime(date_filter, '%Y-%m-%d')
                    query = text(str(query) + " AND date = :date_filter")
                    params['date_filter'] = date_filter
                except ValueError:
                    logger.error(f"Invalid date filter: {date_filter}")
                    flash('Invalid date format for single date. Please use YYYY-MM-DD.', 'error')
                    date_filter = None
            elif start_date and end_date:
                try:
                    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
                    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
                    if start_date_dt > end_date_dt:
                        flash('Start date cannot be after end date.', 'error')
                        start_date, end_date = None, None
                    else:
                        query = text(str(query) + " AND date BETWEEN :start_date AND :end_date")
                        params['start_date'] = start_date_dt
                        params['end_date'] = end_date_dt
                        logger.info(f"Applied date range filter: {start_date} to {end_date}")
                except ValueError:
                    logger.error(f"Invalid date range: start_date={start_date}, end_date={end_date}")
                    flash('Invalid date format for range. Please use YYYY-MM-DD.', 'error')
                    start_date, end_date = None, None
            
            # Count total records for pagination
            count_query = text(str(query).replace("SELECT *", "SELECT COUNT(*) as total"))
            total_records = connection.execute(count_query, params).fetchone()._mapping['total']
            total_pages = math.ceil(total_records / rows_per_page) if rows_per_page > 0 else 1
            
            if page < 1:
                page = 1
            if page > total_pages:
                page = total_pages if total_pages > 0 else 1
            
            query = text(str(query) + " ORDER BY date DESC LIMIT :limit OFFSET :offset")
            params['limit'] = rows_per_page
            params['offset'] = (page - 1) * rows_per_page
            result = connection.execute(query, params).fetchall()
            
            for u in result:
                u_dict = u._mapping
                jainam_records.append({
                    'row_id': u_dict['row_id'],
                    'date': u_dict['date'].strftime("%Y-%m-%d") if u_dict['date'] else '',
                    'user_id': u_dict['user_id'],
                    'alias': u_dict['alias'] or '',
                    'allocation': float(u_dict['allocation'] or 0),
                    'MTM': float(u_dict['MTM'] or 0),
                    'max_loss': float(u_dict['max_loss'] or 0),
                    'algo': str(u_dict['algo'] or ''),
                    'broker': u_dict['broker'] or '',
                    'user_details_url': url_for('jainam.user_details', row_id=u_dict['row_id'])
                })
            logger.info(f"Retrieved {len(jainam_records)} records for page {page}")

            # Fallback query if no records
            if not jainam_records:
                logger.warning(f"No records found for brokers {jainam_brokers} or MEGASERV with filters. Trying fallback query.")
                query = text("SELECT * FROM jainam WHERE (broker IN (:b1, :b2, :b3, :b4, :b5) OR user_id LIKE :u)")
                params = {'b1': 'JAINAM_CTRADE_DL', 'b2': 'SREDJAINAM_CTRADE', 'b3': 'SREDJAINAM_103', 'b4': 'SREDJAINAM2_P', 'b5': 'ACHINTYA', 'u': '%MEGASERV%'}
                if search:
                    query = text(str(query) + " AND (user_id LIKE :s1 OR alias LIKE :s2)")
                    params['s1'] = f"%{search}%"
                    params['s2'] = f"%{search}%"
                if user_id:
                    query = text(str(query) + " AND user_id = :user_id")
                    params['user_id'] = user_id
                if date_filter:
                    query = text(str(query) + " AND date = :date_filter")
                    params['date_filter'] = date_filter
                elif start_date and end_date:
                    query = text(str(query) + " AND date BETWEEN :start_date AND :end_date")
                    params['start_date'] = start_date
                    params['end_date'] = end_date
                count_query = text(str(query).replace("SELECT *", "SELECT COUNT(*) as total"))
                total_records = connection.execute(count_query, params).fetchone()._mapping['total']
                total_pages = math.ceil(total_records / rows_per_page) if rows_per_page > 0 else 1
                query = text(str(query) + " ORDER BY date DESC LIMIT :limit OFFSET :offset")
                params['limit'] = rows_per_page
                params['offset'] = (page - 1) * rows_per_page
                result = connection.execute(query, params).fetchall()
                for u in result:
                    u_dict = u._mapping
                    jainam_records.append({
                        'row_id': u_dict['row_id'],
                        'date': u_dict['date'].strftime("%Y-%m-%d") if u_dict['date'] else '',
                        'user_id': u_dict['user_id'],
                        'alias': u_dict['alias'] or '',
                        'allocation': float(u_dict['allocation'] or 0),
                        'MTM': float(u_dict['MTM'] or 0),
                        'max_loss': float(u_dict['max_loss'] or 0),
                        'algo': str(u_dict['algo'] or ''),
                        'broker': u_dict['broker'] or '',
                        'user_details_url': url_for('jainam.user_details', row_id=u_dict['row_id'])
                    })
                logger.info(f"Fallback query retrieved {len(jainam_records)} records")
            
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({
                    'jainam': jainam_records,
                    'page': page,
                    'total_pages': total_pages,
                    'total_results': total_records,
                    'rows_per_page': rows_per_page
                })
        except Exception as e:
            logger.error(f"Error querying jainam records: {e}")
            flash(f"Error loading data: {str(e)}", "error")
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({'error': str(e)}), 500
        finally:
            if connection:
                connection.close()

        return render_template('jainam/index.html', 
                              jainam=jainam_records, 
                              default_start_date=start_date or default_start_date, 
                              default_end_date=end_date or default_end_date, 
                              date_filter=date_filter,
                              search=search,
                              user_id=user_id,
                              page=page, 
                              total_pages=total_pages, 
                              total_results=total_records,
                              rows_per_page=rows_per_page)

    @jainam_bp.route('/view_table', methods=['GET'])
    @admin_required
    def view_table():
        logger.info(f"Request: {request.method} {request.url}")
        jainam_brokers = ['JAINAM_CTRADE_DL', 'SREDJAINAM_CTRADE', 'SREDJAINAM_103', 'SREDJAINAM2_P', 'ACHINTYA']
        jainam_records = []
        start_date = request.args.get('start_date', '')
        end_date = request.args.get('end_date', '')
        date_filter = request.args.get('date', '')
        search = request.args.get('search', '')
        user_id = request.args.get('user_id', '')
        page = int(request.args.get('page', 1))
        try:
            rows_per_page = int(request.args.get('rows_per_page', 50))
            if rows_per_page not in [50, 100, 500, 1000]:
                rows_per_page = 50
        except ValueError:
            rows_per_page = 50

        default_start_date, default_end_date = get_latest_date_range()
        if not start_date and not end_date and not date_filter:
            date_filter = default_end_date.strftime('%Y-%m-%d') if default_end_date else date.today().strftime('%Y-%m-%d')
        default_start_date = default_start_date.strftime('%Y-%m-%d') if default_start_date else ''
        default_end_date = default_end_date.strftime('%Y-%m-%d') if default_end_date else ''

        connection = None
        try:
            connection = db_engine.connect()
            query = text("SELECT * FROM jainam WHERE (broker IN (:b1, :b2, :b3, :b4, :b5) OR user_id LIKE :u)")
            params = {'b1': 'JAINAM_CTRADE_DL', 'b2': 'SREDJAINAM_CTRADE', 'b3': 'SREDJAINAM_103', 'b4': 'SREDJAINAM2_P', 'b5': 'ACHINTYA', 'u': '%MEGASERV%'}
            
            if search:
                query = text(str(query) + " AND (user_id LIKE :s1 OR alias LIKE :s2)")
                params['s1'] = f"%{search}%"
                params['s2'] = f"%{search}%"
            if user_id:
                query = text(str(query) + " AND user_id = :user_id")
                params['user_id'] = user_id
            if date_filter:
                try:
                    datetime.strptime(date_filter, '%Y-%m-%d')
                    query = text(str(query) + " AND date = :date_filter")
                    params['date_filter'] = date_filter
                except ValueError:
                    logger.error(f"Invalid date filter: {date_filter}")
                    flash('Invalid date format for single date. Please use YYYY-MM-DD.', 'error')
                    date_filter = None
            elif start_date and end_date:
                try:
                    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
                    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
                    if start_date_dt > end_date_dt:
                        flash('Start date cannot be after end date.', 'error')
                        start_date, end_date = None, None
                    else:
                        query = text(str(query) + " AND date BETWEEN :start_date AND :end_date")
                        params['start_date'] = start_date_dt
                        params['end_date'] = end_date_dt
                        logger.info(f"Applied date range filter: {start_date} to {end_date}")
                except ValueError:
                    logger.error(f"Invalid date range: start_date={start_date}, end_date={end_date}")
                    flash('Invalid date format for range. Please use YYYY-MM-DD.', 'error')
                    start_date, end_date = None, None
            
            count_query = text(str(query).replace("SELECT *", "SELECT COUNT(*) as total"))
            total_records = connection.execute(count_query, params).fetchone()._mapping['total']
            logger.info(f"Total Jainam records for brokers {jainam_brokers} or MEGASERV: {total_records}")
            total_pages = math.ceil(total_records / rows_per_page) if rows_per_page > 0 else 1
            
            if page < 1:
                page = 1
            if page > total_pages:
                page = total_pages if total_pages > 0 else 1
            
            query = text(str(query) + " ORDER BY date DESC LIMIT :limit OFFSET :offset")
            params['limit'] = rows_per_page
            params['offset'] = (page - 1) * rows_per_page
            result = connection.execute(query, params).fetchall()
            
            for u in result:
                u_dict = u._mapping
                jainam_records.append({
                    'row_id': u_dict['row_id'],
                    'date': u_dict['date'].strftime("%Y-%m-%d") if u_dict['date'] else '',
                    'user_id': u_dict['user_id'],
                    'alias': u_dict['alias'] or '',
                    'allocation': float(u_dict['allocation'] or 0),
                    'MTM': float(u_dict['MTM'] or 0),
                    'max_loss': float(u_dict['max_loss'] or 0),
                    'algo': str(u_dict['algo'] or ''),
                    'broker': u_dict['broker'] or '',
                    'user_details_url': url_for('jainam.user_details', row_id=u_dict['row_id'])
                })
            logger.info(f"Retrieved {len(jainam_records)} records for page {page}")

            return jsonify({
                'jainam': jainam_records,
                'page': page,
                'total_pages': total_pages,
                'total_results': total_records,
                'rows_per_page': rows_per_page
            })
        except Exception as e:
            logger.error(f"Error querying jainam records: {e}")
            return jsonify({'error': str(e)}), 500
        finally:
            if connection:
                connection.close()

    @jainam_bp.route('/user_details/<int:row_id>', methods=['GET'])
    @admin_required
    def user_details(row_id):
        logger.info(f"Request: {request.method} {request.url}")
        connection = None
        try:
            connection = db_engine.connect()
            result = connection.execute(text("SELECT * FROM jainam WHERE row_id = :row_id"), {'row_id': row_id}).fetchone()
            if not result:
                flash("Record not found.", "error")
                return redirect(url_for('jainam.index'))
            
            jainam_record = dict(result._mapping)
            result = connection.execute(text("SELECT * FROM user_partner_data WHERE user_id = :user_id AND date = :date"), 
                                      {'user_id': jainam_record['user_id'], 'date': jainam_record['date']}).fetchall()
            partners = [dict(row._mapping) for row in result]
            
            aliases = ['PS', 'VT', 'GB', 'RD', 'RM']
            additional_rows = []
            for alias in aliases:
                partner = next((p for p in partners if p['alias'] == alias), None)
                additional_rows.append({
                    'alias': alias,
                    'allocation': float(partner['allocation'] or 0) if partner else 0,
                    'calculated_mtm': float(partner['mtm'] or 0) if partner else 0,
                    'max_loss': float(partner['max_loss'] or 0) if partner else 0
                })
            
            allocation = float(jainam_record['allocation'] or 0)
            mtm_ratio = (float(jainam_record['MTM'] or 0) / allocation) if allocation != 0 else 0
            max_loss_ratio = (float(jainam_record['max_loss'] or 0) / allocation) if allocation != 0 else 0
            
            jainam_data = {
                'row_id': jainam_record['row_id'],
                'date': jainam_record['date'].strftime("%Y-%m-%d") if jainam_record['date'] else '',
                'user_id': jainam_record['user_id'],
                'alias': jainam_record['alias'] or '',
                'allocation': float(jainam_record['allocation'] or 0),
                'MTM': float(jainam_record['MTM'] or 0),
                'max_loss': float(jainam_record['max_loss'] or 0),
                'algo': str(jainam_record['algo'] or ''),
                'broker': jainam_record['broker'] or '',
                'mtm_ratio': mtm_ratio,
                'max_loss_ratio': max_loss_ratio,
                'is_main': True
            }
            
            partner_data = [
                {
                    'row_id': p['row_id'],
                    'date': p['date'].strftime("%Y-%m-%d") if p['date'] else '',
                    'user_id': p['user_id'],
                    'alias': p['alias'] or '',
                    'allocation': float(p['allocation'] or 0),
                    'MTM': float(p['mtm'] or 0),
                    'max_loss': float(p['max_loss'] or 0),
                    'algo': str(p['algo'] or ''),
                    'broker': p['broker'] or '',
                    'is_main': p['is_main']
                } for p in partners if float(p['allocation'] or 0) > 0
            ]
            
            return render_template('jainam/user_form.html', user=jainam_data, partners=partner_data, additional_rows=additional_rows)
        except Exception as e:
            logger.error(f"Error loading user details: {e}")
            flash(f"Error loading user details: {str(e)}", "error")
            return redirect(url_for('jainam.index'))
        finally:
            if connection:
                connection.close()

    @jainam_bp.route('/save_distribution/<int:row_id>', methods=['POST'])
    @admin_required
    def save_distribution(row_id):
        logger.info(f"Request: {request.method} {request.url} with row_id={row_id}")
        connection = None
        try:
            if not request.is_json:
                logger.error("Request content type is not JSON")
                return jsonify({'success': False, 'error': 'Content-Type must be application/json'}), 400
            
            data = request.get_json()
            if not isinstance(data, dict) or 'partners' not in data:
                logger.error(f"Invalid JSON payload: {data}")
                return jsonify({'success': False, 'error': 'Invalid JSON format: Missing "partners" key'}), 400
            
            partners = data.get('partners', [])
            if not isinstance(partners, list):
                logger.error(f"Invalid partners data: {partners}")
                return jsonify({'success': False, 'error': '"partners" must be a list'}), 400
            
            logger.debug(f"Received partners data: {partners}")

            connection = db_engine.connect()
            trans = connection.begin()
            result = connection.execute(text("SELECT * FROM jainam WHERE row_id = :row_id"), {'row_id': row_id}).fetchone()
            if not result:
                logger.error(f"Jainam record not found for row_id={row_id}")
                return jsonify({'success': False, 'error': 'Jainam record not found'}), 404
            
            jainam_record = dict(result._mapping)
            allocation = float(jainam_record['allocation'] or 0)
            mtm_ratio = (float(jainam_record['MTM'] or 0) / allocation) if allocation != 0 else 0
            max_loss_ratio = (float(jainam_record['max_loss'] or 0) / allocation) if allocation != 0 else 0

            connection.execute(text("DELETE FROM user_partner_data WHERE user_id = :user_id AND date = :date AND is_main = :is_main"), 
                              {'user_id': jainam_record['user_id'], 'date': jainam_record['date'], 'is_main': False})
            connection.execute(text("DELETE FROM partner_distributions WHERE user_id = :user_id"), 
                              {'user_id': jainam_record['user_id']})

            valid_aliases = {'PS', 'VT', 'GB', 'RD', 'RM'}
            for row in partners:
                alias = row.get('alias', '').strip()
                if not alias or alias not in valid_aliases:
                    logger.warning(f"Skipping invalid or missing alias: {alias}")
                    continue
                
                try:
                    alloc = float(row.get('allocation', 0)) if row.get('allocation') is not None else 0
                    if alloc < 0:
                        logger.warning(f"Skipping negative allocation for alias {alias}: {alloc}")
                        continue
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid allocation value for alias {alias}: {row.get('allocation')}, error: {e}")
                    continue

                calculated_mtm = alloc * mtm_ratio if alloc else 0
                max_loss = alloc * max_loss_ratio if alloc else 0

                connection.execute(text("""
                    INSERT INTO partner_distributions (user_id, alias, allocation, calculated_mtm, max_loss)
                    VALUES (:user_id, :alias, :allocation, :calculated_mtm, :max_loss)
                """), {
                    'user_id': jainam_record['user_id'],
                    'alias': alias,
                    'allocation': alloc,
                    'calculated_mtm': calculated_mtm,
                    'max_loss': max_loss
                })
                
                connection.execute(text("""
                    INSERT INTO user_partner_data (user_id, alias, allocation, mtm, max_loss, date, broker, algo, is_main)
                    VALUES (:user_id, :alias, :allocation, :mtm, :max_loss, :date, :broker, :algo, :is_main)
                """), {
                    'user_id': jainam_record['user_id'],
                    'alias': alias,
                    'allocation': alloc,
                    'mtm': calculated_mtm,
                    'max_loss': max_loss,
                    'date': jainam_record['date'],
                    'broker': jainam_record['broker'],
                    'algo': jainam_record['algo'],
                    'is_main': False
                })
            
            trans.commit()
            
            result = connection.execute(text("SELECT * FROM user_partner_data WHERE user_id = :user_id AND date = :date AND is_main = :is_main"), 
                                      {'user_id': jainam_record['user_id'], 'date': jainam_record['date'], 'is_main': False}).fetchall()
            updated_partners = [dict(row._mapping) for row in result]
            aliases = ['PS', 'VT', 'GB', 'RD', 'RM']
            updated_additional_rows = []
            for alias in aliases:
                partner = next((p for p in updated_partners if p['alias'] == alias), None)
                updated_additional_rows.append({
                    'alias': alias,
                    'allocation': float(partner['allocation'] or 0) if partner else 0,
                    'calculated_mtm': float(partner['mtm'] or 0) if partner else 0,
                    'max_loss': float(partner['max_loss'] or 0) if partner else 0
                })
            
            updated_partner_data = [
                {
                    'row_id': p['row_id'],
                    'date': p['date'].strftime("%Y-%m-%d") if p['date'] else '',
                    'user_id': p['user_id'],
                    'alias': p['alias'] or '',
                    'allocation': float(p['allocation'] or 0),
                    'MTM': float(p['mtm'] or 0),
                    'max_loss': float(p['max_loss'] or 0),
                    'algo': str(p['algo'] or ''),
                    'broker': p['broker'] or '',
                    'is_main': p['is_main']
                } for p in updated_partners
            ]

            logger.info(f"Distribution saved for user_id={jainam_record['user_id']}, date={jainam_record['date']}")
            return jsonify({
                'success': True,
                'partners': updated_partner_data,
                'additional_rows': updated_additional_rows
            })
        except Exception as e:
            if connection:
                trans.rollback()
            logger.error(f"Error in save_distribution: {e}")
            return jsonify({'success': False, 'error': f'Error: {str(e)}'}), 500
        finally:
            if connection:
                connection.close()



    @jainam_bp.route('/dashboard', methods=['GET'])
    @admin_required
    def dashboard():
        logger.info(f"Request: {request.method} {request.url}")
        connection = None
        try:
            start_date = request.args.get('start_date', '')
            end_date = request.args.get('end_date', '')
            user_id = request.args.get('user_id', '')
            partner = request.args.get('partner', '')
            date_filter = request.args.get('date', '')
            page = int(request.args.get('page', 1))
            try:
                rows_per_page = int(request.args.get('rows_per_page', 50))
                if rows_per_page not in [50, 100, 500, 1000]:
                    rows_per_page = 50
            except ValueError:
                rows_per_page = 50
            default_start_date, default_end_date = get_latest_date_range()
            default_start_date = default_start_date.strftime('%Y-%m-%d') if default_start_date else ''
            default_end_date = default_end_date.strftime('%Y-%m-%d') if default_end_date else ''

            connection = db_engine.connect()
            jainam_brokers = ['JAINAM_CTRADE_DL', 'SREDJAINAM_CTRADE', 'SREDJAINAM_103', 'SREDJAINAM2_P', 'ACHINTYA']
            query = text("SELECT * FROM jainam WHERE (broker IN (:b1, :b2, :b3, :b4, :b5) OR user_id LIKE :u)")
            params = {'b1': 'JAINAM_CTRADE_DL', 'b2': 'SREDJAINAM_CTRADE', 'b3': 'SREDJAINAM_103', 'b4': 'SREDJAINAM2_P', 'b5': 'ACHINTYA', 'u': '%MEGASERV%'}

            if user_id:
                query = text(str(query) + " AND user_id = :user_id")
                params['user_id'] = user_id
            if date_filter:
                try:
                    datetime.strptime(date_filter, '%Y-%m-%d')
                    query = text(str(query) + " AND date = :date_filter")
                    params['date_filter'] = date_filter
                except ValueError:
                    logger.error(f"Invalid date filter: {date_filter}")
                    flash('Invalid date format for single date. Please use YYYY-MM-DD.', 'error')
                    date_filter = None
            if start_date and end_date and not date_filter:
                try:
                    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
                    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
                    if start_date_dt > end_date_dt:
                        flash('Start date cannot be after end date.', 'error')
                        start_date, end_date = None, None
                    else:
                        query = text(str(query) + " AND date BETWEEN :start_date AND :end_date")
                        params['start_date'] = start_date_dt
                        params['end_date'] = end_date_dt
                        logger.info(f"Applied date range filter: {start_date} to {end_date}")
                except ValueError:
                    logger.error(f"Invalid date range: start_date={start_date}, end_date={end_date}")
                    flash('Invalid date format for range. Please use YYYY-MM-DD.', 'error')
                    start_date, end_date = None, None

            count_query = text(str(query).replace("SELECT *", "SELECT COUNT(*) as total"))
            total_records = connection.execute(count_query, params).fetchone()._mapping['total']
            total_pages = math.ceil(total_records / rows_per_page) if rows_per_page > 0 else 1

            if page < 1:
                page = 1
            if page > total_pages:
                page = total_pages if total_pages > 0 else 1

            query = text(str(query) + " ORDER BY date DESC")
            result = connection.execute(query, params).fetchall()
            
            grouped_data = []
            unique_keys = set()
            unique_dates = set()
            for jainam_record in result:
                jainam_record_dict = jainam_record._mapping
                jainam_key = (jainam_record_dict['user_id'], jainam_record_dict['date'].strftime('%Y-%m-%d') if jainam_record_dict['date'] else '')
                if jainam_key not in unique_keys:
                    unique_keys.add(jainam_key)
                    if jainam_record_dict['date']:
                        unique_dates.add(jainam_record_dict['date'].strftime('%Y-%m-%d'))
                    main_data = {
                        'row_id': jainam_record_dict['row_id'],
                        'user_id': jainam_record_dict['user_id'],
                        'alias': jainam_record_dict['alias'] or '',
                        'allocation': float(jainam_record_dict['allocation'] or 0),
                        'mtm': float(jainam_record_dict['MTM'] or 0),
                        'max_loss': float(jainam_record_dict['max_loss'] or 0),
                        'partner_alias': '',
                        'partner_allocation': 0,
                        'partner_mtm': 0,
                        'partner_max_loss': 0,
                        'is_main': True,
                        'date': jainam_record_dict['date'].strftime('%Y-%m-%d') if jainam_record_dict['date'] else '',
                        'broker': jainam_record_dict['broker'] or '',
                        'algo': str(jainam_record_dict['algo'] or ''),
                        'user_details_url': url_for('jainam.user_details', row_id=jainam_record_dict['row_id'])
                    }
                    partner_query = text("SELECT * FROM user_partner_data WHERE user_id = :user_id AND date = :date AND is_main = :is_main AND broker IN (:b1, :b2, :b3, :b4, :b5)")
                    partner_params = {'user_id': jainam_record_dict['user_id'], 'date': jainam_record_dict['date'], 'is_main': False, 
                                    'b1': 'JAINAM_CTRADE_DL', 'b2': 'SREDJAINAM_CTRADE', 'b3': 'SREDJAINAM_103', 'b4': 'SREDJAINAM2_P', 'b5': 'ACHINTYA'}
                    if partner:
                        partner_query = text(str(partner_query) + " AND alias = :partner")
                        partner_params['partner'] = partner
                    partner_query = text(str(partner_query) + " ORDER BY alias")
                    partners_data = [
                        {
                            'row_id': partner._mapping['row_id'],
                            'user_id': partner._mapping['user_id'],
                            'alias': partner._mapping['alias'] or '',
                            'allocation': float(partner._mapping['allocation'] or 0),
                            'mtm': float(partner._mapping['mtm'] or 0),
                            'max_loss': float(partner._mapping['max_loss'] or 0),
                            'partner_alias': partner._mapping['alias'] or '',
                            'partner_allocation': float(partner._mapping['allocation'] or 0),
                            'partner_mtm': float(partner._mapping['mtm'] or 0),
                            'partner_max_loss': float(partner._mapping['max_loss'] or 0),
                            'is_main': False,
                            'date': partner._mapping['date'].strftime('%Y-%m-%d') if partner._mapping['date'] else '',
                            'broker': partner._mapping['broker'] or '',
                            'algo': str(partner._mapping['algo'] or ''),
                            'user_details_url': url_for('jainam.user_details', row_id=partner._mapping['row_id'])
                        } for partner in connection.execute(partner_query, partner_params).fetchall() if float(partner._mapping['allocation'] or 0) > 0
                    ]
                    grouped_data.append({
                        'main': main_data,
                        'partners': partners_data
                    })

            unique_dates = sorted(list(unique_dates), reverse=True)

            start_idx = (page - 1) * rows_per_page
            end_idx = start_idx + rows_per_page
            grouped_data = grouped_data[start_idx:end_idx]

            # Partner stats query with average allocation per user_id
            partner_query = text("""
                SELECT alias, algo, user_id, date, mtm, allocation, max_loss
                FROM user_partner_data 
                WHERE is_main = :is_main AND broker IN (:b1, :b2, :b3, :b4, :b5)
            """)
            partner_params = {'is_main': False, 'b1': 'JAINAM_CTRADE_DL', 'b2': 'SREDJAINAM_CTRADE', 'b3': 'SREDJAINAM_103', 'b4': 'SREDJAINAM2_P', 'b5': 'ACHINTYA'}
            if partner:
                partner_query = text(str(partner_query) + " AND alias = :partner")
                partner_params['partner'] = partner
            if user_id:
                partner_query = text(str(partner_query) + " AND user_id = :user_id")
                partner_params['user_id'] = user_id
            if date_filter:
                try:
                    datetime.strptime(date_filter, '%Y-%m-%d')
                    partner_query = text(str(partner_query) + " AND date = :date_filter")
                    partner_params['date_filter'] = date_filter
                except ValueError:
                    logger.error(f"Invalid date filter for partner stats: {date_filter}")
                    date_filter = None
            if start_date and end_date and not date_filter:
                try:
                    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
                    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
                    if start_date_dt <= end_date_dt:
                        partner_query = text(str(partner_query) + " AND date BETWEEN :start_date AND :end_date")
                        partner_params['start_date'] = start_date_dt
                        partner_params['end_date'] = end_date_dt
                except ValueError:
                    logger.error(f"Invalid date range for partner stats: start_date={start_date}, end_date={end_date}")
                    start_date, end_date = None, None

            # Execute query and convert to DataFrame for processing
            partner_data = connection.execute(partner_query, partner_params).fetchall()
            df = pd.DataFrame([partner._mapping for partner in partner_data])

            partner_stats = {}
            if not df.empty:
                # Group by alias (partner) and user_id to calculate average allocation
                user_avg_allocation = df.groupby(['alias', 'user_id'])['allocation'].mean().reset_index()
                # Sum average allocations per partner
                partner_total_allocation = user_avg_allocation.groupby('alias')['allocation'].sum().to_dict()
                # Sum MTM per partner
                partner_total_mtm = df.groupby('alias')['mtm'].sum().to_dict()

                # Build partner_stats with user-level data
                for alias, group in df.groupby('alias'):
                    partner_stats[alias] = {
                        'users': [
                            {
                                'user_id': row['user_id'],
                                'algo': str(row['algo'] or ''),
                                'mtm': float(row['mtm'] or 0),
                                'allocation': float(row['allocation'] or 0),
                                'max_loss': float(row['max_loss'] or 0),
                                'date': row['date'].strftime('%Y-%m-%d') if row['date'] else ''
                            } for _, row in group.iterrows() if float(row['allocation'] or 0) > 0
                        ],
                        'total_allocation': partner_total_allocation.get(alias, 0),
                        'total_mtm': partner_total_mtm.get(alias, 0)
                    }

            # Algo-wise stats query (unchanged)
            algo_query = text("""
                SELECT algo, user_id, date, SUM(mtm) as total_mtm, SUM(allocation) as total_allocation, SUM(max_loss) as total_max_loss
                FROM user_partner_data 
                WHERE is_main = :is_main AND broker IN (:b1, :b2, :b3, :b4, :b5)
            """)
            algo_params = {'is_main': False, 'b1': 'JAINAM_CTRADE_DL', 'b2': 'SREDJAINAM_CTRADE', 'b3': 'SREDJAINAM_103', 'b4': 'SREDJAINAM2_P', 'b5': 'ACHINTYA'}
            if partner:
                algo_query = text(str(algo_query) + " AND alias = :partner")
                algo_params['partner'] = partner
            if user_id:
                algo_query = text(str(algo_query) + " AND user_id = :user_id")
                algo_params['user_id'] = user_id
            if date_filter:
                try:
                    datetime.strptime(date_filter, '%Y-%m-%d')
                    algo_query = text(str(algo_query) + " AND date = :date_filter")
                    algo_params['date_filter'] = date_filter
                except ValueError:
                    logger.error(f"Invalid date filter for algo stats: {date_filter}")
                    date_filter = None
            if start_date and end_date and not date_filter:
                try:
                    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
                    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
                    if start_date_dt <= end_date_dt:
                        algo_query = text(str(algo_query) + " AND date BETWEEN :start_date AND :end_date")
                        algo_params['start_date'] = start_date_dt
                        algo_params['end_date'] = end_date_dt
                except ValueError:
                    logger.error(f"Invalid date range for algo stats: start_date={start_date}, end_date={end_date}")
                    start_date, end_date = None, None
            algo_query = text(str(algo_query) + " GROUP BY algo, user_id, date ORDER BY date DESC")
            algo_wise_data = {}
            for algo in connection.execute(algo_query, algo_params).fetchall():
                algo_dict = algo._mapping
                algo_name = str(algo_dict['algo'] or '')
                if algo_name not in algo_wise_data:
                    algo_wise_data[algo_name] = {'users': []}
                if float(algo_dict['total_allocation'] or 0) > 0:
                    algo_wise_data[algo_name]['users'].append({
                        'user_id': algo_dict['user_id'],
                        'algo': str(algo_dict['algo'] or ''),
                        'mtm': float(algo_dict['total_mtm'] or 0),
                        'allocation': float(algo_dict['total_allocation'] or 0),
                        'max_loss': float(algo_dict['total_max_loss'] or 0),
                        'date': algo_dict['date'].strftime('%Y-%m-%d') if algo_dict['date'] else ''
                    })

            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({
                    'grouped_data': grouped_data,
                    'partner_stats': partner_stats,
                    'algo_wise_data': algo_wise_data,
                    'page': page,
                    'total_pages': total_pages,
                    'total_results': total_records,
                    'rows_per_page': rows_per_page,
                    'unique_dates': unique_dates
                })

            return render_template('jainam/dashboard.html', 
                                grouped_data=grouped_data, 
                                partner_stats=partner_stats,
                                algo_wise_data=algo_wise_data,
                                default_start_date=start_date or default_start_date, 
                                default_end_date=end_date or default_end_date, 
                                user_id=user_id,
                                partner=partner,
                                date_filter=date_filter,
                                page=page, 
                                total_pages=total_pages, 
                                total_results=total_records,
                                rows_per_page=rows_per_page,
                                unique_dates=unique_dates)
        except Exception as e:
            logger.error(f"Error loading dashboard: {e}")
            flash(f"Error loading dashboard: {str(e)}", 'error')
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({'error': str(e)}), 500
            return redirect(url_for('jainam.index', start_date=start_date or default_start_date, end_date=end_date or default_end_date, rows_per_page=rows_per_page, page=1))
        finally:
            if connection:
                connection.close()
                
    @jainam_bp.route('/export_partner_stats', methods=['GET'])
    @admin_required
    def export_partner_stats():
        logger.info(f"Request: {request.method} {request.url}")
        connection = None
        try:
            start_date = request.args.get('start_date', '')
            end_date = request.args.get('end_date', '')
            user_id = request.args.get('user_id', '')
            if start_date and end_date:
                try:
                    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                    if start_date > end_date:
                        return Response("Start date cannot be after end date", status=400)
                except ValueError:
                    logger.error(f"Invalid date range: start_date={start_date}, end_date={end_date}")
                    return Response("Invalid date format", status=400)

            connection = db_engine.connect()
            query = text("SELECT * FROM user_partner_data WHERE is_main = :is_main")
            params = {'is_main': False}
            if user_id:
                query = text(str(query) + " AND user_id = :user_id")
                params['user_id'] = user_id
            if start_date and end_date:
                query = text(str(query) + " AND date BETWEEN :start_date AND :end_date")
                params['start_date'] = start_date
                params['end_date'] = end_date
            query = text(str(query) + " ORDER BY date DESC")
            result = connection.execute(query, params).fetchall()

            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(['Partner', 'Date', 'User ID', 'Algo', 'MTM', 'Allocation', 'Max Loss'])

            for partner in result:
                partner_dict = partner._mapping
                if float(partner_dict['allocation'] or 0) > 0:
                    writer.writerow([
                        partner_dict['alias'] or '',
                        partner_dict['date'].strftime('%Y-%m-%d') if partner_dict['date'] else '',
                        partner_dict['user_id'],
                        str(partner_dict['algo'] or ''),
                        float(partner_dict['mtm'] or 0),
                        float(partner_dict['allocation'] or 0),
                        float(partner_dict['max_loss'] or 0)
                    ])

            output.seek(0)
            return Response(
                output,
                mimetype='text/csv',
                headers={'Content-Disposition': 'attachment;filename=partner_stats.csv'}
            )
        except Exception as e:
            logger.error(f"Error exporting partner stats: {e}")
            return Response(f"Error exporting partner stats: {str(e)}", status=500)
        finally:
            if connection:
                connection.close()

    @jainam_bp.route('/export_user_partner_details', methods=['GET'])
    @admin_required
    def export_user_partner_details():
        logger.info(f"Request: {request.method} {request.url}")
        connection = None
        try:
            start_date = request.args.get('start_date', '')
            end_date = request.args.get('end_date', '')
            user_id = request.args.get('user_id', '')
            if start_date and end_date:
                try:
                    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                    if start_date > end_date:
                        return Response("Start date cannot be after end date", status=400)
                except ValueError:
                    logger.error(f"Invalid date range: start_date={start_date}, end_date={end_date}")
                    return Response("Invalid date format", status=400)

            connection = db_engine.connect()
            query = text("SELECT * FROM jainam WHERE (broker IN (:b1, :b2, :b3, :b4, :b5) OR user_id LIKE :u)")
            params = {'b1': 'JAINAM_CTRADE_DL', 'b2': 'SREDJAINAM_CTRADE', 'b3': 'SREDJAINAM_103', 'b4': 'SREDJAINAM2_P', 'b5': 'ACHINTYA', 'u': '%MEGASERV%'}
            if user_id:
                query = text(str(query) + " AND user_id = :user_id")
                params['user_id'] = user_id
            if start_date and end_date:
                query = text(str(query) + " AND date BETWEEN :start_date AND :end_date")
                params['start_date'] = start_date
                params['end_date'] = end_date
            query = text(str(query) + " ORDER BY date DESC")
            result = connection.execute(query, params).fetchall()

            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(['User ID', 'Alias', 'Partner', 'Allocation', 'MTM', 'Max Loss', 'Algo', 'Broker', 'Date'])

            unique_keys = set()
            for jainam_record in result:
                jainam_record_dict = jainam_record._mapping
                jainam_key = (jainam_record_dict['user_id'], jainam_record_dict['date'].strftime('%Y-%m-%d') if jainam_record_dict['date'] else '')
                if jainam_key not in unique_keys:
                    unique_keys.add(jainam_key)
                    writer.writerow([
                        jainam_record_dict['user_id'],
                        jainam_record_dict['alias'] or '',
                        '',
                        float(jainam_record_dict['allocation'] or 0),
                        float(jainam_record_dict['MTM'] or 0),
                        float(jainam_record_dict['max_loss'] or 0),
                        str(jainam_record_dict['algo'] or ''),
                        jainam_record_dict['broker'] or '',
                        jainam_record_dict['date'].strftime('%Y-%m-%d') if jainam_record_dict['date'] else ''
                    ])
                    partner_result = connection.execute(text("SELECT * FROM user_partner_data WHERE user_id = :user_id AND date = :date AND is_main = :is_main AND allocation > 0 ORDER BY alias"), 
                                                      {'user_id': jainam_record_dict['user_id'], 'date': jainam_record_dict['date'], 'is_main': False}).fetchall()
                    for partner in partner_result:
                        partner_dict = partner._mapping
                        writer.writerow([
                            partner_dict['user_id'],
                            partner_dict['alias'] or '',
                            partner_dict['alias'] or '',
                            float(partner_dict['allocation'] or 0),
                            float(partner_dict['mtm'] or 0),
                            float(partner_dict['max_loss'] or 0),
                            str(partner_dict['algo'] or ''),
                            partner_dict['broker'] or '',
                            partner_dict['date'].strftime('%Y-%m-%d') if partner_dict['date'] else ''
                        ])

            output.seek(0)
            return Response(
                output,
                mimetype='text/csv',
                headers={'Content-Disposition': 'attachment;filename=user_partner_details.csv'}
            )
        except Exception as e:
            logger.error(f"Error exporting user and partner details: {e}")
            return Response(f"Error exporting user and partner details: {str(e)}", status=500)
        finally:
            if connection:
                connection.close()

    # Constants
    VALID_BROKERS = {
        'JAINAM_CTRADE_DL',
        'SREDJAINAM_CTRADE',
        'SREDJAINAM_103',
        'SREDJAINAM2_P',
        'ACHINTYA'
    }
    USER_ID_FILTER_KEYWORD = 'MEGASERV'
    DEFAULT_PARTNERS = ['PS', 'VT', 'GB', 'RD', 'RM']

    @jainam_bp.route('/partner_details', methods=['GET'])
    @admin_required
    def partner_details():
        logger.info(f"Request: {request.method} {request.url}")
        partner_id = request.args.get('id', '')
        start_date = request.args.get('start_date', '')
        end_date = request.args.get('end_date', '')
        date_filter = request.args.get('date', '')
        page = int(request.args.get('page', 1))
        try:
            rows_per_page = int(request.args.get('rows_per_page', 50))
            if rows_per_page not in [50, 100, 500, 1000]:
                rows_per_page = 50
        except ValueError:
            rows_per_page = 50

        default_start_date, default_end_date = get_latest_date_range()
        if not start_date and not end_date and not date_filter:
            date_filter = default_end_date.strftime('%Y-%m-%d') if default_end_date else date.today().strftime('%Y-%m-%d')
        default_start_date = default_start_date.strftime('%Y-%m-%d') if default_start_date else ''
        default_end_date = default_end_date.strftime('%Y-%m-%d') if default_end_date else ''

        if not partner_id:
            logger.error("Partner ID is required.")
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({'error': 'Partner ID is required.'}), 400
            flash("Partner ID is required.", "error")
            return redirect(url_for('jainam.dashboard'))

        connection = None
        try:
            connection = db_engine.connect()
            
            # Fetch unique algos for the partner
            algo_query = text("SELECT DISTINCT algo FROM user_partner_data WHERE alias = :partner_id AND algo IS NOT NULL AND (broker IN :brokers OR user_id LIKE :user_id)")
            params = {'partner_id': partner_id, 'brokers': tuple(VALID_BROKERS), 'user_id': f'%{USER_ID_FILTER_KEYWORD}%'}
            if start_date and end_date:
                try:
                    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
                    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
                    if start_date_dt > end_date_dt:
                        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                            return jsonify({'error': 'Start date cannot be after end date.'}), 400
                        flash('Start date cannot be after end date.', 'error')
                        start_date, end_date = None, None
                    else:
                        algo_query = text(str(algo_query) + " AND date BETWEEN :start_date AND :end_date")
                        params['start_date'] = start_date_dt
                        params['end_date'] = end_date_dt
                except ValueError:
                    logger.error(f"Invalid date range: start_date={start_date}, end_date={end_date}")
                    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                        return jsonify({'error': 'Invalid date format for range. Please use YYYY-MM-DD.'}), 400
                    flash('Invalid date format for range. Please use YYYY-MM-DD.', 'error')
                    start_date, end_date = None, None
            elif date_filter:
                try:
                    datetime.strptime(date_filter, '%Y-%m-%d')
                    algo_query = text(str(algo_query) + " AND date = :date_filter")
                    params['date_filter'] = date_filter
                except ValueError:
                    logger.error(f"Invalid date filter: {date_filter}")
                    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                        return jsonify({'error': 'Invalid date format for single date. Please use YYYY-MM-DD.'}), 400
                    flash('Invalid date format for single date. Please use YYYY-MM-DD.', 'error')
                    date_filter = None
            algo_result = connection.execute(algo_query, params).fetchall()
            unique_algos = [row._mapping['algo'] for row in algo_result if row._mapping['algo']]

            # Fetch unique dates for the partner
            date_query = text("SELECT DISTINCT date FROM user_partner_data WHERE alias = :partner_id AND date IS NOT NULL AND (broker IN :brokers OR user_id LIKE :user_id)")
            params = {'partner_id': partner_id, 'brokers': tuple(VALID_BROKERS), 'user_id': f'%{USER_ID_FILTER_KEYWORD}%'}
            if start_date and end_date:
                try:
                    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
                    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
                    if start_date_dt > end_date_dt:
                        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                            return jsonify({'error': 'Start date cannot be after end date.'}), 400
                        flash('Start date cannot be after end date.', 'error')
                        start_date, end_date = None, None
                    else:
                        date_query = text(str(date_query) + " AND date BETWEEN :start_date AND :end_date")
                        params['start_date'] = start_date_dt
                        params['end_date'] = end_date_dt
                except ValueError:
                    logger.error(f"Invalid date range: start_date={start_date}, end_date={end_date}")
                    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                        return jsonify({'error': 'Invalid date format for range. Please use YYYY-MM-DD.'}), 400
                    flash('Invalid date format for range. Please use YYYY-MM-DD.', 'error')
                    start_date, end_date = None, None
            elif date_filter:
                try:
                    datetime.strptime(date_filter, '%Y-%m-%d')
                    date_query = text(str(date_query) + " AND date = :date_filter")
                    params['date_filter'] = date_filter
                except ValueError:
                    logger.error(f"Invalid date filter: {date_filter}")
                    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                        return jsonify({'error': 'Invalid date format for single date. Please use YYYY-MM-DD.'}), 400
                    flash('Invalid date format for single date. Please use YYYY-MM-DD.', 'error')
                    date_filter = None
            date_result = connection.execute(date_query, params).fetchall()
            unique_dates = sorted([row._mapping['date'].strftime('%Y-%m-%d') for row in date_result], reverse=True)

            # Fetch partner data grouped by algo
            partner_query = text("""
                SELECT algo, user_id, date, allocation, mtm, max_loss, broker
                FROM user_partner_data
                WHERE alias = :partner_id AND is_main = :is_main AND (broker IN :brokers OR user_id LIKE :user_id)
            """)
            params = {'partner_id': partner_id, 'is_main': False, 'brokers': tuple(VALID_BROKERS), 'user_id': f'%{USER_ID_FILTER_KEYWORD}%'}
            if date_filter:
                try:
                    datetime.strptime(date_filter, '%Y-%m-%d')
                    partner_query = text(str(partner_query) + " AND date = :date_filter")
                    params['date_filter'] = date_filter
                except ValueError:
                    logger.error(f"Invalid date filter: {date_filter}")
                    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                        return jsonify({'error': 'Invalid date format for single date. Please use YYYY-MM-DD.'}), 400
                    flash('Invalid date format for single date. Please use YYYY-MM-DD.', 'error')
                    date_filter = None
            elif start_date and end_date:
                try:
                    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
                    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
                    if start_date_dt > end_date_dt:
                        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                            return jsonify({'error': 'Start date cannot be after end date.'}), 400
                        flash('Start date cannot be after end date.', 'error')
                        start_date, end_date = None, None
                    else:
                        partner_query = text(str(partner_query) + " AND date BETWEEN :start_date AND :end_date")
                        params['start_date'] = start_date_dt
                        params['end_date'] = end_date_dt
                        logger.info(f"Applied date range filter: {start_date} to {end_date}")
                except ValueError:
                    logger.error(f"Invalid date range: start_date={start_date}, end_date={end_date}")
                    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                        return jsonify({'error': 'Invalid date format for range. Please use YYYY-MM-DD.'}), 400
                    flash('Invalid date format for range. Please use YYYY-MM-DD.', 'error')
                    start_date, end_date = None, None
            
            # Count total records for pagination
            count_query = text(str(partner_query).replace("SELECT algo, user_id, date, allocation, mtm, max_loss, broker", "SELECT COUNT(*) as total"))
            total_records = connection.execute(count_query, params).fetchone()._mapping['total']
            total_pages = math.ceil(total_records / rows_per_page) if rows_per_page > 0 else 1
            
            if page < 1:
                page = 1
            if page > total_pages:
                page = total_pages if total_pages > 0 else 1
            
            partner_query = text(str(partner_query) + " ORDER BY algo, date DESC LIMIT :limit OFFSET :offset")
            params['limit'] = rows_per_page
            params['offset'] = (page - 1) * rows_per_page
            partner_result = connection.execute(partner_query, params).fetchall()

            # Organize data by algo
            algo_stats = {}
            for row in partner_result:
                row_dict = row._mapping
                algo = row_dict['algo'] or 'Unknown'
                if algo not in algo_stats:
                    algo_stats[algo] = {
                        'users': [],
                        'total_allocation': 0,
                        'total_mtm': 0,
                        'total_max_loss': 0
                    }
                algo_stats[algo]['users'].append({
                    'user_id': row_dict['user_id'],
                    'date': row_dict['date'].strftime('%Y-%m-%d') if row_dict['date'] else '',
                    'allocation': float(row_dict['allocation'] or 0),
                    'mtm': float(row_dict['mtm'] or 0),
                    'max_loss': float(row_dict['max_loss'] or 0),
                    'broker': row_dict['broker'] or '',
                    'daily_data': [{
                        'date': row_dict['date'].strftime('%Y-%m-%d') if row_dict['date'] else '',
                        'allocation': float(row_dict['allocation'] or 0),
                        'mtm': float(row_dict['mtm'] or 0),
                        'max_loss': float(row_dict['max_loss'] or 0)
                    }]
                })
                algo_stats[algo]['total_allocation'] += float(row_dict['allocation'] or 0)
                algo_stats[algo]['total_mtm'] += float(row_dict['mtm'] or 0)
                algo_stats[algo]['total_max_loss'] += float(row_dict['max_loss'] or 0)

            logger.info(f"Retrieved {len(algo_stats)} algorithms for partner {partner_id}")

            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({
                    'partner_id': partner_id,
                    'algo_stats': algo_stats,
                    'unique_algos': unique_algos,
                    'unique_dates': unique_dates,
                    'page': page,
                    'total_pages': total_pages,
                    'rows_per_page': rows_per_page,
                    'default_start_date': default_start_date,
                    'default_end_date': default_end_date
                })

            return render_template('jainam/partner_details.html',
                                partner_id=partner_id,
                                algo_stats=algo_stats,
                                unique_algos=unique_algos,
                                unique_dates=unique_dates,
                                date_filter=date_filter,
                                default_start_date=default_start_date,
                                default_end_date=default_end_date,
                                page=page,
                                total_pages=total_pages,
                                rows_per_page=rows_per_page)
        except Exception as e:
            logger.error(f"Error querying partner details: {e}")
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({
                    'error': str(e),
                    'partner_id': partner_id,
                    'algo_stats': {},
                    'unique_algos': [],
                    'unique_dates': [],
                    'page': page,
                    'total_pages': 1,
                    'rows_per_page': rows_per_page,
                    'default_start_date': default_start_date,
                    'default_end_date': default_end_date
                }), 500
            flash(f"Error loading partner details: {str(e)}", "error")
            return redirect(url_for('jainam.dashboard'))
        finally:
            if connection:
                connection.close()

    @jainam_bp.route('/export_csv', methods=['GET'])
    @admin_required
    def export_csv():
        logger.info(f"Request: {request.method} {request.url}")
        jainam_brokers = ['JAINAM_CTRADE_DL', 'SREDJAINAM_CTRADE', 'SREDJAINAM_103', 'SREDJAINAM2_P', 'ACHINTYA']
        start_date = request.args.get('start_date', '')
        end_date = request.args.get('end_date', '')
        date_filter = request.args.get('date', '')
        search = request.args.get('search', '')
        user_id = request.args.get('user_id', '')
        page = int(request.args.get('page', 1))
        try:
            rows_per_page = int(request.args.get('rows_per_page', 50))
            if rows_per_page not in [50, 100, 500, 1000]:
                rows_per_page = 50
        except ValueError:
            rows_per_page = 50

        connection = None
        try:
            connection = db_engine.connect()
            query = text("SELECT * FROM jainam WHERE (broker IN (:b1, :b2, :b3, :b4, :b5) OR user_id LIKE :u)")
            params = {'b1': 'JAINAM_CTRADE_DL', 'b2': 'SREDJAINAM_CTRADE', 'b3': 'SREDJAINAM_103', 'b4': 'SREDJAINAM2_P', 'b5': 'ACHINTYA', 'u': '%MEGASERV%'}
            
            if search:
                query = text(str(query) + " AND (user_id LIKE :s1 OR alias LIKE :s2)")
                params['s1'] = f"%{search}%"
                params['s2'] = f"%{search}%"
            if user_id:
                query = text(str(query) + " AND user_id = :user_id")
                params['user_id'] = user_id
            if date_filter:
                try:
                    datetime.strptime(date_filter, '%Y-%m-%d')
                    query = text(str(query) + " AND date = :date_filter")
                    params['date_filter'] = date_filter
                except ValueError:
                    logger.error(f"Invalid date filter: {date_filter}")
                    return Response("Invalid date format", status=400)
            elif start_date and end_date:
                try:
                    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                    if start_date > end_date:
                        return Response("Start date cannot be after end date", status=400)
                    query = text(str(query) + " AND date BETWEEN :start_date AND :end_date")
                    params['start_date'] = start_date
                    params['end_date'] = end_date
                except ValueError:
                    logger.error(f"Invalid date range: start_date={start_date}, end_date={end_date}")
                    return Response("Invalid date format", status=400)

            # Apply pagination for export
            query = text(str(query) + " ORDER BY date DESC LIMIT :limit OFFSET :offset")
            params['limit'] = rows_per_page
            params['offset'] = (page - 1) * rows_per_page
            
            result = connection.execute(query, params).fetchall()

            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(['Date', 'User ID', 'Alias', 'Allocation', 'MTM', 'Max Loss', 'Algo', 'Broker'])

            for jainam_record in result:
                jainam_record_dict = jainam_record._mapping
                writer.writerow([
                    jainam_record_dict['date'].strftime("%Y-%m-%d") if jainam_record_dict['date'] else '',
                    jainam_record_dict['user_id'],
                    jainam_record_dict['alias'] or '',
                    float(jainam_record_dict['allocation'] or 0),
                    float(jainam_record_dict['MTM'] or 0),
                    float(jainam_record_dict['max_loss'] or 0),
                    str(jainam_record_dict['algo'] or ''),
                    jainam_record_dict['broker'] or ''
                ])

            output.seek(0)
            return Response(
                output,
                mimetype='text/csv',
                headers={'Content-Disposition': 'attachment;filename=jainam_filtered.csv'}
            )
        except Exception as e:
            logger.error(f"Error exporting CSV: {e}")
            return Response(f"Error exporting CSV: {str(e)}", status=500)
        finally:
            if connection:
                connection.close()

    # Initialize database tables
    def init_app(app):
        with app.app_context():
            logger.info("Creating database tables for jainam blueprint")
            try:
                check_and_update_schema()
                Auth.init_db()
            except Exception as e:
                logger.error(f"Failed to initialize database: {str(e)}")
                raise

# Log blueprint registration
logger.info("Jainam blueprint defined")