import logging
import re
from flask import session
from passlib.hash import bcrypt
from sqlalchemy import text
from utils import get_db_connection

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Auth:
    """Handle user authentication and role management"""
    
    @staticmethod
    def init_db():
        """Initialize MySQL database with auth table if it doesn't exist"""
        engine = get_db_connection()
        if not engine:
            logger.error("Failed to initialize database: No connection")
            raise Exception("Database connection failed")
        
        try:
            with engine.connect() as connection:
                connection.execute(text('''
                    CREATE TABLE IF NOT EXISTS auth (
                        email VARCHAR(255) PRIMARY KEY,
                        password VARCHAR(255) NOT NULL,
                        role VARCHAR(50) NOT NULL,
                        code VARCHAR(50) NOT NULL
                    )
                '''))
                result = connection.execute(text("SELECT COUNT(*) FROM auth"))
                count = result.fetchone()[0]
                logger.debug(f"Auth table user count: {count}")
                if count == 0:
                    default_users = [
                        ("avinash@megaserve.tech", bcrypt.hash("admin123"), "admin", "2004"),
                        ("rajat@megaserve.tech", bcrypt.hash("user123"), "admin", "2005"),
                        ("yash@megaserve.tech", bcrypt.hash("user123"), "user", "2006"),
                        ("vinod@megaserve.tech", bcrypt.hash("user123"), "user", "2007"),
                        ("bansi@megaserve.tech", bcrypt.hash("user123"), "user", "2008"),
                        ("admin1@megaserve.tech", bcrypt.hash("admin123"), "admin", "2009"),  # Added
                        ("user1@megaserve.tech", bcrypt.hash("user123"), "user", "2010")    # Added
                    ]
                    connection.execute(
                        text("INSERT INTO auth (email, password, role, code) VALUES (:email, :password, :role, :code)"),
                        [{"email": email, "password": password, "role": role, "code": code} for email, password, role, code in default_users]
                    )
                    connection.commit()
                    logger.info("Initialized auth table with 7 default users")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise Exception(f"Failed to initialize database: {e}")
        finally:
            if 'connection' in locals():
                connection.close()

    @staticmethod
    def is_valid_email(email):
        """Validate email format and domain"""
        if not email:
            logger.warning("Empty email provided for validation")
            return False
        pattern = r'^[a-zA-Z0-9._%+-]+@megaserve\.tech$'
        valid = bool(re.match(pattern, email))
        logger.debug(f"Email validation: {email} -> {valid}")
        return valid

    @staticmethod
    def user_exists(email):
        """Check if a user exists in the auth table"""
        engine = get_db_connection()
        if not engine:
            logger.error("User existence check failed: No database connection")
            return False
        
        try:
            with engine.connect() as connection:
                email = email.strip().lower() if email else ""
                logger.debug(f"Checking existence of user: {email}")
                result = connection.execute(
                    text("SELECT email FROM auth WHERE email = :email"),
                    {"email": email}
                ).fetchone()
                exists = bool(result)
                logger.debug(f"User {email} exists: {exists}")
                return exists
        except Exception as e:
            logger.error(f"Error checking user existence for {email}: {e}")
            return False
        finally:
            if 'connection' in locals():
                connection.close()

    @staticmethod
    def authenticate(email, password):
        """Authenticate user with hashed password"""
        engine = get_db_connection()
        if not engine:
            logger.error("Authentication failed: No database connection")
            return None
        
        try:
            with engine.connect() as connection:
                email = email.strip().lower() if email else ""
                password = password.strip() if password else ""
                logger.debug(f"Attempting authentication for email: {email}")
                
                result = connection.execute(
                    text("SELECT password, role FROM auth WHERE email = :email"),
                    {"email": email}
                ).fetchone()
                
                if result:
                    if bcrypt.verify(password, result[0]):
                        logger.info(f"Authentication successful for {email}")
                        return {
                            "email": email,
                            "role": result[1],
                            "authenticated": True
                        }
                    logger.warning(f"Authentication failed for {email}: incorrect password")
                else:
                    logger.warning(f"Authentication failed for {email}: user not found")
                return None
        except Exception as e:
            logger.error(f"Database error during authentication for {email}: {e}")
            return None
        finally:
            if 'connection' in locals():
                connection.close()

    @staticmethod
    def verify_code(email, code):
        """Verify security code for password reset"""
        engine = get_db_connection()
        if not engine:
            logger.error("Code verification failed: No database connection")
            return False
        
        try:
            with engine.connect() as connection:
                email = email.strip().lower() if email else ""
                logger.debug(f"Verifying code for email: {email}, code: {code}")
                result = connection.execute(
                    text("SELECT code FROM auth WHERE email = :email"),
                    {"email": email}
                ).fetchone()
                if result:
                    valid = result[0] == code
                    logger.debug(f"Code verification for {email}: {valid}")
                    return valid
                logger.warning(f"Code verification failed: {email} not found")
                return False
        except Exception as e:
            logger.error(f"Database error during code verification for {email}: {e}")
            return False
        finally:
            if 'connection' in locals():
                connection.close()

    @staticmethod
    def reset_password(email, new_password):
        """Update user password in database"""
        engine = get_db_connection()
        if not engine:
            logger.error("Password reset failed: No database connection")
            return False
        
        try:
            with engine.connect() as connection:
                email = email.strip().lower() if email else ""
                logger.debug(f"Attempting password reset for email: {email}")
                result = connection.execute(
                    text("SELECT email FROM auth WHERE email = :email"),
                    {"email": email}
                ).fetchone()
                if result:
                    hashed_password = bcrypt.hash(new_password)
                    connection.execute(
                        text("UPDATE auth SET password = :password WHERE email = :email"),
                        {"password": hashed_password, "email": email}
                    )
                    connection.commit()
                    logger.info(f"Successfully reset password for {email}")
                    return True
                logger.warning(f"Password reset failed: {email} not found")
                return False
        except Exception as e:
            logger.error(f"Error resetting password for {email}: {e}")
            return False
        finally:
            if 'connection' in locals():
                connection.close()

    @staticmethod
    def add_user(email, password, code, role="user"):
        """Add new user to database, restricting role to admin or user"""
        engine = get_db_connection()
        if not engine:
            logger.error("User addition failed: No database connection")
            return False
        
        try:
            with engine.connect() as connection:
                email = email.strip().lower() if email else ""
                logger.debug(f"Attempting to add user: {email}, role: {role}, code: {code}")
                if not Auth.is_valid_email(email):
                    logger.warning(f"Invalid email format: {email}")
                    return False
                if role not in ['admin', 'user']:
                    logger.warning(f"Invalid role: {role}. Only 'admin' or 'user' allowed")
                    return False
                result = connection.execute(
                    text("SELECT email FROM auth WHERE email = :email"),
                    {"email": email}
                ).fetchone()
                if result:
                    logger.warning(f"User {email} already exists")
                    return False
                hashed_password = bcrypt.hash(password)
                connection.execute(
                    text("INSERT INTO auth (email, password, role, code) VALUES (:email, :password, :role, :code)"),
                    {"email": email, "password": hashed_password, "role": role, "code": code}
                )
                connection.commit()
                logger.info(f"Added new user: {email} with role {role}")
                return True
        except Exception as e:
            logger.error(f"Error adding user {email}: {e}")
            return False
        finally:
            if 'connection' in locals():
                connection.close()

    @staticmethod
    def get_user_role(email):
        """Get user role"""
        engine = get_db_connection()
        if not engine:
            logger.error("Role retrieval failed: No database connection")
            return None
        
        try:
            with engine.connect() as connection:
                email = email.strip().lower() if email else ""
                logger.debug(f"Retrieving role for email: {email}")
                result = connection.execute(
                    text("SELECT role FROM auth WHERE email = :email"),
                    {"email": email}
                ).fetchone()
                role = result[0] if result else None
                logger.debug(f"Role for {email}: {role}")
                return role
        except Exception as e:
            logger.error(f"Error retrieving role for {email}: {e}")
            return None
        finally:
            if 'connection' in locals():
                connection.close()

    @staticmethod
    def get_all_users():
        """Retrieve all users from the auth table"""
        engine = get_db_connection()
        if not engine:
            logger.error("Failed to retrieve users: No database connection")
            return []
        
        try:
            with engine.connect() as connection:
                result = connection.execute(
                    text("SELECT email, role, code FROM auth")
                ).fetchall()
                users = [{"email": row[0], "role": row[1], "code": row[2]} for row in result]
                logger.info(f"Retrieved {len(users)} users from auth table: {[user['email'] for user in users]}")
                return users
        except Exception as e:
            logger.error(f"Error retrieving users: {e}")
            return []
        finally:
            if 'connection' in locals():
                connection.close()

    @staticmethod
    def delete_user(email):
        """Delete a user from the auth table"""
        engine = get_db_connection()
        if not engine:
            logger.error("User deletion failed: No database connection")
            return False
        
        try:
            with engine.connect() as connection:
                email = email.strip().lower() if email else ""
                logger.debug(f"Attempting to delete user: {email}")
                result = connection.execute(
                    text("SELECT email FROM auth WHERE email = :email"),
                    {"email": email}
                ).fetchone()
                if not result:
                    logger.warning(f"User {email} not found for deletion")
                    return False
                connection.execute(
                    text("DELETE FROM auth WHERE email = :email"),
                    {"email": email}
                )
                connection.commit()
                logger.info(f"Deleted user: {email}")
                return True
        except Exception as e:
            logger.error(f"Error deleting user {email}: {e}")
            return False
        finally:
            if 'connection' in locals():
                connection.close()

    @staticmethod
    def update_role(email, new_role):
        """Update user role in the auth table"""
        engine = get_db_connection()
        if not engine:
            logger.error("Role update failed: No database connection")
            return False
        
        try:
            with engine.connect() as connection:
                email = email.strip().lower() if email else ""
                logger.debug(f"Attempting to update role for {email} to {new_role}")
                if new_role not in ['admin', 'user']:
                    logger.warning(f"Invalid role: {new_role}. Only 'admin' or 'user' allowed")
                    return False
                result = connection.execute(
                    text("SELECT email FROM auth WHERE email = :email"),
                    {"email": email}
                ).fetchone()
                if not result:
                    logger.warning(f"User {email} not found for role update")
                    return False
                connection.execute(
                    text("UPDATE auth SET role = :role WHERE email = :email"),
                    {"role": new_role, "email": email}
                )
                connection.commit()
                logger.info(f"Updated role for {email} to {new_role}")
                return True
        except Exception as e:
            logger.error(f"Error updating role for {email}: {e}")
            return False
        finally:
            if 'connection' in locals():
                connection.close()

    @staticmethod
    def update_password_and_code(email, new_password, new_code):
        """Update user password and code in the auth table"""
        engine = get_db_connection()
        if not engine:
            logger.error("Password and code update failed: No database connection")
            return False
        
        try:
            with engine.connect() as connection:
                email = email.strip().lower() if email else ""
                logger.debug(f"Attempting to update password and code for {email}, new_code: {new_code}")
                result = connection.execute(
                    text("SELECT email FROM auth WHERE email = :email"),
                    {"email": email}
                ).fetchone()
                if not result:
                    logger.warning(f"User {email} not found for password/code update")
                    return False
                hashed_password = bcrypt.hash(new_password)
                connection.execute(
                    text("UPDATE auth SET password = :password, code = :code WHERE email = :email"),
                    {"password": hashed_password, "code": new_code, "email": email}
                )
                connection.commit()
                logger.info(f"Updated password and code for {email}")
                return True
        except Exception as e:
            logger.error(f"Error updating password/code for {email}: {e}")
            return False
        finally:
            if 'connection' in locals():
                connection.close()

    @staticmethod
    def logout():
        """Clear session"""
        session.clear()
        logger.info("User logged out")