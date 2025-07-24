# configure.py

import os
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

APP_CONFIG = {
    'PORT': int(os.getenv('FLASK_PORT', 8000)),  # Default to 8000
    'HOST': os.getenv('FLASK_HOST', '0.0.0.0'),  # Default to 0.0.0.0 for Docker
    'DEBUG': os.getenv('FLASK_DEBUG', 'True').lower() == 'true',  # Accepts 'True' or 'False'
    'SECRET_KEY': os.getenv('FLASK_SECRET_KEY', 'your-secure-secret-key-1234567890'),
}
