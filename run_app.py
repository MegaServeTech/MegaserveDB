# run_app.py
from app import app
from configure import APP_CONFIG
import os
from dotenv import load_dotenv
import os

# Load variables from .env
load_dotenv()


if __name__ == '__main__':
    port = int(os.environ.get('FLASK_PORT', APP_CONFIG['PORT']))
    app.run(host=APP_CONFIG['HOST'], port=port)
