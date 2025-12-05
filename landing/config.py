import os
from dotenv import load_dotenv

# Загружаем .env файл
load_dotenv()

# Google Sheets API
GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID')
GOOGLE_SHEET_NAME = os.getenv('GOOGLE_SHEET_NAME', 'Form Responses 1')

# Database
USE_POSTGRESQL = os.getenv('USE_POSTGRESQL', 'true').lower() == 'true'
USE_AIRTABLE = os.getenv('USE_AIRTABLE', 'false').lower() == 'true'

# PostgreSQL
POSTGRESQL_CONFIG = {
    'host': os.getenv('POSTGRESQL_HOST', 'localhost'),
    'port': os.getenv('POSTGRESQL_PORT', '5432'),
    'database': os.getenv('POSTGRESQL_DB', 'leads_db'),
    'user': os.getenv('POSTGRESQL_USER', 'postgres'),
    'password': os.getenv('POSTGRESQL_PASSWORD')
}

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', 'cubinez85')

# Email
SMTP_CONFIG = {
    'server': os.getenv('SMTP_SERVER', 'smtp.cubinez.ru'),
    'port': int(os.getenv('SMTP_PORT', '25')),
    'user': os.getenv('SMTP_USER', ''),
    'password': os.getenv('SMTP_PASSWORD', ''),
    'to': os.getenv('EMAIL_TO', 'postfix@cubinez.ru')
}

# Logging - обрабатываем путь из .env
LOG_FILE = os.getenv('LOG_FILE', 'leads.log')

# Проверяем наличие обязательных переменных
if not GOOGLE_SHEET_ID:
    raise ValueError("GOOGLE_SHEET_ID должен быть установлен в .env файле")
