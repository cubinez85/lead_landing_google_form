#!/usr/bin/env python3
"""
Полная система автоматизации обработки заявок из Google Forms
"""

import json
import logging
import os
import sys
import time
import smtplib
import asyncio
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Optional

import schedule
import psycopg2
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from telegram import Bot
from telegram.error import TelegramError

# Загружаем переменные окружения
load_dotenv()

# Конфигурация
class Config:
    @staticmethod
    def get_sheet_id() -> str:
        return os.getenv('GOOGLE_SHEET_ID', '')
    
    @staticmethod
    def get_sheet_name() -> str:
        return os.getenv('GOOGLE_SHEET_NAME', 'Form Responses 1')
    
    @staticmethod
    def use_postgresql() -> bool:
        return os.getenv('USE_POSTGRESQL', 'true').lower() == 'true'
    
    @staticmethod
    def get_postgresql_config() -> Dict:
        return {
            'host': os.getenv('POSTGRESQL_HOST', 'localhost'),
            'port': os.getenv('POSTGRESQL_PORT', '5432'),
            'database': os.getenv('POSTGRESQL_DB', 'leads_db'),
            'user': os.getenv('POSTGRESQL_USER', 'postgres'),
            'password': os.getenv('POSTGRESQL_PASSWORD', '')
        }
    
    @staticmethod
    def get_telegram_token() -> Optional[str]:
        return os.getenv('TELEGRAM_BOT_TOKEN')
    
    @staticmethod
    def get_telegram_chat_id() -> Optional[str]:
        return os.getenv('TELEGRAM_CHAT_ID')
    
    @staticmethod
    def get_smtp_config() -> Dict:
        return {
            'server': os.getenv('SMTP_SERVER', 'localhost'),
            'port': int(os.getenv('SMTP_PORT', '25')),
            'user': os.getenv('SMTP_USER', ''),
            'password': os.getenv('SMTP_PASSWORD', ''),
            'to': os.getenv('EMAIL_TO', '')
        }
    
    @staticmethod
    def get_log_file() -> str:
        return os.getenv('LOG_FILE', 'leads.log')

# Настройка логирования
def setup_logging():
    log_file = Config.get_log_file()
    log_dir = os.path.dirname(log_file)
    
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

logger = setup_logging()

class StateManager:
    """Управление состоянием (последняя обработанная строка)"""
    
    STATE_FILE = 'state.json'
    
    def __init__(self):
        self.state = {}
        self.load()
    
    def load(self):
        try:
            if os.path.exists(self.STATE_FILE):
                with open(self.STATE_FILE, 'r', encoding='utf-8') as f:
                    self.state = json.load(f)
                logger.info(f"Состояние загружено из {self.STATE_FILE}")
        except Exception as e:
            logger.error(f"Ошибка загрузки состояния: {e}")
            self.state = {}
    
    def save(self):
        try:
            with open(self.STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Ошибка сохранения состояния: {e}")
    
    def get_last_row(self, sheet_id: str, sheet_name: str) -> int:
        key = f"{sheet_id}:{sheet_name}"
        return self.state.get(key, 1)
    
    def set_last_row(self, sheet_id: str, sheet_name: str, row: int):
        key = f"{sheet_id}:{sheet_name}"
        self.state[key] = row
        self.save()

class GoogleSheetsManager:
    """Управление подключением к Google Sheets"""
    
    def __init__(self):
        self.service = None
        self.authenticate()
    
    def authenticate(self):
        """Аутентификация через сервисный аккаунт"""
        try:
            sa_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            if not sa_path:
                raise ValueError("GOOGLE_APPLICATION_CREDENTIALS не установлена")
            
            if not os.path.exists(sa_path):
                raise FileNotFoundError(f"Файл не найден: {sa_path}")
            
            credentials = service_account.Credentials.from_service_account_file(
                sa_path,
                scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            )
            
            self.service = build('sheets', 'v4', credentials=credentials)
            logger.info("✅ Google Sheets API готов")
            
        except Exception as e:
            logger.error(f"❌ Ошибка аутентификации: {e}")
            raise
    
    def get_sheet_data(self) -> Optional[List[List]]:
        """Получение данных из таблицы"""
        try:
            sheet_id = Config.get_sheet_id()
            sheet_name = Config.get_sheet_name()
            
            if not sheet_id:
                logger.error("GOOGLE_SHEET_ID не установлен")
                return None
            
            result = self.service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=f"'{sheet_name}'!A:Z"
            ).execute()
            
            values = result.get('values', [])
            return values
            
        except HttpError as e:
            if e.resp.status == 403:
                logger.error("❌ Доступ запрещен к таблице")
            elif e.resp.status == 404:
                logger.error(f"❌ Таблица не найдена: {Config.get_sheet_id()}")
            else:
                logger.error(f"❌ Ошибка Google Sheets: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных: {e}")
            return None

class DatabaseManager:
    """Управление базой данных PostgreSQL"""
    
    def __init__(self):
        self.connection = None
        if Config.use_postgresql():
            self.connect()
    
    def connect(self):
        """Подключение к PostgreSQL"""
        try:
            config = Config.get_postgresql_config()
            self.connection = psycopg2.connect(**config)
            self.connection.autocommit = True
            self.create_table()
            logger.info("✅ Подключение к PostgreSQL установлено")
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к PostgreSQL: {e}")
            self.connection = None
    
    def create_table(self):
        """Создание таблицы если не существует"""
        if not self.connection:
            return
        
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS leads (
                    id SERIAL PRIMARY KEY,
                    timestamp VARCHAR(100),
                    full_name VARCHAR(255),
                    email VARCHAR(255),
                    raw_data JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.close()
            logger.info("✅ Таблица leads проверена/создана")
        except Exception as e:
            logger.error(f"❌ Ошибка создания таблицы: {e}")
    
    def save_lead(self, submission: Dict):
        """Сохранение заявки в базу данных"""
        if not self.connection:
            logger.warning("⚠️  Нет подключения к PostgreSQL, пропускаем сохранение")
            return
        
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                INSERT INTO leads (timestamp, full_name, email, raw_data)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (
                submission.get('Отметка времени', ''),
                submission.get('ФИО (обязательное поле)', ''),
                submission.get('Электронная почта (обязательное поле)', ''),
                json.dumps(submission, ensure_ascii=False)
            ))
            
            lead_id = cursor.fetchone()[0]
            cursor.close()
            logger.info(f"✅ Заявка сохранена в PostgreSQL (ID: {lead_id})")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения в PostgreSQL: {e}")

class NotificationManager:
    """Управление уведомлениями"""
    
    def __init__(self):
        self.telegram_bot = None
        self.init_telegram()
    
    def init_telegram(self):
        """Инициализация Telegram бота"""
        token = Config.get_telegram_token()
        chat_id = Config.get_telegram_chat_id()
        
        if token and chat_id:
            try:
                self.telegram_bot = Bot(token=token)
                logger.info("✅ Telegram бот инициализирован")
            except Exception as e:
                logger.error(f"❌ Ошибка инициализации Telegram бота: {e}")
                self.telegram_bot = None
        else:
            logger.info("⚠️  Telegram уведомления отключены (не указан токен или chat_id)")
    
    def send_telegram(self, submission: Dict):
        """Отправка уведомления в Telegram"""
        if not self.telegram_bot:
            return
        
        try:
            message = self._format_telegram_message(submission)
            chat_id = Config.get_telegram_chat_id()
            
            if not chat_id or not chat_id.isdigit():
                logger.warning("⚠️  Неверный Telegram chat_id")
                return
            
            async def _send():
                try:
                    await self.telegram_bot.send_message(
                        chat_id=int(chat_id),
                        text=message,
                        parse_mode='HTML'
                    )
                    logger.info("✅ Уведомление отправлено в Telegram")
                except TelegramError as e:
                    logger.error(f"❌ Ошибка отправки в Telegram: {e}")
            
            asyncio.run(_send())
            
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке Telegram: {e}")
    
    def send_email(self, submission: Dict):
        """Отправка уведомления на email"""
        try:
            config = Config.get_smtp_config()
            to_email = config.get('to')
            
            if not to_email:
                logger.info("⚠️  Email уведомления отключены (не указан получатель)")
                return
            
            from_email = config.get('user') or 'noreply@example.com'
            
            msg = MIMEMultipart()
            msg['From'] = from_email
            msg['To'] = to_email
            msg['Subject'] = 'Новая заявка с формы регистрации'
            
            body = self._format_email_message(submission)
            msg.attach(MIMEText(body, 'plain', 'utf-8'))
            
            server = smtplib.SMTP(config['server'], config['port'], timeout=10)
            
            if config.get('user') and config.get('password'):
                server.starttls()
                server.login(config['user'], config['password'])
            
            server.send_message(msg)
            server.quit()
            
            logger.info(f"✅ Email отправлен на {to_email}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка отправки email: {e}")
    
    def _format_telegram_message(self, submission: Dict) -> str:
        return f"""
<b>🎯 Новая заявка с формы регистрации</b>

<b>📅 Время:</b> {submission.get('Отметка времени', 'Не указано')}
<b>👤 ФИО:</b> {submission.get('ФИО (обязательное поле)', 'Не указано')}
<b>📧 Email:</b> {submission.get('Электронная почта (обязательное поле)', 'Не указано')}
<b>🕐 Обработано:</b> {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}
"""
    
    def _format_email_message(self, submission: Dict) -> str:
        return f"""
Новая заявка с формы регистрации

Время отправки: {submission.get('Отметка времени', 'Не указано')}
ФИО: {submission.get('ФИО (обязательное поле)', 'Не указано')}
Email: {submission.get('Электронная почта (обязательное поле)', 'Не указано')}
Время обработки: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}

Полные данные:
{json.dumps(submission, ensure_ascii=False, indent=2)}
"""

def process_submissions():
    """Основная функция обработки заявок"""
    logger.info("=" * 60)
    logger.info("🔍 ПРОВЕРКА НОВЫХ ЗАЯВОК")
    logger.info("=" * 60)
    
    try:
        # Инициализируем компоненты
        state = StateManager()
        sheets = GoogleSheetsManager()
        db = DatabaseManager()
        notifier = NotificationManager()
        
        # Получаем данные из таблицы
        data = sheets.get_sheet_data()
        if not data:
            logger.info("📭 Нет данных в таблице")
            return
        
        sheet_id = Config.get_sheet_id()
        sheet_name = Config.get_sheet_name()
        last_row = state.get_last_row(sheet_id, sheet_name)
        
        logger.info(f"📊 Всего строк в таблице: {len(data)}")
        logger.info(f"📌 Последняя обработанная строка: {last_row}")
        
        # Проверяем есть ли новые строки
        if len(data) <= last_row:
            logger.info("✅ Нет новых заявок")
            return
        
        # Обрабатываем новые строки
        headers = data[0]
        new_rows = data[last_row:]
        
        logger.info(f"🎉 Найдено новых заявок: {len(new_rows)}")
        
        processed_count = 0
        for i, row in enumerate(new_rows, 1):
            try:
                # Создаем словарь с данными
                submission = {}
                for j, header in enumerate(headers):
                    submission[header] = row[j] if j < len(row) else ''
                
                # Логируем
                email = submission.get('Электронная почта (обязательное поле)', 'без email')
                logger.info(f"[{i}/{len(new_rows)}] Обработка: {email}")
                
                # Сохраняем в базу данных
                db.save_lead(submission)
                
                # Отправляем уведомления
                notifier.send_telegram(submission)
                notifier.send_email(submission)
                
                processed_count += 1
                logger.info(f"[{i}/{len(new_rows)}] ✅ Заявка обработана")
                
            except Exception as e:
                logger.error(f"[{i}/{len(new_rows)}] ❌ Ошибка обработки заявки: {e}")
                continue
        
        # Обновляем состояние
        state.set_last_row(sheet_id, sheet_name, len(data))
        
        logger.info(f"🎯 ИТОГ: Обработано {processed_count}/{len(new_rows)} заявок")
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в process_submissions: {e}")

def check_environment():
    """Проверка необходимых переменных окружения"""
    logger.info("🔍 Проверка окружения...")
    
    required = ['GOOGLE_SHEET_ID', 'GOOGLE_APPLICATION_CREDENTIALS']
    missing = []
    
    for var in required:
        if not os.getenv(var):
            missing.append(var)
    
    if missing:
        logger.error(f"❌ Отсутствуют обязательные переменные: {', '.join(missing)}")
        return False
    
    # Проверяем файл сервисного аккаунта
    sa_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not os.path.exists(sa_path):
        logger.error(f"❌ Файл сервисного аккаунта не найден: {sa_path}")
        return False
    
    logger.info("✅ Окружение проверено успешно")
    return True

def main():
    """Главная функция"""
    logger.info("=" * 60)
    logger.info("🚀 ЗАПУСК СИСТЕМЫ АВТОМАТИЗАЦИИ ЗАЯВОК")
    logger.info(f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
    logger.info("=" * 60)
    
    logger.info(f"📊 Таблица: {Config.get_sheet_id()}")
    logger.info(f"📝 Лист: {Config.get_sheet_name()}")
    logger.info(f"💾 PostgreSQL: {'включен' if Config.use_postgresql() else 'выключен'}")
    logger.info(f"📱 Telegram: {'включен' if Config.get_telegram_token() else 'выключен'}")
    logger.info(f"📧 Email: {'включен' if Config.get_smtp_config().get('to') else 'выключен'}")
    
    # Проверяем окружение
    if not check_environment():
        logger.error("❌ Завершение из-за ошибок в окружении")
        return 1
    
    # Настраиваем регулярные проверки
    interval = int(os.getenv('CHECK_INTERVAL', '30'))
    schedule.every(interval).seconds.do(process_submissions)
    logger.info(f"⏰ Проверки каждые {interval} секунд")
    
    # Первая проверка сразу
    process_submissions()
    
    # Основной цикл
    logger.info("✅ Система запущена и работает...")
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("\n🛑 Завершение работы по запросу пользователя")
    except Exception as e:
        logger.error(f"❌ Неожиданная ошибка: {e}")
        return 1
    
    logger.info("✅ Система остановлена")
    return 0

if __name__ == '__main__':
    sys.exit(main())
