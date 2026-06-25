📋 Промпт проекта: Система автоматизации заявок с Tilda
🎯 Общее описание
Полнофункциональная система автоматизации обработки заявок с Google Forms, интегрированная с сайтом на Tilda. Система автоматически:
Обрабатывает заявки из Google Forms
Создаёт пользователей с временными паролями
Отправляет уведомления админу (email, VK, телеграм)
Отправляет приветственные письма пользователям
Предоставляет API для аутентификации и управления
Имеет админ-панель для управления пользователями и заявками

Создание виртуального окружения
cd lead_landing_google_form
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r landing/requirements.txt

Настройка базы данных
# Создание БД и пользователя
sudo -u postgres psql
CREATE DATABASE leads_db;
CREATE USER leads_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE leads_db TO leads_user;
\q

# Запуск миграций
psql -U leads_user -d leads_db -h localhost -f landing/app/migrations/001_add_users_tokens.sql
psql -U leads_user -d leads_db -h localhost -f landing/app/migrations/002_add_admin.sql
psql -U leads_user -d leads_db -h localhost -f landing/app/migrations/003_must_change_password.sql

Настройка SSL
# Self-signed сертификат для тестирования
# Создание папки для сертификатов
sudo mkdir -p /etc/nginx/ssl

# Генерация self-signed сертификата
sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout /etc/nginx/ssl/lead-landing.key \
  -out /etc/nginx/ssl/lead-landing.crt \
  -subj "/C=RU/ST=Moscow/L=Moscow/O=YourOrg/CN=your-domain.com"

# Установка прав
sudo chmod 644 /etc/nginx/ssl/lead-landing.crt
sudo chmod 600 /etc/nginx/ssl/lead-landing.key

# Перезапуск Nginx
sudo systemctl reload nginx

Настройка systemd сервисов
# Background worker
sudo cp systemd/leads-automation.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable leads-automation
sudo systemctl start leads-automation

# Flask API
sudo cp systemd/password-reset-api.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable password-reset-api
sudo systemctl start password-reset-api

Создание папки для логов
sudo mkdir -p /var/log/leads-automation
sudo chown $USER:$USER /var/log/leads-automation 

Генерация JWT_SECRET_KEY:
python3 -c "import secrets; print(secrets.token_urlsafe(32))"

Создание первого администратора
cd /home/lead_landing_google_form/landing
source /home/lead_landing_google_form/venv/bin/activate

python3 << 'EOF'
from passlib.context import CryptContext
import psycopg2
from config import POSTGRESQL_CONFIG

pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")

email = input("Email администратора: ")
password = input("Пароль (мин. 6 символов): ")

if len(password) < 6:
    print("Пароль слишком короткий!")
    exit(1)

password_hash = pwd_context.hash(password)

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

try:
    cursor.execute("""
        INSERT INTO users (email, password_hash, is_admin, must_change_password)
        VALUES (%s, %s, TRUE, FALSE)
        ON CONFLICT (email)
        DO UPDATE SET password_hash = EXCLUDED.password_hash, is_admin = TRUE
        RETURNING id, email
    """, (email, password_hash))
    
    user = cursor.fetchone()
    conn.commit()
    print(f"✅ Администратор создан: ID={user[0]}, Email={user[1]}")
    
except Exception as e:
    print(f"❌ Ошибка: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
EOF

Настройка Google Cloud:
Создание проекта и сервисного аккаунта:
Откройте Google Cloud Console

Создайте новый проект или выберите существующий

Перейдите в "APIs & Services" → "Library"

Найдите и включите Google Sheets API

Перейдите в "Credentials" → "Create Credentials" → "Service Account"

Заполните:

Service account name: lead-automation

Role: Editor (или более ограниченные права)

Create key: JSON format

Настройка доступа к таблице:
Откройте Google таблицу

Нажмите "Поделиться"

Добавьте email сервисного аккаунта (находится в JSON файле, поле client_email)

Установите права: "Редактор" (для записи) или "Просмотр" (только чтение)

# Права на конфигурационные файлы
chmod 600 .env
chmod 600 sa-key.json

🚀 Полезные команды
Управление сервисами
# Перезапуск background worker
sudo systemctl restart leads-automation
sudo systemctl status leads-automation

# Перезапуск API
sudo systemctl restart password-reset-api
sudo systemctl status password-reset-api

# Перезапуск Nginx
sudo systemctl reload nginx

Просмотр логов
# Логи background worker
tail -f /var/log/leads-automation/leads.log

# Логи Gunicorn
tail -f /var/log/leads-automation/gunicorn-access.log
tail -f /var/log/leads-automation/gunicorn-error.log

# Логи Nginx
sudo tail -f /var/log/nginx/test-register-tilda-ssl.access.log

Работа с БД
# Подключение к БД
psql -U leads_user -d leads_db -h localhost

# Список пользователей
psql -U leads_user -d leads_db -h localhost -c "SELECT id, email, is_admin, must_change_password FROM users;"

# Список заявок
psql -U leads_user -d leads_db -h localhost -c "SELECT id, email, full_name, created_at FROM leads ORDER BY created_at DESC LIMIT 10;"

# Сброс state.json (обработать все заявки заново)
echo '{"1ILTKFRibB5-q1va7UEYekqJDVYl6O_G8VXLWsaq9yE0:Responses": 1}' > state.json

Тестирование API
# Health check
curl -k https://test-register-tilda.cubinez.ru/api/health

# Вход
curl -k -X POST https://test-register-tilda.cubinez.ru/api/login \
  -H "Content-Type: application/json" \
  -d '{"email": "cubinez85@cubinez.ru", "password": "пароль"}'

# Получить токен в переменную
TOKEN=$(curl -s -X POST http://test-register-tilda.cubinez.ru/api/login \
  -H "Content-Type: application/json" \
  -d '{"email": "cubinez85@cubinez.ru", "password": "пароль"}' | python3 -c "import sys, json; print(json.load(sys.stdin)['token'])")

# Статистика (админ)
curl -s -H "Authorization: Bearer $TOKEN" http://test-register-tilda.cubinez.ru/api/admin/stats | python3 -m json.tool

🌐 Страницы Tilda
URL,Название,Описание
/,main,Главная страница
/login,Страница входа,Форма входа
/forgot-password,Восстановление пароля,Запрос ссылки
/reset-password,Сброс пароля,Установка нового пароля
/change-password-first,change-password-first,Принудительная смена при первом входе
/admin,Admin,Админ-панель
/registration,Регистрация,Google Form


