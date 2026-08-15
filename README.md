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

# создать /etc/nginx/ssl/openssl.cnf
[req]
default_bits = 2048
prompt = no
default_md = sha256
distinguished_name = dn
req_extensions = v3_req

[dn]
C = RU
ST = Moscow
L = Moscow
O = Cubinez
OU = IT
CN = wordpress.cubinez.ru

[v3_req]
subjectAltName = @alt_names

[alt_names]
DNS.1 = wordpress.cubinez.ru
DNS.2 = localhost
IP.1 = 127.0.0.1

Шаг 1: Пересоздайте сертификат с правильным флагом

# Удалите старые сертификаты
sudo rm -f /etc/nginx/ssl/test-register-tilda.*

# Создайте новый сертификат с явным указанием extensions
sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout /etc/nginx/ssl/test-register-tilda.key \
  -out /etc/nginx/ssl/test-register-tilda.crt \
  -config /etc/nginx/ssl/openssl.cnf \
  -extensions v3_req

# Установите права
sudo chmod 644 /etc/nginx/ssl/test-register-tilda.crt
sudo chmod 600 /etc/nginx/ssl/test-register-tilda.key

Шаг 2: Проверьте, что SAN теперь есть

openssl x509 -in /etc/nginx/ssl/test-register-tilda.crt -text -noout | grep -A 3 "Alternative"

Должно показать:

            X509v3 Subject Alternative Name: 
                DNS:test-register-tilda.cubinez.ru, DNS:localhost, IP Address:127.0.0.1

Шаг 3: Перезапустите Nginx
sudo nginx -t
sudo systemctl reload nginx

Шаг 4: Скопируйте сертификат в Windows и установите его

# Скопируйте на рабочий стол Windows
cp /etc/nginx/ssl/test-register-tilda.crt /mnt/c/Users/Oleg/OneDrive/Desktop/test-register-tilda.crt

Затем в Windows:
Откройте файл test-register-tilda.crt на рабочем столе
Нажмите "Установить сертификат"
Выберите "Текущий пользователь" → Далее
Выберите "Поместить все сертификаты в следующее хранилище"
Нажмите "Обзор" → выберите "Доверенные корневые центры сертификации"
Далее → Готово
Шаг 5: Полностью закройте браузер и проверьте
Важно: Chrome/Edge нужно закрыть полностью (включая все окна).

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
# добавить сертификат в wsl
sudo cp /etc/nginx/ssl/test-register-tilda.crt /usr/local/share/ca-certificates/test-register-tilda.crt
sudo update-ca-certificates

# Chrome/Edge требуют, чтобы в сертификате было расширение SAN (Subject Alternative Name). Проверьте:
openssl x509 -in /etc/nginx/ssl/test-register-tilda.crt -noout -ext subjectAltName

# Если вывод пустой или с ошибкой — браузер может всё равно ругаться, даже с установленным сертификатом. Тогда пересоздайте сертификат с SAN:
sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout /etc/nginx/ssl/test-register-tilda.key \
  -out /etc/nginx/ssl/test-register-tilda.crt \
  -subj "/CN=test-register-tilda.cubinez.ru" \
  -addext "subjectAltName=DNS:test-register-tilda.cubinez.ru,DNS:localhost,IP:127.0.0.1"

sudo nginx -t && sudo systemctl reload nginx
sudo update-ca-certificates

# Health check без -k
curl -i https://test-register-tilda.cubinez.ru/api/health

# Health check with -k
curl -k https://test-register-tilda.cubinez.ru/api/health

# Вход
# 1. Логин (POST) — получите токен
curl -i -X POST https://test-register-tilda.cubinez.ru/api/login \
  -H "Content-Type: application/json" \
  -d '{"email":"cubinez85@cubinez.ru","password":"ВАШ_ПАРОЛЬ"}'

# 2. Админ-статистика с токеном
curl -i https://test-register-tilda.cubinez.ru/api/admin/stats \
  -H "Authorization: Bearer ТОКЕН_ИЗ_ОТВЕТА"

🌐 Страницы Tilda# 
URL,Название,Описание
/,main,Главная страница
/login,Страница входа,Форма входа
/forgot-password,Восстановление пароля,Запрос ссылки
/reset-password,Сброс пароля,Установка нового пароля
/change-password-first,change-password-first,Принудительная смена при первом входе
/admin,Admin,Админ-панель
/registration,Регистрация,Google Form


