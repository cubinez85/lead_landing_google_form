from flask import Blueprint, request, jsonify
from passlib.context import CryptContext
from datetime import datetime, timedelta
from pydantic import BaseModel
import jwt
from functools import wraps
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import secrets
import os

from app.database import get_connection, get_cursor
from config import SMTP_CONFIG, SMTP_FROM, FRONTEND_URL, RESET_TOKEN_EXPIRES_HOURS, JWT_SECRET_KEY, JWT_EXPIRATION_HOURS

import threading
from concurrent.futures import ThreadPoolExecutor

auth_bp = Blueprint('auth', __name__, url_prefix='/api')

# Хэширование паролей
pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")

# Глобальный пул потоков для отправки email
email_executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="flask_email")

def send_reset_email(to_email: str, reset_link: str) -> bool:
    """Асинхронная отправка email со ссылкой сброса пароля"""
    
    def send_sync():
        try:
            msg = MIMEMultipart()
            msg['From'] = 'cubinez85@cubinez.ru'
            msg['To'] = to_email
            msg['Subject'] = 'Восстановление пароля'

            body = f"""Здравствуйте!

Вы запросили восстановление пароля. Перейдите по ссылке для установки нового пароля:

{reset_link}

Если вы не запрашивали восстановление пароля, проигнорируйте это письмо.

Ссылка действительна в течение {RESET_TOKEN_EXPIRES_HOURS} часа(ов).
"""
            msg.attach(MIMEText(body, 'plain', 'utf-8'))

            server = smtplib.SMTP(SMTP_CONFIG['server'], SMTP_CONFIG['port'], timeout=10)
            server.ehlo()
            server.send_message(msg)
            server.quit()
            print(f"[EMAIL DEBUG] ✅ Email успешно отправлен на {to_email}", flush=True)
            return True
        except Exception as e:
            print(f"[EMAIL DEBUG] ❌ Ошибка отправки email: {e}", flush=True)
            import traceback
            traceback.print_exc()
            return False
    
    # Запускаем в отдельном потоке
    future = email_executor.submit(send_sync)
    
    def log_result(future):
        try:
            result = future.result(timeout=30)
            if result:
                print(f"[EMAIL DEBUG] ✅ Письмо сброса пароля отправлено на {to_email}", flush=True)
        except Exception as e:
            print(f"[EMAIL DEBUG] ❌ Ошибка: {e}", flush=True)
    
    future.add_done_callback(log_result)
    print(f"[EMAIL DEBUG] 📧 Запущена асинхронная отправка на {to_email}", flush=True)
    return True  # Возвращаем True сразу, не дожидаясь отправки

@auth_bp.route('/forgot-password', methods=['POST'])
def forgot_password():
    """Запрос на сброс пароля"""
    try:
        data = request.get_json()
        if not data or 'email' not in data:
            return jsonify({'error': 'Email обязателен'}), 400

        email = data['email'].strip().lower()
        
        conn = get_connection()
        cursor = get_cursor(conn)
        
        try:
            # Найти пользователя
            cursor.execute("SELECT id, email FROM users WHERE email = %s", (email,))
            user = cursor.fetchone()
            
            # Всегда возвращаем одинаковый ответ (безопасность)
            if not user:
                return jsonify({
                    'message': 'Если email зарегистрирован, ссылка для сброса пароля отправлена'
                }), 200
            
            # Сгенерировать токен
            reset_token = secrets.token_urlsafe(32)
            expires_at = datetime.now() + timedelta(hours=RESET_TOKEN_EXPIRES_HOURS)
            
            # Удалить старые неиспользованные токены этого пользователя
            cursor.execute("""
                DELETE FROM password_reset_tokens 
                WHERE user_id = %s AND used = FALSE
            """, (user['id'],))
            
            # Сохранить токен в БД
            cursor.execute("""
                INSERT INTO password_reset_tokens (user_id, token, expires_at)
                VALUES (%s, %s, %s)
            """, (user['id'], reset_token, expires_at))
            conn.commit()
            
            # Отправить email
            reset_link = f"{FRONTEND_URL}/reset-password?token={reset_token}"
            send_reset_email(user['email'], reset_link)
            
            return jsonify({
                'message': 'Если email зарегистрирован, ссылка для сброса пароля отправлена'
            }), 200
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"❌ Ошибка в forgot-password: {e}")
        return jsonify({'error': 'Внутренняя ошибка сервера'}), 500


@auth_bp.route('/reset-password', methods=['POST'])
def reset_password():
    """Сброс пароля по токену"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'Данные не предоставлены'}), 400
        
        token = data.get('token', '').strip()
        new_password = data.get('new_password', '')
        confirm_password = data.get('confirm_password', '')
        
        if not token:
            return jsonify({'error': 'Токен обязателен'}), 400
        
        # Проверка совпадения паролей
        if new_password != confirm_password:
            return jsonify({'error': 'Пароли не совпадают'}), 400
        
        if len(new_password) < 6:
            return jsonify({'error': 'Пароль должен быть не менее 6 символов'}), 400
        
        conn = get_connection()
        cursor = get_cursor(conn)
        
        try:
            # Найти токен
            cursor.execute("""
                SELECT user_id, expires_at, used 
                FROM password_reset_tokens 
                WHERE token = %s
            """, (token,))
            token_data = cursor.fetchone()
            
            if not token_data:
                return jsonify({'error': 'Неверная ссылка'}), 400
            
            if token_data['used']:
                return jsonify({'error': 'Ссылка уже использована'}), 400
            
            if datetime.now() > token_data['expires_at']:
                return jsonify({'error': 'Ссылка истекла'}), 400
            
            # Обновить пароль
            password_hash = pwd_context.hash(new_password)
            cursor.execute("""
                UPDATE users 
                SET password_hash = %s, must_change_password = FALSE
                WHERE id = %s
            """, (password_hash, token_data['user_id']))
            
            # Пометить токен как использованный
            cursor.execute("""
                UPDATE password_reset_tokens 
                SET used = TRUE 
                WHERE token = %s
            """, (token,))
            
            conn.commit()
            
            return jsonify({'message': 'Пароль успешно изменён'}), 200
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"❌ Ошибка в reset-password: {e}")
        return jsonify({'error': 'Внутренняя ошибка сервера'}), 500


@auth_bp.route('/health', methods=['GET'])
def health_check():
    """Проверка работоспособности API"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        return jsonify({'status': 'ok', 'database': 'connected'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'database': str(e)}), 500

# ============ LOGIN & AUTH ============

from config import JWT_SECRET_KEY, JWT_EXPIRATION_HOURS


def token_required(f):
    """Декоратор для защиты маршрутов"""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        
        # Получаем токен из заголовка Authorization
        if 'Authorization' in request.headers:
            auth_header = request.headers['Authorization']
            if auth_header.startswith('Bearer '):
                token = auth_header.split(' ')[1]
        
        if not token:
            return jsonify({'error': 'Токен не предоставлен'}), 401
        
        try:
            # Декодируем токен
            payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=['HS256'])
            user_id = payload['user_id']
            
            # Получаем пользователя из БД
            conn = get_connection()
            cursor = get_cursor(conn)
            cursor.execute(
                "SELECT id, email, is_admin, is_active FROM users WHERE id = %s",
                (user_id,)
            )
            current_user = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not current_user:
                return jsonify({'error': 'Пользователь не найден'}), 401
            
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Токен истёк'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Неверный токен'}), 401
        except Exception as e:
            return jsonify({'error': 'Ошибка авторизации'}), 401
        
        return f(current_user, *args, **kwargs)
    return decorated


class LoginRequest(BaseModel):
    email: str
    password: str


@auth_bp.route('/login', methods=['POST'])
def login():
    """Вход пользователя"""
    try:
        data = request.get_json()
        print(f"[DEBUG] Вход: {data}", flush=True)

        if not data or 'email' not in data or 'password' not in data:
            return jsonify({'error': 'Email и пароль обязательны'}), 400

        email = data['email'].strip().lower()
        password = data['password']

        conn = get_connection()
        cursor = get_cursor(conn)

        try:
            # Найти пользователя — добавили is_admin и is_active в SELECT
            cursor.execute("""
                SELECT id, email, password_hash, must_change_password, is_admin, is_active 
                FROM users WHERE email = %s
            """, (email,))
            user = cursor.fetchone()

            if not user:
                return jsonify({'error': 'Неверный email или пароль'}), 401

            # Проверить пароль
            if not pwd_context.verify(password, user['password_hash']):
                return jsonify({'error': 'Неверный email или пароль'}), 401

            # Проверить, не заблокирован ли пользователь
            if not user.get('is_active', True):
                return jsonify({'error': 'Аккаунт заблокирован. Обратитесь к администратору.'}), 403

            # Получить флаг обязательной смены пароля
            must_change = user.get('must_change_password', False)

            # Создать JWT токен
            token_payload = {
                'user_id': user['id'],
                'email': user['email'],
                'must_change_password': must_change,
                'is_admin': user.get('is_admin', False),
                'exp': datetime.utcnow() + timedelta(hours=JWT_EXPIRATION_HOURS),
                'iat': datetime.utcnow()
            }

            token = jwt.encode(token_payload, JWT_SECRET_KEY, algorithm='HS256')

            print(f"[DEBUG] Пользователь {user['email']} вошёл успешно. Must change: {must_change}", flush=True)

            return jsonify({
                'message': 'Вход выполнен успешно',
                'token': token,
                'must_change_password': must_change,
                'user': {
                    'id': user['id'],
                    'email': user['email'],
                    'is_admin': user.get('is_admin', False),
                    'must_change_password': must_change,
                    'is_active': user.get('is_active', True)
                }
            }), 200

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        print(f"❌ Ошибка в login: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return jsonify({'error': 'Внутренняя ошибка сервера'}), 500

@auth_bp.route('/me', methods=['GET'])
@token_required
def get_current_user(current_user):
    """Получить данные текущего пользователя"""
    return jsonify({
        'user': {
            'id': current_user['id'],
            'email': current_user['email'],
            'is_admin': current_user.get('is_admin', False),
            'is_active': current_user.get('is_active', True)
        }
    }), 200

class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str
    confirm_password: str


@auth_bp.route('/change-password', methods=['POST'])
@token_required
def change_password(current_user):
    """Смена пароля (требует авторизацию)"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'Данные не предоставлены'}), 400
        
        old_password = data.get('old_password', '')
        new_password = data.get('new_password', '')
        confirm_password = data.get('confirm_password', '')
        
        # Проверка совпадения паролей
        if new_password != confirm_password:
            return jsonify({'error': 'Новые пароли не совпадают'}), 400
        
        if len(new_password) < 6:
            return jsonify({'error': 'Пароль должен быть не менее 6 символов'}), 400
        
        conn = get_connection()
        cursor = get_cursor(conn)
        
        try:
            # Получить текущий хэш пароля
            cursor.execute("SELECT password_hash FROM users WHERE id = %s", (current_user['id'],))
            user = cursor.fetchone()
            
            if not user:
                return jsonify({'error': 'Пользователь не найден'}), 404
            
            # Проверить старый пароль
            if not pwd_context.verify(old_password, user['password_hash']):
                return jsonify({'error': 'Неверный текущий пароль'}), 401
            
            # Обновить пароль
            new_password_hash = pwd_context.hash(new_password)
            cursor.execute("""
                UPDATE users 
                SET password_hash = %s, must_change_password = FALSE
                WHERE id = %s
            """, (new_password_hash, current_user['id']))
            conn.commit()
            
            print(f"[DEBUG] Пользователь {current_user['email']} изменил пароль", flush=True)
            
            return jsonify({'message': 'Пароль успешно изменён'}), 200
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"❌ Ошибка в change-password: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return jsonify({'error': 'Внутренняя ошибка сервера'}), 500


@auth_bp.route('/logout', methods=['POST'])
@token_required
def logout(current_user):
    """Выход (на клиенте нужно удалить токен)"""
    return jsonify({'message': 'Выход выполнен успешно'}), 200

class ForceChangePasswordRequest(BaseModel):
    new_password: str
    confirm_password: str


@auth_bp.route('/force-change-password', methods=['POST'])
@token_required
def force_change_password(current_user):
    """Принудительная смена пароля при первом входе (не требует старого пароля)"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'Данные не предоставлены'}), 400
        
        new_password = data.get('new_password', '')
        confirm_password = data.get('confirm_password', '')
        
        # Проверка совпадения паролей
        if new_password != confirm_password:
            return jsonify({'error': 'Пароли не совпадают'}), 400
        
        if len(new_password) < 6:
            return jsonify({'error': 'Пароль должен быть не менее 6 символов'}), 400
        
        # Проверка сложности пароля
        if not any(c.isdigit() for c in new_password):
            return jsonify({'error': 'Пароль должен содержать хотя бы одну цифру'}), 400
        if not any(c.isalpha() for c in new_password):
            return jsonify({'error': 'Пароль должен содержать хотя бы одну букву'}), 400
        
        conn = get_connection()
        cursor = get_cursor(conn)
        
        try:
            # Обновить пароль и сбросить флаг
            new_password_hash = pwd_context.hash(new_password)
            cursor.execute("""
                UPDATE users 
                SET password_hash = %s, must_change_password = FALSE
                WHERE id = %s
            """, (new_password_hash, current_user['id']))
            conn.commit()
            
            print(f"[DEBUG] Пользователь {current_user['email']} сменил пароль при первом входе", flush=True)
            
            return jsonify({
                'message': 'Пароль успешно изменён',
                'must_change_password': False
            }), 200
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"❌ Ошибка в force-change-password: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return jsonify({'error': 'Внутренняя ошибка сервера'}), 500
