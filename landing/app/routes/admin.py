from flask import Blueprint, request, jsonify
from functools import wraps
import jwt
import hashlib
import logging
from datetime import datetime

from app.database import get_connection, get_cursor
from config import JWT_SECRET_KEY, POSTGRESQL_CONFIG

admin_bp = Blueprint('admin', __name__, url_prefix='/api/admin')
logger = logging.getLogger(__name__)


def admin_required(f):
    """Декоратор: только для администраторов"""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        
        if 'Authorization' in request.headers:
            auth_header = request.headers['Authorization']
            if auth_header.startswith('Bearer '):
                token = auth_header.split(' ')[1]
        
        if not token:
            return jsonify({'error': 'Токен не предоставлен'}), 401
        
        try:
            payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=['HS256'])
            user_id = payload['user_id']
            
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
                return jsonify({'error': 'Пользователь не найден'}), 404
            
            if not current_user['is_admin']:
                return jsonify({'error': 'Доступ запрещён: требуются права администратора'}), 403
            
            if not current_user['is_active']:
                return jsonify({'error': 'Аккаунт заблокирован'}), 403
            
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Токен истёк'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Неверный токен'}), 401
        except Exception as e:
            logger.error(f"Ошибка авторизации админа: {e}")
            return jsonify({'error': 'Ошибка авторизации'}), 401
        
        return f(current_user, *args, **kwargs)
    return decorated


def log_action(admin_id, action, target_type=None, target_id=None, details=None):
    """Логирование действий администратора"""
    try:
        conn = get_connection()
        cursor = get_cursor(conn)
        cursor.execute("""
            INSERT INTO admin_logs (admin_id, action, target_type, target_id, details, ip_address, user_agent)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            admin_id, action, target_type, target_id,
            details,
            request.remote_addr,
            request.headers.get('User-Agent', '')
        ))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка логирования: {e}")


# ============ СТАТИСТИКА ============

@admin_bp.route('/stats', methods=['GET'])
@admin_required
def get_stats(current_user):
    """Получить статистику системы"""
    print(f"[ADMIN STATS] Запрос от пользователя: {current_user}", flush=True)
    try:
        conn = get_connection()
        cursor = get_cursor(conn)
        
        # Всего пользователей
        cursor.execute("SELECT COUNT(*) as count FROM users")
        row = cursor.fetchone()
        print(f"[ADMIN STATS] users row: {row}, type: {type(row)}", flush=True)
        total_users = row['count'] if isinstance(row, dict) else row[0]
        
        # Активных пользователей
        cursor.execute("SELECT COUNT(*) as count FROM users WHERE is_active = TRUE")
        row = cursor.fetchone()
        active_users = row['count'] if isinstance(row, dict) else row[0]
        
        # Администраторов
        cursor.execute("SELECT COUNT(*) as count FROM users WHERE is_admin = TRUE")
        row = cursor.fetchone()
        admin_count = row['count'] if isinstance(row, dict) else row[0]
        
        # Всего заявок
        try:
            cursor.execute("SELECT COUNT(*) as count FROM leads")
            row = cursor.fetchone()
            total_leads = row['count'] if isinstance(row, dict) else row[0]
            
            cursor.execute("""
                SELECT COUNT(*) as count FROM leads 
                WHERE created_at >= NOW() - INTERVAL '7 days'
            """)
            row = cursor.fetchone()
            recent_leads = row['count'] if isinstance(row, dict) else row[0]
            
            cursor.execute("""
                SELECT COUNT(*) as count FROM leads 
                WHERE created_at >= CURRENT_DATE
            """)
            row = cursor.fetchone()
            today_leads = row['count'] if isinstance(row, dict) else row[0]
        except Exception as e:
            print(f"[ADMIN STATS] Ошибка leads: {e}", flush=True)
            total_leads = 0
            recent_leads = 0
            today_leads = 0
        
        # Токены сброса пароля
        try:
            cursor.execute("""
                SELECT COUNT(*) as count FROM password_reset_tokens 
                WHERE used = FALSE AND expires_at > NOW()
            """)
            row = cursor.fetchone()
            active_tokens = row['count'] if isinstance(row, dict) else row[0]
        except Exception as e:
            print(f"[ADMIN STATS] Ошибка tokens: {e}", flush=True)
            active_tokens = 0
        
        cursor.close()
        conn.close()
        
        result = {
            'stats': {
                'total_users': total_users,
                'active_users': active_users,
                'admin_count': admin_count,
                'total_leads': total_leads,
                'recent_leads_7d': recent_leads,
                'today_leads': today_leads,
                'active_reset_tokens': active_tokens
            }
        }
        
        print(f"[ADMIN STATS] Результат: {result}", flush=True)
        return jsonify(result), 200
        
    except Exception as e:
        import traceback
        print(f"[ADMIN STATS] ОШИБКА: {e}", flush=True)
        print(f"[ADMIN STATS] TRACEBACK:\n{traceback.format_exc()}", flush=True)
        return jsonify({'error': f'Внутренняя ошибка сервера: {str(e)}'}), 500

# ============ ПОЛЬЗОВАТЕЛИ ============

@admin_bp.route('/users', methods=['GET'])
@admin_required
def get_users(current_user):
    """Получить список пользователей с пагинацией"""
    try:
        print(f"[DEBUG /users] Начало обработки запроса", flush=True)
        print(f"[DEBUG /users] Параметры: page={request.args.get('page')}, per_page={request.args.get('per_page')}, search={request.args.get('search')}", flush=True)
        
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 20))
        search = request.args.get('search', '').strip()
        
        if page < 1:
            page = 1
        if per_page < 1 or per_page > 100:
            per_page = 20
        
        offset = (page - 1) * per_page
        
        print(f"[DEBUG /users] Вычисленные значения: page={page}, per_page={per_page}, offset={offset}, search='{search}'", flush=True)
        
        conn = get_connection()
        cursor = get_cursor(conn)
        
        # Построение запроса с поиском
        where_clause = ""
        params = []
        
        if search:
            where_clause = "WHERE email ILIKE %s"
            params.append(f"%{search}%")
            print(f"[DEBUG /users] Поиск по email: {search}", flush=True)
        
        # Общее количество
        count_query = f"SELECT COUNT(*) FROM users {where_clause}"
        print(f"[DEBUG /users] Count query: {count_query}", flush=True)
        print(f"[DEBUG /users] Count params: {params}", flush=True)
        
        cursor.execute(count_query, params)
        count_result = cursor.fetchone()
        print(f"[DEBUG /users] Count result: {count_result}", flush=True)
        
        total = count_result['count'] if count_result else 0
        print(f"[DEBUG /users] Total users: {total}", flush=True)
        
        # Получение пользователей
        query = f"""
            SELECT id, email, is_admin, is_active, created_at, updated_at
            FROM users 
            {where_clause}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """
        print(f"[DEBUG /users] Main query: {query}", flush=True)
        print(f"[DEBUG /users] Main params: {params + [per_page, offset]}", flush=True)
        
        cursor.execute(query, params + [per_page, offset])
        users = cursor.fetchall()
        print(f"[DEBUG /users] Получено пользователей: {len(users)}", flush=True)
        
        cursor.close()
        conn.close()
        
        users_list = []
        for u in users:
            print(f"[DEBUG /users] Обработка пользователя: {u}", flush=True)
            users_list.append({
                'id': u['id'],
                'email': u['email'],
                'is_admin': u['is_admin'],
                'is_active': u['is_active'],
                'created_at': u['created_at'].isoformat() if u['created_at'] else None,
                'updated_at': u['updated_at'].isoformat() if u['updated_at'] else None
            })
        
        result = {
            'users': users_list,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'total_pages': (total + per_page - 1) // per_page
            }
        }
        
        print(f"[DEBUG /users] Успешный ответ: {len(users_list)} пользователей", flush=True)
        return jsonify(result), 200
        
    except Exception as e:
        print(f"[ERROR /users] Ошибка: {type(e).__name__}: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Внутренняя ошибка сервера: {str(e)}'}), 500

@admin_bp.route('/users/<int:user_id>', methods=['GET'])
@admin_required
def get_user(current_user, user_id):
    """Получить детали пользователя"""
    try:
        conn = get_connection()
        cursor = get_cursor(conn)
        
        cursor.execute("""
            SELECT id, email, is_admin, is_active, created_at, updated_at
            FROM users WHERE id = %s
        """, (user_id,))
        user = cursor.fetchone()
        
        # Статистика пользователя
        cursor.execute("SELECT COUNT(*) FROM leads WHERE email = %s", (user['email'],))
        leads_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) FROM password_reset_tokens WHERE user_id = %s
        """, (user_id,))
        tokens_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        if not user:
            return jsonify({'error': 'Пользователь не найден'}), 404
        
        return jsonify({
            'user': {
                'id': user['id'],
                'email': user['email'],
                'is_admin': user['is_admin'],
                'is_active': user['is_active'],
                'created_at': user['created_at'].isoformat() if user['created_at'] else None,
                'updated_at': user['updated_at'].isoformat() if user['updated_at'] else None,
                'leads_count': leads_count,
                'tokens_count': tokens_count
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Ошибка получения пользователя: {e}")
        return jsonify({'error': 'Внутренняя ошибка сервера'}), 500


@admin_bp.route('/users/<int:user_id>/toggle-active', methods=['POST'])
@admin_required
def toggle_user_active(current_user, user_id):
    """Заблокировать/разблокировать пользователя"""
    try:
        if user_id == current_user['id']:
            return jsonify({'error': 'Нельзя заблокировать самого себя'}), 400
        
        conn = get_connection()
        cursor = get_cursor(conn)
        
        cursor.execute("SELECT id, email, is_active FROM users WHERE id = %s", (user_id,))
        user = cursor.fetchone()
        
        if not user:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Пользователь не найден'}), 404
        
        new_status = not user['is_active']
        cursor.execute("""
            UPDATE users SET is_active = %s WHERE id = %s
        """, (new_status, user_id))
        conn.commit()
        
        log_action(
            current_user['id'],
            'toggle_active',
            'user',
            user_id,
            {'email': user['email'], 'new_status': new_status}
        )
        
        cursor.close()
        conn.close()
        
        action = 'разблокирован' if new_status else 'заблокирован'
        logger.info(f"Админ {current_user['email']} {action} пользователя {user['email']}")
        
        return jsonify({
            'message': f"Пользователь {action}",
            'is_active': new_status
        }), 200
        
    except Exception as e:
        logger.error(f"Ошибка изменения статуса: {e}")
        return jsonify({'error': 'Внутренняя ошибка сервера'}), 500


@admin_bp.route('/users/<int:user_id>/toggle-admin', methods=['POST'])
@admin_required
def toggle_user_admin(current_user, user_id):
    """Назначить/снять права администратора"""
    try:
        if user_id == current_user['id']:
            return jsonify({'error': 'Нельзя изменить свои права'}), 400
        
        conn = get_connection()
        cursor = get_cursor(conn)
        
        cursor.execute("SELECT id, email, is_admin FROM users WHERE id = %s", (user_id,))
        user = cursor.fetchone()
        
        if not user:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Пользователь не найден'}), 404
        
        new_status = not user['is_admin']
        cursor.execute("""
            UPDATE users SET is_admin = %s WHERE id = %s
        """, (new_status, user_id))
        conn.commit()
        
        log_action(
            current_user['id'],
            'toggle_admin',
            'user',
            user_id,
            {'email': user['email'], 'new_status': new_status}
        )
        
        cursor.close()
        conn.close()
        
        action = 'назначен администратором' if new_status else 'лишён прав администратора'
        logger.info(f"Админ {current_user['email']} {action} пользователя {user['email']}")
        
        return jsonify({
            'message': f"Пользователь {action}",
            'is_admin': new_status
        }), 200
        
    except Exception as e:
        logger.error(f"Ошибка изменения прав: {e}")
        return jsonify({'error': 'Внутренняя ошибка сервера'}), 500


@admin_bp.route('/users/<int:user_id>', methods=['DELETE'])
@admin_required
def delete_user(current_user, user_id):
    """Удалить пользователя"""
    try:
        if user_id == current_user['id']:
            return jsonify({'error': 'Нельзя удалить самого себя'}), 400
        
        conn = get_connection()
        cursor = get_cursor(conn)
        
        cursor.execute("SELECT id, email FROM users WHERE id = %s", (user_id,))
        user = cursor.fetchone()
        
        if not user:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Пользователь не найден'}), 404
        
        cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()
        
        log_action(
            current_user['id'],
            'delete_user',
            'user',
            user_id,
            {'email': user['email']}
        )
        
        cursor.close()
        conn.close()
        
        logger.info(f"Админ {current_user['email']} удалил пользователя {user['email']}")
        
        return jsonify({'message': 'Пользователь удалён'}), 200
        
    except Exception as e:
        logger.error(f"Ошибка удаления пользователя: {e}")
        return jsonify({'error': 'Внутренняя ошибка сервера'}), 500


# ============ ЗАЯВКИ (LEADS) ============

@admin_bp.route('/leads', methods=['GET'])
@admin_required
def get_leads(current_user):
    """Получить список заявок с пагинацией"""
    try:
        print(f"[DEBUG /leads] Начало обработки запроса", flush=True)
        
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 20))
        search = request.args.get('search', '').strip()
        
        if page < 1:
            page = 1
        if per_page < 1 or per_page > 100:
            per_page = 20
        
        offset = (page - 1) * per_page
        
        conn = get_connection()
        cursor = get_cursor(conn)
        
        where_clause = ""
        params = []
        
        if search:
            where_clause = "WHERE full_name ILIKE %s OR email ILIKE %s"
            params.extend([f"%{search}%", f"%{search}%"])
        
        count_query = f"SELECT COUNT(*) FROM leads {where_clause}"
        cursor.execute(count_query, params)
        count_result = cursor.fetchone()
        total = count_result['count'] if count_result else 0
        
        query = f"""
            SELECT id, timestamp, full_name, email, created_at
            FROM leads
            {where_clause}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """
        cursor.execute(query, params + [per_page, offset])
        leads = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        leads_list = []
        for l in leads:
            leads_list.append({
                'id': l['id'],
                'timestamp': l['timestamp'],
                'full_name': l['full_name'],
                'email': l['email'],
                'created_at': l['created_at'].isoformat() if l['created_at'] else None
            })
        
        return jsonify({
            'leads': leads_list,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'total_pages': (total + per_page - 1) // per_page
            }
        }), 200
        
    except Exception as e:
        print(f"[ERROR /leads] Ошибка: {type(e).__name__}: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Внутренняя ошибка сервера: {str(e)}'}), 500

@admin_bp.route('/leads/<int:lead_id>', methods=['GET'])
@admin_required
def get_lead(current_user, lead_id):
    """Получить детали заявки"""
    try:
        conn = get_connection()
        cursor = get_cursor(conn)
        
        cursor.execute("""
            SELECT id, timestamp, full_name, email, raw_data, created_at
            FROM leads WHERE id = %s
        """, (lead_id,))
        lead = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not lead:
            return jsonify({'error': 'Заявка не найдена'}), 404
        
        return jsonify({
            'lead': {
                'id': lead['id'],
                'timestamp': lead['timestamp'],
                'full_name': lead['full_name'],
                'email': lead['email'],
                'raw_data': lead['raw_data'],
                'created_at': lead['created_at'].isoformat() if lead['created_at'] else None
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Ошибка получения заявки: {e}")
        return jsonify({'error': 'Внутренняя ошибка сервера'}), 500


@admin_bp.route('/leads/<int:lead_id>', methods=['DELETE'])
@admin_required
def delete_lead(current_user, lead_id):
    """Удалить заявку"""
    try:
        conn = get_connection()
        cursor = get_cursor(conn)
        
        cursor.execute("SELECT id, email FROM leads WHERE id = %s", (lead_id,))
        lead = cursor.fetchone()
        
        if not lead:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Заявка не найдена'}), 404
        
        cursor.execute("DELETE FROM leads WHERE id = %s", (lead_id,))
        conn.commit()
        
        log_action(
            current_user['id'],
            'delete_lead',
            'lead',
            lead_id,
            {'email': lead['email']}
        )
        
        cursor.close()
        conn.close()
        
        return jsonify({'message': 'Заявка удалена'}), 200
        
    except Exception as e:
        logger.error(f"Ошибка удаления заявки: {e}")
        return jsonify({'error': 'Внутренняя ошибка сервера'}), 500


# ============ ЛОГИ АДМИНИСТРАТОРА ============

@admin_bp.route('/logs', methods=['GET'])
@admin_required
def get_logs(current_user):
    """Получить логи действий администраторов"""
    try:
        print(f"[DEBUG /logs] Начало обработки запроса", flush=True)
        
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        
        if page < 1:
            page = 1
        if per_page < 1 or per_page > 200:
            per_page = 50
        
        offset = (page - 1) * per_page
        
        conn = get_connection()
        cursor = get_cursor(conn)
        
        cursor.execute("SELECT COUNT(*) FROM admin_logs")
        count_result = cursor.fetchone()
        total = count_result['count'] if count_result else 0
        
        cursor.execute("""
            SELECT al.id, al.action, al.target_type, al.target_id, 
                   al.details, al.ip_address, al.user_agent, al.created_at,
                   u.email as admin_email
            FROM admin_logs al
            LEFT JOIN users u ON al.admin_id = u.id
            ORDER BY al.created_at DESC
            LIMIT %s OFFSET %s
        """, (per_page, offset))
        logs = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        logs_list = []
        for l in logs:
            logs_list.append({
                'id': l['id'],
                'action': l['action'],
                'target_type': l['target_type'],
                'target_id': l['target_id'],
                'details': l['details'],
                'ip_address': l['ip_address'],
                'user_agent': l['user_agent'],
                'admin_email': l['admin_email'],
                'created_at': l['created_at'].isoformat() if l['created_at'] else None
            })
        
        return jsonify({
            'logs': logs_list,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'total_pages': (total + per_page - 1) // per_page
            }
        }), 200
        
    except Exception as e:
        print(f"[ERROR /logs] Ошибка: {type(e).__name__}: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Внутренняя ошибка сервера: {str(e)}'}), 500
