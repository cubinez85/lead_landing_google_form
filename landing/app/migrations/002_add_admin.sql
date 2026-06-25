-- Миграция: добавление админских возможностей
-- Запуск: psql -U leads_user -d leads_db -h localhost -f app/migrations/002_add_admin.sql

-- 1. Добавляем поле is_admin в таблицу users
ALTER TABLE users 
ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT FALSE;

-- 2. Добавляем поле is_active (для блокировки)
ALTER TABLE users 
ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;

-- 3. Делаем первого пользователя администратором
UPDATE users SET is_admin = TRUE WHERE email = 'cubinez85@cubinez.ru';

-- 4. Создаём таблицу логов действий администратора
CREATE TABLE IF NOT EXISTS admin_logs (
    id SERIAL PRIMARY KEY,
    admin_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    action VARCHAR(50) NOT NULL,
    target_type VARCHAR(50),
    target_id INTEGER,
    details JSONB,
    ip_address VARCHAR(45),
    user_agent TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_admin_logs_admin ON admin_logs(admin_id);
CREATE INDEX IF NOT EXISTS idx_admin_logs_action ON admin_logs(action);
CREATE INDEX IF NOT EXISTS idx_admin_logs_created ON admin_logs(created_at);

-- 5. Создаём таблицу сессий (для отслеживания активных входов)
CREATE TABLE IF NOT EXISTS user_sessions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    token_hash VARCHAR(255) NOT NULL,
    ip_address VARCHAR(45),
    user_agent TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_sessions_user ON user_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_token ON user_sessions(token_hash);

-- 6. Функция для логирования
CREATE OR REPLACE FUNCTION log_admin_action(
    p_admin_id INTEGER,
    p_action VARCHAR,
    p_target_type VARCHAR DEFAULT NULL,
    p_target_id INTEGER DEFAULT NULL,
    p_details JSONB DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    INSERT INTO admin_logs (admin_id, action, target_type, target_id, details)
    VALUES (p_admin_id, p_action, p_target_type, p_target_id, p_details);
END;
$$ LANGUAGE plpgsql;
