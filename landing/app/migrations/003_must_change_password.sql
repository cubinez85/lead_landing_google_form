-- Миграция: обязательная смена пароля при первом входе

-- Добавляем поле must_change_password
ALTER TABLE users 
ADD COLUMN IF NOT EXISTS must_change_password BOOLEAN DEFAULT FALSE;

-- Для существующих пользователей, созданных через Google Forms, устанавливаем TRUE
UPDATE users SET must_change_password = TRUE WHERE created_at < NOW();

-- Админ не должен менять пароль принудительно
UPDATE users SET must_change_password = FALSE WHERE is_admin = TRUE;

COMMENT ON COLUMN users.must_change_password IS 'Флаг обязательной смены пароля при первом входе';
