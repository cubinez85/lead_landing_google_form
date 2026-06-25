#!/usr/bin/env python3
"""
Точка входа для Flask API (восстановление пароля)
Запускается параллельно с main.py (обработка заявок)
"""

import sys
import os
from dotenv import load_dotenv

# Загружаем .env из той же папки
load_dotenv()

from app import create_app

app = create_app()

if __name__ == '__main__':
    port = int(os.getenv('FLASK_PORT', '8087'))
    print(f"🚀 Запуск Password Reset API на порту {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
