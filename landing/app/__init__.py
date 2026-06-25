from flask import Flask
from flask_cors import CORS
from app.routes.auth import auth_bp
from app.routes.admin import admin_bp


def create_app():
    """Фабрика Flask приложения"""
    app = Flask(__name__)
    
    # CORS - разрешаем все источники и заголовки
    CORS(app, resources={
        r"/*": {
            "origins": "*",
            "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            "allow_headers": ["Content-Type", "Authorization", "Accept"],
            "expose_headers": ["Content-Type"],
            "supports_credentials": False,
            "max_age": 3600
        }
    })
    
    # Регистрация Blueprint
    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp)
    
    return app
