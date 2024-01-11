from flask import Flask

def create_app():
    app = Flask(__name__)

    from .route import slack_bp
    app.register_blueprint(slack_bp)

    return app