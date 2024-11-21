from flask import Flask
from config import Config
from extensions import db
from flask_cors import CORS
def create_app():
# 初始化 Flask 应用和配置
  app = Flask(__name__)
  app.config.from_object(Config)
# 初始化数据库
  db.init_app(app)
  CORS(app)
# 注册控制器
  from controllers.scheduler_controller import schedular_controller
  from controllers.car_controller import page_controller
  app.register_blueprint(schedular_controller)
  app.register_blueprint(page_controller)
  return app
if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)

