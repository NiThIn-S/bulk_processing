import config
from gunicorn.app.base import BaseApplication


bind = f"0.0.0.0:{config.PORT}"
workers = int(config.WORKERS)
keepalive = 800
timeout = 600


class Application(BaseApplication):
    def load_config(self):
        s = self.cfg.set
        s('bind', bind)
        s('workers', workers)
        s('keepalive', keepalive)
        s('timeout', timeout)
        s('preload_app', True)
        s('worker_class', "uvicorn.workers.UvicornWorker")
        s('accesslog', None)
        s('errorlog', "-")
        s('loglevel', config.LOG_LEVEL)

    def load(self):
        from main import app
        return app


Application().run()
