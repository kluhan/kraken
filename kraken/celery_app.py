from celery import Celery

from . import celery_config

app = Celery("gpk")

app.config_from_object(celery_config)

if __name__ == "__main__":
    app.start()
