import os, sys
# sys.path.append(os.getcwd())
from celery import Celery
from flask import current_app
from celery.schedules import timedelta


def make_celery(app):
    """
    Create and configure the Celery application instance.
    """
    celery = Celery(
        "backend",
        broker=app.config["CELERY_BROKER_URL"] if app else "redis://redis:6379/0",
        backend=app.config["CELERY_RESULT_BACKEND"] if app else "redis://redis:6379/0",
        timezone='UTC',
        include=[
            "src.Journals.views.tasks"
        ]
    )
    # ensure the schedule directory exists
    schedule_path = "./celery-beat/celerybeat-schedule"
    os.makedirs(os.path.dirname(schedule_path), exist_ok=True)
    celery.conf.beat_schedule_filename = schedule_path
    
    if app:
        celery.conf.update(app.config)
        
        class ContextTask(celery.Task):
            def __call__(self, *args, **kwargs):
                with app.app_context():
                    return self.run(*args, **kwargs)

        celery.Task = ContextTask

    return celery

celery = make_celery(current_app)
# Configure periodic tasks (Celery Beat)
# celery.conf.beat_schedule = {
#     "background-sync-protein-database": {
#         "task": "shared-task-sync-protein-database",
#         "schedule": timedelta(days=30),  # Every 5 minutes
#     }
# }