from celery.schedules import crontab

beat_schedule = {
    "health-check-every-60s": {
        "task": "app.tasks.periodic_health_check",
        "schedule": 60.0,
    },
}