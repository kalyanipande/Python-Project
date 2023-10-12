from django.apps import AppConfig


class UamConfig(AppConfig):
    name = 'uam'

    def ready(self):
        # To trigger the ccr_updater scheduler
        from app.services import ccr_updater
        ccr_updater.start()