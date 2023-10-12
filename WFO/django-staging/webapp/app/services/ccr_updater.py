from apscheduler.schedulers.background import BackgroundScheduler
from .ccr_jobs import CCRIntergration
from .fir_jobs import FIR_Updater
from .tag_update_jobs import Tag_Updater
from .lexicon_schedular_jobs import Lexicon_updater
from .email_schedular import email_updater

def start():
	print("ccr updater started")
	# It will be triggered from uam.apps file
	scheduler = BackgroundScheduler(timezone="UTC",max_instances=1)
	ccr_obj = CCRIntergration()
	fir_schedule_api_obj = FIR_Updater()
	tag_update_api_obj = Tag_Updater()
	lexicon_api_obj = Lexicon_updater()
	email_api_obj = email_updater()
	scheduler.add_job(ccr_obj.schedule_api,trigger='cron', hour='05', minute='00')
	# scheduler.add_job(fir_schedule_api_obj.schedule_fir_api, trigger='cron', hour='06', minute='00')
	scheduler.add_job(tag_update_api_obj.schedule_tag_api, trigger='cron', hour='07', minute='00')
	scheduler.add_job(lexicon_api_obj.schedule_lexicon_api, trigger='cron', day_of_week='mon', hour='08', minute='00')
	#testing purpose
	# scheduler.add_job(email_api_obj.schedule_email_api, trigger='interval' , minutes=2,max_instances=1,replace_existing=True)

	# scheduler.add_job(email_api_obj.schedule_email_api, trigger='cron', day_of_week='mon', hour='09', minute='00',max_instances=1,replace_existing=True)

	scheduler.start()

