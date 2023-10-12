from .ccr_updater import start
from rest_framework.views import APIView
from rest_framework.response import Response
from route.settings import LOGGING
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("WFOLogger")
import traceback

from .ccr_jobs import CCRIntergration
from .fir_jobs import FIR_Updater
from .tag_update_jobs import Tag_Updater
from .lexicon_schedular_jobs import Lexicon_updater
from .email_schedular import email_updater
class CCRTESTCHECK(APIView):
    def post(self, request, format=None):
        try:
            api_name = request.data.get('apiname')
            print('=================Here')
            # start()
            if api_name == 'CCR':
                print("running CCR")
                ccr_obj = CCRIntergration()
                ccr_obj.schedule_api()
            elif api_name == 'FIR':
                print("======running fir connect")
                fir_schedule_api_obj = FIR_Updater()
                fir_schedule_api_obj.schedule_fir_api()
            elif api_name == 'TAG':
                print("running TAG")
                tag_update_obj = Tag_Updater()
                tag_update_obj.schedule_tag_api()
            elif api_name == 'LEXICON':
                print("running LEXICON")
                lexicon_api_obj = Lexicon_updater()
                lexicon_api_obj.schedule_lexicon_api()
            elif api_name == 'EMAIL':
                print("inside email updater")
                email_api_obj = email_updater()
                email_api_obj.schedule_email_api()
            # df = ccr_obj.get_S3_data()
            return Response({"data": "after start executed"}, status=200)
        except Exception as e:
            logger.error(str(e),traceback.format_exc())
            return Response(data={"data": "Internal server error"},status=500)
