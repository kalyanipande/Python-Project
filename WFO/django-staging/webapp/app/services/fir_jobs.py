import requests
import json
from route import settings
from route.settings import LOGGING
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("WFOLogger")
from django.db import connection
import traceback

class FIR_Updater:
    def get_access_token(self):
        # Generate the fir_dataverse token by client_credentials
        token_url = settings.OAUTHTOKEN_ENDPOINT
        data = {"client_id": settings.CLIENT_ID,
                "client_secret": settings.CLIENT_SECRET,
                "scope ": settings.SCOPE,
                "grant_type": "client_credentials"}
        logger.info("The data is: {}".format(data))
        response = requests.post(token_url, data=data)
        logger.info("The response is {}".format(response))
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            logger.error("The response is {}".format(response))
            return

    def schedule_fir_api(self):
        try:
            token = self.get_access_token()
            logger.info("The access token is {}".format(token))
            # case_id = self.request.query_params.get('case_id', None)
            # logger.info("The requested case_id is {}".format(case_id))
            if token:
                access_token = token["access_token"]
                url = settings.API_URL
                import datetime
                today = ((str)(datetime.datetime.today())).split(" ")[0]
                prev_date = ((str)(datetime.datetime.today() - datetime.timedelta(days=2))).split(" ")[0]
                print("today : ", today)
                print("prev day :", prev_date)
                url = url + "?$filter=(createdon ge '" + prev_date + "T12:25:49Z') and (createdon le '" + today + "T11:38:23Z')"
                headers = {
                    'ContentType': 'application/x-www-form-urlencoded',
                    "Authorization": 'Bearer '+access_token,
                }
                response = requests.get(url,headers=headers)
                if response.text is not None and response.text != "" and response.status_code == 200:
                    data = json.loads(response.text)
                    logger.info("The Response Status Is {}".format(response.status_code))
                    if len(data["value"]) > 0:
                        print("data :", len(data["value"]))
                        cursor = connection.cursor()
                        for idx in range(len(data["value"])):
                            fir_casenumber = data["value"][idx]["crbdd_fir_casenumber"]
                            fir_initialtag = data["value"][idx]["crbdd_fir_initialtag"]
                            fir_description = data["value"][idx]["crbdd_fir_title"]
                            print("case number :",fir_casenumber)
                            print("fir_initialtag :", fir_initialtag)
                            print("fir_description :", fir_description)
                            select_query = (
                                "select fir_casenumber from app_fir_dataverse where fir_casenumber='{}' ".format(
                                    fir_casenumber))
                            cursor.execute(select_query)
                            row = cursor.fetchone()
                            if row is None:
                                print("inserting data")
                                try:
                                    cursor.execute(
                                    '''insert into app_fir_dataverse(fir_casenumber,fir_initialtag,fir_title) values(%s,%s,%s)''',
                                    (fir_casenumber, fir_initialtag, fir_description))
                                except Exception as e:
                                    print(str(e),traceback.format_exc())
                        connection.close()



        except Exception as e:
            logger.error(str(e),traceback.format_exc())