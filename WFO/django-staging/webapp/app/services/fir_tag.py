import requests
import json
from rest_framework.views import APIView
from rest_framework.response import Response
from route.settings import LOGGING
import logging.config

logging.config.dictConfig(LOGGING)
logger = logging.getLogger("WFOLogger")
import traceback
from route import settings
from django.db import connection


# here fir keys are used but not working so we have used ccr keys in below access token method
# def get_access_token():
#     # Generate the fir_dataverse token by client_credentials
#     token_url =settings.OAUTHTOKEN_ENDPOINT
#     data = {"client_id": settings.CLIENT_ID,
#             "client_secret": settings.CLIENT_SECRET,
#             "scope ": settings.SCOPE,
#             "grant_type":"client_credentials"}
#     logger.info("The data is: {}".format(data))
#     response = requests.post(token_url, data=data)
#     logger.info("The response is {}".format(response))
#     if response.status_code == 200:
#         return json.loads(response.text)
#     else:
#         logger.error("The response is {}".format(response))
#         return

class FirTagGenerator(APIView):
    """To Get the fir_initial_tag if it exist in the FIR Dataverse
    :query_params name: case_id
    :return: fir_initial_tag in json format
    """

    def get_access_token(self):
        # Generate the CCR_dataverse token by client_credentials
        token_url = settings.CCR_OAUTHTOKEN_ENDPOINT
        data = {"client_id": settings.CCR_CLIENT_ID,
                "client_secret": settings.CCR_CLIENT_SECRET,
                "scope ": settings.CCR_SCOPE,
                "grant_type": "client_credentials"}
        response = requests.post(token_url, data=data)
        logger.info("The response is {}".format(response))
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            logger.error("The response is {}".format(response))
            return

    def get(self, request, format=None):
        try:
            case_id = self.request.query_params.get('case_id', None)
            token = self.get_access_token()
            logger.info("The access token is {}".format(token))

            logger.info("The requested case_id is {}".format(case_id))
            if token:
                access_token = token["access_token"]
                url = settings.API_URL
                url = url + f"?$filter=(crbdd_fir_casenumber eq '{case_id}')"
                headers = {
                    'ContentType': 'application/x-www-form-urlencoded',
                    "Authorization": 'Bearer ' + access_token,
                }
                response = requests.get(url, headers=headers)
                if response.text is not None and response.text != "" and response.status_code == 200:
                    data = json.loads(response.text)
                    logger.info("The Response Status Is {}".format(response.status_code))
                    import pandas as pd
                    df = pd.DataFrame(data['value'])
                    if len(data["value"]) > 0:
                        print("data has some value")
                        for idx in range(len(data["value"])):
                            fir_casenumber = data["value"][idx]["crbdd_fir_casenumber"]
                            if fir_casenumber == case_id:
                                print("here=============================")
                                fir_initialtag = data["value"][idx]["crbdd_fir_initialtag"]
                                fir_description = data["value"][idx]["crbdd_fir_title"]
                                if fir_initialtag:
                                    return Response(
                                        {"fir_initialtag": fir_initialtag, "fir_description": fir_description},
                                        status=response.status_code)
            else:
                logger.error("The access token is {}".format(token) + " and Authentication got Failed")
                return Response(data={"data": "Internal server error"}, status=500)
            return Response({"data": "Could not find the Tag."
                                     "Please chose the tag from product list"}, status=202)

        except Exception as e:
            logger.error(str(e), traceback.format_exc())
            return Response(data={"data": "Internal server error"}, status=500)


class FirDescriptionGenerator(APIView):
    """To Get the crbdd_fir_title if it exist in the FIR Dataverse
    :query_params name: case_id
    :return: crbdd_fir_title in json format
    """

    def get(self, request, format=None):
        try:
            token = get_access_token()
            logger.info("The access token is {}".format(token))
            case_id = self.request.query_params.get('case_id', None)
            logger.info("The requested case_id is {}".format(case_id))
            if token:
                access_token = token["access_token"]
                url = settings.API_URL
                headers = {
                    'ContentType': 'application/x-www-form-urlencoded',
                    "Authorization": 'Bearer ' + access_token,
                }
                response = requests.get(url, headers=headers)
                if response.text is not None and response.text != "" and response.status_code == 200:
                    data = json.loads(response.text)
                    logger.info("The Response Status Is {}".format(response.status_code))
                    if len(data["value"]) > 0:
                        for idx in range(len(data["value"])):
                            fir_casenumber = data["value"][idx]["crbdd_fir_casenumber"]
                            if fir_casenumber == case_id:
                                crbdd_fir_title = data["value"][idx]["crbdd_fir_title"]
                                if crbdd_fir_title:
                                    return Response({"crbdd_fir_title": crbdd_fir_title, },
                                                    status=response.status_code)
            else:
                logger.error("The access token is {}".format(token) + " and Authentication got Failed")
                return Response(data={"data": "Internal server error"}, status=500)
            return Response({"data": "Could not find the Description."
                                     "Please enter manually in description filed"}, status=202)
        except Exception as e:
            logger.error(str(e), traceback.format_exc())
            return Response(data={"data": "Internal server error"}, status=500)
