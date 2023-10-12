import os
import time

from django.views.decorators.csrf import csrf_exempt
from rest_framework.views import APIView
import logging.config
import traceback
import requests
import json
import pandas as pd
# from django.conf import settings
from route.settings import LOGGING
from route import settings
# third-party imports
from rest_framework import status, viewsets, filters
from rest_framework.decorators import action
from rest_framework.response import Response
from app.api.serializers import CaseSerializer
from app.enums import STATUS
from app.models import Case
from app.services.cases import (create_case, get_analysis_case_data,
                                update_status, get_case, list_files_from_s3,
                                relevant_files_from_s3)
from .permissions import IsActive
from ..conf import ENVIRONMENT

logging.config.dictConfig(LOGGING)
logger = logging.getLogger("WFOLogger")


# v1_address='https://gateway.ibus-fp.net/'
v1_address=ENVIRONMENT["digimops_config"]["digimops_url"]
digimop_user = ENVIRONMENT["digimops_config"]["user"]
digimop_password = ENVIRONMENT["digimops_config"]["password"]
# base_url = ENVIRONMENT["digimops_config"]["base_url"]


class DigimopsAPI(APIView):
    permission_classes = (IsActive,)

    serializer_class = CaseSerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['case_id_new', 'case_id']
    ordering_fields = ['case_id', 'status']

    def get_queryset(self, **kwargs):
        case_id_new = self.kwargs['pk']
        queryset = Case.objects.filter(case_id_new__iexact=case_id_new)
        case_status = self.request.query_params.get('status', None)

        if case_status:
            cas_val = get_case(case_status)
            queryset = queryset.filter(status=cas_val)
        return queryset

    def get_access_token(self):
        token = None
        try:
            payload = {"username": digimop_user, "password": digimop_password, "grant_type": "password",
                       "client_id": "clientTokenAccess"}
            url = v1_address + "token"
            logger.info("The url is: {}".format(url))
            headers = {'content-type': 'application/x-www-form-urlencoded'}
            logger.info("call the post to generate the token")
            r = requests.post(url, headers=headers, data=payload, verify=False)
            logger.info("call is DOne the post to generate the token")
            response = r.json()
            logger.info("The response is ",response)
            if r.status_code == 200:
                logger.info("The response is: {}".format(response))
                json_str = json.dumps(response)
                resp = json.loads(json_str)
                token = (resp['access_token'])
                logger.info("The token: {}".format(str(token)))
                return token
            else:
                logger.error("The Error is: {}".format(response))
        except Exception as e:
            logger.error(str(e), traceback.format_exc())


    @csrf_exempt
    def post(self, request, *args, **kwargs):
        try:
            # base_url =f'https://wfo-test-validation-staging.americas.abi.dyn.nesc.nokia.net/' \
            #                f'#/pages/table/case-recomendation/'
            base_url = settings.WFO_DATA_URL
            logger.info("The digimops base url is {} ".format(str(base_url)))
            case_id = request.data.get('case_id')
            case_id_new = request.data.get('case_id_new', None)
            input_text = request.data.get('input_text')
            user = request.data.get('user', None)
            digimop_instance_id = request.data.get('digimop_instance_id',"")
            digimop_operation_id = request.data.get('digimop_operation_id')
            freq = request.query_params.get("frequency", 0)
            frequency = int(freq)
            logger.info("The entry frequency value is: {}".format(frequency))
            logger.info("case analysis api calling")
            if digimop_instance_id == "" or digimop_instance_id == None:
                digimop_instance_id='default'
                request.data['digimop_instance_id']=digimop_instance_id
            if digimop_operation_id == "" or digimop_operation_id == None:
                digimop_operation_id='default'
                request.data['digimop_operation_id']=digimop_operation_id
            casedetails = request.data
            casedetails["user_name"] = user
            result = {"actions": [], "logs": []}
            df = pd.DataFrame()
            try:
                logger.info("create a case: {}".format(case_id))
                instance = Case.objects.get(case_id__iexact=case_id, input_text__iexact=input_text,
                                            case_id_new__iexact=case_id_new, user_name__iexact=user,
                                            digimop_instance_id__iexact=digimop_instance_id)
                instance.status = STATUS.PENDING
                serializer = CaseSerializer(instance, data=request.data)
            except Case.DoesNotExist as e:
                serializer = CaseSerializer(data=request.data)
            serializer.is_valid(raise_exception=True)
            case_instance = serializer.save()
            request.data['release']=""
            is_case_created = create_case(request.data, case_instance, request)
            logger.info("case creating {} , {}".format(case_id, case_instance.case_id_new))
            case_recommendations_url = base_url + str(case_instance.case_id_new)
            logger.info("The case recommendations url is : {}".format(case_recommendations_url))
            timeout = time.time() + 60 * 3  # 3 minutes from now
            if is_case_created:
                while True:
                    status_instance = Case.objects.get(case_id_new__iexact=str(case_instance.case_id_new)).status
                    case_status_value = status_instance
                    logger.info("The case status value is:{} ".format(case_status_value))
                    if case_status_value in [2,3,4,5] or time.time() >= timeout:
                        break
                    time.sleep(2)
                if case_status_value == 2:
                    Case.objects.filter(case_id_new=case_instance.case_id_new).update(feedback=True)
                    status_data = "success"
                    message = 'Recommendations Generated'
                    try:
                        api_url = 'http://{}:{}/request/search'.format(
                            settings.QUERY_AGGREGATOR_URL,
                            settings.QUERY_AGGREGATOR_PORT
                        )
                        logger.info("The elastic url is {}".format(api_url))
                        payload = {
                            "queryType": "SearchAlarmWithZipID",
                            "case_id": (str)(case_instance.case_id_new),
                            "indexName": settings.ELASTIC_RECOMMEND_INDEX
                        }
                        time.sleep(15)
                        recallapi_attempts = 10
                        while recallapi_attempts>0:
                            r = requests.post(url=api_url, json=payload)
                            logger.info("The Elastic request  is {}".format(r))
                            if r.text is not None and r.text != "" and r.status_code == 200:
                                data = json.loads(r.text)
                                logger.info("The elastic  data is: {}".format(data))
                                if len(data["rows"]) > 0 :
                                    for row in data["rows"][0]["data"]:
                                        if row["frequency (%)"] >= frequency:
                                            if "action" in row.keys():
                                                logger.info("The result action data came from new ML model")
                                                res = {"frequency": row["frequency (%)"], "suggestions": row["action"],"order": row["order"],"helpfullness": row["helpfulness (%)"], "cases": row["cases"]}
                                                result["actions"].append(res)
                                            elif "log" in row.keys():
                                                logger.info("The result log data came from new ML model")
                                                res = {"frequency": row["frequency (%)"], "suggestions": row["log"],"order": row["order"],"helpfullness": row["helpfulness (%)"], "cases": row["cases"]}
                                                result["logs"].append(res)
                                    break
                                else:
                                    logger.info("The recallapi_attempts value is {}".format(recallapi_attempts))
                                    recallapi_attempts = recallapi_attempts-1
                            elif r.status_code == 500:
                                logger.info("The response in elastic is {}".format(r.text))
                                logger.info("The status code in elastic is {}".format(r.status_code))
                                break
                    except Exception as e:
                        logger.error(str(e), traceback.format_exc())

                    logger.info("matching tag  api calling")
                    try:
                        api_url = 'http://{}:{}/request/search'.format(
                            settings.QUERY_AGGREGATOR_URL,
                            settings.QUERY_AGGREGATOR_PORT
                        )
                        payload = {
                            "queryType": "WfoRelevantCases",
                            "case_id": (str)(case_instance.case_id_new),
                            "indexName": settings.ELASTIC_RELEVANT_INDEX
                        }
                        r = requests.post(url=api_url, json=payload)
                        logger.info("Elastic Matching search{}".format(r))
                        if r.text is not None and r.text != "" and r.status_code == 200:
                            data = json.loads(r.text)
                            logger.info("Elastic search data is {}".format(data))
                            logger.info("data type is {}".format(type(data)))
                            col_names = ["caseId", "description", "SN", "tagName"]
                            df = pd.DataFrame(data["data"], columns=col_names)
                        if df.empty:
                            logger.info("Dataframe is {}".format(df))
                    except Exception as e:
                        logger.error(str(e), traceback.format_exc())

                elif case_status_value == 3:
                    status_data = "failed"
                    message = 'Failed Due to No Similar Cases Found, Please click below WFO URL to re-submit with updated inputs.'
                elif case_status_value == 4:
                    status_data = "failed"
                    message = 'Failed Due to Internal Error'
                    case_recommendations_url = ''
                elif case_status_value == 5:
                    status_data = "failed"
                    message = "Failed due to insufficient similar cases for recommendations, Please click below WFO URL to re-submit with updated inputs."
                else:
                    status_data = "failed"
                    message = "TIME OUT ERROR OCCURRED"
                    case_recommendations_url = ''
                if message:
                    df1 = df.rename(
                        columns={"caseId": "caseid", "tagName": "tag", "description": "case_description",
                                 "SN": "sno"})
                    result2 = df1.to_json(orient="records")
                    final_data = json.loads(result2)
                    data = {
                        "status": status_data,
                        "message": message,
                        "digimop_operation_id": case_instance.digimop_operation_id,
                        "case_recommendations_url": case_recommendations_url,
                        "recommendations": result,
                        "matching_tags": final_data
                    }
                    logger.info("The sending data is:{}".format(data))
                    return Response(data={"data":data},
                                        status=status.HTTP_201_CREATED)
            else:
                logger.info("The case is not created and error occurred")
                return Response(
                    data={"data": "Error while processing data"},
                    status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            logger.error(str(e), traceback.format_exc())


    def post_data(self,data, instance_id, token):
        try:
            headers = {'Authorization': 'Bearer ' + str(token), 'Content-Type': 'application/json'}
            form_data = {"Data": [{"Skipped": False, "InputData": {"Response_Body": data},
                                   "DigimopOperationId": "id__37__380__598__nokia-alice-get-status___1"}, ],
                         "UserCredentials": {}}
            # logger.info("form_data is: {}".format(form_data))
            url = v1_address + "v1/workflow/instance/" + str(instance_id) + "/resume"
            logger.info("the digimop url is ",url)
            # logger.info("from_data type is : {}".format(type(form_data)))
            # form_data = json.dumps(form_data)
            # logger.info("form_data: {}".format(form_data))
            # print({"url":url,"form_data":form_data,"headers":headers})
            r = requests.post(url, data=form_data, headers=headers, verify=False)
            logger.info("The digimop response code is ", r.status_code)
            if r.status_code == 200:
                logger.info("The digimop response code is 200 and ",r.text)
                return r.json()
            else:
                logger.info("The digimop FAILED response is ",r.status_code)
        except Exception as e:
            logger.error(str(e), traceback.format_exc())
