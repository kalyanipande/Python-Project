import time
import logging.config
import traceback
import requests
import json
import pandas as pd
from django.conf import settings
from route.settings import LOGGING
# third-party imports
from rest_framework import status, viewsets, filters
from rest_framework.decorators import action
from rest_framework.response import Response
from app.api.serializers import CaseSerializer
from app.enums import STATUS
from app.models import Case, CaseTracker
from django.db.models import Q
from app.services.cases import (create_case, get_analysis_case_data,
                                update_status, get_case, list_files_from_s3,
                                relevant_files_from_s3)
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("WFOLogger")
from django.db import connection

class TestViewset(viewsets.ReadOnlyModelViewSet):
    """
    test api
    """
    queryset = []

    @action(methods=['get'], detail=False)
    def test_value(self, request, *args, **kwargs):
        """
        pull config values from settings
        by using the params
        """
        data = {
            "config_key": "test_value"
        }
        return Response(
            data,
            status=status.HTTP_200_OK
        )


class CasesViewset(viewsets.ModelViewSet):
    """
    creation , updation and analysis of cases
    """
    serializer_class = CaseSerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['case_id_new', 'case_id']
    ordering_fields = ['case_id', 'status']

    def get_queryset(self,**kwargs):
        """
        Optionally restricts the returned purchases to a given user,
        by filtering against a `username` query parameter in the URL.
        """
        # username = self.request.session.get("preferred_username", None)
        # logger.info("The logged user name is: {}".format(username))
        # queryset = Case.objects.filter(user_name=username).order_by('-modified_at')

        case_id_new = self.kwargs['pk']
        queryset = Case.objects.filter(case_id_new__iexact=case_id_new)
        case_status = self.request.query_params.get('status', None)

        if case_status:
            cas_val = get_case(case_status)
            queryset = queryset.filter(status=cas_val)
        return queryset

    def create(self, request, *args, **kwargs):
        """
        create update case and call DE api for case
        creation updation
        """
        case_id = request.data.get('case_id')
        case_id_new = request.data.get('case_id_new',None)
        input_text = request.data.get('input_text')
        username = user = self.request.session.get("preferred_username", None)
        casedetails = request.data
        casedetails["user_name"] = request.session.get("preferred_username", None)
        try:
            logger.info("create a case: {}".format(case_id))
            instance = Case.objects.get(case_id__iexact=case_id,input_text__iexact=input_text,
                                        user_name__iexact=user,case_id_new__iexact=case_id_new )
            instance.status = STATUS.PENDING
            serializer = self.get_serializer(instance, data=request.data)
        except Case.DoesNotExist as e:
            serializer = self.get_serializer(data=casedetails)
        serializer.is_valid(raise_exception=True)
        _case = case_instance = serializer.save()
        if _case.user_name == username:
            case_tracker = CaseTracker()
            case_tracker.case_id = _case.case_id
            case_tracker.case_id_new = _case.case_id_new
            case_tracker.input_text = _case.input_text
            case_tracker.tag = _case.tag
            case_tracker.user_name = username
            case_tracker.digimop_instance_id = _case.digimop_instance_id
            case_tracker.digimop_operation_id = _case.digimop_operation_id
            case_tracker.status = _case.status
            case_tracker.feedback = False
            case_tracker.is_owner = True if _case.user_name == username else False
            case_tracker.release = _case.release
            case_tracker.save()
        logger.info("after case tracker")
        old_tag = request.data['tag']
        mapping_dict = {'LEAirScaleBSC': 'AirScaleBSC',
                        'LEAirScaleRNC': 'AirScaleRNC',
                        'LEBSC3i': 'BSC3i',
                        'LEDCAP': 'DCAP',
                        'LEFlexiBSC': 'FlexiBSC',
                        'LEFlexiEDGEBaseStation': 'FlexiEDGEBaseStation',
                        'LEFlexiMultiradioBTSTDLTE': 'FlexiMultiradioBTSTDLTE',
                        'LEL3DataCollectorMegamon': 'L3DataCollectorMegamon',
                        'LEmcBSC': 'MulticontrollerBSC',
                        'LEmcRNC': 'MulticontrollerRNC',
                        'LENetAct': 'NetAct',
                        'LENetActCloud': 'NetActCloud',
                        'LESingleRAN': 'SingleRAN',
                        'LEWCDMABTS': 'WCDMABTS',
                        'LEWCDMAOMS': 'WCDMAOMS',
                        'LEWCDMARNC': 'WCDMARNC',
                        'LE5G': '5G',
                        'LEEdenNet': 'EdenNet',
                        'LELTEBTS': 'LTEBTS'
                        }
        for key in mapping_dict.keys():
            if old_tag.startswith(key):
                # print("=====here==========")
                new_tag = old_tag.replace(key, mapping_dict[key], 1)
                print("new tag :", new_tag)
                request.data['tag'] = new_tag

        release_v = request.data['release']
        if release_v == "nan":
            request.data['release']=""
        print("request data before DS : ", request.data)

        is_case_created = create_case(request.data, case_instance, request)
        logger.info("case creating {} , {}".format(case_id, case_instance.case_id_new))
        if is_case_created:
            data = {
                "data": "your request is being processed",
                "case_id": case_instance.case_id_new
            }
            return Response(
                data,
                status=status.HTTP_201_CREATED
            )
        return Response(
            data={"data": "Error while processing data"},
            status=status.HTTP_400_BAD_REQUEST
        )

    @action(detail=True)
    def case_analysis(self, request, *args, **kwargs):
        """
        get order, freq, suggestions data as per actions,
        info req. and logs of case
        """
        freq = request.query_params.get("frequency", 0)
        frequency = int(freq)
        logger.info("The entry frequency value is: {}".format(frequency))
        logger.info("case analysis api calling")

        username = request.session.get("preferred_username")
        case_id_new = kwargs.get('pk')

        _case = Case.objects.filter(case_id_new=case_id_new).first()
        if _case is not None:
            _case_tracker = CaseTracker.objects.filter(user_name=username, case_id_new=case_id_new).first()
            if _case_tracker is None and _case.user_name == username :
                case_tracker = CaseTracker()
                case_tracker.case_id = _case.case_id
                case_tracker.case_id_new = _case.case_id_new
                case_tracker.input_text = _case.input_text
                case_tracker.tag = _case.tag
                case_tracker.user_name = username
                case_tracker.digimop_instance_id = _case.digimop_instance_id
                case_tracker.digimop_operation_id = _case.digimop_operation_id
                case_tracker.status = _case.status
                case_tracker.feedback = False
                case_tracker.is_owner = True if _case.user_name == username else False
                case_tracker.save()
        try:
            api_url = 'http://{}:{}/request/search'.format(
                settings.QUERY_AGGREGATOR_URL,
                settings.QUERY_AGGREGATOR_PORT
            )
            logger.info("The QA url is {}".format(api_url))
            payload = {
                "queryType": "SearchAlarmWithZipID",
                "case_id": kwargs.get("pk"),
                "indexName": settings.ELASTIC_RECOMMEND_INDEX
            }
            # time.sleep(5)
            recallapi_attempts = 10
            result = {"data": {"1": [], "2": []}}
            while recallapi_attempts>0:
                r = requests.post(url=api_url, json=payload)
                logger.info("The QA request  is {}".format(r))
                if r.text is not None and r.text != "" and r.status_code == 200:
                    data = json.loads(r.text)
                    logger.info("The QA recommendation data is: {}".format(data))
                    # result = {"data": {"1": [], "2": []}}
                    if len(data["rows"]) > 0 :
                        for row in data["rows"][0]["data"]:
                            if row["frequency (%)"] >= frequency:
                                if "action" in row.keys():
                                    logger.info("The result action data came from new ML model")
                                    res = {"frequency": row["frequency (%)"], "suggestions": row["action"],"order": row["order"],"helpfullness": row["helpfulness (%)"], "cases": row["cases"]}
                                    result["data"]["1"].append(res)
                                elif "log" in row.keys():
                                        logger.info("The result log data came from new ML model")
                                        res = {"frequency": row["frequency (%)"], "suggestions": row["log"],"order": row["order"],"helpfullness": row["helpfulness (%)"], "cases": row["cases"]}
                                        result["data"]["2"].append(res)

                        cursor = connection.cursor()
                        result["data"]["cases_mapping"] = []
                        for loop in range(1, 3):
                            for i in range(len(result["data"][str(loop)])):
                                temp = result["data"][str(loop)][i]
                                if isinstance(temp["cases"], list):
                                    for j in range(len(temp["cases"])):
                                        case_num = temp["cases"][j]
                                        case_num = (str)((int)(case_num))
                                        case_id = ''
                                        select_query = ("select caseidnumber from app_case_number_mapping where casenumber='{}' ".format(case_num))
                                        cursor.execute(select_query)
                                        row = cursor.fetchone()
                                        if row is not None:
                                            case_id = row[0]
                                        result["data"]["cases_mapping"].append({temp["cases"][j]: case_id})
                                else:
                                    case_id=''
                                    case_num = temp["cases"]
                                    case_num = (str)((int)(case_num))
                                    select_query = ("select caseidnumber from app_case_number_mapping where casenumber='{}' ".format(case_num))
                                    cursor.execute(select_query)
                                    row = cursor.fetchone()
                                    if row is not None:
                                        case_id = row[0]
                                    result["data"]["cases_mapping"].append({temp["cases"]: case_id})

                        return Response(data=result,status=status.HTTP_200_OK)
                    else:
                        logger.info("The recallapi_attempts value is {}",format(recallapi_attempts))
                        recallapi_attempts = recallapi_attempts-1
                elif r.status_code == 500:
                    logger.info("The response is {}".format(r.text))
                    logger.info("The status code is {}".format(r.status_code))
                    return Response(
                        data={"data": "Internal server error in QA"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )
            else:
                return Response(data=result,status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(str(e),traceback.format_exc())
            return Response(
                data={"data": "Internal server error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    @action(detail=True)
    def similar_id_count(self, request, *args, **kwargs):
        """
        To get similar count if file exist in ES
        """
        logger.info("similar count api calling")
        try:
            api_url = 'http://{}:{}/request/search'.format(
                settings.QUERY_AGGREGATOR_URL,
                settings.QUERY_AGGREGATOR_PORT
            )
            payload = {
                "queryType": "TotalCount",
                "case_id": kwargs.get("pk"),
                "indexName": settings.ELASTIC_SIMILAR_INDEX
            }
            r = requests.post(url=api_url, json=payload)
            logger.info("QA request  is {}".format(r.content))
            if r.text is not None and r.text != "" and r.status_code == 200:
                data = r.text
                logger.info(" QA data  is {}".format(data))
                final_data = {"data": [{"content_length": data}]}
                logger.info(" final count  is {}".format(final_data))
                return Response(
                    data=final_data,
                    status=status.HTTP_200_OK
                )
            elif r.status_code == 500:
                logger.info("The response is {}".format(r.text))
                logger.info("The status code is {}".format(r.status_code))
                return Response(
                    data={"data": "Internal server error in QA"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            elif r.status_code == 204:
                logger.info("The response is {}".format(r.text))
                logger.info("The status code is {}".format(r.status_code))
                return Response(
                    data={"data": "No Data is Available"},
                    status=status.HTTP_204_NO_CONTENT
                )
        except Exception as e:
            logger.error(str(e),traceback.format_exc())
            return Response(
                data={"data": "Internal server error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    @action(detail=True)
    def matching_tag_table(self, request, *args, **kwargs):
        """
        To get all similar case id's with description if file exist in ES
        """
        logger.info("matching tag  api calling")
        try:
            api_url = 'http://{}:{}/request/search'.format(
                settings.QUERY_AGGREGATOR_URL,
                settings.QUERY_AGGREGATOR_PORT
            )
            payload = {
                "queryType": "WfoRelevantCases",
                "case_id": kwargs.get("pk"),
                "indexName": settings.ELASTIC_RELEVANT_INDEX
            }
            r = requests.post(url=api_url, json=payload)
            logger.info("QA request is {}".format(r))
            if r.text is not None and r.text != "" and r.status_code == 200:
                data = json.loads(r.text)
                logger.info("QA data is {}".format(data))
                logger.info("data type is {}".format(type(data)))
                col_names = ["caseId", "description", "SN", "tagName"]
                flag=0
                try:
                    exact_matching = data["exact_matching"]
                    flag=1
                except Exception as e:
                    logger.error(str(e), traceback.format_exc())

                df = pd.DataFrame(data["data"], columns=col_names)
                if df.empty:
                    return Response(
                        data={"data": []},
                        status=status.HTTP_200_OK
                    )
                logger.info("Dataframe is {}".format(df))

                df1 = df.rename(columns={"caseId": "caseid","tagName": "tag","description": "case_description", "SN": "sno"})
                result = df1.to_json(orient="records")
                final_data = json.loads(result)

                cursor = connection.cursor()
                for i in range(len(final_data)):
                    temp = final_data
                    # print(temp[i])
                    case_num = temp[i]["caseid"]
                    case_num = (str)((int)(case_num))
                    caseidnumber = ''
                    casetag = ''
                    casetitle = ''
                    releaseversion=''
                    select_query = (
                        "select caseidnumber,casetag,casetitle,releaseversion from app_case_number_mapping where casenumber='{}' ".format(
                            case_num))
                    cursor.execute(select_query)
                    row = cursor.fetchone()
                    if row is not None:
                        caseidnumber = row[0]
                        casetag = row[1]
                        casetitle = row[2]
                        releaseversion=row[3]

                    final_data[i]["cases_mapping"] = caseidnumber
                    final_data[i]["tag"] = casetag
                    final_data[i]["case_description"] = casetitle
                    final_data[i]["releaseversion"] = releaseversion
                if flag==1:
                    return Response(
                        data={"data": final_data,"exact_matching":exact_matching},
                        status=status.HTTP_200_OK
                    )
                else:
                    return Response(
                        data={"data": final_data},
                        status=status.HTTP_200_OK
                    )

            elif r.status_code == 500:
                logger.info("The response is {}".format(r.text))
                logger.info("The status code is {}".format(r.status_code))
                return Response(
                    data={"data": "Internal server error in QA"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
        except Exception as e:
            logger.error(str(e),traceback.format_exc())
            return Response(
                data={"data": "Internal server error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )