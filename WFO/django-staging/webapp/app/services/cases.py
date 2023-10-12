import json
import boto3
import requests
import time
from django.conf import settings
from itertools import groupby
from app.enums import CATEGORY, STATUS
from app.models import CaseDetail
import pandas as pd
from io import StringIO
from uuid import UUID
import logging.config
import traceback

from app.models import Case, CaseTracker
from django.db.models import Q

logging.config.dictConfig(settings.LOGGING)
logger = logging.getLogger("WFOLogger")


class UUIDEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, UUID):
            # if the obj is uuid, we simply return the value of uuid
            return obj.hex
        return json.JSONEncoder.default(self, obj)

def create_case(data,case_instance,request):
    """
    call case api of DE to create or update case
    :param data:{
        "case_id": "CAS-headadasd",
        "input_text":"'2G Sites sectors down after BSC Software upgrade",
        "tag": "mcBSC"
        }
    :param case_instance: case model object
    :return: True or False
    """
    try:
        case_created = False
        post_data = {
            "userId": "sumitg",
            "requestId": "4567",
            "cases": []
        }
        data['case_id'] = str(case_instance.case_id_new)
        post_data['cases'].append(data)
        json_data = json.dumps(post_data)
        logger.info("The input data is:{}".format(json_data))
        logger.info("Data received from Angular")
        logger.info("Calling DE API")
        response = requests.post(settings.POST_CASE_URL, data=json_data)
        if response.status_code == 200 and response.json().get('status') == '200':
            case_created = True
        else:
            case_instance.status = STATUS.FAILURE
            case_instance.save(update_fields=['status'])

            case_trackers = CaseTracker.objects.filter(case_id_new=case_instance.case_id_new)
            for case_tracker in case_trackers:
                case_tracker.status = STATUS.FAILURE
                case_tracker.save(update_fields=['status'])

        logger.info("Received response")
        logger.info("The status code is {}".format(response.status_code))
        logger.info("The response is {}".format(response.text))
        return case_created
    except Exception as e:
        logger.error(str(e),traceback.format_exc())

def read_file_from_s3(prefix_file_name):
    """
    connect with s3 and read file if file exist
    and load it in json
    :param prefix_file_name: case_id
    :return: file_data in json format
    """
    file_exist = False
    data = []
    s3boto = boto3.resource(service_name='s3', aws_access_key_id=settings.ACCESS_KEY,
                            aws_secret_access_key=settings.S3_SECRET_KEY, endpoint_url=settings.ENDPOINT,
                            use_ssl=True, verify=False
                            )
    try:
        key_name = settings.RECOMMENDATIONS_PATH + "/" + prefix_file_name + '_recommendations.json'
        s3_obj = s3boto.Object(
            bucket_name=settings.BUCKET,
            key=key_name
            )
        response = s3_obj.get()
        data = json.loads(response['Body'].read())
        file_exist = True
    except Exception as e:
        logger.error(str(e),traceback.format_exc())
    return file_exist, data

def get_analysis_case_data(case_instance, frequency):
    """
    get analysis data from s3 and process it and
    save it in db and delete prev records
    :param case_instance:
    :return: case da

    """
    try:
        case_id = case_instance.case_id
        case_uuid = str(case_instance.case_id_new)
        case_status = case_instance.status
        file_exist, analysis_data = read_file_from_s3(case_uuid)
        bulk_list = []
        if not file_exist:
            return []
        for case_dict in analysis_data:
            case_detail = CaseDetail()
            case_detail.case_uuid = case_instance
            case_detail.order = case_dict.get('order')
            case_detail.frequency = case_dict.get('frequency (%)')
            action = case_dict.get('action')
            info_req = case_dict.get('information requests')
            if action:
                case_detail.category = CATEGORY.ACTION
                case_detail.suggestions = action
            elif info_req:
                case_detail.category = CATEGORY.INFORMATION_REQUEST
                case_detail.suggestions = info_req
            else:
                case_detail.category = CATEGORY.LOG_REQUEST
                case_detail.suggestions = case_dict.get('log requests')
                # case_detail.case = case_instance
            bulk_list.append(case_detail)
        case_instance.casedetail_set.all().delete()
        CaseDetail.objects.bulk_create(bulk_list)
        case_instance.status = STATUS.SUCCESS
        logger.info("case id is: {}".format(case_instance.case_id_new))
        case_instance.save(update_fields=['status'])

        case_trackers = CaseTracker.objects.filter(case_id_new=case_instance.case_id_new)
        for case_tracker in case_trackers:
            case_tracker.status = STATUS.FAILURE
            case_tracker.save(update_fields=['status'])

        detailed_data = detailed_case_data(case_instance, frequency)
        return detailed_data
    except Exception as e:
        logger.error(str(e),traceback.format_exc())

def detailed_case_data(case_instance, frequency):
    """
    groupby to fetch case detail data according to category
    :param case_instance: case model object
    :return: data in list of dict for every category
    i.e action , information request and log requests
    """
    data_dict = {}
    filter_kwargs = {}
    if frequency:
        filter_kwargs['frequency__gte'] = frequency
    case_detail_data = case_instance.casedetail_set.filter(
        **filter_kwargs
    ).order_by(
        'category', 'order', '-frequency'
    ).values(
        'suggestions',
        'order',
        'frequency',
        'category'
    )
    for key, key_group in groupby(case_detail_data, lambda data_obj: data_obj.get('category')):
        data_dict[key] = list(key_group)
    return data_dict

def update_status(case_instance):
    """
    if status is pending than check in s3 that
    file exist for the case , if it exist ,change
    status from pending to success.
    :param case_instance: case model instance
    :return: case instance
    """
    case_id = case_instance.case_id
    case_status = case_instance.status
    if case_status == STATUS.PENDING:
        file_exist, data = read_file_from_s3(case_id)
        if file_exist:
            case_instance.status = STATUS.SUCCESS
            case_instance.save(update_fields=['status'])

            case_trackers = CaseTracker.objects.filter(case_id_new=case_instance.case_id_new)
            for case_tracker in case_trackers:
                case_tracker.status = STATUS.FAILURE
                case_tracker.save(update_fields=['status'])

        if not file_exist:
            case_instance.status = STATUS.No_Similar_Cases_Found
            time.sleep(60)
            case_instance.save(update_fields=['status'])

            case_trackers = CaseTracker.objects.filter(case_id_new=case_instance.case_id_new)
            for case_tracker in case_trackers:
                case_tracker.status = STATUS.FAILURE
                case_tracker.save(update_fields=['status'])

    return case_instance


def get_case(case_status):
    val = None
    if case_status:
        if case_status.lower() == 'pending':
            val = 1
        elif case_status.lower() == 'success':
            val = 2
        elif case_status.lower() == 'no_similar_cases_found':
            val = 3
        elif case_status.lower() == 'failure':
            val = 4
        else:
            val = 5
    return val

def list_files_from_s3(file_name):
    """
    connect with s3 and read file if file exist
    and load it in json
    :param prefix_file_name: case_id
    :return: file_data in json format
    """
    try:
        logger.info("list out similar case ids from s3")
        records = []
        s3 = boto3.resource(service_name='s3', aws_access_key_id=settings.ACCESS_KEY,
                                aws_secret_access_key=settings.S3_SECRET_KEY, endpoint_url=settings.ENDPOINT,
                                use_ssl=True, verify=False
                                )
        path = settings.S3_SIMILAR_CASES_PATH
        bucket = settings.BUCKET
        s3_bucket = s3.Bucket(bucket)
        final_tag = "tags_{0}".format(file_name.lower())
        logger.info("tag file is: {}".format(final_tag))
        files_in_s3 = [f for f in s3_bucket.objects.filter(Prefix=path).all() if final_tag in f.key.lower()]
        logger.info("tag files from s3 : {}".format(files_in_s3))
        for obj in files_in_s3:
            body = obj.get()['Body'].read().decode('utf-8')
            content_dict = {}
            content = body.split("\n")
            content_dict["content_length"] = len(content)
            content_dict["content"] = content
            records.append(content_dict)
        return records
    except Exception as e:
        logger.error(str(e),traceback.format_exc())

def relevant_files_from_s3(file_name):
    """
    connect with s3 and read file if file exist
    and load it in json
    :param prefix_file_name: case_id
    :return: file_data in json format
    """
    try:
        logger.info("list out matching tag ids from s3")
        s3 = boto3.resource(service_name='s3', aws_access_key_id=settings.ACCESS_KEY,
                                aws_secret_access_key=settings.S3_SECRET_KEY, endpoint_url=settings.ENDPOINT,
                                use_ssl=True, verify=False
                                )

        path = settings.S3_SIMILAR_CASES_PATH
        bucket = settings.BUCKET
        s3_bucket = s3.Bucket(bucket)
        final_tag = "relevant_cases_{0}".format(file_name.lower())
        logger.info("relevant file is: {}".format(final_tag))
        files_in_s3 = [f for f in s3_bucket.objects.filter(Prefix=path).all() if final_tag in f.key.lower()]
        logger.info("relevant files from s3 : {}".format(files_in_s3))
        for obj in files_in_s3:
            body = obj.get()['Body'].read().decode('utf-8')
            col_names = ["Name", "CaseId", "Tag", "CaseDescription", "SNO"]
            df = pd.read_csv(StringIO(body), header=None, delimiter=",", names=col_names)
            df.rename(columns={0: "Name"}, inplace=True)
            df['CaseId'] = df['Name'].str.split(' ', expand=True)[0]
            df['Tag'] = df['Name'].str.split(' ', expand=True)[1]
            df['CaseDescription'] = df['Name'].str.split(' ').str[2:].apply(lambda x: ' '.join(x))
            df['SNO'] = df.reset_index().index
            logger.info("Dataframe columns:{}".format(df.columns))
            result = [{"caseid": x, "tag": y, "case_description": z, "sno": str(w)} for x, y, z, w in
                      zip(df['CaseId'], df['Tag'], df['CaseDescription'], df['SNO'])]
        return result
    except Exception as e:
        logger.error(str(e),traceback.format_exc())