import numpy as np
import requests
import json
import pandas as pd
from django.conf import settings
import boto3
from route.settings import LOGGING
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("WFOLogger")
import traceback
from route import settings
from io import StringIO
from django.db import connection
import django.db
import re

class CCRIntergration:
    """
    Fetch the CCR table data and write it in the S3 as excel file.
    """
    def get_access_token(self):
        # Generate the CCR_dataverse token by client_credentials
        token_url =settings.CCR_OAUTHTOKEN_ENDPOINT
        data = {"client_id": settings.CCR_CLIENT_ID,
                "client_secret": settings.CCR_CLIENT_SECRET,
                "scope ": settings.CCR_SCOPE,
                "grant_type":"client_credentials"}
        response = requests.post(token_url, data=data)
        logger.info("The response is {}".format(response))
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            logger.error("The response is {}".format(response))
            return
    def get_S3_data(self):
        bucket = settings.BUCKET
        key_name = settings.S3_CONFIG_DATA_PATH + "dna_transformed/ccr_wfo_casesummary_proddata.csv"
        s3boto = boto3.resource(service_name='s3', aws_access_key_id=settings.ACCESS_KEY,
                                aws_secret_access_key=settings.S3_SECRET_KEY, endpoint_url=settings.ENDPOINT,
                                use_ssl=True, verify=False
                                )
        file_content = s3boto.Object(bucket, key_name).get()
        body = file_content['Body']
        csv_string = StringIO(body.read().decode('utf-8'))
        df = pd.read_csv(csv_string)
        return df


    def modificationproductname(self,tag):
        mapping_dict = {'LEAirScaleBSC':'AirScaleBSC',
                        'LEAirScaleRNC':'AirScaleRNC',
                        'LEBSC3i':'BSC3i',
                        'LEDCAP':'DCAP',
                        'LEFlexiBSC':'FlexiBSC',
                        'LEFlexiEDGEBaseStation':'FlexiEDGEBaseStation',
                        'LEFlexiMultiradioBTSTDLTE':'FlexiMultiradioBTSTDLTE',
                        'LEL3DataCollectorMegamon':'L3DataCollectorMegamon',
                        'LEmcBSC':'MulticontrollerBSC',
                        'LEmcRNC':'MulticontrollerRNC',
                        'LENetAct':'NetAct',
                        'LENetActCloud':'NetActCloud',
                        'LESingleRAN':'SingleRAN',
                        'LEWCDMABTS':'WCDMABTS',
                        'LEWCDMAOMS':'WCDMAOMS',
                        'LEWCDMARNC':'WCDMARNC',
                        'LE5G':'5G',
                        'LEEdenNet':'EdenNet',
                        'LELTEBTS':'LTEBTS'
                        }
        # print(type(tag),tag)
        for key in mapping_dict.keys():
            if tag.startswith(key):
                tag = tag.replace(key,mapping_dict[key],1)

        return tag
    def schedule_api(self):
        """
        Call schedule API to fetch CCR data
        """
        # Fetch the CCR_dataverse and insert it to excel file
        try:
            token = self.get_access_token()
            if token:
                access_token = token["access_token"]
                import datetime
                today = ((str)(datetime.datetime.today())).split(" ")[0]
                prev_date = ((str)(datetime.datetime.today() - datetime.timedelta(days=2))).split(" ")[0]

                url = settings.CCR_API_URL
                url = url + "?$filter=(createdon ge '" + prev_date + "T12:25:49Z') and (createdon le '" + today + "T11:38:23Z')"
                headers = {
                    'ContentType': 'application/x-www-form-urlencoded',
                    "Authorization": 'Bearer ' + access_token,
                }
                response = requests.get(url, headers=headers)
                result = response.json()
                data = result['value']
                if data:
                    columns_data = data[0].keys()
                    df = pd.DataFrame(data=data, columns=columns_data)
                    df_s3 = pd.DataFrame()
                    try:
                        df_s3 = self.get_S3_data()
                        df_s3 = df_s3.drop('Unnamed: 0', axis=1)
                    except Exception as e:
                        logger.error(str(e), traceback.format_exc())
                    logger.info("shape of df_s3 is : {}".format(df_s3.shape))
                    df['crbdd_ccr_sequencenumber'] = df['crbdd_ccr_sequencenumber'].astype(str)
                    df_s3['crbdd_ccr_sequencenumber'] = df_s3['crbdd_ccr_sequencenumber'].astype(str)
                    common = df.merge(df_s3, on=['crbdd_ccr_sequencenumber'])
                    logger.info("shape of common : {}".format(common.shape))
                    new_df = df[(~df.crbdd_ccr_sequencenumber.isin(common.crbdd_ccr_sequencenumber))]
                    df = new_df
                    logger.info("shape of unique value df : {}".format(df.shape))
                    case_number_lst = df['crbdd_ccr_casenumber'].to_list()
                    case_id_lst = df['crbdd_ccr_caseid'].to_list()
                    product_name_lst = df['crbdd_ccr_product'].to_list()
                    cursor = connection.cursor()
                    count = 0
                    for case_num, case_id,product_name in zip(case_number_lst, case_id_lst,product_name_lst):
                        if case_num is None:
                            case_num = ''
                        if case_id is None:
                            case_id = ''
                        case_num = (str)((int)(case_num))
                        product_name = re.sub('[^A-Za-z0-9]+', '', product_name)
                        if case_id != '' and case_num != '':
                            try:
                                cursor.execute('''insert into app_case_number_mapping(casenumber,caseidnumber,productname) values(%s,%s,%s)''',(case_num,case_id,product_name))
                                count = count + 1
                            except django.db.utils.IntegrityError:
                                logger.info("Case number already exists :{}".format(case_num))
                                select_query = (
                                    "select productname from app_case_number_mapping where casenumber='{}' ".format(case_num))
                                cursor.execute(select_query)
                                row = cursor.fetchone()
                                if row[0] is None:
                                    try:
                                        update_query = ("update app_case_number_mapping set productname='{}' where casenumber='{}' ").format(product_name, case_num)
                                        cursor.execute(update_query)
                                    except Exception as e:
                                        logger.info("not able to update product name {} {}".format( str(e), traceback.format_exc()))
                    logger.info("The number of documents inserted in mapping are :{}".format(count))

                    df_s3 = pd.concat([df, df_s3])
                    df_s3['new_tag'] = df_s3['crbdd_ccr_tag'].apply(lambda tag: self.modificationproductname(tag))

                    case_number_lst2 = df_s3['crbdd_ccr_casenumber'].to_list()
                    case_title_lst = df_s3['crbdd_ccr_structuredcasetitle'].to_list()
                    case_tag_lst = df_s3['crbdd_ccr_tag'].to_list()
                    release_version_lst = df_s3['crbdd_ccr_sf_productrelease'].to_list()

                    logger.info("length of case num 2  : {}".format(len(case_number_lst2)))

                    for case_num, case_title,case_tag,release_version in zip(case_number_lst2, case_title_lst,case_tag_lst,release_version_lst):
                        if case_num is None:
                            case_num = ''
                        if case_title is None:
                            case_title = ''
                        if case_tag is None:
                            case_tag=''
                        if release_version is None:
                            release_version=''
                        case_num = (str)((int)(case_num))
                        try:
                            select_query = (
                                "select casetitle,casetag,releaseversion from app_case_number_mapping where casenumber='{}' ".format(case_num))
                            cursor.execute(select_query)
                            row = cursor.fetchone()
                            if row[0] is None or row[1] is None or row[2] is None or row[2] is np.NaN:
                                try:
                                    update_query = (
                                        "update app_case_number_mapping set casetitle='{}' , casetag='{}',releaseversion='{}' where casenumber='{}' ").format(
                                        case_title,case_tag,release_version, case_num)
                                    cursor.execute(update_query)
                                except Exception as e:
                                    logger.info(
                                        "not able to update case title {} {}".format(str(e), traceback.format_exc()))
                        except Exception as e:
                            logger.info(
                                "case number not found {} {}".format(str(e), traceback.format_exc()))

                    logger.info("shape of df_s3 final is {}".format(df_s3.shape))
                    connection.close()
                    csv_buffer = StringIO()
                    df_s3.to_csv(csv_buffer)
                    bucket = settings.BUCKET
                    key_name = settings.S3_CONFIG_DATA_PATH + "dna_transformed/ccr_wfo_casesummary_proddata.csv"
                    logger.info("The key name is :{}".format(key_name))
                    s3boto = boto3.resource(service_name='s3', aws_access_key_id=settings.ACCESS_KEY,
                                            aws_secret_access_key=settings.S3_SECRET_KEY, endpoint_url=settings.ENDPOINT,
                                            use_ssl=True, verify=False
                                            )
                    s3boto.Object(bucket, key_name).put(Body=csv_buffer.getvalue())
                    logger.info("The response status_code is {}".format(response.status_code))
                else:
                    logger.info("The response data {} and the count is {}".format(data,len(data)))
            else:
                logger.error("The access token is {}".format(token) + " and Authentication got Failed")
        except Exception as e:
            logger.error(str(e), traceback.format_exc())
