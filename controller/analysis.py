from collections import OrderedDict

import xlsxwriter

from ..dao.LogBundleDAO import *
from ..modules.Imports import *
from ..modules.settings import *

# import numpy as np
import pandas as pd
from es.elastic.api import connect
from django.conf import settings as ss
import logging.config
from datetime import datetime
from dateutil.parser import parse
import math

logging.config.dictConfig(ss.LOGGING)
logger = logging.getLogger("AliceLogger")


def get_all_categories_of_syslog(zip_id):
    elk_ml_index = 'ca-syslog'
    body = {
        "size" : 0,
        "query" : {
            "term" : {
                "zip_id.keyword" : {
                    "value" : "%s" % zip_id,
                    "boost" : 1.0
                }
            }
        },
        "_source" : False,
        "stored_fields" : "_none_",
        "aggregations" : {
            "groupby" : {
                "composite" : {
                    "size" : 1000,
                    "sources" : [
                        {
                            "category_name" : {
                                "terms" : {
                                    "field" : "category.keyword",
                                    "missing_bucket" : True,
                                    "order" : "asc"
                                }
                            }
                        }
                    ]
                }
            }
        }
    }
    logger.info('GET %s/_search' % elk_ml_index)
    logger.info('%s' % json.dumps(body))
    categories = []
    try:
        res = get_elastic_data(elk_ml_index, body)
        for bucket in res['aggregations']['groupby']['buckets']:
            categories.append(bucket['key']['category_name'])
    except Exception as e:
        logger.error(traceback.format_exc())
    return categories


# @analysis_ns.route("/gu_products")
# @analysis_ns.doc(parser=req_token_header,body=req_upload_products)
class GetUploadProducts(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get Products for Selected User
        """
        return MicroservicePOST(request)


# @analysis_ns.route("/gup_bundles")
# @analysis_ns.doc(parser=req_token_header,body=req_upload_bundles)
class GetUploadBundles(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get Bundle List for Selected User nd Product
        """
        return MicroservicePOST(request)


# @analysis_ns.route("/gupb_ml")
# @analysis_ns.doc(parser=req_token_header,body=req_upload_ml_list)
class GetMLList(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get ML Details
        """
        return MicroservicePOST(request)

class Get_CA_csvdownload(APIView):
    permission_classes = (IsActive,)

    def get_filenamewithdirectory(self, name):
        splittedpath = name.split('@')[-1]
        splittedname = splittedpath.split('/')
        if splittedpath.count("/") >= 4:

            parent = splittedname[-3] if splittedname[-2].endswith('--xz') else splittedname[-2]
            dir = parent.split("--")[0]
            dir_name = dir[dir.index("_") + 1:] if "_" in dir else dir

            filename = dir_name + "_" + splittedname[-1]
        else:
            filename = splittedname[-1]

        return filename


    @csrf_exempt
    def get(self, request, zip_id):
        """
        Get CSV Downloads
        """

        _clone_zip_id = zip_id
        query = f"""
            select coalesce(parent_zip_id, zip_id) as zip_id
            from pm_log_files
            where zip_id='{_clone_zip_id}'
            limit 1
        ;
        """
        _pm_log_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
        if len(_pm_log_details) > 0:
            zip_id = _pm_log_details[0]['zip_id']

        try:

            resp = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            resp['Content-Disposition'] = "attachment; filename="+zip_id+".xlsx"
            logger.info("getting component analysis data")
            query="SELECT c.composite_name,(SELECT json_agg(jvalue) FROM (SELECT json_build_object('log_files_id', b.id,'file_name', replace(b.input_file,'/ephemeral',''),'type', b.type,'is_proactive_composite_file', is_proactive_composite_file,'is_proactive_individual_file', is_proactive_individual_file) AS jvalue FROM log_files b WHERE b.pc_log_files_id = a.pc_log_files_id AND (b.is_proactive_individual_file = 1 or b.is_proactive_composite_file = 1) AND a.composite_id = b.composite_id ORDER BY b.is_proactive_composite_file DESC, b.is_proactive_individual_file DESC) a2) as childs FROM log_files a INNER JOIN pc_log_files pc ON pc.log_files_id = a.id INNER JOIN composites c ON c.composite_id = pc.composite_id WHERE a.is_proactive_composite_file = 1 AND a.zip_id='"+zip_id+"' ORDER BY c.composite_name ASC;"
            result = connectFetchJSONWihtoutQueryDataNoResponse(query)
            logger.info("Size of result")
            logger.info(len(result))
            length = len(result)

            if length > 0:

                workbook = xlsxwriter.Workbook(resp,{'in_memory':True})
                bold = workbook.add_format({'bold': 1})
                headings = ['Repitition Count', 'Unit', 'Type', 'Signature']
                cursorrow = 1

                df = pd.DataFrame()
                logger.info("Inside loop")
                for i in range(length):
                    composite = result[i]
                    if not composite['composite_name'] == 'syslog':
                        for child in composite["childs"]:
                            if child['is_proactive_individual_file'] == 1:
                                p = child['file_name']
                                filename = self.get_filenamewithdirectory(p)
                                child["file_name"] = filename
                            else:
                                p = child['file_name']
                                filename = p.split("/")[-1]
                                child["file_name"] = filename
                            child["composite_name"] = composite["composite_name"]
                            df = df.append(child, ignore_index=True)
                if not df.empty:
                    df["log_files_id"] = df['log_files_id'].apply(int)
                    df["log_files_id"] = df['log_files_id'].apply(str)
        
                    query1 = {
                        "from": 0, "size": 10000,
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "terms": {
                                            "file_id.keyword": df['log_files_id'].tolist()
                                        }
                                    },
                                    {
                                        "term": {
                                            "zip_id.keyword": {
                                                "value": zip_id
                                            }
                                        }
                                    }
                                ]
                            }
                        },
                        "_source": {
                            "includes": [
                                "*"
                            ],
                            "excludes": []
                        },
                        "sort": [
                            {
                                "repetition_count": {
                                    "order": "desc",
                                    "missing": "_first",
                                    "unmapped_type": "keyword"
                                }
                            }
                        ]
                    }
                    logger.info("GET component-analysis-query1 ")
                    logger.info(query1)
                    result1 = get_elastic_data("component-analysis-query1", query1)
                    maintable = pd.DataFrame()

                    if result1 is not False:
                        if not result1['hits']['total']['value'] == 0:
                            logger.info("Data available in elastic")
                            i = 0
                            for hit in result1['hits']['hits']:
                                maintable = maintable.append(hit['_source'],ignore_index=True)
                            maintable = maintable.merge(df, how="left", left_on="file_id", right_on="log_files_id")
                            print(maintable.columns)
                            maintablecopy = maintable.copy()
                            groupby_compname = maintablecopy.groupby(['composite_name'])
                            dfg = [groupby_compname.get_group(x) for x in groupby_compname.groups]

                            for df2 in dfg:
                                if not df2.empty:
                                    df2 = df2.reset_index()
                                    sheetname = df2["composite_name"][0]
                                    print(sheetname)
                                    worksheet = workbook.add_worksheet(df2["composite_name"][0])
                                    cursorrow = 1
                                    df2copy = df2.copy()
                                    groupby_fileid = df2copy.groupby(['file_id'])
                                    dfg2 = [groupby_fileid.get_group(y) for y in groupby_fileid.groups]
                                    for df3 in dfg2:
                                        if not df3.empty:
                                            df3.sort_values(by=["repetition_count"], ascending=False, inplace=True)
                                            df3 = df3.reset_index()
                                            filename = df3["file_name"][0]
                                            print(df3)

                                            worksheet.write('A' + str(cursorrow), df3["file_name"][0], bold)
                                            cursorrow = cursorrow + 1
                                            worksheet.write_row('A' + str(cursorrow), headings, bold)
                                            cursorrow = cursorrow + 1
                                            worksheet.write_column('A' + str(cursorrow), df3['repetition_count'])
                                            worksheet.write_column('B' + str(cursorrow), df3['unit'])
                                            worksheet.write_column('C' + str(cursorrow), df3['type_x'])
                                            worksheet.write_column('D' + str(cursorrow), df3['signature'])
                                            chart1 = workbook.add_chart({'type': 'pie'})
                                            rowcount = df3.shape[0] if df3.shape[0] < 10 else 10

                                            chart1.add_series({
                                                'data_labels': {'series_name': False},

                                                'categories': [sheetname, cursorrow-1, 3, cursorrow + rowcount -2, 3],
                                                'values': [sheetname, cursorrow-1, 0, cursorrow + rowcount-2, 0],
                                            })
                                            chart1.set_title({'name': 'Top 10 signatures present in ' + filename})
                                            chart1.set_style(10)
                                            chart1.set_size({'width': 1050, 'height': 370})
                                            worksheet.insert_chart('K' + str(cursorrow), chart1, {'x_offset': 25, 'y_offset': 10})

                                            if rowcount < 20:
                                                cursorrow = cursorrow + df3.shape[0] + 15
                                            else:
                                                cursorrow = cursorrow + df3.shape[0] + 5

                df = pd.DataFrame()
                logger.info("Inside loop")
                for i in range(length):
                    composite = result[i]
                    if composite['composite_name'] == 'syslog':
                        categories = get_all_categories_of_syslog(zip_id)
                        for category_name in categories:
                            if category_name is not None:
                                child = {
                                    'category_name': category_name,
                                    'type': 'category'
                                }
                                df = df.append(child, ignore_index=True)
                if not df.empty:
                    query1 = {
                        "from": 0, "size": 10000,
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "terms": {
                                            "category.keyword": df['category_name'].tolist()
                                        }
                                    },
                                    {
                                        "term": {
                                            "zip_id.keyword": {
                                                "value": zip_id
                                            }
                                        }
                                    }
                                ]
                            }
                        },
                        "_source": {
                            "includes": [
                                "*"
                            ],
                            "excludes": []
                        },
                        "sort": [
                            {
                                "repetition_count": {
                                    "order": "desc",
                                    "missing": "_first",
                                    "unmapped_type": "keyword"
                                }
                            }
                        ]
                    }
                    logger.info("GET ca-syslog-query1 ")
                    logger.info(query1)
                    result1 = get_elastic_data("ca-syslog-query1", query1)
                    maintable = pd.DataFrame()

                    if result1 is not False:
                        if not result1['hits']['total']['value'] == 0:
                            logger.info("Data available in elastic")
                            i = 0
                            for hit in result1['hits']['hits']:
                                maintable = maintable.append(hit['_source'],ignore_index=True)
                            # file_id : category
                            # input_file_id : category_name
                            maintable = maintable.merge(df, how="left", left_on="category", right_on="category_name")
                            print(maintable.columns)
                            maintablecopy = maintable.copy()
                            groupby_compname = maintablecopy.groupby(['category'])
                            dfg = [groupby_compname.get_group(x) for x in groupby_compname.groups]

                            for df2 in dfg:
                                if not df2.empty:
                                    df2 = df2.reset_index()
                                    worksheet = None
                                    sheetname = 'syslog'
                                    try:
                                        worksheet = workbook.add_worksheet(sheetname)
                                        cursorrow = 1
                                    except:
                                        worksheet = workbook.get_worksheet_by_name(sheetname)
                                    df2copy = df2.copy()
                                    groupby_fileid = df2copy.groupby(['category'])
                                    dfg2 = [groupby_fileid.get_group(y) for y in groupby_fileid.groups]
                                    for df3 in dfg2:
                                        if not df3.empty:
                                            df3.sort_values(by=["repetition_count"], ascending=False, inplace=True)
                                            df3 = df3.reset_index()
                                            category = df3["category"][0]
                                            print(df3)

                                            worksheet.write('A' + str(cursorrow), category, bold)
                                            cursorrow = cursorrow + 1
                                            worksheet.write_row('A' + str(cursorrow), headings, bold)
                                            cursorrow = cursorrow + 1
                                            worksheet.write_column('A' + str(cursorrow), df3['repetition_count'])
                                            worksheet.write_column('B' + str(cursorrow), df3['unit'])
                                            worksheet.write_column('C' + str(cursorrow), df3['type_x'])
                                            worksheet.write_column('D' + str(cursorrow), df3['signature'])
                                            chart1 = workbook.add_chart({'type': 'pie'})
                                            rowcount = df3.shape[0] if df3.shape[0] < 10 else 10

                                            chart1.add_series({
                                                'data_labels': {'series_name': False},

                                                'categories': [sheetname, cursorrow-1, 3, cursorrow + rowcount -2, 3],
                                                'values': [sheetname, cursorrow-1, 0, cursorrow + rowcount-2, 0],
                                            })
                                            chart1.set_title({'name': 'Top 10 signatures present in ' + category})
                                            chart1.set_style(10)
                                            chart1.set_size({'width': 1050, 'height': 370})
                                            worksheet.insert_chart('K' + str(cursorrow), chart1, {'x_offset': 25, 'y_offset': 10})

                                            if rowcount < 20:
                                                cursorrow = cursorrow + df3.shape[0] + 15
                                            else:
                                                cursorrow = cursorrow + df3.shape[0] + 5

                workbook.close()
                logger.info("Xlsx generated")
                # resp = HttpResponse(output.read(), mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                # resp['Content-Disposition'] = "attachment; filename="+zip_id+".xlsx"
                return resp

            else:
                json_data = {"data": [], "message": "Data not available", "status": 200}
                return response_wrapper(200, json_data)
        except:
            logger.error(traceback.format_exc())
            json_data = {"data": [], "message": "Data not available", "status": 200}
            return response_wrapper(500, json_data)


class GetComponentDrilldown(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        try:
            req = request
            data = getRequestJSON(req)
            zip_id = data["zip_id"]
            file_id = data["input_file_id"]
            logger.info("Zip id {}".format(zip_id))
            logger.info("File id {}".format(file_id))
            logger.info("getting component analysis drilldown data")

            query1 = {
                "from": 0, "size": 10000,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "file_id.keyword": {
                                        "value": file_id
                                    }
                                }
                            },
                            {
                                "term": {
                                    "zip_id.keyword": {
                                        "value": zip_id
                                    }
                                }
                            }
                        ]
                    }
                },
                "_source": {
                    "includes": [
                        "repetition_count",
                        "signature",
                        "unit",
                        "type"
                    ],
                    "excludes": []
                },
                "sort": [
                    {
                        "repetition_count": {
                            "order": "desc",
                            "missing": "_first",
                            "unmapped_type": "keyword"
                        }
                    }
                ]
            }
            logger.info("GET component-analysis-query1 ")
            logger.info(query1)
            result1 = get_elastic_data("component-analysis-query1", query1)
            maintable = list()
            piechart = list()

            if result1 == False:
                json_data = {"data": [], "message": "Data not available", "status": 200}
                return response_wrapper(200, json_data)
            else:
                if result1['hits']['total']['value'] == 0:
                    json_data = {"data": [], "message": "Data not available", "status": 200}
                    return response_wrapper(200, json_data)
                else:
                    i = 0
                    for hit in result1['hits']['hits']:
                        maintable.append(hit['_source'])
                    df = pd.DataFrame(maintable)
                    df.rename(columns={"signature": "name", "repetition_count": "y"}, inplace=True)
                    df2 = df.groupby('name').agg({'y':'sum'}).reset_index().sort_values(['y'], ascending=False)
                    piechart = df2.to_dict('records')[:10]
                        # if i < 10:
                        #     piechart.append({"name": hit['_source']['signature'], "y": hit['_source']['repetition_count']})
                        #     i += 1

            query2 = {
                "size": 0,
                "aggs": {
                    "topfivetable": {
                        "filter": {
                            "bool": {
                                "must": [
                                    {
                                        "term": {
                                            "file_id.keyword": {
                                                "value": file_id
                                            }
                                        }
                                    },
                                    {
                                        "term": {
                                            "zip_id.keyword": {
                                                "value": zip_id
                                            }
                                        }
                                    }
                                ]
                            }
                        },
                        "aggs": {
                            "Signature": {
                                "terms": {
                                    "field": "signature.keyword",
                                    "size": 10000
                                },
                                "aggs": {
                                    "top": {
                                        "top_hits": {
                                            "_source": {
                                                "includes": [
                                                    "message_count",
                                                    "signature",
                                                    "unit",
                                                    "type",
                                                    "complete_message"
                                                ]
                                            },
                                            "sort": [
                                                {
                                                    "message_count": {
                                                        "order": "desc",
                                                        "missing": "_first",
                                                        "unmapped_type": "keyword"
                                                    }
                                                }
                                            ],
                                            "size": 5
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            logger.info("GET component-analysis-query2 ")
            logger.info(query2)
            result2 = get_elastic_data("component-analysis-query2", query2)
            topfivetable = {}
            if result2 == False:
                json_data = {"data": [], "message": "Data not available", "status": 200}
                return response_wrapper(200, json_data)
            else:
                if result2['hits']['total']['value'] == 0:
                    json_data = {"data": [], "message": "Data not available", "status": 200}
                    return response_wrapper(200, json_data)
                else:
                    for hit in result2['aggregations']['topfivetable']['Signature']['buckets']:
                        sourcelist = list()
                        for source in hit['top']['hits']['hits']:
                            sourcelist.append(source['_source'])
                        topfivetable[hit['key']]=sourcelist
            json_data = {"data": {"maintable": maintable, "piechart": piechart, "topfivetable": topfivetable}, "message": "", "status": "200"}
            return response_wrapper(200, json_data)

        except:
            logger.error(traceback.format_exc())
            json_data = {"data": [], "message": "Data not available", "status": 500}
            return response_wrapper(500, json_data)

class GetComponentDrilldownNew(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        try:
            req = request
            data = getRequestJSON(req)
            
            _clone_zip_id = data['zip_id']
            _clone_input_file_id = data['input_file_id']

            # query = f"""
            # select coalesce(
            #     pc_log_files.parent_log_files_id,
            #     pi_log_files.parent_log_files_id,
            #     pc_log_files.log_files_id,
            #     pi_log_files.log_files_id
            # ) as log_files_id,
            # coalesce(
            #     pc_log_files.parent_zip_id,
            #     pi_log_files.parent_zip_id,
            #     pc_log_files.zip_id,
            #     pi_log_files.zip_id
            # ) as zip_id
            # from pc_log_files, pi_log_files
            # where (pc_log_files.zip_id='{_clone_zip_id}' and pc_log_files.log_files_id = '{_clone_input_file_id}') 
            #     or (pi_log_files.zip_id='{_clone_zip_id}' and pi_log_files.log_files_id = '{_clone_input_file_id}')
            # """
            # _log_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
            # if len(_log_details) > 0:
            #     data['input_file_id'] = _log_details[0]['log_files_id']
            #     data['zip_id'] = zip_id = _log_details[0]['zip_id']

            query = f"""
                select parent_log_files_id, log_files_id, 
                    parent_zip_id, zip_id
                from pi_log_files
                where zip_id='{_clone_zip_id}' and log_files_id = '{_clone_input_file_id}'
                limit 1
            ;
            """
            _pi_log_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
            if len(_pi_log_details) > 0:
                if _pi_log_details[0]['parent_log_files_id'] is not None:
                    data['input_file_id'] = _pi_log_details[0]['parent_log_files_id']
                    data['zip_id'] = zip_id = _pi_log_details[0]['parent_zip_id']
            
            query = f"""
                select parent_log_files_id, log_files_id, 
                    parent_zip_id, zip_id
                from pc_log_files
                where zip_id='{_clone_zip_id}' 
                    and (log_files_id = '{_clone_input_file_id}' or parent_log_files_id = '{_clone_input_file_id}')
                limit 1
            ;
            """
            _pc_log_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
            if len(_pc_log_details) > 0:
                if _pc_log_details[0]['parent_log_files_id'] is not None:
                    data['input_file_id'] = _pc_log_details[0]['parent_log_files_id']
                    data['zip_id'] = zip_id = _pc_log_details[0]['parent_zip_id']

            print('data', data)

            zip_id = data["zip_id"]
            file_id = data["input_file_id"]
            max_result = data["max_result"] if "max_result" in data else 10
            try:
                max_result = int(max_result)
            except :
                max_result = 10
            logger.info("Zip id {}".format(zip_id))
            logger.info("File id {}".format(file_id))
            logger.info("getting component analysis drilldown data")

            maintable = []
            piechart = []
            api_url = 'http://{}:{}/request/search'.format(
                settings.ENVIRONMENT[settings.env]['spark_config']['QA_IP'],
                settings.ENVIRONMENT[settings.env]['spark_config']['QA_PORT']
            )
            payload ={
                "zipID": zip_id,
                "indexName": "component-analysis-query1",
                "file_id": str(file_id),
                "queryType": "SearchForCAQuary1"
            }
            logger.info(api_url)
            logger.info(payload)
            r = requests.post(url=api_url, json=payload)
            if r.text is not None and r.text != "":
                logger.info("Response Received from QA")
                try:
                    return_dict = json.loads(r.text)
                    maintable = return_dict['rows']
                    if len(maintable) == 0:
                        json_data = {"data": [], "message": "Data not available in qa", "status": 200}
                        return response_wrapper(200, json_data)
                    df = pd.DataFrame(maintable)
                    df.rename(columns={"signature": "name", "repetition_count": "y"}, inplace=True)
                    df2 = df.groupby('name').agg({'y':'sum'}).reset_index().sort_values(['y'], ascending=False)
                    df2 = df2.sort_values(['y'], ascending=[False])

                    piechart = df2.to_dict('records')[:max_result]
                except:
                    logger.error(traceback.format_exc())
                    json_data = {"data": [], "message": "Data not available", "status": 500}
                    return response_wrapper(500, json_data)


            topfiletable = {}
            api_url = 'http://{}:{}/request/search'.format(
                settings.ENVIRONMENT[settings.env]['spark_config']['QA_IP'],
                settings.ENVIRONMENT[settings.env]['spark_config']['QA_PORT']
            )
            payload ={
                "zipID": zip_id,
                "indexName": "component-analysis-query2",
                "file_id": str(file_id),
                "queryType": "SearchForCAQuary2"
            }
            logger.info(api_url)
            logger.info(payload)
            r = requests.post(url=api_url, json=payload)
            if r.text is not None and r.text != "":
                logger.info("Response Received from QA")
                try:
                    return_dict = json.loads(r.text)
                    result = return_dict['rows']
                    if len(result) == 0:
                        json_data = {"data": [], "message": "Data not available in qa", "status": 200}
                        return response_wrapper(200, json_data)

                    df = pd.DataFrame(result)
                    signature_names = list(set(df['signature'].to_list()))

                    for signature_name in signature_names:
                        df_new = df[df['signature'].astype('str') == signature_name]
                        topfiletable[signature_name] = df_new.to_dict('records')
                except:
                    logger.error(traceback.format_exc())
                    json_data = {"data": [], "message": "Data not available", "status": 500}
                    return response_wrapper(500, json_data)

            json_data = {"data": {"maintable": maintable, "piechart": piechart, "topfivetable": topfiletable}, "message": "", "status": "200"}
            return response_wrapper(200, json_data)

        except:
            logger.error(traceback.format_exc())
            json_data = {"data": [], "message": "Data not available", "status": 500}
            return response_wrapper(500, json_data)

class GetComponentDrilldownSys(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        try:
            req = request
            data = getRequestJSON(req)
            
            if 'zip_id' in data:
                _clone_zip_id = data['zip_id']
                query = f"""
                    select coalesce(parent_zip_id, zip_id) as zip_id
                    from pm_log_files
                    where zip_id='{_clone_zip_id}'
                    limit 1
                ;
                """
                _pm_log_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(_pm_log_details) > 0:
                    data['zip_id'] = _pm_log_details[0]['zip_id']
            
            zip_id = data["zip_id"]
            
            category = data["category"] if "category" in data else None
            if category == "composite":
                category = None
            max_result = data["max_result"] if "max_result" in data else 10
            try:
                max_result = int(max_result)
            except :
                max_result = 10
            logger.info("Zip id {}".format(zip_id))
            logger.info("Category {}".format(category))
            logger.info("getting component syslog analysis drilldown data")

            query1 = {
                "from": 0, "size": 10000,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "zip_id.keyword": {
                                        "value": zip_id
                                    }
                                }
                            }
                        ]
                    }
                },
                "_source": {
                    "includes": [
                        "repetition_count",
                        "signature",
                        "unit",
                        "type"
                    ],
                    "excludes": []
                },
                "sort": [
                    {
                        "repetition_count": {
                            "order": "desc",
                            "missing": "_first",
                            "unmapped_type": "keyword"
                        }
                    }
                ]
            }
            
            if category is not None:
                query1['query']['bool']['must'].append({
                    "term": {
                        "category.keyword": {
                            "value": category
                        }
                    }
                })
            
            logger.info("GET ca-syslog-query1 ")
            logger.info(query1)
            result1 = get_elastic_data("ca-syslog-query1", query1)
            maintable = list()
            piechart = list()

            if result1 == False:
                json_data = {"data": [], "message": "Data not available", "status": 200}
                return response_wrapper(200, json_data)
            else:
                if result1['hits']['total']['value'] == 0:
                    json_data = {"data": [], "message": "Data not available", "status": 200}
                    return response_wrapper(200, json_data)
                else:
                    i = 0
                    for hit in result1['hits']['hits']:
                        maintable.append(hit['_source'])
                    df = pd.DataFrame(maintable)
                    df.rename(columns={"signature": "name", "repetition_count": "y"}, inplace=True)
                    df2 = df.groupby('name').agg({'y':'sum'}).reset_index().sort_values(['y'], ascending=False)
                    piechart = df2.to_dict('records')[:max_result]
                    # if i < 10:
                    #     piechart.append({"name": hit['_source']['signature'], "y": hit['_source']['repetition_count']})
                    #     i += 1

            query2 = {
                "size": 0,
                "aggs": {
                    "topfivetable": {
                        "filter": {
                            "bool": {
                                "must": [
                                    {
                                        "term": {
                                            "zip_id.keyword": {
                                                "value": zip_id
                                            }
                                        }
                                    }
                                ]
                            }
                        },
                        "aggs": {
                            "Signature": {
                                "terms": {
                                    "field": "signature.keyword",
                                    "size": 10000
                                },
                                "aggs": {
                                    "top": {
                                        "top_hits": {
                                            "_source": {
                                                "includes": [
                                                    "message_count",
                                                    "signature",
                                                    "unit",
                                                    "type",
                                                    "complete_message"
                                                ]
                                            },
                                            "sort": [
                                                {
                                                    "message_count": {
                                                        "order": "desc",
                                                        "missing": "_first",
                                                        "unmapped_type": "keyword"
                                                    }
                                                }
                                            ],
                                            "size": 5
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if category is not None:
                query2['aggs']['topfivetable']['filter']['bool']['must'].append({
                    "term": {
                        "category.keyword": {
                            "value": category
                        }
                    }
                })

            logger.info("GET ca-syslog-query2 ")
            logger.info(query2)
            result2 = get_elastic_data("ca-syslog-query2", query2)
            topfivetable = {}
            if result2 == False:
                json_data = {"data": [], "message": "Data not available", "status": 200}
                return response_wrapper(200, json_data)
            else:
                if result2['hits']['total']['value'] == 0:
                    json_data = {"data": [], "message": "Data not available", "status": 200}
                    return response_wrapper(200, json_data)
                else:
                    for hit in result2['aggregations']['topfivetable']['Signature']['buckets']:
                        sourcelist = list()
                        for source in hit['top']['hits']['hits']:
                            sourcelist.append(source['_source'])
                        topfivetable[hit['key']]=sourcelist
            json_data = {"data": {"maintable": maintable, "piechart": piechart, "topfivetable": topfivetable}, "message": "", "status": "200"}
            return response_wrapper(200, json_data)

        except:
            logger.error(traceback.format_exc())
            json_data = {"data": [], "message": "Data not available", "status": 500}
            return response_wrapper(500, json_data)

class GetComponentAnalysis(APIView):
    permission_classes = (IsActive,)

    
    @csrf_exempt
    def post(self, request):
        try:
            req = request
            data = getRequestJSON(req)
            if 'zip_id' in data:
                _clone_zip_id = data['zip_id']
                query = f"""
                    select coalesce(parent_zip_id, zip_id) as zip_id
                    from pm_log_files
                    where zip_id='{_clone_zip_id}'
                    limit 1
                ;
                """
                _pm_log_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(_pm_log_details) > 0:
                    data['zip_id'] = _pm_log_details[0]['zip_id']
            zip_id = data["zip_id"]
            logger.info("getting component analysis data")
            query="SELECT c.composite_name,(SELECT json_build_object('file_name', regexp_replace(t.input_file,'.+/',''), 'log_files_id', t.id) FROM log_files t WHERE t.pm_log_files_id = a.pm_log_files_id AND t.is_proactive_master_file = 1 LIMIT 1) as master_file_details,(SELECT json_agg(jvalue) FROM (SELECT json_build_object('log_files_id', b.id,'file_name', replace(b.input_file,'/ephemeral',''),'type', b.type,'is_proactive_composite_file', is_proactive_composite_file,'is_proactive_individual_file', is_proactive_individual_file) AS jvalue FROM log_files b WHERE b.pc_log_files_id = a.pc_log_files_id AND (b.is_proactive_individual_file = 1 or b.is_proactive_composite_file = 1) AND a.composite_id = b.composite_id ORDER BY b.is_proactive_composite_file DESC, b.is_proactive_individual_file DESC) a2) as childs FROM log_files a INNER JOIN pc_log_files pc ON pc.log_files_id = a.id INNER JOIN composites c ON c.composite_id = pc.composite_id WHERE a.is_proactive_composite_file = 1 AND a.zip_id='"+zip_id+"' ORDER BY c.composite_name ASC;"
            result = connectFetchJSONWihtoutQueryDataNoResponse(query)
            lenght = len(result)
            for i in range(lenght):
                composite = result[i]
                if composite['composite_name'] == 'syslog':
                    _child = {}
                    for child in composite["childs"]:
                        if child['is_proactive_composite_file'] == 1:
                            p = child['file_name']
                            filename = p.split("/")[-1]
                            child["file_name"] = filename
                            
                            # child["type"] = "category"
                            # child["category_name"] = "composite"
                            # del child["log_files_id"]
                            # del child["is_proactive_composite_file"]
                            # del child["is_proactive_individual_file"]
                            
                            _child = child
                    result[i]['childs'] = [ _child ]
                    categories = get_all_categories_of_syslog(zip_id)
                    for category_name in categories:
                        if category_name is not None:
                            result[i]['childs'].append({
                                'file_name': category_name,
                                'category_name': category_name,
                                'type': 'category'
                            })
                    continue
                for child in composite["childs"]:
                    if child['is_proactive_composite_file'] == 1:
                        p = child['file_name']
                        filename = p.split("/")[-1]
                        child["file_name"] = filename
                    else:
                        child["file_name"] = self.get_filenamewithdirectory(child['file_name'])
            json_data = {"data": result, "message": "", "status": 200}
            logger.info(json_data)
            return response_wrapper(200, json_data)

        except:
            logger.error(traceback.format_exc())
            json_data = {"data": [], "message": "Data not available", "status": 200}
            return response_wrapper(500, json_data)

    def get_filenamewithdirectory(self, name):
        splittedpath = name.split('@')[-1]
        splittedname = splittedpath.split('/')
        if splittedpath.count("/") >= 4:
            parent = splittedname[-3] if splittedname[-2].endswith('--xz') else splittedname[-2]
            dir = parent.split("--")[0]
            dir_name = dir[dir.index("_") + 1:] if "_" in dir else dir
            filename = dir_name + "_" + splittedname[-1]
        else:
            filename = splittedname[-1]
        return filename


# @analysis_ns.route("/gsearch")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_gsearch)
class GetGeneralSearch(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get Global Search
        """
        # import msal
        # import requests
        try:
            req = request
            # logger.info("Analysis started")
            # config = {
            #     "client_id": "ae227b16-090c-4801-84c6-30cd44cceb8c",
            #     "secret": "_I189Pe7lwl46XSY-k9m~~J5NT9L-xgF~R",
            #     "authority": "https://login.microsoftonline.com/nokia.onmicrosoft.com",
            #     "scope": ["https://graph.microsoft.com/.default"],
            #     "group_id": "9f347923-62b8-4598-b93b-56178e90828a",
            # }
            # app = msal.ConfidentialClientApplication(config["client_id"], authority=config["authority"],
            #                                          client_credential=config["secret"])
            #
            # result = None
            # result = app.acquire_token_silent(config["scope"], account=None)
            # # import pdb; pdb.set_trace()
            # if not result:
            #     result = app.acquire_token_for_client(scopes=config["scope"])
            # access_token = result["access_token"]
            # headers = {"Authorization": "Bearer {0}".format(access_token)}
            # url = "https://graph.microsoft.com/v1.0/groups/{0}/drive/root/children".format(config["group_id"])
            # response = requests.get(url, headers=headers)
            # logger.info("Graph request response ---- ")
            # logger.info(response.content)
            # users_url = "https://graph.microsoft.com/v1.0/users"
            # user_response_data = requests.get(users_url, headers=headers)
            # logger.info("user_response_data ---- ")
            # logger.info(user_response_data.content)
            # logger.info("Analysis ended")
            data = getRequestJSON(req)
            filter_name = data["filter_name"]
            page_no = data['page_no'] if 'page_no' in data else 1
            page_size = data['page_size'] if 'page_size' in data else 10
            #
            # page_no = str(page_no)
            # page_size = str(page_size)

            filterConditions = []
            if "filter_name" in data:
                filterConditions.append(" concat(b.prod_name,' ', c.username, ' ' ,a.log_name,' ',a.tag_name) ilike '%" + filter_name + "%' ")
            if "products" in data:
                filterConditions.append(" b.prod_id in ('" + "','".join(data["products"]) + "') ") if len(data['products']) > 0 else print("")
            if "users" in data:
                filterConditions.append(" c.user_id in ('" + "','".join(data["users"]) + "') ") if len(data['users']) > 0 else print("")
            if "intra_groups" in data:
                if isinstance(data["intra_groups"], list):
                    filterConditions.append(" a.intra_groups in ('" + "','".join(data["intra_groups"]) + "') ") if len(data['intra_groups']) > 0 else print("")
                if isinstance(data["intra_groups"], str):
                    filterConditions.append(" a.intra_groups ilike '" + data["intra_groups"] + "' ") if len(data['intra_groups']) > 0 else print("")
            if "bundles" in data:
                filterConditions.append(" a.id in ('" + "','".join(data["bundles"]) + "') ") if len(data['bundles']) > 0 else print("")
            if "tag_name" in data:
                filterConditions.append(" a.tag_name in ('" + "','".join(data["tag_name"]) + "') ") if len(data['tag_name']) > 0 else print("")
            filterConditionsStr = ' and '.join(filterConditions)
            offset = (page_no - 1) * page_size 
            limit = page_size 

            query = f"""
                select a.id as zip_id,a.case_id,fir.fir_title as case_title,c.username as user_name,a.user_id as user_id,b.prod_name as prod_name,
                    a.prod_id as prod_id,a.log_name,a.tag_name,cast(gl.status_msg::text as jsonb),a.archive_status,
                    a.associated_log_bundles::text[],a.intra_groups,
                    case 
                        when g.status_msg is null then false 
                        else true
                    end as show_ml,
                    case when es.emailstatusarr='completed' then true else false end as show_feedback,
                    es.status as is_completed, a.real_zip_id, a.last_accessed_at::varchar, a.is_reprocess_bundle
                from log_bundle a 
                left join global_status g on a.id=g.zip_id and g.status_msg ilike 'ML Job Execution Completed%'
                left join products b on a.prod_id=b.prod_id 
                left join users c on a.user_id=c.user_id 
                left join emailstatus es on a.id=es.zip_id 
                left join (select zip_id,json_agg(status_msg order by id) as status_msg from global_status group by zip_id) gl on gl.zip_id=a.id
                left join app_fir_dataverse fir on fir.fir_casenumber=a.case_id
                where {filterConditionsStr} and c.isdeleted=0 and a.isdeleted!=1 
                group by a.id ,c.username ,a.user_id,b.prod_name,a.prod_id,a.log_name,a.tag_name,
                    gl.status_msg::text,a.archive_status,g.status_msg,es.status,es.emailstatusarr,
                    a.real_zip_id, a.last_accessed_at, a.is_reprocess_bundle, fir.fir_title
                order by to_timestamp(right(a.zip_name, 19), 'YYYY-MM-DD HH24:MI:SS'::text) desc
                offset {offset} limit {limit}
            """
            logger.info("gsearch_query:" + query)
            resp = connectFetchJSONWithoutQueryData(query)
            resp_dict = json.loads(resp.content)
            total_rows = 0

            query = f"""
                select count(*)
                from log_bundle a
                left join products b on a.prod_id=b.prod_id
                left join users c on a.user_id=c.user_id
                left join status d on a.status=d.id
                left join (select zip_id,json_agg(status_msg order by id) as status_msg from global_status group by zip_id) gl on gl.zip_id=a.id
                where {filterConditionsStr} and c.isdeleted=0 and a.isdeleted!=1
            """
            logger.info("gsearch_count: " + query)
            gtotlaRows = connectFetchJSONWihtoutQueryDataNoResponse(query)
            if len(gtotlaRows) > 0:
                total_rows = gtotlaRows[0]["count"]
            
            l = len(resp_dict['data'])
            for i in range(l):
                data = resp_dict['data'][i]
                data['parent_bundle'] = False
                if data['real_zip_id'] is None:
                    try:
                        import datetime as dt
                        year, month, day = data['last_accessed_at'].split(' ')[0].split('-')
                        today = dt.date.today()
                        date = dt.date(int(year), int(month), int(day))
                        if ((today - date).days < 7):
                            data['parent_bundle'] = True
                    except:
                        print(traceback.format_exc())                

                associated_log_ids = data['associated_log_bundles']
                associated_log_bundles = []
                log_bundle_dao = LogBundleDAO()
                for bundle_id in (associated_log_ids or []):
                    log_bundle = log_bundle_dao.get_log_bundle(bundle_id)
                    associated_log_bundles.append(log_bundle)
                data['associated_log_bundles'] = associated_log_bundles
                
                query = """
                    select age(
                        ended, started
                    )::text as upload_time_processing 
                    from (
                        select log_bundle.id,zip_id,created_at as started 
                        from global_status 
                        inner join log_bundle on log_bundle.id = global_status.zip_id
                        where log_bundle.id='{}' 
                            and status_msg~*'Zip file Received'
                    ) a 
                    left join (
                        select zip_id,created_at as ended 
                        from global_status 
                        where zip_id='{}' 
                            and status_msg~*'Bundle Processing - Started'
                    ) b on a.id=b.zip_id
                    limit 1
                """.format(data['zip_id'], data['zip_id'])
                upload_time_processing = connectFetchJSONWihtoutQueryDataNoResponse(query)
                data['upload_time_processing'] = ""
                if len(upload_time_processing) > 0:
                    data['upload_time_processing'] = upload_time_processing[0]['upload_time_processing']


                resp_dict['data'][i] = data

            resp_dict['total_rows'] = total_rows
            resp = response_wrapper_plain_jsondumps(200, resp_dict)
            return resp
        except Exception as e:
            logger.error(traceback.format_exc())
            json_data = {"data": {"status": "Failed to load the page"}, "message": "Failed to load the page", "title": "Error"}
            return response_wrapper(500, json_data)


# @analysis_ns.route("/gupb_analysis_all")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_viewall)
class GetAnalysisAllView(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get Analysis Structure/Rule/Decrypts/Advance Analysis/Status History
        """
        try:
            # json_data = {
            #     "data": {"status": "Data not received"},
            #     "message": "Data not received", "title": "Error"}
            # return response_wrapper(500, json_data)
            logger.info("GetAnalysisAllView")
            req = request
            data = getRequestJSON(req)
            zip_id = data["zip_id"]
            if 'page_no' in data:
                page_no = data["page_no"]
                page_size = data["page_size"]
            else:
                page_no = 1
                page_size = 10000
            page_type = data["page_type"]
            list_of_file_ids = list()
            is_filter = False
            is_script = False
            listval = list()

            # cache_path_keys = (str(req.path)+"-keys-new1/"+str(zip_id)+"/"+str(page_no)+"/"+str(page_size)+"/"+str(page_type))
            # cache_path_properties = (str(req.path)+"-properties-new1/"+str(zip_id)+"/"+str(page_no)+"/"+str(page_size)+"/"+str(page_type))

            filter_query = ""
            script = ""
            # cache_path = (str(req.path)+"-new1/"+str(zip_id)+"/"+str(page_type)+"/"+str(page_no)+"/"+str(page_size))
            global_filter = ''
            if 'global_filter' in data:
                global_filter = data['global_filter']

            strFilteredCondition = filterApplyForTablesAnalysis(data, page_type, global_filter)
            if strFilteredCondition != "":
                is_filter = True

            if "filter" in data and (page_type == "ml"):
                logger.info("Inside filter and ml")
                filter_data_main = data["filter"]
                if "filter_query" in filter_data_main:
                    filter_query = filter_data_main["filter_query"]
                    if len(filter_query) > 0:
                        is_filter = True

                        # _cache_properties_keywords = []
                        # _cache_properties = cache.get(settings.redis_prefix+cache_path_properties, version=settings.redis_cache_version)
                        #
                        # if _cache_properties is None:
                        #     _cache_properties = {}
                        #
                        # for _key, _value in _cache_properties.items():
                        #     try:
                        #         if _value['fields']['keyword']['type'] == 'keyword':
                        #             _cache_properties_keywords.append(_key)
                        #     except:
                        #         pass
                        #
                        for i in range(len(filter_query)):
                            _query = filter_query[i]['query_string']['query']
                            if ".keyword:" not in _query:
                                _arr = _query.split(':', 1)
                                _col = _arr[0].strip()
                                _value = _arr[1].strip() if len(_arr) > 1 else ''
                            # if _col in _cache_properties_keywords:
                            #     filter_query[i]['query_string'] = {
                            #         'default_field': _col + '.keyword',
                            #         'query': _value,
                            #         'analyzer': 'keyword',
                            #     }
                            elif ":" in _query:
                                _arr = _query.split(':', 1)
                                _col = _arr[0].strip()
                                _value = _arr[1].strip() if len(_arr) > 1 else ''
                                filter_query[i]['query_string'] = {
                                    'default_field': _col,
                                    'query': _value,
                                }

                if "script" in filter_data_main:
                    logger.info("inside script")
                    script = filter_data_main["script"]
                    print(script)
                    if len(script) > 0:
                        is_script = True
                        for i in range(len(script)):
                            script[i]['script']['script']['source'] = script[i]['script']['script']['source'].replace('\n', '').replace('\r', '')
                            if script[i]['script']['script']['source'] == "timestamp_day_quator_script":
                                script[i]['script']['script']['source'] = "if (!!doc.containsKey('timestamp.keyword') && !doc['timestamp.keyword'].empty) { def timestamp = doc['timestamp.keyword'].value; if (timestamp.length() > 15) { def hour_str = timestamp.substring(11, 13); int hour = Integer.parseInt(hour_str);  if (params.quator == -1 || params.quator == '*') { return  true; } if (params.quator == 1) { if (hour == 0 || hour == 24) { return  true; } } def computed_quator = ( hour / 6 ) + 1; if (params.quator == computed_quator) { return true; } } } return false;"
                            if script[i]['script']['script']['source'] == "time_day_quator_script":
                                script[i]['script']['script']['source'] = "if (!!doc.containsKey('time.keyword') && !doc['time.keyword'].empty) { def time = doc['time.keyword'].value; if (time.length() > 15) { def hour_str = time.substring(9, 11); int hour = Integer.parseInt(hour_str);  if (params.quator == -1 || params.quator == '*') { return  true; } if (params.quator == 1) { if (hour == 0 || hour == 24) { return  true; } } def computed_quator = ( hour / 6 ) + 1; if (params.quator == computed_quator) { return true; } } } return false;"

            # if settings.redis_prefix+cache_path in cache and (not is_filter) and (not is_script):
            #     cache_data = cache.get(settings.redis_prefix+cache_path, version=settings.redis_cache_version)
            #     print("Data from Redis Cache path is "+str(cache_path))
            #     return cache_data

            if (page_type == "structured"):
                logger.info("Inside structured")
                if len(global_filter) > 0:
                    mustval = list()
                    mustval.append({'match': {'zip_id.keyword': str(zip_id)}})
                    mustnotvalue = list()
                    excluded_datefields = [1, 2]
                    if len(excluded_datefields) > 0:
                        index = settings.ENVIRONMENT[settings.env]["elk_config"]["parser_index"]
                        body = {
                            'query': {
                                'bool': {
                                    'must': mustval
                                }
                            }
                        }

                        logger.info('GET %s/_search' % index)
                        logger.info('%s' % json.dumps(body))
                        # count = settings.es.count(index=index, body=body)
                        count = get_elastic_count(index, body)
                        total_no_lines_parser_file_id = count['count']
                        logger.info("total_no_lines_parser_file_id = {}".format(total_no_lines_parser_file_id))

                        mustval.append({"query_string": {"query": "*" + str(global_filter) + "*"}})
                        mustnotvalue.append({"query_string": {"query": "*" + str(global_filter) + "*", 'fields': ["output_file_id", "input_file_id", "gui_rules_parser_name"]}})

                        index = settings.ENVIRONMENT[settings.env]["elk_config"]["parser_index"]
                        body = {
                            '_source': {
                                'include': ['output_file_id']
                            },
                            'from': 0,
                            'size': total_no_lines_parser_file_id,
                            'query': {
                                'bool': {
                                    'must': mustval,
                                    'must_not': mustnotvalue
                                }
                            }
                        }
                        logger.info('GET %s/_search' % index)
                        logger.info('%s' % json.dumps(body))
                        # res_file_id= settings.es.search(index=index,body=body)
                        res_file_id = get_elastic_data(index, body)
                        if res_file_id == False:
                            pass
                        count_mapping = list()
                        for hit in res_file_id['hits']['hits']:
                            count_mapping.append(hit['_source'])
                        logger.info("count_mapping :{}".format(count_mapping))
                        if len(count_mapping) > 0:
                            df_count_mapping = pd.DataFrame(count_mapping)
                            dict_count_val = df_count_mapping.to_dict('list')
                            list_count_vals = dict_count_val['output_file_id']
                            list_of_file_ids = list(set(list_count_vals))
                            logger.info(list_of_file_ids)
                    else:
                        json_data = {"data": {"archive_status": getarchivestatus(zip_id), "is_filter": is_filter, "columns": [], "rows": [], "total_no_lines": 0, "initial_page": 0}, "message": "Data Not Found", "status": 404, "title": "error"}
                        resp = response_wrapper_plain_jsondumps(404, json_data)
                        return resp
                gupb_structured = []
                if len(global_filter) > 0:
                    query = """
                        select a.id, a.structured_file, input_file as file, b.name as parser, a.status,
                            a.no_lines as parsed_lines, a.serr as remarks, b.column_sequence 
                        from slog_files a 
                        inner join log_bundle l on a.zip_id=l.id 
                        inner join v_grok b on a.pattern_id=b.grok_id and l.prod_id=b.prod_id 
                        inner join log_files c on a.log_files_id=c.id and a.zip_id = c.zip_id
                        where a.zip_id='{}' and a.id in ({}) 
                        order by a.no_lines desc
                        offset ({}-1)*{} 
                        limit {}
                    """.format(zip_id, str(check_valueofarray_str(list_of_file_ids)), page_no, page_size, page_size)
                else:
                    query = """
                        select a.id, a.structured_file, input_file as file, b.name as parser, a.status,
                            a.no_lines as parsed_lines, a.serr as remarks, b.column_sequence 
                        from slog_files a 
                        inner join log_bundle l on a.zip_id=l.id 
                        inner join v_grok b on a.pattern_id=b.grok_id and l.prod_id=b.prod_id 
                        inner join log_files c on a.log_files_id=c.id and a.zip_id=c.zip_id
                        where a.zip_id='{}' {} 
                        order by a.no_lines desc
                        offset ({}-1)*{} 
                        limit {}
                    """.format(zip_id, strFilteredCondition, page_no, page_size, page_size)
                gupb_structured = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(gupb_structured) == 0:
                    gupb_structured = []

                total_rows = 0
                if len(global_filter) > 0:
                    query = """
                        select count(*) 
                        from slog_files a 
                        inner join log_bundle l on a.zip_id=l.id 
                        inner join v_grok b on a.pattern_id=b.grok_id and l.prod_id=b.prod_id 
                        inner join log_files c on a.log_files_id=c.id and a.zip_id = c.zip_id
                        where a.zip_id='{}' and a.id in ({})
                    """.format(zip_id, str(check_valueofarray_str(list_of_file_ids)))
                else:
                    query = """
                        select count(*) 
                        from slog_files a 
                        inner join log_bundle l on a.zip_id=l.id 
                        inner join v_grok b on a.pattern_id=b.grok_id and l.prod_id=b.prod_id 
                        inner join log_files c on a.log_files_id=c.id and a.zip_id = c.zip_id
                        where a.zip_id='{}'
                    """.format(zip_id)
                gtotlaRows = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(gtotlaRows) > 0:
                    total_rows = gtotlaRows[0]["count"]

                de_success_failure_rows = 0
                if len(global_filter) > 0:
                    query = """
                        select count(*) 
                        from slog_files a 
                        inner join log_bundle l on a.zip_id=l.id 
                        inner join v_grok b on a.pattern_id=b.grok_id and l.prod_id=b.prod_id 
                        inner join log_files c on a.log_files_id=c.id and a.zip_id = c.zip_id
                        where a.zip_id='{}' and a.id in ({}) and a.status in (2,3)
                    """.format(zip_id, str(check_valueofarray_str(list_of_file_ids)))
                else:
                    query = """
                        select count(*) 
                        from slog_files a 
                        inner join log_bundle l on a.zip_id=l.id 
                        inner join v_grok b on a.pattern_id=b.grok_id and l.prod_id=b.prod_id 
                        inner join log_files c on a.log_files_id=c.id and a.zip_id = c.zip_id
                        where a.zip_id='{}' and a.status in (2,3)
                    """.format(zip_id)
                gtotlaSuccessFailureRows = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(gtotlaSuccessFailureRows) > 0:
                    de_success_failure_rows = gtotlaSuccessFailureRows[0]["count"]

                json_data = {"data": {"zip_id": zip_id, "archive_status": getarchivestatus(zip_id), "is_filter": is_filter, "gupb_structured": gupb_structured, "total_rows": total_rows, "de_success_failure_rows": de_success_failure_rows, "status_type": "structured"}, "message": "Data received", "status": 200}
            elif (page_type == "rule"):
                logger.info("Inside rules")
                gupb_rule = []
                logBundleFilter = ''
                if data.get('filter') and data['filter'].get('logBundleFilter'):
                    logBundleFilter = "having '{{{}}}' @> array_agg(distinct lrp.bundle_id)::text[]".format(",".join(data['filter'].get('logBundleFilter')))
                """
                query = "with log as(select unnest(associated_log_bundles) as bundle_id " \
                        "from log_bundle where id = '{}' union values('{}'::uuid))" \
                        ", log_product as (" \
                        "select id as bundle_id, prod_id from log_bundle where id in (select bundle_id from log))" \
                        "select a.id,string_agg(distinct bb.name,'<br>') as rule_name," \
                        "string_agg(distinct gg.name,'<br>') as parser_name,a.status,a.no_lines,a.serr as error,bb.column_sequence " \
                        "from rlog_files a "\
                        "inner join log_bundle l on a.zip_id=l.id " \
                        "inner join slog_files_mapping_rule c on c.id=a.slog_mapping_id and c.zip_id = l.id " \
                        "inner join log_files d on d.id=c.log_files_id and d.zip_id = a.zip_id " \
                        "inner join slog_files e on e.id=d.slog_files_id " \
                        "inner join rules bb on a.rule_id=bb.rule_id " \
                        "inner join rule_grok_mapping rg on a.rule_id = rg.rule_id " \
                        "inner join v_grok gg on gg.grok_id=rg.grok_id " \
                        "inner join rule_prod_mapping rp on a.rule_id=rp.rule_id " \
                        "inner join products lrp on rp.prod_id=lrp.prod_id " \
                        "where a.zip_id='{}' {} " \
                        "group by a.id,a.status,a.no_lines,a.serr,bb.column_sequence" \
                        " {} " \
                        " order by a.no_lines desc offset ({}-1)*{} limit {}"\
                    .format(zip_id,zip_id,zip_id,strFilteredCondition,logBundleFilter,page_no,page_size,page_size)
                """
                query = """
                    with log as(
                        select unnest(associated_log_bundles) as bundle_id 
                        from log_bundle where id = '{}' union values('{}'::uuid)
                    ), 
                    log_product as (
                        select id as bundle_id, prod_id from log_bundle where id in (select bundle_id from log)
                    )
                    select a.id, string_agg(distinct bb.name,'<br>') as rule_name, string_agg(distinct gg.name,'<br>') as parser_name, a.status, 
                        a.no_lines, a.serr as error, bb.column_sequence
                    from rlog_files a
                    inner join log_bundle l on a.zip_id=l.id
                    inner join slog_files_mapping_rule c on c.id=a.slog_mapping_id and c.zip_id=l.id
                    inner join log_files d on d.id=c.log_files_id  and d.zip_id = a.zip_id
                    inner join slog_files e on e.id=d.slog_files_id
                    inner join rules bb on a.rule_id=bb.rule_id
                    inner join rule_grok_mapping rg on a.rule_id = rg.rule_id
                    inner join v_grok gg on gg.grok_id=rg.grok_id
                    inner join rule_prod_mapping rp on a.rule_id=rp.rule_id
                    inner join products lrp on rp.prod_id=lrp.prod_id
                    where a.zip_id='{}' {}
                    group by a.id, a.status, a.no_lines, a.serr, bb.column_sequence 
                    {}
                    order by a.no_lines desc offset ({}-1)*{} limit {}
                """.format(zip_id, zip_id, zip_id, strFilteredCondition, logBundleFilter, page_no, page_size, page_size)
                gupb_rule = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(gupb_rule) == 0:
                    gupb_rule = []

                total_rows = 0
                query = "select count(*) from rlog_files where zip_id='{}'".format(zip_id)
                gtotlaRows = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(gtotlaRows) > 0:
                    total_rows = gtotlaRows[0]["count"]

                de_success_failure_rows = 0
                query = "select count(*) from rlog_files where zip_id='{}' and status in (2,3)".format(zip_id)
                gtotlaSuccessFailureRows = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(gtotlaSuccessFailureRows) > 0:
                    de_success_failure_rows = gtotlaSuccessFailureRows[0]["count"]

                json_data = {"data": {"zip_id": zip_id, "archive_status": getarchivestatus(zip_id), "is_filter": is_filter, "gupb_rule": gupb_rule, "total_rows": total_rows, "de_success_failure_rows": de_success_failure_rows, "status_type": "rule"}, "message": "Rule data received", "status": 200}
            elif (page_type == "decrypt"):
                
                # _clone_zip_id = zip_id
                # query = f"""
                #     select coalesce(parent_zip_id, id) as zip_id
                #     from log_bundle
                #     where id='{_clone_zip_id}'
                #     limit 1
                # ;
                # """
                # _log_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
                # if len(_log_details) > 0:
                #     zip_id = _log_details[0]['zip_id']

                logger.info("Inside decrypt")
                gupb_decrypted = []
                query = """
                    select distinct a.id as d_id,b.id as id,b.no_lines as total_lines,decrypted_file,a.status,a.no_lines,a.serr as error 
                    from dlog_files a 
                    left join log_files b on a.decrypted_file=b.input_file and b.zip_id = a.zip_id
                    where a.zip_id='{}' {} 
                    order by id 
                    offset ({}-1)*{} 
                    limit {}
                """.format(zip_id, strFilteredCondition, page_no, page_size, page_size)
                gupb_decrypted = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(gupb_decrypted) == 0:
                    gupb_decrypted = []

                total_rows = 0
                query = "select count(*) from dlog_files where zip_id='{}'".format(zip_id)
                gtotlaRows = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(gtotlaRows) > 0:
                    total_rows = gtotlaRows[0]["count"]

                de_success_failure_rows = 0
                query = "select count(*) from dlog_files where zip_id='{}' and status in (2,3)".format(zip_id)
                gtotlaSuccessFailureRows = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(gtotlaSuccessFailureRows) > 0:
                    de_success_failure_rows = gtotlaSuccessFailureRows[0]["count"]

                json_data = {"data": {"zip_id": zip_id, "archive_status": getarchivestatus(zip_id), "is_filter": is_filter, "gupb_decrypted": gupb_decrypted, "total_rows": total_rows, "de_success_failure_rows": de_success_failure_rows, "status_type": "decrypt"}, "message": "Decrypt data received", "status": 200}
            elif (page_type == "rawdata"):
                logger.info("Inside rawdata")
                gupb_rawdata = []
                query = """
                    select a.id,a.input_file as file_name,a.no_lines as total_lines,a.ranomalies as anomalies,'' as ml_anomalies 
                    from log_files a 
                    left join rule_matching_rows bb on a.id=bb.input_file 
                    where a.type in ('file','decrypts') and a.zip_id='{}' {} 
                    group by a.id,a.input_file, a.no_lines, a.ranomalies
                    order by a.id offset ({}-1)*{} 
                    limit {}
                """.format(zip_id, strFilteredCondition, page_no, page_size, page_size)
                gupb_rawdata = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(gupb_rawdata) == 0:
                    gupb_rawdata = []

                total_rows = 0
                query = """
                    select count(*) from (
                        select a.id,a.input_file as file_name,a.no_lines as total_lines,sum(b.no_lines) as instances,count(output_linenum) as Failures,'' as ml_anomalies 
                        from log_files a 
                        left join slog_files b on a.id=b.log_files_id 
                        left join rule_matching_rows bb on a.id=bb.input_file 
                        where a.type in ('file','decrypts') and a.zip_id='{}' 
                        group by a.id,a.input_file,a.no_lines
                    ) a
                """.format(zip_id)
                gtotlaRows = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(gtotlaRows) > 0:
                    total_rows = gtotlaRows[0]["count"]

                json_data = {"data": {"zip_id": zip_id, "archive_status": getarchivestatus(zip_id), "is_filter": is_filter, "gupb_rawdata": gupb_rawdata, "total_rows": total_rows, "status_type": "rawdata"}, "message": "Raw data received", "status": 200}
            elif (page_type == "statushistory"):
                logger.info("Inside statushistory")
                gstatus_history = []
                if strFilteredCondition != "":
                    strFilteredCondition = " where True " + strFilteredCondition
                query = """select * 
                    from (
                        select ROW_NUMBER () OVER (ORDER BY id) as s_no,status_msg,types,to_char(created_at, 'YYYY-Mon-DD HH24:MI:SS')::text as created_at 
                        from global_status 
                        where zip_id='{}' order by id desc
                    ) a {} 
                    offset ({}-1)*{} 
                    limit {}
                """.format(zip_id, strFilteredCondition, page_no, page_size, page_size)
                gstatus_history = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(gstatus_history) == 0:
                    gstatus_history = []

                total_rows = 0
                query = "select count(*) from global_status where zip_id='{}'".format(zip_id)
                gtotlaRows = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(gtotlaRows) > 0:
                    total_rows = gtotlaRows[0]["count"]

                json_data = {"data": {"zip_id": zip_id, "archive_status": getarchivestatus(zip_id), "is_filter": is_filter, "gstatus_history": gstatus_history, "total_rows": total_rows, "status_type": "statushistory"}, "message": "Data Retrieved", "status": 200}
            elif page_type == "ml":
                logger.info("Inside ml")
                listval = list()
                keys = list()
                total_no_lines = 0
                elk_ml_index = ""
                query = "select elk_ml_index from log_bundle a left join products b on a.prod_id=b.prod_id where a.id='{}'".format(zip_id)
                ml_index_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(ml_index_details) > 0:
                    elk_ml_index = ml_index_details[0]["elk_ml_index"]
                if elk_ml_index != "" and elk_ml_index is not None:
                    mustval = list()
                    mustval.append({'match': {'zip_id.keyword': str(zip_id)}})
                    if is_filter:
                        mustval.extend(filter_query)
                    if is_script:
                        mustval.extend(script)
                    if elk_ml_index == "fn_output":

                        page_size = int(page_size / 3)
                        fromValue = (page_no - 1) * page_size
                        body = {
                            '_source': {'excludes': ['zip_id']},
                            'from': fromValue, 'size': page_size,
                            'sort': {
                                'accuracy': 'desc'
                            },
                            'query': {
                                'bool': {
                                    'must': mustval
                                }
                            },
                            "collapse": {
                                "field": "UniqueID.keyword",
                                "inner_hits": {
                                    "name": "order by accuracy",
                                    "size": 3,
                                    "sort": [
                                        {
                                            "accuracy": {"order": "desc", "unmapped_type": "long"}
                                        }
                                    ]
                                }
                            }
                        }
                        logger.info('GET %s/_search' % elk_ml_index)
                        logger.info('%s' % json.dumps(body))
                        try:
                            # res= settings.es.search(index=elk_ml_index,body=body)
                            res = get_elastic_data(elk_ml_index, body)
                            if res == False:
                                json_data = {'data': {"zip_id": zip_id, "archive_status": getarchivestatus(zip_id),
                                                      'is_filter': is_filter, 'columns': keys, 'rows': [],
                                                      "total_no_lines": total_no_lines, "status_type": "ml"},
                                             'message': "ML data received", 'status': 200}
                                resp = response_wrapper_plain_jsondumps(200, json_data)
                                return resp
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            json_data = {'data': {"zip_id": zip_id, "archive_status": getarchivestatus(zip_id),
                                                  'is_filter': is_filter, 'columns': keys, 'rows': [],
                                                  "total_no_lines": total_no_lines, "status_type": "ml"},
                                         'message': "ML data received", 'status': 200}
                            resp = response_wrapper_plain_jsondumps(200, json_data)
                            return resp
                        # mappings = settings.es.indices.get_mapping(
                        #    index=elk_ml_index)
                        # mappings = mappings[elk_ml_index]["mappings"]
                        body = {
                            'query': {
                                'bool': {
                                    'must': mustval
                                }
                            }
                        }
                        logger.info('GET %s/_count' % elk_ml_index)
                        logger.info('%s' % json.dumps(body))
                        # count = settings.es.count(index=elk_ml_index, body=body)
                        count = get_elastic_count(elk_ml_index, body)
                        total_no_lines = count['count']
                        for hit in res['hits']['hits']:
                            inner_hits = hit['inner_hits']
                            for hit1 in inner_hits['order by accuracy']['hits']['hits']:
                                listval.append(hit1['_source'])
                    else:
                        page_size = int(page_size)
                        fromValue = (page_no - 1) * page_size
                        body = {
                            '_source': {
                                'excludes': ['zip_id']
                            },
                            'from': fromValue,
                            'size': page_size,
                            'sort': {
                                'accuracy': 'desc'
                            },
                            'query': {
                                'bool': {
                                    'must': mustval
                                }
                            }
                        }
                        logger.info('GET %s/_search' % elk_ml_index)
                        logger.info('%s' % json.dumps(body))
                        try:
                            # res = settings.es.search(index=elk_ml_index,body=body)
                            res = get_elastic_data(elk_ml_index, body)
                            if res == False:
                                json_data = {'data': {"zip_id": zip_id, "archive_status": getarchivestatus(zip_id),
                                                      'is_filter': is_filter, 'columns': keys, 'rows': [],
                                                      "total_no_lines": total_no_lines, "status_type": "ml"},
                                             'message': "ML data received", 'status': 200}
                                resp = response_wrapper_plain_jsondumps(200, json_data)
                                return resp
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            json_data = {'data': {"zip_id": zip_id, "archive_status": getarchivestatus(zip_id),
                                                  'is_filter': is_filter, 'columns': keys, 'rows': [],
                                                  "total_no_lines": total_no_lines, "status_type": "ml"},
                                         'message': "ML data received", 'status': 200}
                            resp = response_wrapper_plain_jsondumps(200, json_data)
                            return resp
                        # mappings = settings.es.indices.get_mapping(
                        #    index=elk_ml_index)
                        # mappings = mappings[elk_ml_index]["mappings"]
                        body = {
                            'query': {
                                'bool': {
                                    'must': mustval
                                }
                            }
                        }
                        logger.info('GET %s/_count' % elk_ml_index)
                        logger.info('%s' % json.dumps(body))
                        # count = settings.es.count(index=elk_ml_index, body=body)
                        count = get_elastic_count(elk_ml_index, body)
                        total_no_lines = count['count']
                        for hit in res['hits']['hits']:
                            listval.append(hit['_source'])
                    if len(listval) > 0:
                        dicts = listval[0]
                        keys = list(dicts.keys())
                json_data = {'data': {"zip_id": zip_id, "archive_status": getarchivestatus(zip_id), 'is_filter': is_filter, 'columns': keys, 'rows': listval, "total_no_lines": total_no_lines, "status_type": "ml"}, 'message': "ML data received", 'status': 200}
            else:
                json_data = {"data": {"status": "options not available"}, "message": "Options Not Available", "status": 200}

            resp = response_wrapper_plain_jsondumps(200, json_data)
            return resp
        except Exception as e:
            logger.error(traceback.format_exc())
            json_data = {
                "data": {"status": "Data not received"},
                "message": "Data not received", "title": "Error"}
            return response_wrapper(500, json_data)

def get_datetime(value, fuzzy=False):
    if isinstance(value, list):
        value = value[0]
    try:
        p = parse(value, fuzzy=fuzzy)
        return p
    except ValueError:
        return None

def get_file_id_dict_details(file_id_list):
    file_id_dict_details = {}
    if len(file_id_list) > 0:
        query = """
            select id, replace(input_file,'/ephemeral','') as input_file
            from log_files
            where id in (%s);
        """ % ( ",".join([ "'%s'" % i for i in file_id_list]) )
        file_id_and_file_path = connectFetchJSONWihtoutQueryDataNoResponse(query)
        if len(file_id_and_file_path) > 0:
            for _i in file_id_and_file_path:
                _id = str(_i['id'])
                _input_file = _i['input_file']
                file_id_dict_details[ _id ] = _input_file
    return file_id_dict_details

# @analysis_ns.route("/ungroup_ml_data_datelist")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_viewall)
class GetUngroupMLDataDateList(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        try:
            req = request
            data = getRequestJSON(req)
            if 'zip_id' in data:
                _clone_zip_id = data['zip_id']
                query = f"select real_zip_id from log_bundle where id='{_clone_zip_id}'"
                details = connectFetchJSONWithoutResponse(query, '')
                if len(details) > 0:
                    print(details)
                    _zip_id = details[0]["real_zip_id"]
                    if _zip_id is not None:
                        zip_id = _zip_id
                        data['zip_id'] = zip_id
            zip_id = data["zip_id"]

            elk_ml_index_de = None
            query = "select elk_ml_index_de, prod_name from log_bundle a left join products b on a.prod_id=b.prod_id where a.id='{}'".format(zip_id)
            ml_index_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
            if len(ml_index_details) > 0:
                elk_ml_index_de = ml_index_details[0]["elk_ml_index_de"]

            api_url = 'http://{}:{}/request/search'.format(
                settings.ENVIRONMENT[settings.env]['spark_config']['QA_IP'],
                settings.ENVIRONMENT[settings.env]['spark_config']['QA_PORT']
            )
            payload = {
                "zipID": zip_id,
                "indexName":  elk_ml_index_de,
                "queryType": "SearchWithDateWiseMLData"
            }
            logger.info(api_url)
            logger.info(payload)
            r = requests.post(url=api_url, json=payload)
            # if r.text is None or r.text == "":
            #     json_data = {
            #     "data": {"status": "Data not received"},
            #     "message": "Data not received", "title": "Error"}
            #     return response_wrapper(500, json_data)
            if r.text is not None and r.text != "":
                logger.info("Response Received from QA")
                rows = []
                try:
                    return_dict = json.loads(r.text)
                    rows = return_dict['rows']
                except:
                    pass
                df = pd.DataFrame(rows)

                date_list = []
                if "time" in df.columns:
                    df["ml_date_filter__datetime"] = df["time"].apply(lambda x: get_datetime(x))
                    date_list = df["ml_date_filter__datetime"].tolist()
                    date_list = list(
                        set([str(e.date()) for e in date_list if e is not None if not str(e.date()) == "NaT"])
                    )
                    date_list = sorted(date_list)

                if "timestamp" in df.columns:
                    df["ml_date_filter__datetime"] = df["timestamp"].apply(lambda x: get_datetime(x))
                    date_list = df["ml_date_filter__datetime"].tolist()
                    date_list = list(
                        set([str(e.date()) for e in date_list if e is not None if not str(e.date()) == "NaT"])
                    )
                    date_list = sorted(date_list)

                json_data = {'data': {"zip_id": zip_id, "date_list": date_list}, 'message': "ML data received", 'status': 200}

                resp = response_wrapper_plain_jsondumps(200, json_data)
                return resp
        except Exception as e:
            logger.error(traceback.format_exc())
            json_data = {
                "data": {"status": "Data not received"},
                "message": "Data not received", "title": "Error"}
            return response_wrapper(500, json_data)


def get_quator_hours_list(quator = 1):
    quator = int(quator)
    if quator == 1:
        return [0, 1, 2, 3, 4, 5, 6]
    if quator == 2:
        return [6, 7, 8, 9, 10, 11, 12]
    if quator == 3:
        return [12, 13, 14, 15, 16, 17, 18]
    if quator == 4:
        return [18, 19, 20, 21, 22, 23, 0]


# @analysis_ns.route("/ungroup_ml_data")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_viewall)
class GetUngroupMLData(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        try:
            req = request
            data = getRequestJSON(req)
            if 'zip_id' in data:
                _clone_zip_id = data['zip_id']
                query = f"select real_zip_id from log_bundle where id='{_clone_zip_id}'"
                details = connectFetchJSONWithoutResponse(query, '')
                if len(details) > 0:
                    print(details)
                    _zip_id = details[0]["real_zip_id"]
                    if _zip_id is not None:
                        zip_id = _zip_id
                        data['zip_id'] = zip_id
            zip_id = data["zip_id"]
            page_no = 1
            page_size = 50
            is_filter = False
            if 'page_no' in data:
                page_no = data["page_no"]
                page_size = data["page_size"]
            column_filters = {}
            if 'column_filters' in data:
                column_filters = data['column_filters']

            elk_ml_index_de = None
            query = "select elk_ml_index_de, prod_name from log_bundle a left join products b on a.prod_id=b.prod_id where a.id='{}'".format(zip_id)
            ml_index_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
            if len(ml_index_details) > 0:
                elk_ml_index_de = ml_index_details[0]["elk_ml_index_de"]

            api_url = 'http://{}:{}/request/search'.format(
                settings.ENVIRONMENT[settings.env]['spark_config']['QA_IP'],
                settings.ENVIRONMENT[settings.env]['spark_config']['QA_PORT']
            )
            payload = {
                "zipID": zip_id,
                "indexName":  elk_ml_index_de,
                # "date" : "2016-04-19",
                "queryType": "SearchWithDateWiseMLData"
            }
            logger.info(api_url)
            logger.info(payload)
            r = requests.post(url=api_url, json=payload)
            if r.text is not None and r.text != "":
                logger.info("Response Received from QA")
                rows = []
                try:
                    return_dict = json.loads(r.text)
                    rows = return_dict['rows']
                except:
                    pass
                df = pd.DataFrame(rows)

                date_list = []
                if "time" in df.columns:
                    df["ml_date_filter__datetime"] = df["time"].apply(lambda x: get_datetime(x))
                    date_list = df["ml_date_filter__datetime"].tolist()
                    date_list = list(
                        set([str(e.date()) for e in date_list if e is not None if not str(e.date()) == "NaT"])
                    )
                    date_list = sorted(date_list)

                if "timestamp" in df.columns:
                    df["ml_date_filter__datetime"] = df["timestamp"].apply(lambda x: get_datetime(x))
                    date_list = df["ml_date_filter__datetime"].tolist()
                    date_list = list(
                        set([str(e.date()) for e in date_list if e is not None if not str(e.date()) == "NaT"])
                    )
                    date_list = sorted(date_list)

                if "FILE_PATH" in df.columns and not "filename" in df.columns:
                    df["filename"] = df["FILE_PATH"].apply(lambda x: os.path.split(x)[1])
                elif ("file_id" in df.columns or "input_file_id" in df.columns) and not "FILE_PATH" in df.columns:
                    key = "file_id"
                    if "input_file_id" in df.columns:
                        key = "input_file_id"
                    file_id_dict_details = {}
                    file_id_list = list(set(df[key].tolist()))
                    file_id_dict_details = get_file_id_dict_details(file_id_list)
                    get_file_path = lambda x: file_id_dict_details[x] if x in file_id_dict_details else ""
                    df["FILE_PATH"] = df[key].apply(lambda x: get_file_path(str(x)))

                print(df)

                columns_types = { k:str(v) for k, v in dict(df.dtypes).items()}

                for _column, _value in column_filters.items():
                    if _value is None or _value == "" or _value == "*":
                        continue
                    if _column in df.columns:
                        if columns_types[_column] == "object":
                            _value = str(_value)
                            df = df[df[_column].astype('str').str.contains(_value)]
                        if columns_types[_column] == "int64":
                            _value = str(_value)
                            df = df[df[_column].astype('str').str.contains(_value)]
                        if columns_types[_column] == "float64":
                            _value = str(_value)
                            df = df[df[_column].astype('str').str.contains(_value)]

                    if "__" in _column:
                        _new_column, _type = _column.split('__')
                        if not "ml_date_filter__datetime" in df.columns:
                            continue
                        if _type == "date":
                            # if _new_column in columns_list:
                            _value = str(_value)
                            df = df[(df["ml_date_filter__datetime"].apply(lambda x: x.strftime("%Y-%m-%d") == _value if x is not None else False))]
                        if _type == "quator":
                            # if _new_column in columns_list:
                            _value = str(_value)
                            _list = get_quator_hours_list(_value)
                            df = df[df["ml_date_filter__datetime"].apply(lambda x: x.hour in _list if x is not None else False)]

                total_no_lines = len(df)

                start = (page_no - 1) * page_size
                end = page_no * page_size

                df = df[start:end]

                if 'time' in df.columns:
                    print(df[['time', 'ml_date_filter__datetime']])
                if 'timestamp' in df.columns:
                    print(df[['timestamp', 'ml_date_filter__datetime']])
                if 'ml_date_filter__datetime' in df.columns:
                    df = df.drop(columns=['ml_date_filter__datetime'])
                if 'ml_date_filter' in df.columns:
                    df['ml_date_filter'] = df['ml_date_filter'].apply(lambda x: 0 if math.isnan(x) else x)
                columns = list(df.columns)
                # rows = df.to_dict('records')
                rows = [row.dropna().to_dict() for index, row in df.iterrows()]

                json_data = {'data': {"zip_id": zip_id, "archive_status": getarchivestatus(zip_id), 'is_filter': is_filter, 'columns': columns, 'rows': rows, "total_no_lines": total_no_lines, "status_type": "ml", "date_list": date_list}, 'message': "ML data received", 'status': 200}

                resp = response_wrapper_plain_jsondumps(200, json_data)
                return resp
        except Exception as e:
            logger.error(traceback.format_exc())
        json_data = {
            "data": {"status": "Data not received"},
            "message": "Data not received", "title": "Error"}
        return response_wrapper(500, json_data)


def getarchivestatus(zip_id):
    try:
        query = "select archive_status from log_bundle where id='" + str(zip_id) + "'"
        archive_status = connectFetchJSONWihtoutQueryDataNoResponse(query)
        if len(archive_status) > 0:
            return str(archive_status[0]["archive_status"])

    except Exception as e:
        logger.error(traceback.format_exc())


class GetAnalysisAllViewDownload(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        try:
            logger.info("""Get Analysis csv Structure/Rule/Decrypts/Advance Analysis/Status History""")
            req = request

            data = getRequestJSON(req)
            logger.info(data)
            if 'zip_id' in data:
                _clone_zip_id = data['zip_id']
                query = f"select real_zip_id from log_bundle where id='{_clone_zip_id}'"
                details = connectFetchJSONWithoutResponse(query, '')
                if len(details) > 0:
                    print('details', details)
                    _zip_id = details[0]["real_zip_id"]
                    if _zip_id is not None:
                        zip_id = _zip_id
                        data['zip_id'] = zip_id
            zip_id = data["zip_id"]
            page_type = data["page_type"]
            filter_columns = data["filter_columns"] if "filter_columns" in data else []
            logger.info("Zip id={}, pagetype={}".format(zip_id, page_type))
            list_of_file_ids = list()
            is_filter = False
            is_script = False
            listval = list()
            filter_query = ""
            script = ""
            if (page_type == "structured"):
                query = """
                    select a.id, a.structured_file, input_file as file, b.name as parser, a.status,
                        a.no_lines as parsed_lines, a.serr as remarks, b.column_sequence 
                    from slog_files a 
                    left join log_bundle l on a.zip_id=l.id 
                    left join v_grok b on a.pattern_id=b.grok_id and l.prod_id=b.prod_id 
                    left join log_files c on a.log_files_id=c.id and c.zip_id=a.zip_id
                    where a.zip_id='{}' 
                    order by a.no_lines desc
                """.format(zip_id)  # ,strFilteredCondition)
                gupb_structured = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(gupb_structured) == 0:
                    gupb_structured = []
                json_data = gupb_structured
            elif (page_type == "rule"):
                gupb_rule = []
                logBundleFilter = ''
                query = """
                    with log as(
                        select unnest(associated_log_bundles) as bundle_id 
                        from log_bundle where id = '{}' union values('{}'::uuid)
                    ), 
                    log_product as (
                        select id as bundle_id, prod_id from log_bundle where id in (select bundle_id from log)
                    )
                    select a.id, string_agg(distinct bb.name,'<br>') as rule_name, string_agg(distinct gg.name,'<br>') as parser_name, a.status, 
                        a.no_lines, a.serr as error, bb.column_sequence
                    from rlog_files a
                    inner join log_bundle l on a.zip_id=l.id
                    inner join slog_files_mapping_rule c on c.id=a.slog_mapping_id and c.zip_id=l.id
                    inner join log_files d on d.id=c.log_files_id and d.zip_id=a.zip_id
                    inner join slog_files e on e.id=d.slog_files_id
                    inner join rules bb on a.rule_id=bb.rule_id
                    inner join rule_grok_mapping rg on a.rule_id = rg.rule_id
                    inner join v_grok gg on gg.grok_id=rg.grok_id
                    inner join rule_prod_mapping rp on a.rule_id=rp.rule_id
                    inner join products lrp on rp.prod_id=lrp.prod_id
                    where a.zip_id='{}'
                    group by a.id, a.status, a.no_lines, a.serr, bb.column_sequence 
                    
                    order by a.no_lines desc 
                """.format(zip_id, zip_id, zip_id)  # ,strFilteredCondition,logBundleFilter,page_no,page_size,page_size)
                logger.info("rule-query-debug: " + query)
                gupb_rule = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(gupb_rule) == 0:
                    gupb_rule = []
                json_data = gupb_rule
            elif (page_type == "decrypt"):
                gupb_decrypted = []
                query = "select distinct a.id as d_id,b.id as id,b.no_lines as total_lines,decrypted_file,a.status,a.no_lines,a.serr as error from dlog_files a left join log_files b on a.decrypted_file=b.input_file where a.zip_id='{}' {} order by id".format(zip_id)  # ,strFilteredCondition,page_no,page_size,page_size)
                logger.info("decrypt-tab-debug: " + query)
                gupb_decrypted = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(gupb_decrypted) == 0:
                    gupb_decrypted = []
                json_data = gupb_decrypted
            elif (page_type == "rawdata"):
                gupb_rawdata = []
                query = """
                    select a.id,a.input_file as file_name,a.no_lines as total_lines,a.ranomalies as anomalies,'' as ml_anomalies 
                    from log_files a 
                    left join rule_matching_rows bb on a.id=bb.input_file 
                    where a.type in ('file','decrypts') and a.zip_id='{}' 
                    group by a.id,a.input_file, a.no_lines, a.ranomalies
                    order by a.id
                """.format(zip_id)  # ,strFilteredCondition,page_no,page_size,page_size)
                gupb_rawdata = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(gupb_rawdata) == 0:
                    gupb_rawdata = []
                json_data = gupb_rawdata
            elif (page_type == "statushistory"):
                gstatus_history = []
                query = "select ROW_NUMBER () OVER (ORDER BY id) as s_no,status_msg,types,to_char(created_at, 'YYYY-Mon-DD HH24:MI:SS')::text as created_at from global_status where zip_id='{}' order by id desc".format(zip_id)  # ,strFilteredCondition,page_no,page_size,page_size)
                gstatus_history = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(gstatus_history) == 0:
                    gstatus_history = []

                json_data = gstatus_history
            elif page_type == "ml":
                listval = list()
                keys = list()
                total_no_lines = 0
                elk_ml_index = ""
                query = "select elk_ml_index from log_bundle a left join products b on a.prod_id=b.prod_id where a.id='{}'".format(zip_id)
                ml_index_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(ml_index_details) > 0:
                    elk_ml_index = ml_index_details[0]["elk_ml_index"]
                if elk_ml_index != "" and elk_ml_index is not None:
                    mustval = list()
                    mustval.append({'match': {'zip_id.keyword': str(zip_id)}})
                    if is_filter:
                        mustval.extend(filter_query)
                    if is_script:
                        mustval.extend(script)
                    if elk_ml_index == "fn_output":
                        body = {
                            '_source': {'excludes': ['zip_id']},
                            'from': 0, 'size': 10000,
                            'sort': {
                                'accuracy': 'desc'
                            },

                            'query': {
                                'bool': {
                                    'must': mustval
                                }
                            },
                            "collapse": {
                                "field": "UniqueID.keyword",
                                "inner_hits": {
                                    "name": "order by accuracy",
                                    "size": 3,
                                    "sort": [
                                        {
                                            "accuracy": {"order": "desc", "unmapped_type": "long"}
                                        }
                                    ]
                                }
                            }
                        }
                        logger.info('GET %s/_search' % elk_ml_index)
                        logger.info('%s' % json.dumps(body))
                        # res= settings.es.search(index=elk_ml_index,body=body)
                        res = get_elastic_data(elk_ml_index, body)
                        if res == False:
                            json_data = {"data": {"status": "Internal server error"},
                                         "message": "Internal server error", "title": "Error"}
                            resp = response_wrapper(500, json_data)
                            return resp
                        for hit in res['hits']['hits']:
                            inner_hits = hit['inner_hits']
                            for hit1 in inner_hits['order by accuracy']['hits']['hits']:
                                listval.append(hit1['_source'])
                    elif elk_ml_index == "gl_output":
                        body = {
                            '_source': {
                                'excludes': ['zip_id']
                            },
                            'from': 0,
                            'size': 10000,
                            'sort': {
                                'similarity_score': 'desc'
                            },
                            'query': {
                                'bool': {
                                    'must': mustval
                                }
                            }
                        }
                        logger.info('GET %s/_search' % elk_ml_index)
                        logger.info('%s' % json.dumps(body))
                        # res = settings.es.search(index=elk_ml_index,body=body)
                        res = get_elastic_data(elk_ml_index, body)
                        if res == False:
                            json_data = {"data": {"status": "Internal server error"},
                                         "message": "Internal server error", "title": "Error"}
                            resp = response_wrapper(500, json_data)
                            return resp
                        for hit in res['hits']['hits']:
                            listval.append(hit['_source'])
                    else:
                        body = {
                            '_source': {
                                'excludes': ['zip_id']
                            },
                            'from': 0,
                            'size': 10000,
                            'sort': {
                                'accuracy': 'desc'
                            },
                            'query': {
                                'bool': {
                                    'must': mustval
                                }
                            }
                        }
                        logger.info('GET %s/_search' % elk_ml_index)
                        logger.info('%s' % json.dumps(body))
                        # res = settings.es.search(index=elk_ml_index,body=body)
                        res = get_elastic_data(elk_ml_index, body)
                        if res == False:
                            json_data = {"data": {"status": "Internal server error"},
                                         "message": "Internal server error", "title": "Error"}
                            resp = response_wrapper(500, json_data)
                            return resp
                        for hit in res['hits']['hits']:
                            listval.append(hit['_source'])
                    if len(listval) > 0:
                        dicts = listval[0]
                        keys = list(dicts.keys())
                json_data: object = listval
            else:
                json_data = []

            if len(json_data) > 0:
                df = pd.DataFrame(json_data)
                columns = df.columns.values.tolist()
                if len(filter_columns) > 0:
                    new_columns = []
                    for filter_column in filter_columns:
                        if filter_column in columns:
                            new_columns.append(filter_column)

                    df = df[new_columns]
                    dfcolsrename = {}
                    for dfkeys in new_columns:
                        if dfkeys in CSVColumns:
                            dfcolsrename[dfkeys] = CSVColumns[dfkeys]
                    # dfcolsrename = {dfkeys: CSVColumns[dfkeys] for dfkeys in new_columns}
                    df.rename(columns=dfcolsrename, inplace=True)
                    df["File name"] = df["File name"].apply(getfilename)
                b = getlistcolumns(df)
                logger.info("List columns: {}".format(b))
                for x in b:
                    df[x] = df[x].apply(getstringjoin)
                # df.columns = columns
                resp = HttpResponse(status=200, content_type='text/csv')
                resp['Content-Disposition'] = "attachment; filename=%s" % page_type + "_download.csv"
                df.to_csv(path_or_buf=resp, index=False)
            else:
                json_data = {"data": {"status": "Received empty data"}, "message": "Received empty data", "title": "Error"}
                resp = response_wrapper(204, json_data)
        except Exception as e:
            logger.error("Exception : {}".format(traceback.format_exc()))
            json_data = {"data": {"status": "Internal server error"}, "message": "Internal server error", "title": "Error"}
            resp = response_wrapper(500, json_data)
        return resp


def getstringjoin(x):
    xx = ','.join(map(str, x))
    return xx


def getfilename(x):
    xx = x.rsplit('/', 1)
    return xx[len(xx) - 1]


def getlistcolumns(dataf):
    dataf = dataf.convert_dtypes(infer_objects=True)
    dtypedf = dataf.dtypes
    a = dtypedf[dtypedf == 'object']
    return list(a.index)


# @analysis_ns.route("/gelastic_datasearch")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_jsonascsvsearch)
class GetJsonAsCSV(APIView):  # not in use
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get Elastic Search Data as JSON
        """
        req = request
        data = getRequestJSON(req)
        zip_id = data['zip_id']
        # file_id = data['file_id']
        file_type = data['file_type']
        page_no = data['page_no']
        page_size = data['page_size']
        folder_name = ""
        fromValue = (int(page_no) - 1) * 50

        listval = list()
        listval_mapping = list()
        keys = list()
        initial_page = 1
        total_no_lines = 0
        is_filter = False
        # data["filter"]={"filter":[{"wildcard": {"log_text": "*load*"}, "query_string": {"query": ""}}],
        # "filter_query":[{"query_string": {"query": "log_text:*load*"}}]}

        mustval = list()
        mustval.append({'match': {'zip_id.keyword': str(zip_id)}})
        if "filter" in data:
            print("----------Check 0---------")
            filter_data_main = data["filter"]
            print(filter_data_main)
            if "filter_query" in filter_data_main:
                filter = filter_data_main["filter_query"]
                print("----------Check 1---------")
                print(filter)
                print("-------check 3-----------")
                print(mustval)
                mustval.extend(filter)
                print("----------Check 4---------")
                print(mustval)
        logger.info(mustval)
        logger.info(file_type)
        if "raw" in file_type:
            index = settings.ENVIRONMENT[settings.env]["elk_config"]["raw_index"]
            body = {
                '_source': {'excludes': ['parser_name', 'rule_applied']},
                'query': {
                    'bool': {
                        'must': mustval
                    }
                }
            }
            logger.info('GET %s/_search' % index)
            logger.info('%s' % json.dumps(body))
            res = get_elastic_data_sort(index, body, "row_index:asc")
            index = settings.ENVIRONMENT[settings.env]["elk_config"]["raw_index"]
            body = {'query': {
                'bool': {
                    'must': mustval
                }
            }
            }
            logger.info('GET %s/_count' % index)
            logger.info('%s' % json.dumps(body))
            # count = settings.es.count(index=index, body=body)
            count = get_elastic_count(index, body)
            total_no_lines = count['count']

        listval = list()
        for hit in res['hits']['hits']:
            listval.append(hit['_source'])
        # print(hit['_source']['line'])
        if len(listval) > 0:
            df = pd.DataFrame(listval)
            # df['row_index']=df['row_index'].astype(int)
            df = df.groupby('input_file_id', as_index=False).agg({'row_index': lambda x: ''.join(str(x)), 'log_text': lambda x: ','.join(x), 'zip_id': 'first'})
            df['input_file_id'] = df['input_file_id'].astype(int)
            # print(type(df['input_file_id']))
            listval = df.to_dict('records')
            for i in listval:
                id = int(i['input_file_id'])
                zip_id = i['zip_id']
                # print(type(zip_id))
                query = "select input_file from log_files where zip_id='{}' and id='{}'".format(zip_id, id)
                input_file = connectFetchJSONWihtoutQueryDataNoResponse(query)
                # print(input_file)
                i['file_name'] = input_file[0]["input_file"]
            json_data = {"data": {"is_filter": is_filter, "columns": keys, "rows": listval, "total_no_lines": total_no_lines, "initial_page": initial_page}, "message": "Data received", "status": 200}
        resp = response_wrapper_plain_jsondumps(200, json_data)
        return resp


# @analysis_ns.route("/gelastic_datasearch_new")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_jsonascsvsearch)
class GetJsonAsCSVNew(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        try:
            logger.info("Get Elastic Search Data as JSON")
            req = request
            data = getRequestJSON(req)
            zip_id = data['zip_id']
            file_type = data['file_type']
            page_no = data['page_no']
            page_size = data['page_size']
            folder_name = ""
            fromValue = (int(page_no) - 1) * 50

            listval = list()
            # listval_mapping = list()
            keys = list()
            initial_page = 1
            total_no_lines = 0
            is_filter = False
            filter1 = []
            filter_query = []

            # cache_path_columns = (str(req.path)+"-columns/"+str(zip_id)+"/raw/"+str(page_no)+"/"+str(page_size))

            api_url = 'http://{}:{}/request/search'.format(
                settings.ENVIRONMENT[settings.env]['spark_config']['QA_IP'],
                settings.ENVIRONMENT[settings.env]['spark_config']['QA_PORT']
            )

            return_dict = {
                'rows': [],
                'columns': [],
                'total_no_lines': 0,
            }

            if "filter" in data:
                filter_data_main = data["filter"]
                if "filter_query" in filter_data_main:
                    filter_query = filter_data_main["filter_query"]
                    for i, _filter_query in enumerate(filter_query):
                        filter_query[i]['query_string']['query'] = _filter_query['query_string']['query'].replace('\\', '')

            logger.info(filter_query)

            if "raw" in file_type:
                payload = {"zipID": zip_id, "page_no": page_no,
                           "page_size": page_size,
                           "indexName": "raw",
                           "queryType": "GlobalColumnSearch",
                           "filter": {
                               "filter": [],
                               "filter_query": filter_query,
                           }}
                logger.info(api_url)
                r = requests.post(url=api_url, json=payload)
                logger.info('POST %s' % api_url)
                logger.info(json.dumps(payload))

                if r.text is not None and r.text != "":
                    logger.info("Response Received from QA")
                    return_dict = json.loads(r.text)
                    listval = return_dict['rows']

            if len(listval) > 0:
                df = pd.DataFrame(listval)
                df = df.groupby('input_file_id', as_index=False).agg({'row_index': lambda x: ''.join(str(x)), 'log_text': lambda x: ','.join(x), 'zip_id': 'first'})
                df['input_file_id'] = df['input_file_id'].astype(int)
                listval = df.to_dict('records')
                for i in listval:
                    id = int(i['input_file_id'])
                    zip_id = i['zip_id']
                    query = "select input_file from log_files where zip_id='{}' and id='{}'".format(zip_id, id)
                    input_file = connectFetchJSONWihtoutQueryDataNoResponse(query)
                    i['file_name'] = input_file[0]["input_file"]

            # if 'columns' in return_dict:
            #     if len(return_dict['columns']) > 0:
            #         cache.set(settings.redis_prefix+cache_path_columns, return_dict['columns'], version=settings.redis_cache_version)
            #     else:
            #         _cache_columns = cache.get(settings.redis_prefix+cache_path_columns, version=settings.redis_cache_version)
            #         return_dict['columns'] = _cache_columns

            return_dict['rows'] = listval
            return_dict['is_filter'] = is_filter
            return_dict['initial_page'] = initial_page

            json_data = {
                "data": return_dict,
                "message": "Data received",
                "status": 200
            }

            resp = response_wrapper_plain_jsondumps(200, json_data)
            return resp
        except:
            logger.error(traceback.format_exc())
            json_data = {
                "data": {"status": "Internal server error"},
                "message": "Internal server error", "title": "Error"}
            return response_wrapper(500, json_data)


# @analysis_ns.route("/gelastic_data")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_jsonascsv)
class GetJsonAsCSV_ElasticData(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get Elastic Search Data as JSON
        """
        req = request
        data = getRequestJSON(req)
        # zip_id = data['zip_id']
        file_id = data['file_id']
        file_type = data['file_type']
        page_no = data['page_no']
        global_filter = ''
        if 'global_filter' in data:
            global_filter = data['global_filter']
        folder_name = ""
        fromValue = (int(page_no) - 1) * 25
        total_no_lines = 0
        initial_page = 0
        is_filter = False

        cache_path_keys = (str(req.path) + "-keys/" + str(file_id) + "/" + str(file_type) + "/" + str(fromValue) + "/" + str(25))
        cache_path_properties = (str(req.path) + "-properties/" + str(file_id) + "/" + str(file_type) + "/" + str(fromValue) + "/" + str(25))

        mustval = list()
        mustval.append({'match': {'output_file_id': str(file_id)}})
        mustnotvalue = list()

        if len(global_filter) > 0 and file_type == "patterns":
            query = "select id,zip_id from slog_files where id=" + str(file_id) + ""
            logger.info(query)
            bdetails = connectFetchJSONWihtoutQueryDataNoResponse(query)
            bundle_details = bdetails[0]
            zip_id = bundle_details['zip_id']
            mustval.append({'match': {'zip_id': str(zip_id)}})
            excluded_datefields = [1, 2]

            if len(excluded_datefields) > 0:
                mustval.append({"query_string": {"query": "*" + str(global_filter) + "*"}})
                mustnotvalue.append({"query_string": {"query": "*" + str(global_filter) + "*", 'fields': ["output_file_id", "input_file_id", "gui_rules_parser_name"]}})
            else:
                json_data = {"data": {'is_filter': is_filter, 'columns': [], 'properties': {}, 'rows': [], "total_no_lines": 0, "initial_page": 0}, "message": "Data Not Found", "title": "error"}
                resp = response_wrapper_plain_jsondumps(404, json_data)
                return resp
        else:
            if "filter" in data:
                print("----------Check 0---------")
                filter_data_main = data["filter"]
                print(filter_data_main)
                if "filter_query" in filter_data_main:
                    filter_query = filter_data_main["filter_query"]

                    if len(filter_query) > 0:
                        # _cache_properties_keywords = []
                        # _cache_properties = cache.get(settings.redis_prefix + cache_path_properties, version=settings.redis_cache_version)

                        # for _key, _value in _cache_properties.items():
                        #     try:
                        #         if _value['fields']['keyword']['type'] == 'keyword':
                        #             _cache_properties_keywords.append(_key)
                        #     except:
                        #         pass

                        for i in range(len(filter_query)):
                            _query = filter_query[i]['query_string']['query']
                            if ".keyword:" not in _query:
                                _arr = _query.split(':', 1)
                                _col = _arr[0].strip()
                                _value = _arr[1].strip() if len(_arr) > 1 else ''
                            # if _col in _cache_properties_keywords:
                            #     filter_query[i]['query_string'] = {
                            #         'default_field': _col + '.keyword',
                            #         'query': _value,
                            #         'analyzer': 'keyword',
                            #     }
                            elif ":" in _query:
                                _arr = _query.split(':', 1)
                                _col = _arr[0].strip()
                                _value = _arr[1].strip() if len(_arr) > 1 else ''
                                filter_query[i]['query_string'] = {
                                    'default_field': _col,
                                    'query': _value,
                                }
                    print("----------Check 1---------")
                    print(filter_query)
                    print("-------check 3-----------")
                    print(mustval)
                    mustval.extend(filter_query)
                    print("----------Check 4---------")
                    print(mustval)
        logger.info(mustval)
        logger.info(file_type)

        if file_type == "patterns":
            index = settings.ENVIRONMENT[settings.env]["elk_config"]["parser_index"]
            body = {'_source': {'excludes': ['zip_id', 'output_file_id', 'input_file_id', 'gui_rules_parser_name', 'parser_name', 'username', 'FILE_PATH']}, 'from': fromValue, 'size': 25, 'query': {
                'bool': {
                    'must': mustval,
                    'must_not': mustnotvalue
                }
            }}
            logger.info('GET %s/_search' % index)
            logger.info('%s' % json.dumps(body))

            res = get_elastic_data(index, body)
            # res = settings.es.search(index=index,body=body)
            body = {'query': {
                'bool': {
                    'must': mustval,
                    'must_not': mustnotvalue
                }
            }}
            logger.info('GET %s/_count' % index)
            logger.info('%s' % json.dumps(body))
            # count = settings.es.count(index=index, body=body)
            count = get_elastic_count(index, body)
            mappings = get_elastic_indices(index)
            mappings = mappings[index]["mappings"]
            total_no_lines = count['count']
        elif file_type == "rules":
            index = settings.ENVIRONMENT[settings.env]["elk_config"]["rules_index"]
            body = {'_source': {'excludes': ['zip_id', 'output_file_id', 'input_file_id', 'rule_applied', 'gui_rules_parser_name', 'parser_name', 'FILE_PATH', 'username']}, 'from': fromValue, 'size': 25, 'query': {
                'bool': {
                    'must': mustval
                }
            }}
            logger.info('GET %s/_search' % index)
            logger.info('%s' % json.dumps(body))
            # res = settings.es.search(index=index,body=body)
            res = get_elastic_data(index, body)
            body = {'query': {
                'bool': {
                    'must': mustval
                }
            }}
            logger.info('GET %s/_count' % index)
            logger.info('%s' % json.dumps(body))
            count = get_elastic_count(index, body)
            mappings = get_elastic_indices(index)
            mappings = mappings[index]["mappings"]
            total_no_lines = count['count']

        listval = list()
        for hit in res['hits']['hits']:
            listval.append(hit['_source'])
            logger.info(hit['_source'])
        logger.info(listval)

        keys = list()
        if len(listval) > 0:
            dicts = listval[0]
            keys = list(dicts.keys())
            logger.info(keys)

        properties = {}
        for key in keys:
            if mappings["properties"].get(key, None) is not None:
                properties[key] = mappings["properties"].get(key)
        #
        # if len(keys) > 0:
        #     cache.set(settings.redis_prefix+cache_path_keys, keys, version=settings.redis_cache_version)
        #     cache.set(settings.redis_prefix+cache_path_properties, properties, version=settings.redis_cache_version)
        #
        # if len(keys) == 0:
        #     keys = cache.get(settings.redis_prefix+cache_path_keys, version=settings.redis_cache_version)
        #     properties = cache.get(settings.redis_prefix+cache_path_properties, version=settings.redis_cache_version)

        # json_data = {'data':listval,'msg':"Token Created",'status':200,'error':''}
        json_data = {"data": {'is_filter': is_filter, 'columns': keys, 'properties': properties, 'rows': listval, "total_no_lines": total_no_lines, "initial_page": initial_page}, "message": "Received data", "status": 200}
        resp = response_wrapper_plain_jsondumps(200, json_data)
        return resp


# @analysis_ns.route("/gelastic_data_new")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_jsonascsv)
class GetJsonAsCSV_ElasticDataNew(APIView):
    permission_classes = (IsActive,)

    def get_columns(self, rows):
        all_keys = list()
        keys_list = [list(row.keys()) for row in rows]
        for keys in keys_list:
            all_keys.extend(keys)
        return set(all_keys)

    def get_sorted_columns(self, rows, priority_columns):
        return_all_columns = list()
        all_columns = self.get_columns(rows)
        priority_columns_list = priority_columns.split(",")
        priority_columns_list = [priority_column.strip() for priority_column in priority_columns_list]
        remaining_columns = set(all_columns) - set(priority_columns_list)
        list(remaining_columns).sort(reverse=False)
        return_all_columns.extend(priority_columns_list)
        return_all_columns.extend(remaining_columns)
        return_all_columns = list(filter(None, return_all_columns))
        return return_all_columns

    def get_sorted_rows(self, rows, sorted_columns):
        return_list_of_dict = []
        for row in rows:
            new_dict = OrderedDict()
            for sorted_column in sorted_columns:
                new_dict[sorted_column] = row.get(sorted_column)
            return_list_of_dict.append(new_dict)
        return return_list_of_dict

    @csrf_exempt
    def post(self, request):
        try:
            req = request
            data = getRequestJSON(req)
            file_type = data['file_type']
            if file_type == "patterns":
                query = """select coalesce(parent_slog_files_id, id) as file_id from slog_files where id = {}""".format(data['file_id'])
                res = connectFetchJSONWihtoutQueryDataNoResponse(query)
                data['file_id'] = res[0]['file_id']
            if file_type == "rules":
                query = """select coalesce(parent_rlog_files_id, id) as file_id from rlog_files where id = {}""".format(data['file_id'])
                res = connectFetchJSONWihtoutQueryDataNoResponse(query)
                data['file_id'] = res[0]['file_id']
            file_id = data['file_id']
            page_type="na" if 'page_type' not in data else data['page_type']
            page_no = data['page_no']
            page_size = 50 if 'page_size' not in data else data['page_size']
            global_filter = ''
            if 'global_filter' in data:
                global_filter = data['global_filter']
            column_sequence = data['column_sequence'] if 'column_sequence' in data else ''
            if column_sequence is None:
                column_sequence = ''
            folder_name = ""
            is_filter = False

            initial_page = page_no

            filter1 = []
            filter_query = []

            # cache_path_columns = (str(req.path)+"-keys/"+str(file_id)+"/"+str(file_type)+"/"+str(page_no)+"/"+str(page_size))

            if "filter" in data:
                filter_data_main = data["filter"]
                if "filter_query" in filter_data_main:
                    if len(filter_data_main['filter_query']) > 0:
                        is_filter = True
                        filter_query = filter_data_main['filter_query']
                        for i, _filter_query in enumerate(filter_query):
                            filter_query[i]['query_string']['query'] = _filter_query['query_string']['query'].replace('\\', '')
            api_url = 'http://{}:{}/request/search'.format(
                settings.ENVIRONMENT[settings.env]['spark_config']['QA_IP'],
                settings.ENVIRONMENT[settings.env]['spark_config']['QA_PORT']
            )

            data = {
                'columns': [],
                'rows': [],
                'total_no_lines': 0,
            }

            if file_type == "patterns":
                payload = {
                    "file_id": file_id,
                    "indexName": "parser",
                    "global_filter": [],  # global_filter,
                    "queryType": "ParserColumnSearch",
                    "page_no": page_no,
                    "page_size": page_size,
                    "filter": {
                        "filter": [],  # filter1,
                        "filter_query": filter_query,
                        "script": []
                    }
                }

                r = requests.post(url=api_url, json=payload)
                logger.info('POST %s' % api_url)
                logger.info(json.dumps(payload))
                if r.text is not None and r.text != "":
                    data = json.loads(r.text)

            elif file_type == "rules":
                payload = {
                    "file_id": file_id,
                    "indexName": "rules",
                    "global_filter": [],  # global_filter,
                    "queryType": "RulesColumnSearch",
                    "page_no": page_no,
                    "page_size": page_size,
                    "filter": {
                        "filter": [],  # filter1,
                        "filter_query": filter_query,
                        "script": []
                    }
                }
                r = requests.post(url=api_url, json=payload)
                logger.info('POST %s' % api_url)
                logger.info(json.dumps(payload))
                if r.text is not None and r.text != "":
                    data = json.loads(r.text)
            if 'rows' in data:
                sorted_columns = self.get_sorted_columns(data['rows'], column_sequence)
                sorted_rows = self.get_sorted_rows(data['rows'], sorted_columns)

                data['columns'] = sorted_columns
                data['rows'] = sorted_rows
            # if 'columns' in data:
            #     if len(data['columns']) > 0:
            #         cache.set(settings.redis_prefix+cache_path_columns, data['columns'], version=settings.redis_cache_version)
            #     else:
            #         _cache_columns = cache.get(settings.redis_prefix+cache_path_columns, version=settings.redis_cache_version)
            #         data['columns'] = _cache_columns
            #
            data['is_filter'] = is_filter
            data['initial_page'] = initial_page
            json_data = {"data": data, "message": "Data has received", "status": 200}
            resp = response_wrapper_plain_jsondumps(200, json_data)
            return resp
        except:
            logger.error(traceback.format_exc())
            json_data = {
                "data": {"status": "Data not received"},
                "message": "Data not received", "title": "Error"}
            return response_wrapper(500, json_data)


# @analysis_ns.route("/csvdownload/{page_type}/{file_id}")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_jsonascsv)
class GetCSVDownloadParserRule_old(APIView):
    permission_classes = (IsActive,)

    def get_columns(self, rows):
        all_keys = list()
        keys_list = [list(row.keys()) for row in rows]
        for keys in keys_list:
            all_keys.extend(keys)
        return set(all_keys)

    def get_sorted_columns(self, rows, priority_columns):
        return_all_columns = list()
        all_columns = self.get_columns(rows)
        priority_columns_list = priority_columns.split(",")
        priority_columns_list = [priority_column.strip() for priority_column in priority_columns_list]
        remaining_columns = set(all_columns) - set(priority_columns_list)
        list(remaining_columns).sort(reverse=False)
        return_all_columns.extend(priority_columns_list)
        return_all_columns.extend(remaining_columns)
        return_all_columns = list(filter(None, return_all_columns))
        return return_all_columns

    def get_sorted_rows(self, rows, sorted_columns):
        return_list_of_dict = []
        for row in rows:
            new_dict = OrderedDict()
            for sorted_column in sorted_columns:
                new_dict[sorted_column] = row.get(sorted_column)
            return_list_of_dict.append(new_dict)
        return return_list_of_dict

    @csrf_exempt
    def get(self, request, page_type, file_id):
        df = None

        if page_type == "patterns":
            query = """select coalesce(parent_slog_files_id, id) as file_id from slog_files where id = {}""".format(data['file_id'])
            res = connectFetchJSONWihtoutQueryDataNoResponse(query)
            file_id = res[0]['file_id']
        if page_type == "rules":
            query = """select coalesce(parent_rlog_files_id, id) as file_id from rlog_files where id = {}""".format(data['file_id'])
            res = connectFetchJSONWihtoutQueryDataNoResponse(query)
            file_id = res[0]['file_id']

        page_no = 1
        page_size = 50
        total_no_lines = 0
        filename = 'emptyfile.csv'
        _filename = 'emptyfile'
        _filename_prefix = 'emtpyfile'
        global_filter = ''
        column_sequence = request.GET.get('column_sequence', '')
        if column_sequence == 'null':
            column_sequence = ''
        folder_name = ""
        is_filter = False

        initial_page = page_no

        filter1 = []
        filter_query = []

        """
        if "filter" in data:
            filter_data_main = data["filter"]
            if "filter_query" in filter_data_main:
                if len(filter_data_main['filter_query']) > 0:
                    is_filter = True
                    filter_query = filter_data_main['filter_query']
                    for i, _filter_query in enumerate(filter_query):
                        filter_query[i]['query_string']['query'] = _filter_query['query_string']['query'].replace('\\', '')
            # if "filter" in filter_data_main:
            #     if len(filter_data_main['filter']) > 0:
            #         is_filter = True
            #         filter1 = filter_data_main['filter']
        """
        api_url = 'http://{}:{}/request/search'.format(
            settings.ENVIRONMENT[settings.env]['spark_config']['QA_IP'],
            settings.ENVIRONMENT[settings.env]['spark_config']['QA_PORT']
        )

        data = {
            'columns': [],
            'rows': [],
            'total_no_lines': 0,
        }

        if page_type in ['patterns', 'rules']:

            if page_type == "patterns":
                query = f"select structured_file as file_name from slog_files where id={file_id}"
                filedetail = connectFetchJSONWithoutResponse(query, '')
                if len(filedetail) > 0:
                    _filename = filedetail[0]["file_name"].split('/')[-1]
                    _filename_prefix = _filename.replace(".csv", "")
                payload = {
                    "file_id": file_id,
                    "indexName": "parser",
                    "global_filter": [],  # global_filter,
                    "queryType": "ParserColumnSearch",
                    "page_no": page_no,
                    "page_size": page_size,
                    "filter": {
                        "filter": [],  # filter1,
                        "filter_query": [],  # filter_query,
                        "script": []
                    }
                }
                r = requests.post(url=api_url, json=payload)
                logger.info('POST %s' % api_url)
                logger.info(json.dumps(payload))
                if r.text is not None and r.text != "":
                    data1 = json.loads(r.text)
                    total_no_lines = data1['total_no_lines']
                    total_page_no = math.ceil(total_no_lines / 10000)

                    sorted_columns = []
                    for _page_no in range(total_page_no):
                        page_no = _page_no + 1
                        payload = {
                            "file_id": file_id,
                            "indexName": "parser",
                            "global_filter": [],  # global_filter,
                            "queryType": "ParserColumnSearch",
                            "page_no": page_no,
                            "page_size": 10000,
                            "filter": {
                                "filter": [],  # filter1,
                                "filter_query": [],  # filter_query,
                                "script": []
                            }
                        }
                        r = requests.post(url=api_url, json=payload)
                        logger.info('POST %s' % api_url)
                        logger.info(json.dumps(payload))
                        if r.text is not None and r.text != "":
                            data = json.loads(r.text)
                            if page_no == 1:
                                sorted_columns = self.get_sorted_columns(data['rows'], column_sequence)
                                df = pd.DataFrame(columns=sorted_columns)
                            sorted_rows = self.get_sorted_rows(data['rows'], sorted_columns)
                            for row in sorted_rows:
                                df = df.append(row, ignore_index=True)

                    response = HttpResponse(content_type='text/csv')
                    response['Content-Disposition'] = "attachment; filename=" + str(_filename)
                    df.to_csv(path_or_buf=response, index=False)
                    return response

            if page_type == "rules":
                query = f"select rule_file as file_name from rlog_files where id={file_id}"
                filedetail = connectFetchJSONWithoutResponse(query, '')
                if len(filedetail) > 0:
                    _filename = filedetail[0]["file_name"].split('/')[-1]
                    _filename_prefix = _filename.replace(".csv", "")
                payload = {
                    "file_id": file_id,
                    "indexName": "rules",
                    "global_filter": [],  # global_filter,
                    "queryType": "RulesColumnSearch",
                    "page_no": page_no,
                    "page_size": page_size,
                    "filter": {
                        "filter": [],  # filter1,
                        "filter_query": [],  # filter_query,
                        "script": []
                    }
                }
                r = requests.post(url=api_url, json=payload)
                logger.info('POST %s' % api_url)
                logger.info(json.dumps(payload))
                if r.text is not None and r.text != "":
                    data1 = json.loads(r.text)
                    total_no_lines = data1['total_no_lines']
                    total_page_no = math.ceil(total_no_lines / 10000)

                    generator_rows = ()
                    sorted_columns = []
                    for _page_no in range(total_page_no):
                        page_no = _page_no + 1
                        payload = {
                            "file_id": file_id,
                            "indexName": "rules",
                            "global_filter": [],  # global_filter,
                            "queryType": "RulesColumnSearch",
                            "page_no": page_no,
                            "page_size": 10000,
                            "filter": {
                                "filter": [],  # filter1,
                                "filter_query": [],  # filter_query,
                                "script": []
                            }
                        }
                        r = requests.post(url=api_url, json=payload)
                        logger.info('POST %s' % api_url)
                        logger.info(json.dumps(payload))
                        if r.text is not None and r.text != "":
                            data = json.loads(r.text)
                            if page_no == 1:
                                sorted_columns = self.get_sorted_columns(data['rows'], column_sequence)
                                df = pd.DataFrame(columns=sorted_columns)
                            sorted_rows = self.get_sorted_rows(data['rows'], sorted_columns)
                            for row in sorted_rows:
                                df = df.append(row, ignore_index=True)

                    response = HttpResponse(content_type='text/csv')
                    response['Content-Disposition'] = "attachment; filename=" + str(_filename)
                    df.to_csv(path_or_buf=response, index=False)
                    return response

        return HttpResponse(status=404)


class Echo:
    def write(self, value):
        return value


# @analysis_ns.route("/csvdownload/{page_type}/{file_id}")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_jsonascsv)
class GetCSVDownloadParserRule(APIView):
    permission_classes = (IsActive,)
    
    def convert_datetime_to_custom_format(self,dt):
        year = dt.year
        month = dt.month
        day = dt.day

        if day <= 7:
            week = 'w01'
        elif day <= 14:
            week = 'w02'
        elif day <= 21:
            week = 'w03'
        else:
            week = 'w04'

        formatted_date = f"y{year}m{month:02d}{week}"
        return formatted_date

    def get_ordered_columns(self, row, priority_columns):
        return_columns = list()
        all_columns = list()
        existing_columns = row.keys()

        priority_columns_list = priority_columns.split(",")
        priority_columns_list = [priority_column.strip() for priority_column in priority_columns_list]
        remaining_columns = set(existing_columns) - set(priority_columns_list)
        list(remaining_columns).sort(reverse=False)
        all_columns.extend(priority_columns_list)
        all_columns.extend(remaining_columns)
        all_columns = list(filter(None, all_columns))

        non_existing_columns = list(set(all_columns) - set(existing_columns))

        for column in all_columns:
            if column not in non_existing_columns:
                return_columns.append(column)

        return return_columns

    @csrf_exempt
    def get(self, request, page_type, file_id):
        """
        Get CSV Downloads
        """
        req = request
        column_sequence = request.GET.get('column_sequence', '')
        if column_sequence == 'null':
            column_sequence = ''
        try:
            query = ""
            total_no_lines = 0
            columns = []
            # columns_str = ""
            index = ""
            if (page_type == "patterns"):
                query = f"select structured_file as file_name from slog_files where id={file_id}"
                mustval = list()
                mustval.append({'match': {'output_file_id': str(file_id)}})
                
                query_date = f"select start_time from slog_files where id={file_id}"
                pattern_date = connectFetchJSONWithoutResponse(query_date, '')
                formatted_datetime = self.convert_datetime_to_custom_format(pattern_date[0]['start_time'])

                index = settings.ENVIRONMENT[settings.env]["elk_config"]["parser_index"] + '_' + formatted_datetime
                body = {
                    'query': {
                        'bool': {
                            'must': mustval
                        }
                    }
                }

                logger.info('GET %s/_count' % index)
                logger.info('%s' % json.dumps(body))
                count = get_elastic_count(index, body)
                total_no_lines = count['count']

                index = settings.ENVIRONMENT[settings.env]["elk_config"]["parser_index"] + '_' + formatted_datetime
                body = {
                    '_source': {
                        'excludes': ['zip_id', 'output_file_id', 'input_file_id', 'gui_rules_parser_name', 'parser_name']
                    },
                    'from': 0,
                    'size': total_no_lines if total_no_lines > 0 else 1,
                    'query': {
                        'bool': {
                            'must': mustval
                        }
                    }
                }

            # logger.info('GET %s/_search' % index)
            # logger.info('%s' % json.dumps(body))
            # # res = settings.es.search(index=index,body=body)
            # res=get_elastic_data(index, body)
            # row = res['hits']['hits'][0]['_source']
            # columns = self.get_ordered_columns(row, column_sequence)
            # columns_str = '"' + '", "'.join(columns) + '"'

            if (page_type == "rules"):
                query = f"select rule_file as file_name from rlog_files where id={file_id}"
                mustval = list()
                mustval.append({'match': {'output_file_id': str(file_id)}})

                index = settings.ENVIRONMENT[settings.env]["elk_config"]["rules_index"]
                body = {
                    'query': {
                        'bool': {
                            'must': mustval
                        }
                    }
                }

                logger.info('GET %s/_count' % index)
                logger.info('%s' % json.dumps(body))
                count = get_elastic_count(index, body)
                total_no_lines = count['count']

                index = settings.ENVIRONMENT[settings.env]["elk_config"]["rules_index"]
                body = {
                    '_source': {
                        'excludes': ['zip_id', 'output_file_id', 'input_file_id', 'rule_applied']
                    },
                    'from': 0,
                    'size': total_no_lines if total_no_lines > 0 else 1,
                    'query': {
                        'bool': {
                            'must': mustval
                        }
                    }
                }

            logger.info('GET %s/_search' % index)
            logger.info('%s' % json.dumps(body))
            # res = settings.es.search(index=index,body=body)
            res = get_elastic_data(index, body)
            row = res['hits']['hits'][0]['_source']
            columns = self.get_ordered_columns(row, column_sequence)
            resarr = []
            for hit in res['hits']['hits']:
                resarr.append(hit['_source'])
            df = pd.DataFrame(resarr)
            df = df[columns]
            cols = df.columns
            dfcolsrename = {}
            for dfkeys in cols:
                if dfkeys in CSVColumns:
                    dfcolsrename[dfkeys] = CSVColumns[dfkeys]
            # dfcolsrename = {dfkeys: CSVColumns[dfkeys] for dfkeys in cols}
            df.rename(columns=dfcolsrename, inplace=True)

            # df.columns = columns
            filedetail = connectFetchJSONWithoutResponse(query, '')
            if len(filedetail) == 0:
                logger.info("File Not Found")
                json_data = {"data": {"status": "Unable to download"}, "message": "Unable to download", "title": "Error"}
                resp = response_wrapper_plain_jsondumps(404, json_data)
            else:
                logger.info("Data received")
                filename = filedetail[0]["file_name"].split('/')[-1]
                resp = HttpResponse(content_type='text/csv')
                resp['Content-Disposition'] = "attachment; filename=%s" % filename
                df.to_csv(path_or_buf=resp, index=False)

        except BaseException as e:
            logger.error(traceback.format_exc())
            json_data = {"data": {"status": "Internal server Error"}, "message": "Internal server Error", "title": "Error"}
            resp = response_wrapper_plain_jsondumps(500, json_data)
        return resp


# @analysis_ns.route("/gelastic_raw_data")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_jsonascsv)
class GetJsonAsCSV_ElasticRawData(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get Elastic Search Data Search as JSON
        """
        try:
            req = request
            data = getRequestJSON(req)
            file_id = data['file_id']
            file_type = data['file_type']
            page_no = data['page_no']
            page_size = data['page_size']
            folder_name = ""
            fromValue = (int(page_no) - 1) * page_size
            elk_ml_index = ""

            file_id_column = "file_id"
            log_line_numbers_column = "log_line_numbers"
            row_index_column = "row_index"

            query = "select zip_id from log_files where id='{}'".format(file_id)
            log_file_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
            if len(log_file_details) > 0:
                query = "select elk_ml_index, prod_name from log_bundle a left join products b on a.prod_id=b.prod_id where a.id='{}'".format(log_file_details[0]["zip_id"])
                ml_index_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(ml_index_details) > 0:
                    elk_ml_index = ml_index_details[0]["elk_ml_index"]
                    prod_name = ml_index_details[0]["prod_name"]
                    if elk_ml_index == "fn_output":
                        file_id_column = "input_file_id"
                        log_line_numbers_column = "line_number"
                        row_index_column = "row_index_search"

            listval = list()
            listval_mapping = list()
            keys = list()
            initial_page = 1
            total_no_lines = 0
            is_filter = False

            rule_mapping_excludeArray = ['zip_id', 'input_file_id', 'output_file_id']
            if "patterns" not in file_type:
                rule_mapping_excludeArray.extend(['parser_name'])
            if "rules" not in file_type:
                rule_mapping_excludeArray.extend(['rule_applied'])
            rule_mapping_includeArray = ['row_index']
            if "patterns" in file_type:
                rule_mapping_includeArray.extend(['parser_name'])
            if "rules" in file_type:
                rule_mapping_includeArray.extend(['rule_applied'])

            indexes_allowed = settings.ENVIRONMENT[settings.env]["elk_config"]["raw_index"]
            if "patterns" in file_type and "rules" in file_type:
                logger.info("All Parser and Rules Takens")
                indexes_allowed = settings.ENVIRONMENT[settings.env]["elk_config"]["parser_index"] + "," + settings.ENVIRONMENT[settings.env]["elk_config"]["rules_index"]
            elif "patterns" in file_type:
                logger.info("All Parser Takens")
                indexes_allowed = settings.ENVIRONMENT[settings.env]["elk_config"]["parser_index"]
            elif "rules" in file_type:
                logger.info("All Rules Takens")
                indexes_allowed = settings.ENVIRONMENT[settings.env]["elk_config"]["rules_index"]

            mustval = list()
            mustval.append({'match': {'input_file_id': str(file_id)}})
            if "filter" in data:
                filter_data_main = data["filter"]
                print(filter_data_main)
                if "filter_query" in filter_data_main:
                    filter = filter_data_main["filter_query"]
                    mustval.extend(filter)
            if 'backward_raw' in file_type:
                file_extentions = set(map(lambda x: math.ceil(x / page_size), [page_no]))
                for x in file_extentions:
                    page_no = x
                initial_page = page_no

            if "raw" not in file_type:
                index = indexes_allowed
                body = {
                    'query': {
                        'bool': {
                            'must': mustval
                        }
                    }
                }
                logger.info('GET %s/_count' % index)
                logger.info('%s' % json.dumps(body))
                # count = settings.es.count(index=index, body=body)
                count = get_elastic_count(index, body)

                total_no_lines = count['count']
                index = settings.ENVIRONMENT[settings.env]["elk_config"]["rules_index"]
                body = {
                    'query': {
                        'bool': {
                            'must': mustval
                        }
                    }
                }
                logger.info('GET %s/_count' % index)
                logger.info('%s' % json.dumps(body))
                # count = settings.es.count(index=index, body=body)
                count = get_elastic_count(index, body)
                rule_count = count['count']

                body = {
                    '_source': {
                        'include': rule_mapping_includeArray
                    },
                    'from': 0,
                    'size': total_no_lines if total_no_lines > 0 else 1,
                    'query': {
                        'bool': {
                            'must': mustval
                        }
                    }
                }
                listval_mapping = list()
                logger.info('GET %s/_search' % indexes_allowed)
                logger.info('%s' % json.dumps(body))
                res_mapping = get_elastic_data_sort(indexes_allowed, body, "row_index:asc")
                for hit in res_mapping['hits']['hits']:
                    listval_mapping.append(hit['_source'])
                if "ml" in file_type:
                    if elk_ml_index != "" and elk_ml_index is not None:
                        matchList = list()
                        matchList.append({'match': {file_id_column: str(file_id)}})
                        body = {
                            'size': 10000,
                            '_source': {'excludes': [file_id_column]},
                            'query': {'bool': {'must': matchList}}
                        }
                        ml_lines = list()
                        logger.info('GET %s/_search' % elk_ml_index)
                        logger.info('%s' % json.dumps(body))
                        # res = settings.es.search(index=elk_ml_index, body=body)
                        res = get_elastic_data(elk_ml_index, body)
                        if res != False:
                            for hit in res['hits']['hits']:
                                if isinstance(hit['_source'][log_line_numbers_column], list):
                                    ml_lines.extend(hit['_source'][log_line_numbers_column])
                                else:
                                    ml_lines.append(hit['_source'][log_line_numbers_column])
                            ml_lines = list(map(int, ml_lines))
                            ml_lines = list(dict.fromkeys(ml_lines))
                            ml_lines.sort()
                if len(listval_mapping) > 0:
                    df_count_mapping = pd.DataFrame(listval_mapping)
                    dict_count_val = df_count_mapping.to_dict('list')
                    list_count_vals = dict_count_val['row_index']
                    row_index_unique = list(set(list_count_vals))
                    total_no_lines = len(row_index_unique)

                    df_row_index = pd.DataFrame(row_index_unique)
                    page_no = (int(page_no) - 1) * int(page_size)
                    page_size = page_no + page_size
                    dict_val = df_row_index.to_dict('list')
                    list_vals = dict_val[0]
                    list_vals.sort()

                    if "ml" in file_type and elk_ml_index != "" and elk_ml_index is not None:
                        list_vals.extend(ml_lines)
                        list_vals = list(dict.fromkeys(list_vals))
                        if len(file_type) == 1:
                            list_vals = ml_lines

                    total_no_lines = len(list_vals)
                    list_vals = list_vals[page_no:page_size]

                    df_raw_mapping = pd.DataFrame(listval_mapping)

                    df_raw_mapping = df_raw_mapping.fillna("")
                    df_raw_mapping = df_raw_mapping.drop_duplicates()

                    if "patterns" in file_type and "rules" in file_type:
                        df_temp_merged = df_raw_mapping
                        df_temp_merged = df_raw_mapping.groupby('row_index').agg({'parser_name': lambda x: ''.join(x)})
                        if int(rule_count) > 0:
                            df_temp_merged['rule_applied'] = df_raw_mapping.groupby('row_index').agg({'rule_applied': lambda x: ''.join(x)})
                        df_raw_mapping = df_temp_merged
                    elif "patterns" in file_type:
                        df_raw_mapping = df_raw_mapping.groupby('row_index').agg({'parser_name': lambda x: ', '.join(x)})
                    elif "rules" in file_type and int(rule_count) > 0:
                        df_raw_mapping = df_raw_mapping.groupby('row_index').agg({'rule_applied': lambda x: ', '.join(x)})

                    body = {
                        '_source': {
                            'include': ['row_index', 'row_index_search', 'parser_name', 'log_text', 'parser_name', 'rule_applied']
                        },
                        'from': 0,
                        'size': total_no_lines if total_no_lines > 0 else 1,
                        'query': {
                            'bool': {
                                'must': [
                                    {'match': {'input_file_id': str(file_id)}},
                                    {"terms": {row_index_column: list_vals}}
                                ],
                            }
                        }
                    }
                    listval = list()
                    index = settings.ENVIRONMENT[settings.env]["elk_config"]["raw_index"]
                    logger.info("GET %s/_search" % index)
                    logger.info("%s" % json.dumps(body))
                    # res = es.search(index=index,body=body,sort="row_index:asc")
                    res = get_elastic_data_sort(index, body, "row_index:asc")
                    for hit in res['hits']['hits']:
                        listval.append(hit['_source'])
                    if len(listval) > 0:
                        dicts = listval[0]
                        keys = list(dicts.keys())

                        df_raw = pd.DataFrame(listval)
                        df_raw = df_raw.replace({pd.np.nan: None})
                        df_merge_col = pd.merge(df_raw, df_raw_mapping, on='row_index', how='left')
                        if int(rule_count) > 0:
                            df_merge_col = df_merge_col.rename(columns={'rule_applied_y': 'rule_applied', 'parser_name_y': 'parser_name'})
                        if "rules" in file_type and int(rule_count) > 0:
                            df_merge_col.loc[df_merge_col['rule_applied'].isnull(), 'rule_applied'] = df_merge_col['rule_applied_x']
                        if "patterns" in file_type:
                            df_merge_col = df_merge_col.rename(columns={'parser_name_y': 'parser_name'})
                            df_merge_col.loc[df_merge_col['parser_name'].isnull(), 'parser_name'] = df_merge_col['parser_name_x']
                        if "ml" in file_type and elk_ml_index != "" and elk_ml_index is not None:
                            df_merge_col['ml_applied'] = False
                            df_merge_col.loc[df_merge_col['row_index'].isin(ml_lines), 'ml_applied'] = True
                            if len(file_type) == 1:
                                df_merge_col = df_merge_col[df_merge_col['ml_applied'] == True]
                                total_no_lines = len(ml_lines)
                        listval = df_merge_col.to_dict('records')
            elif "raw" in file_type:
                logger.info("raw in file")
                index = settings.ENVIRONMENT[settings.env]["elk_config"]["raw_index"]
                body = {
                    '_source': {
                        'excludes': ['zip_id', 'input_file_id']
                    },
                    'from': fromValue,
                    'size': int(page_size),
                    'query': {
                        'bool': {
                            'must': mustval
                        }
                    }
                }

                logger.info('GET %s/_search' % index)
                logger.info('%s' % json.dumps(body))
                # res = settings.es.search(index=index,body=body,sort="row_index:asc")
                res = get_elastic_data_sort(index, body, "row_index:asc")
                index = settings.ENVIRONMENT[settings.env]["elk_config"]["raw_index"]
                body = {
                    'query': {
                        'bool': {
                            'must': mustval
                        }
                    }
                }

                logger.info('GET %s/_count' % index)
                logger.info('%s' % json.dumps(body))
                count = get_elastic_count(index, body)
                total_no_lines = count['count']
                index = indexes_allowed
                body = {
                    'query': {
                        'bool': {
                            'must': [
                                # {'match':{'zip_id':'101'}},
                                {'match': {'input_file_id': str(file_id)}}
                            ]
                        }
                    }
                }

                logger.info('GET %s/_count' % index)
                logger.info('%s' % json.dumps(body))
                count = get_elastic_count(index, body)
                total_no_lines_rawmapping = count['count']

                index = settings.ENVIRONMENT[settings.env]["elk_config"]["rules_index"]
                body = {
                    'query': {
                        'bool': {
                            'must': [
                                # {'match':{'zip_id':'101'}},
                                {'match': {'input_file_id': str(file_id)}}
                            ]
                        }
                    }
                }

                logger.info('GET %s/_count' % index)
                logger.info('%s' % json.dumps(body))
                count = get_elastic_count(index, body)
                rule_count = count['count']
                index = indexes_allowed
                body = {
                    '_source': {'include': rule_mapping_includeArray}, 'from': 0,
                    'size': total_no_lines_rawmapping if total_no_lines_rawmapping > 0 else 1,
                    'query': {
                        'bool': {
                            'must': [
                                {'match': {'input_file_id': str(file_id)}}
                            ]
                        }
                    },
                    "aggs": {
                        "parser_name": {
                            "terms": {
                                "field": "row_index"
                            }
                        }
                    }
                }

                logger.info('GET %s/_search' % index)
                logger.info('%s' % json.dumps(body))
                # res_mapping = settings.es.search(index=index,body=body,sort="row_index:asc")
                res_mapping = get_elastic_data_sort(index, body, "row_index:asc")
                for hit in res['hits']['hits']:
                    listval.append(hit['_source'])
                for hit in res_mapping['hits']['hits']:
                    listval_mapping.append(hit['_source'])
                if len(listval) > 0:
                    dicts = listval[0]
                    keys = list(dicts.keys())
                    df_raw = pd.DataFrame(listval)
                    df_raw = df_raw.replace({pd.np.nan: None})
                    if len(listval_mapping) > 0:
                        df_raw_mapping = pd.DataFrame(listval_mapping)
                        df_raw_mapping = df_raw_mapping.drop_duplicates()
                        df_raw_mapping = df_raw_mapping.fillna("")
                        if "patterns" in file_type and "rules" in file_type:
                            df_temp_merged = df_raw_mapping
                            df_temp_merged = df_raw_mapping.groupby('row_index').agg({'parser_name': lambda x: ''.join(x)})
                            if int(rule_count) > 0:
                                df_temp_merged['rule_applied'] = df_raw_mapping.groupby('row_index').agg({'rule_applied': lambda x: ''.join(x)})
                            df_raw_mapping = df_temp_merged
                        elif "patterns" in file_type:
                            df_raw_mapping = df_raw_mapping.groupby('row_index').agg({'parser_name': lambda x: ''.join(x)})
                        elif "rules" in file_type:
                            df_raw_mapping = df_raw_mapping.groupby('row_index').agg({'rule_applied': lambda x: ''.join(x)})

                        df_merge_col = pd.merge(df_raw, df_raw_mapping, on='row_index', how='left')
                        if int(rule_count) > 0:
                            df_merge_col = df_merge_col.rename(columns={'rule_applied_y': 'rule_applied', 'parser_name_y': 'parser_name'})
                        else:
                            df_merge_col = df_merge_col.rename(columns={'parser_name_y': 'parser_name'})
                        if "rules" in file_type and int(rule_count) > 0:
                            df_merge_col.loc[df_merge_col['rule_applied'].isnull(), 'rule_applied'] = df_merge_col['rule_applied_x']
                        if "patterns" in file_type:
                            df_merge_col.loc[df_merge_col['parser_name'].isnull(), 'parser_name'] = df_merge_col['parser_name_x']
                        if "ml" in file_type:
                            if elk_ml_index != "" and elk_ml_index is not None:
                                matchList = list()
                                matchList.append({'match': {file_id_column: str(file_id)}})
                                body = {
                                    'size': 10000,
                                    '_source': {
                                        'excludes': [file_id_column]
                                    },
                                    'query': {
                                        'bool': {
                                            'must': matchList
                                        }
                                    }
                                }

                                logger.info('GET %s/_search' % elk_ml_index)
                                logger.info('%s' % json.dumps(body))
                                # res = settings.es.search(index=elk_ml_index, body=body)
                                res = get_elastic_data(elk_ml_index, body)
                                if res == False:
                                    json_data = {"data": {"status": "Internal server error"},
                                                 "message": "Internal server error", "title": "Error"}
                                    resp = response_wrapper(500, json_data)
                                    return resp
                                ml_lines = list()
                                for hit in res['hits']['hits']:
                                    if isinstance(hit['_source'][log_line_numbers_column], list):
                                        ml_lines.extend(hit['_source'][log_line_numbers_column])
                                    else:
                                        ml_lines.append(hit['_source'][log_line_numbers_column])
                                ml_lines = list(map(int, ml_lines))
                                ml_lines = list(dict.fromkeys(ml_lines))
                                ml_lines.sort()

                                df_merge_col['ml_applied'] = False
                                df_merge_col.loc[df_merge_col['row_index'].isin(ml_lines), 'ml_applied'] = True

                        listval = df_merge_col.to_dict('records')
                    else:
                        listval = df_raw.to_dict('records')
            json_data = {'data': {'is_filter': is_filter, 'columns': keys, 'rows': listval, "total_no_lines": total_no_lines, "initial_page": initial_page}, "message": "Data received from elastic", "status": 200}
            resp = response_wrapper_plain_jsondumps(200, json_data)
        except BaseException as e:
            logger.error("Exception : {}".format(traceback.format_exc()))
            json_data = {"data": {"status": "Data not recieved"}, "message": "Data not recieved", "title": "Error"}
            resp = response_wrapper(500, json_data)
        return resp


# @analysis_ns.route("/gelastic_raw_data_new")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_jsonascsv)
class GetJsonAsCSV_ElasticRawDataNew(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get Elastic Search Data Search as JSON
        """
        req = request
        data = getRequestJSON(req)
        # zip_id = data['zip_id']
        file_id = data['file_id']
        file_type = data['file_type']
        page_no = data['page_no']
        page_size = data['page_size']
        elk_ml_index_de = None

        initial_page = 0
        listval = list()
        total_no_lines = 0
        is_filter = False
        filter_query = []

        data = {}
        return_dict = {
            'rows': [],
            'columns': [],
            'total_no_lines': 0,
        }

        if 'backward_raw' in file_type:
            file_extentions = set(map(lambda x: math.ceil(x / page_size), [page_no]))
            for x in file_extentions:
                page_no = x
            initial_page = page_no

        if "filter" in data:
            filter_data_main = data["filter"]
            if "filter_query" in filter_data_main:
                filter_query = filter_data_main["filter_query"]
                if len(filter_query) > 0:
                    for i, _filter_query in enumerate(filter_query):
                        filter_query[i]['query_string']['query'] = _filter_query['query_string']['query'].replace('\\', '')
                    is_filter = True

        api_url = 'http://{}:{}/request/search'.format(
            settings.ENVIRONMENT[settings.env]['spark_config']['QA_IP'],
            settings.ENVIRONMENT[settings.env]['spark_config']['QA_PORT']
        )

        query = "select zip_id from log_files where id='{}'".format(file_id)
        log_file_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
        zip_id = log_file_details[0]["zip_id"]
        if len(log_file_details) > 0:
            query = "select elk_ml_index_de from log_bundle a left join products b on a.prod_id=b.prod_id where a.id='{}'".format(zip_id)
            ml_index_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
            if len(ml_index_details) > 0:
                elk_ml_index_de = ml_index_details[0]["elk_ml_index_de"]

        indexes = []
        if "raw" in file_type:
            # indexes.append(settings.ENVIRONMENT[settings.env]["elk_config"]["raw_index"])
            indexes.append('raw')
        if "patterns" in file_type:
            # indexes.append(settings.ENVIRONMENT[settings.env]["elk_config"]["parser_index"])
            indexes.append('parser')
        if "rules" in file_type:
            # indexes.append(settings.ENVIRONMENT[settings.env]["elk_config"]["rules_index"])
            indexes.append('rules')
        if "ml" in file_type:
            if elk_ml_index_de is not None:
                # indexes.append(settings.ENVIRONMENT[settings.env]["elk_config"]["ml_index"])
                indexes.append(elk_ml_index_de)

        payload = {
            "file_id": file_id,
            "indexName": indexes,
            "global_filter": [],
            "queryType": "OverviewColumnSearch",
            "page_no": page_no,
            "page_size": page_size,
            "filter": {
                "filter": [],  # filter1,
                "filter_query": filter_query,
                "script": []
            }
        }
        r = requests.post(url=api_url, json=payload)
        logger.info('POST %s' % api_url)
        logger.info(json.dumps(payload))
        if r.text is not None and r.text != "":
            data = json.loads(r.text)

        return_dict['is_filter'] = is_filter
        return_dict['initial_page'] = initial_page
        return_dict['total_no_lines'] = data['total_no_lines'] if 'total_no_lines' in data else total_no_lines
        return_dict['columns'] = data['columns'] if 'columns' in data else []
        return_dict['rows'] = data['rows'] if 'rows' in data else []

        json_data = {"data": return_dict, "message": "Received data from Elastic", "status": 200}
        resp = response_wrapper_plain_jsondumps(200, json_data)
        return resp


# @analysis_ns.route("/g_csv_json_new")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_jsonascsv)
class GetJsonAsCSV_New(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get CSV Data as JSON
        """
        try:
            req = request
            data = getRequestJSON(req)
            file_id = data['file_id']
            file_type = data['file_type']
            page_no = data['page_no']
            folder_name = ""

            if file_type == "patterns":
                folder_name = "structure"
                query = "select zip_name from slog_files a left join log_bundle b on a.zip_id=b.id where a.id=" + str(file_id)
            elif file_type == "rules":
                folder_name = "rules"
                query = "select zip_name from rlog_files a left join log_bundle b on a.zip_id=b.id where a.id=" + str(file_id)
            result = connectFetchJSONWihtoutQueryDataNoResponse(query)
            if len(result) > 0:
                fileData = result[0]
                zip_name = fileData['zip_name']
                internal_file_page = 'x' + str(page_no).zfill(10)
                file_path = BASE_PATH_DIR + BASE_PATH_DIR_VERSION + "/" + str(zip_name) + "/app_cache/" + str(folder_name) + "/" + str(file_id) + "/" + str(internal_file_page)
                header_path = BASE_PATH_DIR + BASE_PATH_DIR_VERSION + "/" + str(zip_name) + "/app_cache/" + str(folder_name) + "/" + str(file_id) + "/header"
            else:
                json_data = {"data": [], "message": "Empty data received", "status": 200}
                resp = response_wrapper(200, json_data)
                return resp
            tableData = ""
            with open(header_path) as csvfile:
                readCSV = csv.reader(csvfile, delimiter=',')
                rowcount = 0
                column_names = list()
                for row in readCSV:
                    colcount = 0
                    for cell in row:
                        if (rowcount == 0):
                            column_names.append(str(cell))
                        colcount = colcount + 1
                    rowcount = rowcount + 1

            with open(file_path) as csvfile:
                readCSV = csv.reader(csvfile, delimiter=',')
                row_array = list()
                for row in readCSV:
                    rowvalue = list()
                    for cell in row:
                        rowvalue.append(str(cell))
                    row_array.append(rowvalue)
            json_data = {"data": {"columns": column_names, "rows": row_array}, "message": "Get the JSON data as CSV", "status": 200}
            resp = response_wrapper_plain_jsondumps(200, json_data)
            return resp
        except:
            logger.error(traceback.format_exc())
            json_data = {"data": {"status": "Data not received"}, "message": "Data not rceived", "title": "Error"}
            return response_wrapper(500, json_data)


# @analysis_ns.route("/grawdata_pagination")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_rawpaginated)
class GetRawPagination(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get Raw Data Details
        """
        try:
            req = request
            data = getRequestJSON(req)
            file_id = data['file_id']
            file_type = data['file_type']
            setValue_pattern = set()
            setValue_rules = set()
            line_number = 0
            page_no = data['page_no']
            total_no_lines = 0
            page_link_count = 0
            initial_page = 1

            line_number = (page_no - 1) * 25
            line_number = line_number + 1
            query = "select zip_name from log_files a left join log_bundle b on a.zip_id=b.id where a.id=" + str(file_id)
            result = connectFetchJSONWihtoutQueryDataNoResponse(query)
            fileData = result[0]
            zip_name = fileData['zip_name']
            internal_file_page = 'x' + str(page_no).zfill(10)
            file_path = BASE_PATH_DIR + BASE_PATH_DIR_VERSION + "/" + str(zip_name) + "/app_cache/log/" + str(file_id) + "/" + str(internal_file_page)
            query = "select a.id,a.input_file,json_agg(b.matching_rows) as pattern_matching_rows,json_agg(distinct bb.input_linenum) as rule_matching_rows from log_files a left join slog_files b on a.id=b.log_files_id left join rule_matching_rows bb on a.id=bb.input_file where a.id=" + str(file_id) + " group by a.id,a.input_file"
            result = connectFetchJSONWihtoutQueryDataNoResponse(query)
            fileData = result[0]
            if "patterns" in file_type:
                for x in fileData['pattern_matching_rows']:
                    if x is not None:
                        y = json.loads(x)
                        y = list(filter(None, y))
                        setValue_pattern.update(y)
            if "rules" in file_type:
                y = fileData['rule_matching_rows']
                setValue_rules.update(y)
            setValue_pattern = set(filter(None, setValue_pattern))
            setValue_rules = set(filter(None, setValue_rules))
            textFile = list()
            if "raw" not in file_type:
                line_number = 0
                internal_set1 = setValue_pattern.copy()
                internal_set1.update(setValue_rules)
                page_no = int(data['page_no'])
                page_size = int(data['page_size'])
                list_file_extentions = list(internal_set1)
                total_no_lines = len(list_file_extentions)
                page_link_count = int(total_no_lines / page_size) + 1
                page_range_start = int(page_size * (page_no - 1))
                page_range_end = int(page_size * page_no)
                internal_set1 = list_file_extentions[page_range_start:page_range_end]
                file_extentions = set(map(lambda x: math.ceil(x / 25), internal_set1))
                file_extentions = set(sorted(file_extentions))

                for xxx in file_extentions:
                    internal_file_page = 'x' + str(xxx).zfill(10)
                    file_path = BASE_PATH_DIR + BASE_PATH_DIR_VERSION + "/" + str(zip_name) + "/app_cache/log/" + str(file_id) + "/" + str(internal_file_page)
                    tempList = list()
                    if os.path.exists(file_path):
                        with open(file_path, 'r') as f:
                            tempList.extend(f.readlines())
                        print(tempList)
                        line_number = (xxx - 1) * 25
                        line_number = line_number + 1
                        cc = line_number - 1
                        textFile.extend(list(map(lambda x: [(x + 1) + cc, tempList[x]], range(0, len(tempList)))))
            else:
                page_no = int(data['page_no'])
                page_size = int(data['page_size'])
                if 'backward_raw' in file_type:
                    file_extentions = set(map(lambda x: math.ceil(x / page_size), [page_no]))
                    for x in file_extentions:
                        page_no = x
                    initial_page = page_no
                    file_type = ['raw', 'patterns', 'rules']

                page_count = int(page_size / 25)
                page_start = int(page_count * (page_no - 1) + 1)
                for i in range(page_start, page_start + page_count):
                    internal_file_page = 'x' + str(i).zfill(10)
                    file_path = BASE_PATH_DIR + BASE_PATH_DIR_VERSION + "/" + str(zip_name) + "/app_cache/log/" + str(file_id) + "/" + str(internal_file_page)
                    if os.path.isfile(file_path):
                        with open(file_path, 'r') as f:
                            textFile.extend(f.readlines())
                cc = int(page_size * (page_no - 1) + 1)
                textFile = list(map(lambda x: [x + cc, textFile[x]], range(0, len(textFile))))
            map_output = list(map(lambda x: [x[0], x[1], ('patterns_rules' if (x[0] in setValue_pattern and x[0] in setValue_rules) else ('patterns' if x[0] in setValue_pattern else ('rules' if x[0] in setValue_rules else ('raw' if 'raw' in file_type else 'Others'))))], textFile))
            rawData = list(filter(lambda x: (True if 'patterns_rules' in x else (True if 'patterns' in x else (True if 'rules' in x else (True if 'raw' in x else False)))), map_output))
            rows = {"columns": ["row_index,log_text,file_type"], "rows": rawData, "total_no_lines": total_no_lines, "page_link_count": page_link_count, "initial_page": initial_page}
            json_data = {"data": rows, "message": "Received raw data", "status": 200, }
            resp = response_wrapper_plain_jsondumps(200, json_data)
            return resp
        except:
            logger.error(traceback.format_exc())
            json_data = {"data": {"status": "Data not received"}, "message": "Data not received", "title": "Error"}
            return response_wrapper(500, json_data)


# @analysis_ns.route("/gparser_line_details")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_parserline_details)
class GetParserLineDetails(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get Raw Parser Details
        """
        try:
            logger.info("GetParserLineDetails")
            req = request
            data = getRequestJSON(req)
            file_id = data['file_id']

            parser_arr_includes = ['row_index', 'parser_name']
            mustval = list()
            mustval.append({'match': {'input_file_id': str(file_id)}})
            if 'row_index' in data:
                s_no = int(data['row_index'])
                mustval.append({'match': {'row_index': s_no}})
            index = settings.ENVIRONMENT[settings.env]["elk_config"]["parser_index"]
            body = {'query': {
                'bool': {
                    'must': mustval
                }
            }}
            logger.info('GET %s/_count' % index)
            logger.info('%s' % json.dumps(body))
            count = get_elastic_count(index, body)
            total_no_lines = count['count']
            index = settings.ENVIRONMENT[settings.env]["elk_config"]["parser_index"]
            body = {'_source': {'include': parser_arr_includes}, 'from': 0, 'size': total_no_lines, 'query': {
                'bool': {
                    'must': mustval
                }
            }
                    }
            logger.info('GET %s/_search' % index)
            logger.info('%s' % json.dumps(body))
            # res_parser_data = settings.es.search(index=index,body=body,sort="row_index:asc")
            res_parser_data = get_elastic_data_sort(index, body, "row_index:asc")
            listval = list()
            for hit in res_parser_data['hits']['hits']:
                listval.append(hit['_source'])
            df = pd.DataFrame(listval)
            df = df.rename(columns={"row_index": "file_row_id"})
            df = df.drop_duplicates()
            map_final_ouput = df.to_dict('records')
            json_data = {"data": map_final_ouput, "message": "Received parser line details", "status": 200}
            resp = response_wrapper_plain_jsondumps(200, json_data)
            return resp
        except:
            logger.error(traceback.format_exc())
            json_data = {
                "data": {"status": "Internal server error"},
                "message": "Internal server error", "title": "Error"}
            return response_wrapper(500, json_data)


class GetRuleLineDetails(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get Raw Rule Details
        """
        try:
            logger.info("GetRuleLineDetails")
            req = request
            data = getRequestJSON(req)
            file_id = data['file_id']

            parser_arr_includes = ['row_index', 'rule_applied']
            mustval = list()
            mustval.append({"exists": {"field": "rule_applied"}})
            mustval.append({'match': {'input_file_id': str(file_id)}})
            if 'row_index' in data:
                s_no = int(data['row_index'])
                mustval.append({'match': {'row_index': s_no}})
            index = settings.ENVIRONMENT[settings.env]["elk_config"]["rules_index"]
            body = {'query': {
                'bool': {
                    'must': mustval,
                    "must_not": {"term": {"rule_applied.keyword": ""}}
                }
            }}
            logger.info('GET %s/_count' % index)
            logger.info('%s' % json.dumps(body))
            count = get_elastic_count(index, body)
            total_no_lines = count['count']

            index = settings.ENVIRONMENT[settings.env]["elk_config"]["rules_index"]
            body = {'_source': {'include': parser_arr_includes}, 'from': 0, 'size': total_no_lines, 'query': {
                'bool': {
                    'must': mustval,
                    "must_not": {"term": {"rule_applied.keyword": ""}}
                }
            }
                    }
            logger.info('GET %s/_search' % index)
            logger.info('%s' % json.dumps(body))
            res_rule_data = get_elastic_data_sort(index, body, "row_index:asc")

            listval = list()
            for hit in res_rule_data['hits']['hits']:
                listval.append(hit['_source'])
            df = pd.DataFrame(listval)
            df = df.rename(columns={"row_index": "file_row_id", "rule_applied": "rule_name"})
            df = df.drop_duplicates()
            map_final_ouput = df.to_dict('records')
            json_data = {"data": map_final_ouput, "message": "Received rules line details", "status": 200}
            resp = response_wrapper_plain_jsondumps(200, json_data)
            return resp
        except:
            logger.error(traceback.format_exc())
            json_data = {
                "data": {"status": "Internal server error"},
                "message": "Internal server error", "title": "Error"}
            return response_wrapper(500, json_data)


# @analysis_ns.route("/csvdownload")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_csv_download)
class GetCSVDownload(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def get(self, request):
        """
        Get CSV Downloads
        """
        req = request
        try:
            # download_json = getRequestJSON(req)
            id = req.GET.get('id')
            status_type = req.GET.get('status_type')
            # print(type(download_json))
            query = ""
            if (status_type == "ml"):
                query = f"select ml as file_name from ml where ml_id={id}"
            if (status_type == "decrypt"):
                query = f"select input_file as file_name from log_files where id={id}"
                logger.info(query)
                mustval = list()
                mustval.append({'match': {'input_file_id': str(id)}})

                index = settings.ENVIRONMENT[settings.env]["elk_config"]["raw_index"]
                body = {'query': {
                    'bool': {
                        'must': mustval
                    }
                }}
                logger.info('GET %s/_count' % index)
                logger.info('%s' % json.dumps(body))
                count = get_elastic_count(index, body)
                total_no_lines = count['count']
                logger.info(total_no_lines)

                index = settings.ENVIRONMENT[settings.env]["elk_config"]["raw_index"]
                body = {'_source': {'excludes': ['zip_id', 'input_file_id']}, 'from': 0, 'size': total_no_lines, 'query': {
                    'bool': {
                        'must': mustval
                    }
                }
                        }
                logger.info("GET %s/_search" % index)
                logger.info("%s" % json.dumps(body))
                # res = settings.es.search(index=index,body=body,sort="row_index:asc")
                res = get_elastic_data_sort(index, body, "row_index:asc")
            elif (status_type == "structured"):
                query = f"select structured_file as file_name from slog_files where id={id}"
                mustval = list()
                mustval.append({'match': {'output_file_id': str(id)}})
                index = settings.ENVIRONMENT[settings.env]["elk_config"]["parser_index"]
                body = {'query': {
                    'bool': {
                        'must': mustval
                    }
                }}
                logger.info('GET %s/_count' % index)
                logger.info('%s' % json.dumps(body))
                count = get_elastic_count(index, body)
                total_no_lines = count['count']

                index = settings.ENVIRONMENT[settings.env]["elk_config"]["parser_index"]
                body = {'_source': {'excludes': ['zip_id', 'output_file_id', 'input_file_id', 'gui_rules_parser_name', 'parser_name']}, 'from': 0, 'size': total_no_lines, 'query': {
                    'bool': {
                        'must': mustval
                    }
                }
                        }
                logger.info("GET %s/_search" % index)
                logger.info("%s" % json.dumps(body))
                # res = settings.es.search(index=index, body=body)
                res = get_elastic_data(index, body)
            elif (status_type == "rule"):
                query = f"select rule_file as file_name from rlog_files where id={id}"
                mustval = list()
                mustval.append({'match': {'output_file_id': str(id)}})
                index = settings.ENVIRONMENT[settings.env]["elk_config"]["rules_index"]
                body = {'query': {
                    'bool': {
                        'must': mustval
                    }
                }}
                logger.info('GET %s/_count' % index)
                logger.info('%s' % json.dumps(body))
                count = get_elastic_count(index, body)
                total_no_lines = count['count']
                index = settings.ENVIRONMENT[settings.env]["elk_config"]["rules_index"]
                body = {'_source': {'excludes': ['zip_id', 'output_file_id', 'input_file_id', 'rule_applied']}, 'from': 0, 'size': total_no_lines, 'query': {
                    'bool': {
                        'must': mustval
                    }
                }}
                logger.info("GET %s/_search" % index)
                logger.info("%s" % json.dumps(body))
                # res = settings.es.search(index=index,body=body)
                res = get_elastic_data(index, body)
            filedetail = connectFetchJSONWithoutResponse(query, '')
            if len(filedetail) == 0:
                json_data = {"data": {"status": "File Not Found"}, "message": "File Not Found", "title": "Error"}
                resp = response_wrapper_plain_jsondumps(404, json_data)
            else:
                filename = filedetail[0]["file_name"].split('/')[-1]
                listval = list()
                for hit in res['hits']['hits']:
                    listval.append(hit['_source'])
                logger.info(len(listval))
                pd.set_option("display.max_colwidth", 10000)
                df = pd.DataFrame(listval)
                if (status_type == "decrypt"):
                    if "cap_decoder" in filename:
                        resp = HttpResponse(content_type='text/csv')
                        resp['Content-Disposition'] = "attachment; filename=" + str(filename.replace(".csv", ".txt"))
                        df.to_csv(path_or_buf=resp, index=False)
                    else:
                        resp = HttpResponse(content_type='text/plain')
                        resp['Content-Disposition'] = "attachment; filename=" + str(filename.replace(".csv", ".txt"))
                        df['log_text'].to_string(path_or_buf=resp, index=False)
                else:
                    resp = HttpResponse(content_type='text/csv')
                    resp['Content-Disposition'] = "attachment; filename=" + str(filename)
                    df.to_csv(path_or_buf=resp, index=False)
                return resp
        except BaseException as e:
            json_data = {"data": {"status": "Download failed"}, "message": "Download failed", "title": "Error"}
            resp = response_wrapper_plain_jsondumps(500, json_data)
        return resp


# @analysis_ns.route("/csvdownload_new")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_csv_download)
class GetCSVDownloadNew(APIView):
    permission_classes = (IsActive,)

    def get_columns(self, rows):
        all_keys = list()
        keys_list = [list(row.keys()) for row in rows]
        for keys in keys_list:
            all_keys.extend(keys)
            all_keys = list(set(all_keys))
        return set(all_keys)

    def get_sorted_columns(self, rows, priority_columns):
        return_all_columns = list()
        all_columns = self.get_columns(rows)
        priority_columns_list = priority_columns.split(",")
        priority_columns_list = [priority_column.strip() for priority_column in priority_columns_list]
        remaining_columns = set(all_columns) - set(priority_columns_list)
        list(remaining_columns).sort(reverse=False)
        return_all_columns.extend(priority_columns_list)
        return_all_columns.extend(remaining_columns)
        return_all_columns = list(filter(None, return_all_columns))
        return return_all_columns

    def get_sorted_rows(self, rows, sorted_columns):
        return_list_of_dict = []
        for row in rows:
            new_dict = OrderedDict()
            for sorted_column in sorted_columns:
                new_dict[sorted_column] = row.get(sorted_column)
            return_list_of_dict.append(new_dict)
        return return_list_of_dict

    @csrf_exempt
    def get(self, request):
        """
        Get CSV Downloads
        """
        req = request
        try:
            id = req.GET.get('id')
            status_type = req.GET.get('status_type')
            query = ""
            if (status_type == "ml"):
                query = f"select ml as file_name from ml where ml_id={id}"
            if (status_type == "decrypt"):
                query = f"select input_file as file_name from log_files where id={id}"
                mustval = list()
                mustval.append({'match': {'input_file_id': str(id)}})
                index = settings.ENVIRONMENT[settings.env]["elk_config"]["raw_index"]
                body = {'query': {
                    'bool': {
                        'must': mustval
                    }
                }}
                logger.info('GET %s/_count' % index)
                logger.info('%s' % json.dumps(body))
                count = get_elastic_count(index, body)
                total_no_lines = count['count']
                index = settings.ENVIRONMENT[settings.env]["elk_config"]["raw_index"]
                body = {'_source': {'excludes': ['zip_id', 'input_file_id']}, 'from': 0, 'size': total_no_lines, 'query': {
                    'bool': {
                        'must': mustval
                    }
                }
                        }
                logger.info("GET %s/_search" % index)
                logger.info("%s" % json.dumps(body))
                # res = settings.es.search(index=index,body=body,sort="row_index:asc")
                res = get_elastic_data_sort(index, body, "row_index:asc")
            elif (status_type == "structured"):
                query = f"select structured_file as file_name from slog_files where id={id}"
                mustval = list()
                mustval.append({'match': {'output_file_id': str(id)}})
                index = settings.ENVIRONMENT[settings.env]["elk_config"]["parser_index"]
                body = {'query': {
                    'bool': {
                        'must': mustval
                    }
                }}
                logger.info('GET %s/_count' % index)
                logger.info('%s' % json.dumps(body))
                count = get_elastic_count(index, body)
                total_no_lines = count['count']

                index = settings.ENVIRONMENT[settings.env]["elk_config"]["parser_index"]
                body = {'_source': {'excludes': ['zip_id', 'output_file_id', 'input_file_id', 'gui_rules_parser_name', 'parser_name']}, 'from': 0, 'size': total_no_lines, 'query': {
                    'bool': {
                        'must': mustval
                    }
                }
                        }
                logger.info("GET %s/_search" % index)
                logger.info("%s" % json.dumps(body))
                # res = settings.es.search(index=index, body=body)
                res = get_elastic_data(index, body)
            elif (status_type == "rule"):
                query = f"select rule_file as file_name from rlog_files where id={id}"
                mustval = list()
                mustval.append({'match': {'output_file_id': str(id)}})
                index = settings.ENVIRONMENT[settings.env]["elk_config"]["rules_index"]
                body = {'query': {
                    'bool': {
                        'must': mustval
                    }
                }}
                logger.info('GET %s/_count' % index)
                logger.info('%s' % json.dumps(body))
                count = get_elastic_count(index, body)
                total_no_lines = count['count']
                index = settings.ENVIRONMENT[settings.env]["elk_config"]["rules_index"]
                body = {'_source': {'excludes': ['zip_id', 'output_file_id', 'input_file_id', 'rule_applied']}, 'from': 0, 'size': total_no_lines, 'query': {
                    'bool': {
                        'must': mustval
                    }
                }}
                logger.info("GET %s/_search" % index)
                logger.info("%s" % json.dumps(body))
                # res = settings.es.search(index=index, body=body)
                res = get_elastic_data(index, body)
            filedetail = connectFetchJSONWithoutResponse(query, '')
            if len(filedetail) == 0:
                json_data = {"data": {"status": "File Not Found"}, "message": "File Not Found", "title": "Error"}
                resp = response_wrapper_plain_jsondumps(404, json_data)
            else:
                filename = filedetail[0]["file_name"].split('/')[-1]
                listval = list()
                for hit in res['hits']['hits']:
                    listval.append(hit['_source'])
                if len(listval) > 0:
                    if status_type == 'rule' or status_type == 'structured':
                        column_sequence = request.GET.get('column_sequence', '')
                        sorted_columns = self.get_sorted_columns(listval, column_sequence)
                        listval = self.get_sorted_rows(listval, sorted_columns)

                pd.set_option("display.max_colwidth", 10000)
                df = pd.DataFrame(listval)
                if (status_type == "decrypt"):
                    if "cap_decoder" in filename:
                        resp = HttpResponse(content_type='text/csv')
                        resp['Content-Disposition'] = "attachment; filename=" + str(filename.replace(".csv", ".txt"))
                        df.to_csv(path_or_buf=resp, index=False)
                    else:
                        # file_data = df['log_text'].to_string(header=False, index=False)
                        file_data = "\n".join(df['log_text'].tolist())
                        resp = HttpResponse(file_data, content_type='text/plain')
                        resp['Content-Disposition'] = "attachment; filename=" + str(filename.replace(".csv", ".txt"))
                else:
                    resp = HttpResponse(content_type='text/csv')
                    resp['Content-Disposition'] = "attachment; filename=" + str(filename)
                    df.to_csv(path_or_buf=resp, index=False)
                return resp
        except BaseException as e:
            logger.error(traceback.format_exc())
            json_data = {"data": {"status": "Download failed"}, "message": "Download failed", "title": "Error"}
            resp = response_wrapper_plain_jsondumps(500, json_data)
        return resp


# @analysis_ns.route("/dlogbundles")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_delete_logbundle)
class DeleteLogBundel(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        try:
            req = request
            data = getRequestJSON(req)
            zip_id = data['zip_id']
            user_id = data['user_id']
            prod_id = data['prod_id']
            logger.info("Updating log bundle isdeleted to 1 for {}".format(str(zip_id)))
            query = "update log_bundle set isdeleted=1 where id='{}'".format(str(zip_id))
            dummyres = connectFetchJSONWihtoutQueryDataNoResponse(query)
            if zip_id is not None:
                api_url = 'http://{}:{}/delete'.format(settings.ENVIRONMENT[settings.env]["spark_config"]["RM_IP"], settings.ENVIRONMENT[settings.env]["spark_config"]["RM_PORT"])
                delete_data = {"zip_id": zip_id, "jobType": "deleteBundle", "region": settings.env}
                logger.info("Sending delete request to RM through API")
                logger.info(delete_data)
                r = requests.post(url=api_url, json=delete_data)
                if r.status_code == 200:
                    logger.info("Delete request success {}: {}".format(r.status_code, r.text))
                else:
                    logger.info("Delete request failed {}: {}".format(r.status_code, r.text))
            query = "select id as zip_id,replace(zip_name,'.zip','') as zip_name from log_bundle where user_id='{}' and prod_id='{}' and isdeleted=0 order by to_timestamp(right(zip_name, 19), 'YYYY-MM-DD HH24:MI:SS'::text) desc".format(str(user_id), str(prod_id))
            resp = connectFetchJSONWithoutQueryData(query)
            return resp
        except BaseException as e:
            logger.error(traceback.format_exc())
            json_data = {"data": {"satus": "Request Failed"}, "message": "Request Failed", "title": "Error"}
            return response_wrapper_plain_jsondumps(500, json_data)


# @analysis_ns.route("/bulk-dlogbundles")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_delete_logbundle)
class BulkDeleteLogBundel(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        try:
            req = request
            data = getRequestJSON(req)
            zip_ids = data['zip_ids']
            user_id = data['user_id']
            for zip_id in zip_ids:
                logger.info("Updating log bundle isdeleted to 1 for {}".format(str(zip_id)))
                query = "update log_bundle set isdeleted=1 where id='{}'".format(str(zip_id))
                dummyres = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if zip_id is not None:
                    api_url = 'http://{}:{}/delete'.format(settings.ENVIRONMENT[settings.env]["spark_config"]["RM_IP"], settings.ENVIRONMENT[settings.env]["spark_config"]["RM_PORT"])
                    delete_data = {"zip_id": zip_id, "jobType": "deleteBundle", "region": settings.env}
                    logger.info("Sending delete request to RM through API")
                    logger.info(delete_data)
                    r = requests.post(url=api_url, json=delete_data)
                    if r.status_code == 200:
                        logger.info("Delete request success {}: {}".format(r.status_code, r.text))
                    else:
                        logger.info("Delete request failed {}: {}".format(r.status_code, r.text))
            query = "select id as zip_id,replace(zip_name,'.zip','') as zip_name from log_bundle where user_id='{}' and isdeleted=0 order by to_timestamp(right(zip_name, 19), 'YYYY-MM-DD HH24:MI:SS'::text) desc".format(str(user_id))
            resp = connectFetchJSONWithoutQueryData(query)
            return resp
        except BaseException as e:
            logger.error(traceback.format_exc())
            json_data = {"data": {"satus": "Request Failed"}, "message": "Request Failed", "title": "Error"}
            return response_wrapper_plain_jsondumps(500, json_data)


def filterApplyForTablesAnalysis(data, page_type, global_filter):
    filterArr = list()
    # structured_filters = {"file":"input_file","parser":"(case when grok_patterns='' then b.name else grok_patterns end)","status":"(case when a.status=0 then 'In queue awaiting dependency' else (case when a.status=2 then 'Completed' else (case when a.status=1 then 'In Progress' else (case when a.status=3 then 'Failed' else '' end) end) end) end)","parsed_lines":"a.no_lines","remarks":"a.serr"}
    structured_filters = {"file": "(split_part(input_file,'/',array_length(regexp_split_to_array(input_file, '/'),1)))", "parser": "(case when b.name='' or b.name is null then b.grok_patterns else b.name end)", "status": "(case when a.status=0 then 'In queue awaiting dependency' else (case when a.status=2 then 'Completed' else (case when a.status=1 then 'In Progress' else (case when a.status=3 then 'Failed' else '' end) end) end) end)", "parsed_lines": "a.no_lines", "remarks": "a.serr"}
    rule_filters = {"rule_name": "bb.name", "parser_name": "gg.name", "status": "(case when a.status=0 then 'In queue awaiting dependency' else (case when a.status=2 then 'Completed' else (case when a.status=1 then 'In Progress' else (case when a.status=3 then 'Failed' else '' end) end) end) end)", "no_lines": "a.no_lines", "error": "a.serr"}
    decrypt_filters = {"decrypted_file": "decrypted_file", "status": "(case when a.status=0 then 'In queue awaiting dependency' else (case when a.status=2 then 'Completed' else (case when a.status=1 then 'In Progress' else (case when a.status=3 then 'Failed' else '' end) end) end) end)", "no_lines": "a.no_lines", "error": "a.serr"}
    rawdata_filters = {"file_name": "(split_part(a.input_file,'/',array_length(regexp_split_to_array(a.input_file, '/'),1)))", "total_lines": "a.no_lines", "anomalies": "output_linenum"}
    statushistory_filters = {"status_msg": "status_msg", "created_at": "created_at::text", "s_no": "s_no::text"}
    if "filter" in data:
        logger.info(data["filter"])
        if "filter" in data["filter"]:
            data_filter = data["filter"]
            data_filter = data_filter['filter']
            logger.info(data_filter)
            for x in data_filter:
                wildcard = x["wildcard"]
                logger.info(wildcard)
                for k, v in wildcard.items():
                    logger.info(k)
                    logger.info(v)
                    ilike_v = str(v).replace('*', ' ').strip()
                    ilike_v = '%' + ilike_v + '%'
                    if len(global_filter) > 0:
                        v = '*' + str(global_filter) + '*'
                    if page_type == "structured":
                        if k == "parsed_lines":
                            if v != "*":
                                try:
                                    ss = str(v).replace('*', '')
                                    vv = int(ss)
                                    filterArr.append(structured_filters[k] + " = " + str(vv))
                                except Exception:
                                    logger.info("Skipping Filter for " + str(k) + " because of search string is not an integer")
                        else:
                            filterArr.append(structured_filters[k] + " ilike '" + str(v).replace('*', '%') + "'")
                    elif page_type == "rule":
                        if k == "no_lines":
                            if v != "*":
                                try:
                                    ss = str(v).replace('*', '')
                                    vv = int(ss)
                                    filterArr.append(rule_filters[k] + " = " + str(vv))
                                except Exception:
                                    logger.info("Skipping Filter for " + str(k) + " because of search string is not an integer")
                        else:
                            filterArr.append(rule_filters[k] + " ilike '" + str(v).replace('*', '%') + "'")
                    elif page_type == "decrypt":
                        if k == "no_lines":
                            if v != "*":
                                try:
                                    ss = str(v).replace('*', '')
                                    vv = int(ss)
                                    filterArr.append(decrypt_filters[k] + " = " + str(vv))
                                except Exception:
                                    logger.info("Skipping Filter for " + str(k) + " because of search string is not an integer")
                        elif k == "error":
                            if v != "*":
                                filterArr.append(decrypt_filters[k] + " ilike '" + str(v).replace('*', '%') + "'")
                        else:
                            filterArr.append(decrypt_filters[k] + " ilike '" + str(v).replace('*', '%') + "'")
                    elif page_type == "rawdata":
                        if k == "total_lines":
                            if v != "*":
                                try:
                                    ss = str(v).replace('*', '')
                                    vv = int(ss)
                                    filterArr.append(rawdata_filters[k] + " = " + str(vv))
                                except Exception:
                                    logger.info("Skipping Filter for " + str(k) + " because of search string is not an integer")
                        elif k == "anomalies":
                            if v != "*":
                                try:
                                    ss = str(v).replace('*', '')
                                    vv = int(ss)
                                    filterArr.append(rawdata_filters[k] + " = " + str(vv))
                                except Exception:
                                    logger.info("Skipping Filter for " + str(k) + " because of search string is not an integer")
                        elif k == "ml_anomalies":
                            pass
                        else:
                            filterArr.append(rawdata_filters[k] + " ilike '" + str(v).replace('*', '%') + "'")
                    elif page_type == "statushistory":
                        if v != "*":
                            filterArr.append(statushistory_filters[k] + " ilike '" + ilike_v + "'")
    strFiltered = ""
    if len(filterArr) > 0:
        if len(global_filter) > 0:
            strFiltered = ' or '.join(filterArr)
        else:
            strFiltered = ' and '.join(filterArr)
        strFiltered = ' and (' + strFiltered + ')'
        logger.info(strFiltered)
    return strFiltered


# @analysis_ns.route("/umlfeedback")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_delete_logbundle)
class UpdateMLFeedback(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Update ML Feebdack
        """
        try:
            req = request
            data = getRequestJSON(req)
            if 'zip_id' in data:
                _clone_zip_id = data['zip_id']
                query = f"select real_zip_id from log_bundle where id='{_clone_zip_id}'"
                details = connectFetchJSONWithoutResponse(query, '')
                if len(details) > 0:
                    print('details', details)
                    _zip_id = details[0]["real_zip_id"]
                    if _zip_id is not None:
                        zip_id = _zip_id
                        data['zip_id'] = zip_id
            zip_id = data['zip_id']
            caseId = ("'" + data['caseId'] + "'") if 'caseId' in data else 'null'
            tag = data['tag'] if 'tag' in data else []
            tag_str = '[]'
            if type(tag) == list and len(tag) > 0:
                tag_str = "['" + "','".join(tag) + "']"
            elk_ml_index = ""
            query = "select elk_ml_index from log_bundle a left join products b on a.prod_id=b.prod_id where a.id='{}'".format(zip_id)
            logger.info("select query: %s" % query)
            ml_index_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
            if len(ml_index_details) > 0:
                elk_ml_index = ml_index_details[0]["elk_ml_index"]
            if elk_ml_index != "" and elk_ml_index is not None:
                feedback_id = data['feedback_id']
                feedback = data['feedback']

                q = {
                    "script": {
                        "source": """
                            ctx._source.feedback = %s;
                            ctx._source.feedback_case_id = %s;
                            ctx._source.feedback_tags = %s;
                            ctx._source.feedback_created_at = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
                        """ % (feedback, caseId, tag_str),
                        "lang": "painless"
                    },
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"zip_id.keyword": zip_id}},
                                {"terms" if type(feedback_id) == list else "term": {"feedback_id": feedback_id}}
                            ]
                        }
                    }
                }
                logger.info('POST %s/_update_by_query' % elk_ml_index)
                logger.info("%s" % q)
                es.update_by_query(body=q, index=elk_ml_index)
                json_data = {
                    "data": {"status": "Data received"},
                    "message": "Data received"}
                resp = response_wrapper(200, json_data)
            else:
                json_data = {"data": {"status": "zip_id does not contains ML mapping"}, "message": "zip_id does not contains ML mapping", "title": "Error"}
                resp = response_wrapper(400, json_data)
            return resp
        except Exception as e:
            logger.error(traceback.format_exc())
            json_data = {
                "data": {"status": "Internal server error"},
                "message": "Internal server error", "title": "Error"}
            return response_wrapper(500, json_data)


# @analysis_ns.route("/update_raw_info")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_delete_logbundle)
class UpdateRawInfo(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Update RAW analysis comments and anomaly
        """
        try:
            logger.info("Update raw file info")
            req = request
            data = getRequestJSON(req)
            zip_id = data['zip_id']
            if zip_id is not None:
                api_url = 'http://{}:{}/submit'.format(settings.ENVIRONMENT[settings.env]["spark_config"]["RM_IP"], settings.ENVIRONMENT[settings.env]["spark_config"]["RM_PORT"])
                update_row_data = {"zip_id": zip_id, "input_file_id": data['input_file_id'], "jobType": "upsertAnomaly", "data": data['data'], "category": "bundleprocessing"}
                r = requests.post(url=api_url, json=update_row_data)
                logger.info("API call to RM is completed")
                logger.info(str(r.status_code) + " -- " + r.reason + " -- " + r.text)
                json_data = {"data": {"status": "Request received"}, "message": "Request received"}
                resp = response_wrapper_plain_jsondumps(200, json_data)
            else:
                json_data = {"data": {"status": "Bad input data"}, "message": "Bad input data", "title": "Error"}
            resp = response_wrapper(400, json_data)

            return resp
        except:
            logger.error(traceback.format_exc())
            json_data = {
                "data": {"status": "Internal server error"},
                "message": "Internal server error", "title": "Error"}
            return response_wrapper(500, json_data)


# @analysis_ns.route("/retrievearchive")
# @analysis_ns.doc(parser=req_token_header,body=req_analysis_delete_logbundle)
class RetrieveArchivalProcess(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Retrieve Archived Bundle
        """
        try:
            logger.info("Retrieve archival process")
            req = request
            data = getRequestJSON(req)
            zip_id = data['zip_id']

            create_row_data = {"bundleID": str(zip_id), "jobType": "retrieval", "region": settings.ENVIRONMENT[settings.env]["spark_config"]["region"], "category": "bundleprocessing"}
            logger.info(json.dumps(create_row_data))
            api_url = 'http://{}:{}/submit'.format(settings.ENVIRONMENT[settings.env]["spark_config"]["RM_IP"], settings.ENVIRONMENT[settings.env]["spark_config"]["RM_PORT"])
            r = requests.post(url=api_url, json=create_row_data)
            logger.info("API call to RM is completed")
            logger.info(str(r.status_code) + " -- " + r.reason + " -- " + r.text)
            query = "update log_bundle set archive_status=4 where id='{}'".format(str(zip_id))
            dummyres = connectFetchJSONWihtoutQueryDataNoResponse(query)
            json_data = {"data": [], "message": "Sent for Retrieval", "status": 200}
            resp = response_wrapper_plain_jsondumps(200, json_data)
            return resp
        except:
            logger.error(traceback.format_exc())
            json_data = {
                "data": {"status": "Internal server error"},
                "message": "Internal server error", "title": "Error"}
            return response_wrapper(500, json_data)


# @analysis_ns.route("/gupb_overview_bundle")
class GetOverviewOfBundleDetails(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get GetOverviewOfBundleDetails
        """
        try:
            req = request
            data = getRequestJSON(req)
            status_code = 404
            json_data = {}
            zip_id = data["zip_id"]
            overview_bundle_details = list()
            bundle_infos = []
            query = """
                select vb.* 
                from v_overview_bundle_details vb
                where vb.bundle_id='%s';
                """ % (zip_id,)
            logger.info(query)
            bundle_infos = connectFetchJSONWihtoutQueryDataNoResponse(query)
            if len(bundle_infos) > 0:
                overview_bundle_details.extend(bundle_infos)

                parser_processing = []
                query = "select age(to_timestamp(trim(split_part(ended,'@',2)),'YYYY-MM-DD\"T\"HH24:MI:SS'),to_timestamp(trim(split_part(started,'@',2)), 'YYYY-MM-DD\"T\"HH24:MI:SS'))::text as parser_processing from (select zip_id,status_msg as started from global_status where zip_id='{}' and status_msg~*'parser started') a left join (select zip_id,status_msg as ended from global_status where zip_id='{}' and status_msg~*'parser completed') b on a.zip_id=b.zip_id limit 1".format(zip_id, zip_id)
                parser_processing = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(parser_processing) == 0:
                    parser_processing = []
                overview_bundle_details.extend(parser_processing)

                rule_processing = []
                query = "select age(to_timestamp(trim(split_part(ended,'@',2)),'YYYY-MM-DD\"T\"HH24:MI:SS'),to_timestamp(trim(split_part(started,'@',2)), 'YYYY-MM-DD\"T\"HH24:MI:SS'))::text as rule_processing from (select zip_id,status_msg as started from global_status where zip_id='{}' and status_msg~*'rule started') a left join (select zip_id,status_msg as ended from global_status where zip_id='{}' and status_msg~*'rule completed') b on a.zip_id=b.zip_id limit 1".format(zip_id, zip_id)
                rule_processing = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(rule_processing) == 0:
                    rule_processing = []
                overview_bundle_details.extend(rule_processing)

                ml_processing = []
                query = "select age(to_timestamp(trim(split_part(ended,'@',2)),'YYYY-MM-DD\"T\"HH24:MI:SS'),to_timestamp(trim(split_part(started,'@',2)), 'YYYY-MM-DD\"T\"HH24:MI:SS'))::text as ml_processing from (select zip_id,status_msg as started from global_status where zip_id='{}' and status_msg~*'ml job execution started') a left join (select zip_id,status_msg as ended from global_status where zip_id='{}' and status_msg~*'ml job execution completed') b on a.zip_id=b.zip_id limit 1".format(zip_id, zip_id)
                ml_processing = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(ml_processing) == 0:
                    ml_processing = []
                overview_bundle_details.extend(ml_processing)

                timeframe_processing = [{
                    'timeframe_from': None,
                    'timeframe_to': None
                }]
                query = """
                    select timeframe_from, timeframe_to from log_bundle where id='{}'
                """.format(zip_id)
                timeframe_response = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(timeframe_response) > 0:
                    timeframe_processing = [{
                        'timeframe_from': timeframe_response[0]['timeframe_from'],
                        'timeframe_to': timeframe_response[0]['timeframe_to']
                    }]
                overview_bundle_details.extend(timeframe_processing)

                # query = """
                #     select age(
                #         ended, started
                #     )::text as upload_time_processing 
                #     from (
                #         select log_bundle.id,zip_id,created_at as started 
                #         from global_status 
                #         inner join log_bundle on log_bundle.real_zip_id = global_status.zip_id
                #         where log_bundle.id='{}' 
                #             and status_msg~*'Zip file Received'
                #     ) a 
                #     left join (
                #         select zip_id,created_at as ended 
                #         from global_status 
                #         where zip_id='{}' 
                #             and status_msg~*'Bundle Processing - Started'
                #     ) b on a.id=b.zip_id
                #     limit 1
                # """.format(zip_id, zip_id)
                # upload_time_processing = connectFetchJSONWihtoutQueryDataNoResponse(query)
                # overview_bundle_details[0]['upload_time_processing'] = ""
                # if len(upload_time_processing) > 0:
                #     overview_bundle_details[0]['upload_time_processing'] = upload_time_processing[0]['upload_time_processing']


                query = """
                    select age(
                        ended, started
                    )::text as completion_time_processing 
                    from (
                        select zip_id,created_at as started 
                        from global_status 
                        where zip_id='{}' 
                            and status_msg~*'Bundle Processing - Started'
                    ) a 
                    left join (
                        select zip_id,updatedtime as ended 
                        from emailstatus 
                        where zip_id='{}'
                    ) b on a.zip_id=b.zip_id
                    limit 1
                """.format(zip_id, zip_id)
                completion_time_processing = connectFetchJSONWihtoutQueryDataNoResponse(query)

                overview_bundle_details[0]['completion_time_processing'] = ""
                if len(completion_time_processing) > 0:
                    overview_bundle_details[0]['completion_time_processing'] = completion_time_processing[0]['completion_time_processing']

            json_data = {"data": overview_bundle_details, "message": "Received overview of bundles", "status": 200}
            resp = response_wrapper_plain_jsondumps(200, json_data)
            return resp
        except:
            logger.error(traceback.format_exc())
            json_data = {
                "data": {"status": "Internal server error"},
                "message": "Internal server error", "title": "Error"}
            return response_wrapper(500, json_data)


# @analysis_ns.route("/gupb_overview_bundlefiles")
class GetOverviewOfBundleFileDetails(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get GetOverviewOfBundleFileDetails
        """
        resp = None
        try:
            req = request
            data = getRequestJSON(req)
            token = getRequestHeaders(request, KEY_HEADER_TOKEN)
            username = getUserDetailsByToken(token).get('username')
            zip_id = data["zip_id"]

            overview_file_details = []
            query = f"""
                select a.id as file_id,input_file,a.no_lines as total_lines,dcount,scount,rcount,cacount,kscount,'0'::integer as mcount
                from log_files a 
                inner join log_bundle l on l.id = a.zip_id
                inner join users u on l.user_id = u.user_id
                left join (
                    select log_files_id,count(distinct id) as dcount 
                    from dlog_files where zip_id='{zip_id}' 
                    group by log_files_id
                ) b on a.id=b.log_files_id 
                left join (
                    select log_files_id,count(distinct id) as scount 
                    from slog_files 
                    where zip_id='{zip_id}' 
                    group by log_files_id
                ) c on a.id=c.log_files_id  
                left join (
                    select a.log_files_id,count(distinct d.id) as rcount 
                    from slog_files a 
                    left join log_files b on a.id=b.slog_files_id 
                    left join slog_files_mapping_rule c on c.log_files_id=b.id and c.zip_id=a.zip_id
                    left join rlog_files d on d.slog_mapping_id=c.id 
                    where a.zip_id='{zip_id}' group by a.log_files_id
                ) d on a.id=d.log_files_id 
                left join (
                    select log_files_id, 
                        case
                            when count(distinct pi_log_files_id) > 0 then 1
                            else 0
                        end 
                        as cacount 
                    from pi_log_files
                    where zip_id='{zip_id}' group by log_files_id
                ) e on a.id=e.log_files_id 
                left join (
                    select log_files_id, count(distinct ks_log_files_id) as kscount 
                    from ks_log_files
                    where zip_id='{zip_id}' group by log_files_id
                ) f on a.id=f.log_files_id 
                where zip_id='{zip_id}' and a.type in ('file','decrypts')
                    and (
                        u.username = '{username}' 
                            or u.username in (
                                select intraname from alice_groups where intra_groups in (
                                    select intra_groups from alice_groups where intraname = '{username}'
                                )
                            )
                    )
                ;
            """
            overview_file_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
            if len(overview_file_details) == 0:
                overview_file_details = []
            else:
                elk_ml_index_de = ""
                query = "select elk_ml_index_de from log_bundle a left join products b on a.prod_id=b.prod_id where a.id='{}'".format(zip_id)
                ml_index_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
                if len(ml_index_details) > 0:
                    elk_ml_index_de = ml_index_details[0]["elk_ml_index_de"]
                if elk_ml_index_de is not None and elk_ml_index_de != "":
                    grouping = "file_id"
                    if str(elk_ml_index_de) == "ml_fn":
                        grouping = "input_file_id"
                    query = {
                        "zipID": str(zip_id),
                        "indexName": str(elk_ml_index_de),
                        "queryType": "SearchWithAggKeyword",
                        "grouping": grouping
                    }
                    api_url = 'http://{}:{}/request/search'.format(
                        settings.ENVIRONMENT[settings.env]["spark_config"]["QA_IP"],
                        settings.ENVIRONMENT[settings.env]["spark_config"]["QA_PORT"]
                    )
                    r = requests.post(url=api_url, json=query)
                    logger.info('POST %s' % api_url)
                    logger.info('%s' % query)
                    logger.info(str(r.status_code) + " -- " + r.reason + " -- " + r.text)
                    ml_counts_data = []
                    if str(r.status_code) == "200":
                        ml_counts_data = json.loads(r.text)
                        for x in overview_file_details:
                            for y in ml_counts_data:
                                if str(x["file_id"]) == str(y["key"]):
                                    x.update({"mcount": int(y["doc_count"])})
            logger.info("Data Retrieved")
            overview_file_details_dicts = []
            if len(overview_file_details) > 0:
                for i in range(0,len(overview_file_details)):
                    row_dict = dict(overview_file_details[i])
                    row_dict['input_file_name'] = overview_file_details[i]['input_file'].split('/')[-1]
                    overview_file_details_dicts.append(row_dict)
            json_data = {"data": overview_file_details_dicts, "message": "Received overview of bundle details", "status": 200}
            resp = response_wrapper_plain_jsondumps(200, json_data)
        except Exception as e:
            logger.error("Exception in GetOverviewOfBundleFileDetails:")
            logger.error(traceback.format_exc())
            json_data = {"data": {"status": "Bundle details not received"}, "message": "Bundle details not received", "status": 500, "title": "Error"}
            resp = response_wrapper(500, json_data)
        return resp


# @analysis_ns.route("/gbprocessing")
class GetBundleProcessingData(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get GetBundleProcessingData
        """
        req = request
        data = getRequestJSON(req)
        zip_id = data["zip_id"]

        overview_file_details = []
        query = f"select username,a.bundle_id,round(file_size::numeric/(1024*1024))::text as file_size_mb,bundle_extracted_size::text as file_extracted_size,is_inprogress,is_completed,is_completed,a.created_at::text,date_part('minute',now()::timestamp-b.created_at::timestamp)::text as timetaken from bg_task a left join (select zip_id,max(created_at) as created_at from global_status group by zip_id) b on a.bundle_id=b.zip_id left join log_bundle c on a.bundle_id=c.id left join users u on c.user_id=u.user_id where u.isdeleted = 0 and a.bundle_id='{zip_id}' order by a.created_at asc"
        logger.info(query)
        overview_file_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
        json_data = {"data": overview_file_details, "message": "Received overview of bundles", "status": 200}

        resp = response_wrapper_plain_jsondumps(200, json_data)
        return resp


# @analysis_ns.route("/gupb_analysis_ml_data")
class GetAnalysisMLData(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        GetAnalysisMLData
        """
        try:
            logger.info("Getting Analysis ML data")
            req = request
            data = getRequestJSON(req)
            if 'zip_id' in data:
                _clone_zip_id = data['zip_id']
                query = f"select real_zip_id from log_bundle where id='{_clone_zip_id}'"
                details = connectFetchJSONWithoutResponse(query, '')
                if len(details) > 0:
                    print(details)
                    _zip_id = details[0]["real_zip_id"]
                    if _zip_id is not None:
                        zip_id = _zip_id
                        data['zip_id'] = zip_id
            zip_id = data["zip_id"]
            if "groupingName" in data:
                groupingName = data["groupingName"]
                if len(groupingName) > 0:
                    if data["grouping"] == "accuracy":
                        data.update({"queryType": "SearchAccuracyRange"})
                        data.update({"groupingName": str(data["groupingName"][0])})
                    else:
                        data.update({"queryType": "SearchWithTwoFields"})
                        data.update({"groupingName": str(data["groupingName"][0])})
                else:
                    data.update({"queryType": "SearchWithGuiMessage" if data["grouping"] != "accuracy" else "SearchWithAggKeyword"})
            else:
                data.update({"queryType": "SearchWithGuiMessage" if data["grouping"] != "accuracy" else "SearchWithAggKeyword"})
            elk_ml_index_de = ""
            query = "select elk_ml_index_de from log_bundle a left join products b on a.prod_id=b.prod_id where a.id='{}'".format(zip_id)
            ml_index_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
            if len(ml_index_details) > 0:
                elk_ml_index_de = ml_index_details[0]["elk_ml_index_de"]
            if elk_ml_index_de is not None and elk_ml_index_de != "":
                data.update({"indexName": elk_ml_index_de})
                data.update({"zipID": zip_id})
                del data["zip_id"]
                logger.info("Calling query aggregator for ML data :" + str(zip_id))
                api_url = 'http://{}:{}/request/search'.format(settings.ENVIRONMENT[settings.env]["spark_config"]["QA_IP"], settings.ENVIRONMENT[settings.env]["spark_config"]["QA_PORT"])
                logger.info("POST %s" % api_url)
                logger.info("%s" % json.dumps(data))
                r = requests.post(url=api_url, json=data)
                logger.info("Response code from QA: " + str(r.status_code))
                if r.text is not None and r.text != "":
                    data = json.loads(r.text)
                    if "columns" in data:
                        rows = data["rows"]
                        l = len(rows)    
                        columns_list = data["columns"].replace("[", "").replace("]", "").split(",")
                        if "FILE_PATH" in columns_list and not "filename" in columns_list:
                            columns_list.append("filename")
                            for i in range(l):
                                rows[i]["filename"] = os.path.split(rows[i]['FILE_PATH'])[1]
                            data['rows'] = rows
                        if ("file_id" in columns_list or "input_file_id" in columns_list) and not "FILE_PATH" in columns_list:
                            key = "file_id"
                            if "input_file_id" in columns_list:
                                key = "input_file_id"
                            columns_list.append("FILE_PATH")
                            file_id_dict_details = {}
                            file_id_list = list(set([ row[key] for row in rows ]))
                            file_id_dict_details = get_file_id_dict_details(file_id_list)
                            get_file_path = lambda x: file_id_dict_details[x] if x in file_id_dict_details else ""
                            for i in range(l):
                                rows[i]["FILE_PATH"] = get_file_path(rows[i][key])
                            data['rows'] = rows
                        data['columns'] =  "[" + ",".join(columns_list) + "]"
                    json_data = {"data": data, "message": "ML data received", "status": r.status_code, "title": "error"}
                    resp = response_wrapper_plain_jsondumps(200, json_data)
                else:
                    logger.info("No ML data for this bundle " + str(zip_id))
                    json_data = {"data": {"status": "No ML mapping for this bundle"}, "msg": "Data Retrieved", "status": 204, "error": "No ML mapping for this bundle"}
                    resp = response_wrapper_plain_jsondumps(204, json_data)
            else:
                logger.info("No ML data for this bundle " + str(zip_id))
                json_data = {"data": {"status": "No ML mapping for this bundle"}, "msg": "Data Retrieved", "status": 204, "error": "No ML mapping for this bundle"}
                resp = response_wrapper_plain_jsondumps(500, json_data)
            return resp
        except Exception:
            logger.error(traceback.format_exc())
            json_data = {
                "data": {"status": "Internal server error"},
                "message": "Internal server error", "title": "Error"}
            return response_wrapper(500, json_data)


# @analysis_ns.route("/gsearch_filters")
class GetSearchFiltersDetails(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get Get Search Filters
        """
        try:
            logger.info("GetSearchFiltersDetails")
            req = request
            data = getRequestJSON(req)
            products = data["products"]
            users = data["users"]

            filterConditions = []
            if len(products) > 0:
                filterConditions.append(" prod_id in ('" + "','".join(products) + "') ")
            if len(users) > 0:
                filterConditions.append(" user_id in ('" + "','".join(users) + "') ")

            bundleDeatils = []
            tmp = "and ".join(filterConditions)
            if tmp != "":
                tmp = tmp + " and"
            query = "select json_agg(zip_details) as zip_details,json_agg(distinct tag_name) as tag_details from (select jsonb_build_object('zip_id',id,'zip_name',log_name) as zip_details,jsonb_build_object('id',tag_name,'tag_name',tag_name) as tag_name from log_bundle where " + tmp + " isdeleted=0 order by to_timestamp(right(zip_name, 19), 'YYYY-MM-DD HH24:MI:SS'::text) desc) aa"
            bundleDeatils = connectFetchJSONWihtoutQueryDataNoResponse(query)
            if len(bundleDeatils) == 0:
                bundleDeatils = []
            else:
                token = getRequestHeaders(request, KEY_HEADER_TOKEN)
                username = getUserDetailsByToken(token).get('username')
                query = "select DISTINCT alice_groups.intra_groups from alice_groups inner join users on users.username = alice_groups.intraname where users.username = '%s' order by alice_groups.intra_groups asc" % (username)
                group_details = connectFetchJSONWihtoutQueryDataNoResponse(query)
                intra_groups = {'intra_groups': group_details}
                bundleDeatils[0] = {**intra_groups, **bundleDeatils[0]}

            json_data = {"data": bundleDeatils, "message": "Received filtered data", "status": 200, "title": "Error"}

            resp = response_wrapper_plain_jsondumps(200, json_data)
            return resp
        except:
            logger.error(traceback.format_exc())
            json_data = {
                "data": {"status": "Internal server error"},
                "message": "Internal server error", "title": "Error"}
            return response_wrapper(500, json_data)


def _caps_search(req):
    data = getRequestJSON(req)
    token = getRequestHeaders(req, KEY_HEADER_TOKEN)
    user_details = getUserDetailsByToken(token)
    username = user_details['username']
    z = {}
    logger.info(json.dumps(data))
    actualParam = {"filename": "filename", "customer_id_cdb": "customer.id_cdb", "product_id_cdb": "product.id_cdb",
                   "ticket_id": "ticket.ticket_id", "ticket_app_name": "ticket.app_name", "duration": "duration", "startDate": "startDate", "owner": "owner"}
    list(map(lambda x: z.update({actualParam[x]: data[x]}) if x in data else z.update({actualParam[x]: ''}), actualParam))
    z.update({"userid": username})
    logger.info(json.dumps(z))
    searchParameters = z
    logger.info("Executing python command")
    logger.info("python3 /app/app/modules/Caps.py 'search' '" + json.dumps(searchParameters) + "'")
    json_data = processExecutionCommon("python3 /app/app/modules/Caps.py 'search' '" + json.dumps(searchParameters) + "'")
    logger.info("After executing python command")
    logger.info(json_data)
    data_json = json.loads(json_data)
    ui_json = []
    if True if data_json['status'] == 200 else False:
        for x in data_json['data']:
            for y in x['files']:
                #if ( x["scrambled"] is False and y["type"] == "encrypted" and y["status"] in ["encrypting", "uploading", "ready"] ) \
                #    or ( x["scrambled"] is True and y["type"] == "scrambled" and y["status"] in ["wait_scramble", "scrambling", "uploading", "ready"] ):

                if y["type"] == "scrambled" and y["status"] in ["ready"]:
                    tmp_json = {}
                    tmp_json.update({"attachment_id": x['id']})
                    tmp_json.update({"owner": x['owner']})
                    tmp_json.update({"app": x['app']})
                    tmp_json.update({"description": x['description']})
                    tmp_json.update({"purpose": x['purpose']})
                    tmp_json.update({"customer_id": checKeyAvail('id', x['customer'])})
                    tmp_json.update({"customer_id_cdb": checKeyAvail('id_cdb', x['customer'])})
                    tmp_json.update({"customer_name": checKeyAvail('name', x['customer'])})
                    tmp_json.update({"product_id": checKeyAvail('id', x['product'])})
                    tmp_json.update({"product_id_cdb": checKeyAvail('id_cdb', x['product'])})
                    tmp_json.update({"product_name": checKeyAvail('name', x['product'])})
                    tmp_json.update({"file_id": y['id']})
                    tmp_json.update({"file_name": y['name']})
                    tmp_json.update({"size": y['size']})
                    tmp_json.update({"type": y['type']})
                    tmp_json.update({"status": y['status']})
                    tmp_json.update({"progress": y['progress']})
                    tmp_json.update({"detailed_status": y['detailed_status']})
                    ui_json.append(tmp_json)
                    continue

                if y["type"] == "encrypted" and y["status"] in ["encrypting", "uploading", "ready"]:
                    tmp_json = {}
                    tmp_json.update({"attachment_id": x['id']})
                    tmp_json.update({"owner": x['owner']})
                    tmp_json.update({"app": x['app']})
                    tmp_json.update({"description": x['description']})
                    tmp_json.update({"purpose": x['purpose']})
                    tmp_json.update({"customer_id": checKeyAvail('id', x['customer'])})
                    tmp_json.update({"customer_id_cdb": checKeyAvail('id_cdb', x['customer'])})
                    tmp_json.update({"customer_name": checKeyAvail('name', x['customer'])})
                    tmp_json.update({"product_id": checKeyAvail('id', x['product'])})
                    tmp_json.update({"product_id_cdb": checKeyAvail('id_cdb', x['product'])})
                    tmp_json.update({"product_name": checKeyAvail('name', x['product'])})
                    tmp_json.update({"file_id": y['id']})
                    tmp_json.update({"file_name": y['name']})
                    tmp_json.update({"size": y['size']})
                    tmp_json.update({"type": y['type']})
                    tmp_json.update({"status": y['status']})
                    tmp_json.update({"progress": y['progress']})
                    tmp_json.update({"detailed_status": y['detailed_status']})
                    ui_json.append(tmp_json)
                    continue

        json_data = {"data": ui_json, "message": "Data has received", "status": 200, "title": "Error"}
    else:
        logger.error("Command failed and returned {}".format(data_json['status']))
        json_data = data_json
    return ui_json, json_data


# @analysis_ns.route("/gcaps_search")
class GetCapsSearchDetails(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get GetCapsSearchDetails
        """
        try:
            logger.info("Get CAPS search details")
            ui_json, json_data = _caps_search(request)
            resp = response_wrapper_plain_jsondumps(200, json_data)
            return resp
        except:
            logger.error(traceback.format_exc())
            json_data = {
                "data": {"status": "Internal server error"},
                "message": "Internal server error", "title": "Error"}
            return response_wrapper(500, json_data)


class GetDataForAnalysis(APIView):
    permission_classes = (IsActive,)

    def get(self, request):
        """
        Analysis GET Methods
        """
        return MicroserviceGet(request)


# @analysis_ns.route("/gcaps_upload")
class GetCapsUploadDetails(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get GetCapsSearchDetails
        """
        try:
            req = request
            data = getRequestJSON(req)
            attid = data["attachment_id"]
            filename = data["file_name"]
            json_data = {"data": {"status": "Bundle info submitted to ALICE for processing"}, "message": "Data Retrieved", "status": 200, "error": ""}
            resp = response_wrapper_plain_jsondumps(200, json_data)
            return resp
        except:
            logger.error(traceback.format_exc())
            json_data = {
                "data": {"status": "Internal server error"},
                "message": "Internal server error", "title": "Error"}
            return response_wrapper(500, json_data)


class GetCapsDownloadDetails(APIView):
    permission_classes = (IsActive,)

    @csrf_exempt
    def post(self, request):
        """
        Get GetCapsSearchDetails
        """
        try:
            ui_json, json_data = _caps_search(request)
            with open('/tmp/download.csv', 'w', encoding='utf8', newline='') as output_file:
                dict_writer = csv.DictWriter(
                    output_file,
                    ui_json[0].keys() if len(ui_json) > 0 else []
                )
                dict_writer.writeheader()
                dict_writer.writerows(ui_json)
                response = HttpResponse(output_file, content_type='text/csv')
                response['Content-Disposition'] = 'attachment; filename=download.csv'
                return response
        except:
            logger.error(traceback.format_exc())
            json_data = {
                "data": {"status": "Internal server error"},
                "message": "Internal server error", "title": "Error"}
            return response_wrapper(500, json_data)


def check_valueofarray_str(arr):
    logger.info("check_valueofarray_str")
    if len(arr) == 0:
        arr_str = 'null'
    else:
        arr_str = ','.join(map(lambda x: "'" + str(x) + "'", arr))
    return arr_str
