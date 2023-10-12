from .ccr_updater import start
from rest_framework.views import APIView
from rest_framework.response import Response
from route.settings import LOGGING
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("WFOLogger")
from django.views.decorators.csrf import csrf_exempt
from .sqldb_config import getRequestJSON ,response_wrapper,FetchFromPostGres
import traceback


class Queries(APIView):
    @csrf_exempt
    def post(self,request):
        try:
            inputjson = getRequestJSON(request)
            if inputjson['query_type'] == 'For recomendation table data':
                username = inputjson['user_name']
                query = """select case_id,case_id_new,created_at,input_text,modified_at,status,tag ,release
                            from app_case_status
                            where user_name='{}'
                            order by modified_at desc , created_at desc;""".format(username)
            elif inputjson['query_type'] == 'Tag filter decending order':
                username = inputjson['user_name']
                tag = inputjson['tag']
                query = """select case_id,case_id_new,created_at,input_text,modified_at,status,tag,release 
                        from app_case_status
                        where user_name='{}' and tag LIKE '%{}%'
                        order by modified_at desc , created_at desc;""".format(username,tag)
            elif inputjson['query_type'] == 'text filter decending order':
                username = inputjson['user_name']
                input_text = inputjson['input_text']
                query = """select case_id,case_id_new,created_at,input_text,modified_at,status,tag,release 
                                from app_case_status
                                where user_name='{}' and input_text LIKE '%{}%'
                                order by modified_at desc , created_at desc;""".format(username, input_text)
            elif inputjson['query_type'] == 'Status filter decending order':
                username = inputjson['user_name']
                status = inputjson['status']
                query = """select case_id,case_id_new,created_at,input_text,modified_at,status,tag,release 
                                        from app_case_status
                                        where user_name='{}' and status='{}'
                                        order by modified_at desc , created_at desc;""".format(username,status)
            elif inputjson['query_type'] == 'Sorting Data Desc':
                username = inputjson['user_name']
                query = """select case_id,case_id_new,created_at,input_text,modified_at,status,tag,release 
                                    from app_case_status
                                    where user_name='{}'
                                    order by modified_at desc , created_at desc;""".format(username)

            elif inputjson['query_type']== 'Sorting Data Asc':
                username = inputjson['user_name']
                query = """select case_id,case_id_new,created_at,input_text,modified_at,status,tag,release 
                                            from app_case_status
                                            where user_name='{}'
                                            order by modified_at asc , created_at asc;""".format(username)
            elif inputjson['query_type'] == 'case id filter':
                username = inputjson['user_name']
                filterval=inputjson['filterval']
                query = """select case_id,case_id_new,created_at,input_text,modified_at,status,tag,release 
                                                    from app_case_status
                                                    where user_name='{}' and case_id LIKE '%{}%'
                                                    order by case_id desc;""".format(username,filterval)

            elif inputjson['query_type'] == 'case id sorting decending order':
                username = inputjson['user_name']
                query = """select case_id,case_id_new,created_at,input_text,modified_at,status,tag,release 
                                                    from app_case_status
                                                    where user_name='{}'
                                                    order by case_id desc;""".format(username)
            elif inputjson['query_type'] == 'case id sorting ase order':
                username = inputjson['user_name']
                query = """select case_id,case_id_new,created_at,input_text,modified_at,status,tag,release 
                                                            from app_case_status
                                                            where user_name='{}'
                                                            order by case_id asc;""".format(username)
            elif inputjson['query_type'] == 'tag filter':
                username = inputjson['user_name']
                filterval=inputjson['filterval']
                query = """select case_id,case_id_new,created_at,input_text,modified_at,status,tag,release 
                                                    from app_case_status
                                                    where user_name='{}' and tag LIKE '%{}%'
                                                    order by tag desc;""".format(username,filterval)
            elif inputjson['query_type'] == 'tag sorting decending order':
                username = inputjson['user_name']
                query = """select case_id,case_id_new,created_at,input_text,modified_at,status,tag,release 
                                                            from app_case_status
                                                            where user_name='{}'
                                                            order by tag desc;""".format(username)
            elif inputjson['query_type'] == 'tag sorting ase order':
                username = inputjson['user_name']
                query = """select case_id,case_id_new,created_at,input_text,modified_at,status,tag,release 
                                                            from app_case_status
                                                            where user_name='{}'
                                                            order by tag asc;""".format(username)
            elif inputjson['query_type'] == 'text filter':
                username = inputjson['user_name']
                filterval=inputjson['filterval']
                query = """select case_id,case_id_new,created_at,input_text,modified_at,status,tag,release 
                                                    from app_case_status
                                                    where user_name='{}' and input_text LIKE '%{}%'
                                                    order by input_text desc;""".format(username,filterval)
            elif inputjson['query_type'] == 'input sorting decending order':
                username = inputjson['user_name']
                query = """select case_id,case_id_new,created_at,input_text,modified_at,status,tag,release 
                                                        from app_case_status
                                                        where user_name='{}'
                                                        order by input_text desc;""".format(username)
            elif inputjson['query_type'] == 'input sorting ase order':
                username = inputjson['user_name']
                query = """select case_id,case_id_new,created_at,input_text,modified_at,status,tag,release 
                                                        from app_case_status
                                                        where user_name='{}'
                                                        order by input_text asc;""".format(username)
            elif inputjson['query_type'] == 'status filter':
                username = inputjson['user_name']
                filterval=inputjson['filterval']
                query = """select case_id,case_id_new,created_at,input_text,modified_at,status,tag,release 
                                                    from app_case_status
                                                    where user_name='{}' and status='{}'
                                                    order by status desc;""".format(username,filterval)
            elif inputjson['query_type'] == 'status sorting decending order':
                username = inputjson['user_name']
                query = """select case_id,case_id_new,created_at,input_text,modified_at,status,tag,release 
                                                        from app_case_status
                                                        where user_name='{}'
                                                        order by status desc;""".format(username)
            elif inputjson['query_type'] == 'status sorting ase order':
                username = inputjson['user_name']
                query = """select case_id,case_id_new,created_at,input_text,modified_at,status,tag,release 
                                                        from app_case_status
                                                        where user_name='{}'
                                                        order by status asc;""".format(username)
            elif inputjson['query_type'] == 'For live update':
                case_id_new = inputjson['route_case_id']
                query = """select case_id,case_id_new,created_at,input_text,modified_at,status,tag,release 
                                                    from app_case_status
                                                    where case_id_new='{}';""".format(case_id_new)

            else:
                logger.error("Invalid JSON ")
                resp = response_wrapper(500, "JSON input not valid")
                return resp
            resp = FetchFromPostGres(query)
        except Exception as e:
            logger.error(traceback.format_exc())
            resp=response_wrapper(500,str(e))
        return resp







