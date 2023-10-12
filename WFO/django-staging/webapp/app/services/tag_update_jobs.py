import requests
import json
from route import settings
from route.settings import LOGGING
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("WFOLogger")
from django.db import connection
import traceback
from django.db import connections
import pandas as pd

class Tag_Updater:

    def schedule_tag_api(self):
        try:
            df = pd.DataFrame()
            with connections['FIR_SQL'].cursor() as cursor:
                cursor.execute("SELECT  tagname,issuetype,issuedetails,additionaldetails from sws.trv_Case_Handling_CaseTag")
                data = cursor.fetchall()

                df = df.append(pd.DataFrame(data, columns=['tagname','issue_type','issue_details','additional_details']))
                # print(df)
                # for row in data:
                #     print("\n\n\nrow================ :",row)
                cursor.close()
                connection.close()
            df['product_name'] = df['tagname'].apply(lambda tag:tag.split("-")[0])
            df.drop('tagname',axis=1,inplace=True)
            # print(df)

            cursor = connection.cursor()
            count=0
            for index,row in df.iterrows():
                print(index)
                print(row['product_name'],row['issue_type'],row['issue_details'],row['additional_details'])
                select_query = (
                    "select * from tagdetails where product_name='{}' and issue_type='{}' and issue_details='{}' and additional_details='{}'".format(
                        row['product_name'],row['issue_type'],row['issue_details'],row['additional_details']))
                cursor.execute(select_query)
                row_db = cursor.fetchone()
                if row_db is None:
                    print("ready to insert the data------------========================={{{{{{{{{{{")
                    try:
                        cursor.execute(
                            '''insert into tagdetails(product_name,issue_type,issue_details,additional_details) values(%s,%s,%s,%s)''',
                            (row['product_name'],row['issue_type'],row['issue_details'],row['additional_details']))
                        count=count+1
                    except Exception as e:
                        print(str(e), traceback.format_exc())
            logger.info("number of rows inserted : {}".format(count))
            connection.close()



        except Exception as e:
            logger.error(str(e), traceback.format_exc())