import requests
import json
import pandas as pd
from django.conf import settings
import boto3
from route.settings import LOGGING
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("WFOLogger")

from django.db import connection



# Customer.objects.all().using('users')
from django.db import connections

class FIR_Connect:

    def fir_data(self):
        #  connection = pyodbc.connect('Driver={SQL Server};Server=tcp:insightscare-sqlserver.database.windows.net,1433;Database=insights-azsql;Uid=graphql_user;Pwd=Welcome12345;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
        # connection = pyodbc.connect("Driver={ODBC Driver 18 for SQL Server};Server=tcp:insightscare-sqlserver.database.windows.net,1433;Database=insights-azsql;Uid=graphql_user;Pwd={Welcome12345};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
        with connections['FIR_SQL'].cursor() as cursor:
            # cursor=connection.cursor()
            cursor.execute("SELECT top 10 * from pub.trv_qmm_details")
            data = cursor.fetchall()

        # print the rows
            for row in data :
                print(row[1])

            cursor.close()
            connection.close()