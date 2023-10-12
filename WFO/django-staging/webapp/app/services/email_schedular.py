from route import settings
from route.settings import LOGGING
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("WFOLogger")
import boto3
from io import StringIO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import  formatdate
from botocore.exceptions import ClientError
import os
import ast
from django.db import connection
import datetime


class email_updater():
    def send_mail(self,send_from, send_to, subject, links=None,
                  server="mailrelay.int.nokia.com"):
        assert isinstance(send_to, list)
        msg = MIMEMultipart('alternative')
        msg['From'] = send_from
        msg['To'] = ','.join(send_to)
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = subject
        # text_ccr = "CCR LINK :"
        # text_lexicon="LEXICON_LINK"
        html_ccr=f"""<html>
<body>
<p>Hello
<br>
        Please find below the link to access weekly CCR & Lexicon Dump</p>
<p>CCR DATA LINK : 
<a href={links[0]}>CCR LINK</a><br><br></p>
<p>LEXICON DATA LINK : 
<a href={links[1]}>LEXICON LINK</a></p>
<p>Please connect to Nokia VPN for accessing the LINK</p>
<p>Best Regards <br><b>WFO Team</b></p>
</body>
</html>"""

        part1=  MIMEText(html_ccr, 'html')

        msg.attach(part1)
        logger.info("after adding file attachment")
        my_message = msg.as_string()
        smtp = smtplib.SMTP(server)
        smtp.sendmail(send_from, send_to, my_message)
        smtp.close()
    def Move_S3_data(self):
        print("inside get s3 :",settings.S3_CONFIG_DATA_PATH)
        bucket = settings.BUCKET
        print("bucket :",bucket)
        key_name = settings.S3_CONFIG_DATA_PATH + "dna_transformed/ccr_wfo_casesummary_proddata.csv"
        #
        s3boto = boto3.resource(service_name='s3', aws_access_key_id=settings.ACCESS_KEY,
                                aws_secret_access_key=settings.S3_SECRET_KEY, endpoint_url=settings.ENDPOINT,
                                use_ssl=True, verify=False
                                )
        file_content = s3boto.Object(bucket, key_name).get()
        body = file_content['Body']
        csv_string = StringIO(body.read().decode('utf-8'))
        key_name_upload = settings.S3_CONFIG_DATA_PATH + "Shared_data/ccr_wfo_casesummary_proddata.csv"
        s3boto.Object(bucket, key_name_upload).put(Body=csv_string.getvalue())

        logger.info("----------starting for lexicon------------------")
        key_name2 = settings.S3_CONFIG_DATA_PATH + "lexicon/ext_lexicon.json"
        file_content2 = s3boto.Object(bucket, key_name2).get()
        body2 = file_content2['Body']
        csv_string2 = StringIO(body2.read().decode('utf-8'))
        key_name_upload2 = settings.S3_CONFIG_DATA_PATH + "Shared_data/ext_lexicon.json"
        s3boto.Object(bucket, key_name_upload2).put(Body=csv_string2.getvalue())

    def create_presigned_url(self,bucket_name, object_name, expiration=3600):
        # :param expiration: Time in seconds for the presigned URL to remain valid
        s3_client = boto3.client(service_name='s3', aws_access_key_id=settings.ACCESS_KEY,
                                aws_secret_access_key=settings.S3_SECRET_KEY, endpoint_url=settings.ENDPOINT,
                                use_ssl=True, verify=False)
        try:
            response = s3_client.generate_presigned_url('get_object',
                                                        Params={'Bucket': bucket_name,
                                                                'Key': object_name},
                                                        ExpiresIn=expiration)
        except ClientError as e:
            logging.error(e)
            return None
        return response

    def schedule_email_api(self):
        self.Move_S3_data()
        bucket = settings.BUCKET
        key_name_upload = settings.S3_CONFIG_DATA_PATH + "Shared_data/ccr_wfo_casesummary_proddata.csv"
        created_url = self.create_presigned_url(bucket,key_name_upload,604800)
        key_name_upload2 = settings.S3_CONFIG_DATA_PATH + "Shared_data/ext_lexicon.json"
        created_url2 = self.create_presigned_url(bucket,key_name_upload2,604800)

        current_date = datetime.date.today()
        cursor = connection.cursor()
        # cursor.execute(
        #     "select * from app_variables where key = 'Email' and value_timestamp >= '{} 00:00:00'".format(current_date))
        # data = cursor.fetchall()
        # if len(data) == 0:
        self.send_mail("I_EXT_WFO_DE_SUPPORT@internal.nsn.com", ast.literal_eval(os.getenv('EMAIL_MEMBERS')), "Weekly WFO Files", [created_url,created_url2])
        logger.info("email sent")
        # cursor.execute(
        #     "update  app_variables set value_timestamp = {} where key = 'Email';".format(('now()')))
