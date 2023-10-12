from commons import *
import settings
import traceback
import logging.config
logging.config.dictConfig(settings.LOGGING)
logger = logging.getLogger("WFOLogger")
PENDING = 1
SUCCESS = 2
No_Similar_Cases_Found = 3
Failure = 4
Insufficient = 5

def case_status(request):
    try:
        data = request
        logger.warning("the data is: {}".format(data))
        case_id = data['case_id']
        logger.info("case id is: {}" .format(case_id))
        message = data['message']
        print(">>>>The Received message is >>>> ",message)
        status = 2
        if message == "The WFO tool says recommendation found":
            status = SUCCESS
        # elif message == "The WFO tool says No recommendation found" or "The WFO tool says Insufficent Similar Cases":
        elif message == "The WFO tool says Insufficent Similar Cases":
            status = Insufficient
        elif message == "Failure":
            status = Failure
        elif message == "The WFO tool says No relevant cases found":
            status = No_Similar_Cases_Found

        query="update app_case set status="+str(status)+" where case_id_new='"+case_id+"'"
        logger.info("The query is  {}".format(query))
        result = connectFetchJSONWihtoutQueryDataNoResponse(query)

        query="update app_casetracker set status="+str(status)+" where case_id_new='"+case_id+"'"
        logger.info("The query is  {}".format(query))
        result = connectFetchJSONWihtoutQueryDataNoResponse(query)

        logger.info("case id is Updated to {} , {}".format(case_id, status))
    except Exception as e:
        logger.info("case id is not Updated {}".format(case_id))
        logger.error(str(e),traceback.format_exc())


