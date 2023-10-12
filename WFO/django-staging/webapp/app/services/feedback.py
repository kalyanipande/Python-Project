from rest_framework.views import APIView
from rest_framework.response import Response
from django.db import connection
import json
import traceback
from app.enums import STATUS
from django.views.decorators.csrf import csrf_exempt
from app.models import Case, CaseTracker, CaseDetail, Question, Feedback
from app.api.serializers import CaseSerializer, QuestionSerializer, FeedbackSerializer
from route.settings import LOGGING
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("WFOLogger")


class FeedbackUserCaseList(APIView):
    """
    To Get the  list of users based on the id
    """
    @csrf_exempt
    def get(self, request):
        try:
            logger.info("list of user caseid api is calling")
            username = request.session.get("preferred_username", None)
            logger.info("username : {}".format(username))
            case_id_list = CaseTracker.objects.filter(user_name=username, feedback=False, status=2).exclude(user_name='digimops').order_by('-modified_at').values('case_id_new', 'case_id', 'id')
            logger.info("case list  is: {}".format(case_id_list))
            return Response({"data": case_id_list}, status=200)
        except Exception as e:
            logger.error(traceback.format_exc(),str(e))
            json_data = {"data": {"status": "Caseid's are not listed"}, "message": "Caseid's are not listed", "title": "Error"}
            return Response(json_data, status=500)


class FeedbackQuestionList(APIView):
    """
    To Get the  Feedback question form data
    """
    @csrf_exempt
    def get(self, request):
        try:
            logger.info("question api is calling")
            cursor = connection.cursor()
            cursor.execute('''select jsonb_agg(aa) as questions from(
                                select json_build_object('question', json_build_object('question_id', question_id, 'question_text', question_text, 'required', required,'similar_case_rate',similar_case_rate,'suggestion_rate',suggestion_rate,'options', options,'suboptions',suboptions))aa
                                from app_question aq group by question_id, question_text, required,similar_case_rate,suggestion_rate,options,suboptions order by question_id)c''')
            row = cursor.fetchone()
            data = row[0][0]
            logger.info("The Question data is: {}".format(data))
            return Response({"data": data}, status=200)
        except Exception as e:
            logger.error(str(e),traceback.format_exc())
            json_data = {"data": {"status": "Questions are not listed"}, "message": "Questions are not listed", "title":"Error"}
            return Response(json_data, status=500)


class FeedbackOptionsList(APIView):
    """
    To Create the  Feedback data based on the given data
    """
    @csrf_exempt
    def post(self, request):
        try:
            logger.info("Feedback api is calling")
            feedback_list = request.data
            feedback_list['similar_case_rate'] = feedback_list['questions']['similar_case_rate']
            feedback_list['suggestion_rate'] = feedback_list['questions']['suggestion_rate']
            logger.info("The Feedback  data is: {}".format(feedback_list))
            cursor = connection.cursor()
            cursor.execute('''insert into app_feedback(caseid, case_tracker_id,comments,questions,similar_case_rate,suggestion_rate) values(%s,%s,%s,%s,%s,%s)''', 
                [feedback_list['caseId'], feedback_list['id'],feedback_list['comments'], json.dumps(feedback_list['questions']),feedback_list['similar_case_rate'],feedback_list['suggestion_rate']])
            update_query = "update app_casetracker set feedback=true where case_id_new = '%s' and user_name = '%s' " % (str(feedback_list['caseId']), str(request.session.get('preferred_username')))
            cursor.execute(update_query)
            return Response({"data": {"message": "Feedback received successfully"}},status=200)
        except Exception as e:
            logger.error(str(e),traceback.format_exc())
            json_data = {"data": {"status": "Feedback not listed"}, "message": "Feedback not listed", "title": "Error"}
            return Response(json_data, status=500)


class FeedbackDetailView(APIView):
    """
    Get and Update the  Feedback data based on the id
    """

    @csrf_exempt
    def get(self, request, id=None, format=None):
        try:
            logger.info("feedback api is calling")
            cursor = connection.cursor()
            cursor.execute('''select jsonb_agg(aa) as questions from(
                                select json_build_object('question', json_build_object('question_id', question_id, 'question_text', question_text, 'required', required,'similar_case_rate',similar_case_rate,'suggestion_rate',suggestion_rate,'options', options,'suboptions',suboptions))aa
                                from app_question aq group by question_id, question_text, required,similar_case_rate,suggestion_rate,options,suboptions order by question_id)c''')
            row = cursor.fetchone()
            data = row[0][0]
            user_name = self.request.session.get("preferred_username", None)
            _case = Case.objects.filter(case_id_new=id).first()
            case_tracker = CaseTracker.objects.filter(case_id_new=id, user_name=user_name).first()
            if case_tracker is None and _case.user_name == user_name:
                case_tracker = CaseTracker()
                case_tracker.case_id = _case.case_id
                case_tracker.case_id_new = _case.case_id_new
                case_tracker.input_text = _case.input_text
                case_tracker.tag = _case.tag
                case_tracker.user_name = user_name
                case_tracker.digimop_instance_id = _case.digimop_instance_id
                case_tracker.digimop_operation_id = _case.digimop_operation_id
                case_tracker.status = _case.status
                case_tracker.feedback = False
                case_tracker.is_owner = True if _case.user_name == user_name else False
                case_tracker.save()
            result = Feedback.objects.filter(caseId=id, case_tracker=case_tracker).first()
            if result:
                serializer = FeedbackSerializer(result)
                logger.info("The FeedbackSerializer data is: {}".format(serializer.data))
                feedbackdata = serializer.data
                feedback_data = self.feedback_mapping(data, [feedbackdata])
                logger.info("The Feedback  data is: {}".format(feedback_data))
                return Response(status=200, data=feedback_data)
            else:
                return Response(data=data)
        except Exception as e:
            logger.error(str(e), traceback.format_exc())
            return Response(data={"data": "Internal server error"}, status=500)

    def feedback_mapping(self, data, feedbackdata):
        # Checking whether the data is already is provided or not
        data['question']['question_text'] = feedbackdata[0]['comments']
        data['question']['similar_case_rate']=feedbackdata[0]['questions']['similar_case_rate']
        data['question']['suggestion_rate'] = feedbackdata[0]['questions']['suggestion_rate']
        if feedbackdata[0]['questions']['sub_selected']:
            data['question']['sub_selected']=feedbackdata[0]['questions']['sub_selected']
        return data

    @csrf_exempt
    def post(self, request, id=None, format=None):
        try:
            logger.info("Feedback api is calling")
            data = request.data
            user_name = self.request.session.get("preferred_username", None)
            caseTracker = CaseTracker.objects.filter(case_id_new=id, user_name=user_name).first()
            if caseTracker is not None:
                data['case_tracker'] = str(caseTracker.id)
                data['similar_case_rate'] = data['questions']['similar_case_rate']
                data['suggestion_rate'] = data['questions']['suggestion_rate']
                logger.info("The Feedback data is: {}".format(data))
                instance = Feedback.objects.filter(caseId=id, case_tracker=caseTracker.id).first()
                if not instance:
                    serializer = FeedbackSerializer(data=data)
                    if serializer.is_valid():
                        feedback_instance = serializer.save()
                        feedback_data = FeedbackSerializer(feedback_instance).data
                        caseTracker.feedback = True
                        caseTracker.save()
                        return Response({"data": {"message": "Feedback received successfully"}})
                    return Response(serializer.errors, status=400)
                else:
                    serializer = FeedbackSerializer(instance, data=data)
                    if serializer.is_valid():
                        serializer.save()
                        return Response({"data": {"message": "Feedback Updated successfully"}})
                    return Response(serializer.errors, status=400)
            else:
                serializer = FeedbackSerializer(data=data)
                if serializer.is_valid():
                    serializer.save()
                    return Response({"data": {"message": "Feedback Updated successfully"}})
                return Response(serializer.errors, status=400)
        except Exception as e:
            logger.error(str(e),traceback.format_exc())
            json_data = {"data": {"status": "Feedback not listed"}, "message": "Feedback not listed",
                         "title": "Error"}
            return Response(json_data, status=500)
