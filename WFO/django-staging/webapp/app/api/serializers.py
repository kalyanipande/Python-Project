import logging
from rest_framework import serializers
from rest_framework.fields import SerializerMethodField
from rest_framework.serializers import ModelSerializer
from app.models import Case, Question,Feedback
logger = logging.getLogger(__name__)


class CaseSerializer(ModelSerializer):
    """
    case serializer for getting cases for analysis
    """
    status = SerializerMethodField()

    class Meta:
        model = Case
        fields = '__all__'

    def get_status(self, case_obj):
        """
        convert integer status in display format
        """
        status = case_obj.get_status_display()
        return status

    # def validate(self, instance):
    #     """
    #     To validate if the feedback count is more than 2 for success cases
    #     """
    #     print(">>>The user name is <<< ",instance['user_name'])
    #     feedback_count = Case.objects.filter(user_name=instance['user_name'],feedback=False, status=2)
    #     if instance['user_name'] != 'digimops' and len(feedback_count)>=2:
    #         raise serializers.ValidationError("Have more than 2 success cases,Please provide feedback to submit the case")
    #     return instance


class QuestionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Question
        fields = '__all__'


class FeedbackSerializer(serializers.ModelSerializer):
    class Meta:
        model = Feedback
        fields = '__all__'

    def validate(self, validated_data):
        """
        To validate at least one rating is selected and
        if suggestion_rate is 3 then check at least 1 sub-options is selected
        """
        if validated_data['questions']['similar_case_rate'] or validated_data['questions']['suggestion_rate'] :
            if validated_data['questions']['suggestion_rate']==3 and len(validated_data['questions']['sub_selected'])<1:
                raise serializers.ValidationError(" Have to select minimum 1 sub_selected to submit the feedback")
            return validated_data
        else:
            raise serializers.ValidationError(" Have to give star rating to minimum 1 category to submit the feedback")



    # def update(self, instance, validated_data):
    #     """
    #     To update or create the feedback record
    #     """
    #     obj, created = self.Meta.model.objects.update_or_create(
    #         caseId=validated_data.pop("caseId",None),
    #         defaults=validated_data
    #     )
    #     return obj




