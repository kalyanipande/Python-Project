from django.db import models
from django.contrib.postgres.fields import JSONField
import uuid
from django.contrib.postgres.fields import ArrayField
from app.enums import (
    CATEGORY,
    STATUS,RATING
)


class TimeStampedModel(models.Model):
    """
    An abstract base class model that provides self-updating
    ``created`` and ``modified`` fields.
    """
    created_at = models.DateTimeField(auto_now_add=True, editable=False, db_column="created_at")
    modified_at = models.DateTimeField(auto_now=True, db_column="modified_at")

    class Meta:
        abstract = True
        managed = False

class Case(TimeStampedModel):
    case_id = models.CharField(max_length=256, db_column="case_id")
    case_id_new = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False, db_column="case_id_new")
    input_text = models.CharField(max_length=256, db_column="input_text")
    tag = models.CharField(max_length=256, db_column="tag")
    user_name = models.CharField(max_length=256, null=True, blank=True, db_column="user_name")
    digimop_instance_id = models.CharField(max_length=256, null=True, blank=True,db_column="digimop_instance_id")
    digimop_operation_id = models.CharField(max_length=256,null=True, blank=True, db_column="digimop_operation_id")
    status = models.IntegerField(
        blank=True,
        null=True,
        choices=STATUS.choices,
        default=STATUS.PENDING, db_column="status"
    )
    feedback = models.BooleanField(default=False, db_column="feedback")
    release = models.CharField(max_length=255,null=True,blank=True, db_column="release")

    def __str__(self):
        return "%s" % self.case_id_new

    class Meta:
        managed = False

class CaseTracker(TimeStampedModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False, db_column="id")
    case_id = models.CharField(max_length=256, db_column="case_id")
    case_id_new = models.UUIDField(default=uuid.uuid4, editable=False, db_column="case_id_new")
    input_text = models.CharField(max_length=256, db_column="input_text")
    tag = models.CharField(max_length=256, db_column="tag")
    user_name = models.CharField(max_length=256, null=True, blank=True, db_column="user_name")
    digimop_instance_id = models.CharField(max_length=256, null=True, blank=True,db_column="digimop_instance_id")
    digimop_operation_id = models.CharField(max_length=256,null=True, blank=True, db_column="digimop_operation_id")
    status = models.IntegerField(
        blank=True,
        null=True,
        choices=STATUS.choices,
        default=STATUS.PENDING, db_column="status"
    )
    feedback = models.BooleanField(default=False, db_column="feedback")
    is_owner =  models.BooleanField(default=False, db_column="is_owner")
    release = models.CharField(max_length=255,null=True,blank=True, db_column="release")

    def __str__(self):
        return "%s" % self.case_id_new

    class Meta:
        managed = False


class CaseDetail(models.Model):
    case_uuid = models.ForeignKey(Case, on_delete=models.CASCADE, db_column="case_uuid")
    frequency = models.FloatField(db_column="frequency")
    order = models.FloatField(db_column="order")
    category = models.IntegerField(
        blank=True,
        null=True,
        choices=CATEGORY.choices, db_column="category"
    )
    suggestions = models.CharField(null=True, blank=True, max_length=512, db_column="suggestions")

    class Meta:
        managed = False

class Question(models.Model):
    question_id = models.AutoField(primary_key=True, db_column="question_id")
    question_text = models.TextField(null=False, blank=False, db_column="question_text")
    options = models.TextField(max_length=512, blank=True, db_column="options")
    required = models.BooleanField(default=False, db_column="required")
    suboptions = ArrayField(models.CharField(max_length=512), db_column="suboptions")
    similar_case_rate = models.IntegerField(default=RATING.DEFAULT,null=True,
        choices=RATING.choices, db_column="similar_cases_rate"
    )
    suggestion_rate = models.IntegerField(default=RATING.DEFAULT,null=True,
        choices=RATING.choices, db_column="suggestions_rate"
    )
    def __str__(self):
        return self.question_text

    class Meta:
        managed = False

class Feedback(models.Model):
    feedback_id = models.AutoField(primary_key=True, db_column="feedback_id")
    caseId = models.ForeignKey(Case, on_delete=models.CASCADE, related_name="caseId", db_column="caseid")
    case_tracker = models.ForeignKey(CaseTracker, on_delete=models.CASCADE, related_name="+", db_column="case_tracker_id", null=True, blank=True)
    comments = models.TextField(db_column="comments")
    questions = JSONField()
    similar_case_rate = models.IntegerField(
        default=RATING.DEFAULT,
        null=True,
        choices=RATING.choices, db_column="similar_case_rate"
    )
    suggestion_rate = models.IntegerField(
        default=RATING.DEFAULT,
        null=True,
        choices=RATING.choices, db_column="suggestion_rate"
    )
    def __str__(self):
        return self.feedback_id

    class Meta:
        managed = False

    class Case_number_mapping(models.Model):
        caseidnumber = models.TextField(db_column="caseidnumber")
        casenumber = models.TextField(db_column="casenumber")

        class Meta:
            managed = False
