from django.db import models


class CATEGORY(models.IntegerChoices):
    ACTION = 1
    INFORMATION_REQUEST = 2
    LOG_REQUEST = 3


class STATUS(models.IntegerChoices):
    PENDING = 1
    SUCCESS = 2
    No_Similar_Cases_Found = 3
    Failure = 4
    Insufficient = 5

class RATING(models.IntegerChoices):
    DEFAULT = 0
    NOT_HELPFUL = 1
    PARTIALLY_HELPFUL = 2
    VERY_HELPFUL = 3

