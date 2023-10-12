from django.db import models

# Create your models here.

class User(models.Model):
    id = models.UUIDField(primary_key=True)
    username = models.CharField(max_length=250,unique=True)
    email = models.CharField(max_length=250,blank=True,null=True)
    firstName = models.CharField(max_length=250,blank=True,null=True)
    lastName = models.CharField(max_length=250, blank=True, null=True)
    enabled = models.BooleanField(default=True)
    region = models.CharField(max_length=250, blank=True, null=True)
    createdDate = models.CharField(max_length=250, blank=True, null=True)
    updatedDate = models.CharField(max_length=250, blank=True, null=True)
    updateComment = models.CharField(max_length=250, blank=True, null=True)
    userRole = models.CharField(max_length=250,blank=True,null=True)

    employee_name = models.CharField(max_length=250,blank=True,null=True,db_column="employee_name")
    team = models.TextField(max_length=256,blank=True,null=True,db_column="team")
    bu_name = models.CharField(max_length=250,blank=True,null=True,db_column="bu_name")
    slm_email_id = models.CharField(max_length=250,blank=True,null=True,db_column="slm_email_id")

    def __str__(self):
        return self.id

class Regions(models.Model):
    class Meta:
        managed = True
        db_table = 'regions'
    id=models.AutoField(primary_key=True)
    regionname=models.CharField(max_length=20,null=False)

