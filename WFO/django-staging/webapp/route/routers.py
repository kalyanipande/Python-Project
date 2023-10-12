from django.conf.urls import include, url
from app.services import workspace
from app.services import feedback
from app.services import digimops_integration
from app.services import fir_tag,ccr_test,grapghQLReplacement
from rest_framework import routers
from app.api.viewsets import (
    CasesViewset,
    TestViewset
)

router = routers.SimpleRouter()
router.register(r'test', TestViewset, basename='test')
router.register(r'case', CasesViewset, basename='case')

urlpatterns = [
    url(r'', include(router.urls)),
    url(r'^taggenerator', workspace.Taggenerator.as_view(), name='taggenerator'),
    url(r'^feedback/caselist', feedback.FeedbackUserCaseList.as_view(), name='feedback_caselist'),
    url(r'^feedback/questions', feedback.FeedbackQuestionList.as_view(), name='feedback_questions'),
    url(r'^feedback/feedback_data', feedback.FeedbackOptionsList.as_view(), name='feedback_data'),
    url(r'^feedback/(?P<id>[-\w]+)/$',feedback.FeedbackDetailView.as_view(),name='feedback'),
    # baseurl/orch/api/fir-taggenerator/
    url(r'^fir-taggenerator', fir_tag.FirTagGenerator.as_view(), name='fir_taggenerator'),
    url(r'^fir-description-generator', fir_tag.FirDescriptionGenerator.as_view(), name='fir_description_generator'),
    # baseurl/orch/api/digimops-case-analysis/
    url(r'^digimops-case-analysis', digimops_integration.DigimopsAPI.as_view(), name='digimops_case_analysis'),
    url(r'', include('app.services.auth_urls'), name='my_api_root'),
    url(r'^testccr',ccr_test.CCRTESTCHECK.as_view(),name='testing ccr schedule'),
    url(r'^graphreplacement',grapghQLReplacement.Queries.as_view(),name='testing ccr schedule')

]