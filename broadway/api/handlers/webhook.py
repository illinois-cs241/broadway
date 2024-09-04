import logging
from broadway.api.handlers.client import ClientAPIHandler, GradingRunHandler, GradingRunRawHandler
import broadway.api.daos as daos
import broadway.api.definitions as definitions
import broadway.api.models as models
from broadway.api.decorators.auth import (
    authenticate_webhook
)
from broadway.api.handlers.base import BaseAPIHandler
from broadway.api.utils.run import continue_grading_run
from broadway.api.utils.time import get_time
from broadway.api.callbacks.worker import worker_schedule_job
from tornado_json import schema

logger = logging.getLogger(__name__)
    
class WebhookStartGradingHandler(GradingRunRawHandler):
    @authenticate_webhook
    @schema.validate(input_schema=definitions.grading_config, output_schema=definitions.grading_run_out, on_empty_404=True)
    def post(self, *args, **kwargs):
        return super().post(*args, **kwargs)