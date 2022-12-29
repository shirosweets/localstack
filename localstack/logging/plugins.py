import logging
import time

from localstack import config
from localstack.runtime import hooks
from localstack.utils.patch import patch


@hooks.on_infra_start(should_load=config.BOTO_LOG_PERFORMANCE)
def add_boto_performance_logging():
    from botocore.client import BaseClient

    logger = logging.getLogger("localstack.performance")

    @patch(BaseClient._make_api_call)
    def _log_api_call_performance(fn, self, operation_name, api_params):
        then = time.perf_counter()
        result = fn(self, operation_name, api_params)
        took = time.perf_counter() - then
        logger.info(
            "botocore_call:%.3f,%s,%s",
            took * 1000,
            self.meta.service_model.service_name,
            operation_name,
        )
        return result
