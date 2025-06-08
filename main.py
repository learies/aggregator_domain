from datetime import datetime

from src.aggregators.services import aggregator_service
from src.config.logging import logging

logger = logging.getLogger(__name__)


def main():
    aggregator_service.get_aggregate_domains()


if __name__ == "__main__":
    try:
        start_time = datetime.now()
        main()
        logger.info(
            "Finished in %s",
            datetime.now() - start_time,
        )
    except Exception as e:
        logger.error(f"Error: {e}")
        raise e
