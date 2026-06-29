from utils.logger import logger


def log_pipeline_start():

    logger.info(
        "========================================"
    )

    logger.info(
        "PAYMENTS RECONCILIATION PIPELINE STARTED"
    )

    logger.info(
        "========================================"
    )


def log_pipeline_end():

    logger.info(
        "========================================"
    )

    logger.info(
        "PAYMENTS RECONCILIATION PIPELINE COMPLETED"
    )

    logger.info(
        "========================================"
    )