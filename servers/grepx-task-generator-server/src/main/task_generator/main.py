from bootstrap.bootstrap_service import BootstrapService
from logger import setup_logger


def main():
    logger = setup_logger("task-generator")
    
    try:
        logger.info("Starting task generator bootstrap process")
        BootstrapService(
            seed_dir="src/main/resources"
        ).run()
        logger.info("Task generator bootstrap completed successfully")
    except Exception as e:
        logger.error(f"Task generator bootstrap failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
