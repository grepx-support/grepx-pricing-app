# bootstrap/service.py
from pathlib import Path
import logging
from .loader import load_seed_files
from .registry import MODEL_REGISTRY, validate_registry
from .inserter import upsert_rows
from db.session import get_session
from db.engine import engine
from grepx_models.base import Base

logger = logging.getLogger("task-generator")


class BootstrapService:
    def __init__(self, seed_dir: str):
        self.seed_dir = Path(seed_dir)

    def run(self):
        logger.info(f"Validating model registry ({len(MODEL_REGISTRY)} models)")
        validate_registry(MODEL_REGISTRY)
        logger.info("Model registry validation successful")
        
        # Create all tables if they don't exist
        logger.info("Creating database tables if they don't exist...")
        Base.metadata.create_all(engine)
        logger.info("Database tables ready")

        logger.info(f"Loading seed files from: {self.seed_dir}")
        
        session = get_session()
        total_tables = 0
        total_rows = 0
        
        try:
            for table_name, rows in load_seed_files(self.seed_dir):
                if not rows:
                    logger.debug(f"No rows to insert for {table_name}")
                    continue
                
                model = MODEL_REGISTRY.get(table_name)
                if not model:
                    logger.error(f"No model for table '{table_name}' - skipping")
                    continue

                logger.info(f"Bootstrapping {table_name} ({len(rows)} rows)")
                
                # Log first row for debugging
                if rows:
                    logger.debug(f"Sample row from {table_name}: {rows[0]}")

                upsert_rows(session, model, rows)
                session.commit()
                
                total_tables += 1
                total_rows += len(rows)
                logger.info(f"Successfully upserted {len(rows)} rows into {table_name}")

            if total_tables == 0:
                logger.warning("No data was bootstrapped")
            else:
                logger.info(f"Bootstrap complete: {total_tables} tables, {total_rows} total rows")

        except Exception as e:
            logger.error(f"Bootstrap failed: {e}", exc_info=True)
            session.rollback()
            raise
        finally:
            session.close()
            logger.info("Database session closed")
