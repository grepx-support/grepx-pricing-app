"""Database Server Client Test Suite"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

import asyncio
import logging
from datetime import datetime
from client import DatabaseServerClient


# Setup logging
log_file = Path(__file__).parent / f"test_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


async def test_async():
    """Test async operations"""
    
    logger.info("=" * 60)
    logger.info("ASYNC TESTS")
    logger.info("=" * 60)
    
    try:
        async with DatabaseServerClient(timeout=60.0) as client:
            logger.info("\n1. Health check...")
            health = await client.health_async()
            logger.info(f"   [OK] Status: {health['status']}")
            
            logger.info("\n Tests passed")
            logger.info("\n" + "=" * 60)
    
    except Exception as e:
        logger.error(f"\n[ERROR] Test failed: {e}", exc_info=True)
        raise


def test_sync():
    """Test sync operations"""
    
    logger.info("\n" + "=" * 60)
    logger.info("SYNC TESTS")
    logger.info("=" * 60)
    
    try:
        with DatabaseServerClient(timeout=60.0) as client:
            logger.info("\n1. Health check...")
            health = client.health()
            logger.info(f"   [OK] Status: {health['status']}")
            
            logger.info("\n" + "=" * 60)
    
    except Exception as e:
        logger.error(f"\n[ERROR] Test failed: {e}", exc_info=True)
        raise


def main():
    """Run all tests"""
    logger.info("\n" + "=" * 60)
    logger.info("DATABASE SERVER CLIENT TEST SUITE")
    logger.info(f"Log file: {log_file}")
    logger.info("=" * 60)
    
    try:
        asyncio.run(test_async())
        test_sync()
        
        logger.info("\n" + "=" * 60)
        logger.info("TESTS COMPLETED")
        logger.info("=" * 60 + "\n")
        
    except Exception as e:
        logger.error(f"\n[ERROR] Test suite failed: {e}", exc_info=True)


if __name__ == "__main__":
    main()
