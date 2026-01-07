"""Celery main."""
import logging
import signal
import sys
import threading
from pathlib import Path

from celery import Celery

tasks_dir = Path(__file__).parent.parent.parent.parent.parent.parent / "business-tasks"
if tasks_dir.exists():
    sys.path.insert(0, str(tasks_dir))

from .app import create_app
from .config_loader import _load_config

logger = logging.getLogger(__name__)

app = create_app(_load_config())


def start_worker(celery_app: Celery):
    celery_app.worker_main(["worker", "--loglevel=info", "--pool=solo"])


def start_beat(celery_app: Celery):
    celery_app.start(["beat", "--loglevel=info"])


def start_flower():
    import subprocess
    subprocess.run([sys.executable, "-m", "flower", "--port=5555", f"--broker={app.conf.broker_url}"])


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    shutdown_event = threading.Event()

    def handle_shutdown(signum, frame):
        logger.info("Shutting down...")
        shutdown_event.set()

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    threading.Thread(target=start_worker, args=(app,), daemon=True).start()
    threading.Thread(target=start_beat, args=(app,), daemon=True).start()
    threading.Thread(target=start_flower, daemon=True).start()

    logger.info("Celery started - Flower: http://localhost:5555")
    shutdown_event.wait()


if __name__ == "__main__":
    main()
