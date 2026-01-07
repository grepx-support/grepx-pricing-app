#!/usr/bin/env python3
"""
Test end-to-end task flow: Dagster -> Celery
"""
import sys
from pathlib import Path

print("=" * 60)
print("END-TO-END TASK FLOW TEST")
print("=" * 60)

print("\n1. Testing Celery Task Registration:")
print("-" * 60)

sys.path.insert(0, str(Path(__file__).parent / "grepx-celery-server" / "src" / "main"))
sys.path.insert(0, str(Path(__file__).parent / "business-tasks"))

try:
    from celery_app.app import create_app
    from celery_app.config_loader import _load_config
    
    print("  [OK] Loading Celery config...")
    cfg = _load_config()
    
    print("  [OK] Creating Celery app...")
    celery_app = create_app(cfg)
    
    print(f"  [OK] Celery app created: {celery_app.main}")
    
    print("\n  Registered tasks:")
    task_names = [name for name in celery_app.tasks.keys() if not name.startswith('celery.')]
    for task_name in sorted(task_names):
        print(f"    - {task_name}")
    
    print(f"\n  Total registered: {len(task_names)} tasks")
    
    expected_tasks = [
        "tasks.fetch_stock_prices",
        "tasks.calculate_volatility", 
        "tasks.calculate_metrics",
        "tasks.analyze_portfolio",
        "tasks.daily_analysis",
        "tasks.sector_performance"
    ]
    
    print("\n2. Checking Expected Tasks:")
    print("-" * 60)
    
    missing = []
    for expected in expected_tasks:
        if expected in celery_app.tasks:
            print(f"  [OK] {expected} is registered")
        else:
            print(f"  [MISSING] {expected} not registered")
            missing.append(expected)
    
    print("\n3. Testing Task Invocation (Dry Run):")
    print("-" * 60)
    
    for task_name in expected_tasks:
        if task_name in celery_app.tasks:
            task = celery_app.tasks[task_name]
            print(f"  [OK] {task_name}")
            print(f"       Name: {task.name}")
            print(f"       Module: {task.__module__}")
        else:
            print(f"  [SKIP] {task_name} not registered")
    
    print("\n4. Testing Dagster Task Client:")
    print("-" * 60)
    
    sys.path.insert(0, str(Path(__file__).parent / "grepx-dagster-server" / "src" / "main"))
    
    from dagster_app.task_client import TaskClient
    
    print("  [OK] Creating TaskClient...")
    client = TaskClient(broker_url="redis://localhost:6379/0")
    
    print("  [OK] TaskClient created")
    print("  [INFO] To test actual submission, Redis and Celery worker must be running")
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Celery app loaded: YES")
    print(f"Tasks registered: {len(task_names)}")
    print(f"Expected tasks: {len(expected_tasks)}")
    print(f"Missing tasks: {len(missing)}")
    
    if missing:
        print(f"\n[WARNING] Missing tasks: {', '.join(missing)}")
        print("Check that:")
        print("  1. Tasks are in the database (run verify_tasks.py)")
        print("  2. grepx-tasks is in Python path")
        print("  3. Celery config points to correct database")
    else:
        print("\n[SUCCESS] All tasks properly registered!")
        print("\nTo test actual execution:")
        print("  1. Start Redis: redis-server")
        print("  2. Start Celery: cd grepx-celery-server && ./run.sh start")
        print("  3. Start Dagster: cd grepx-dagster-server && ./run.sh start")
        print("  4. Trigger assets in Dagster UI: http://localhost:3000")
    
except Exception as e:
    print(f"\n[ERROR] {e}")
    import traceback
    traceback.print_exc()
    print("\nMake sure:")
    print("  1. Virtual environments are set up (./setup.sh)")
    print("  2. Dependencies are installed")
    print("  3. Database has tasks (run task generator)")

