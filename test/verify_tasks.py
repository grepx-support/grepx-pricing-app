#!/usr/bin/env python3
"""
Verify that all tasks are properly configured and can be called
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "grepx-shared-models" / "src"))

from grepx_models import CeleryTask, Asset, Base
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

DB_URL = "sqlite:///C:/Users/USER/development/grepx-dagster-server/dagster_config_orm.db"

def main():
    print("=" * 60)
    print("TASK VERIFICATION")
    print("=" * 60)
    
    engine = create_engine(DB_URL, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    print("\n1. Checking Celery Tasks in Database:")
    print("-" * 60)
    
    tasks = session.scalars(select(CeleryTask).where(CeleryTask.is_active == True)).all()
    
    expected_tasks = [
        "tasks.fetch_stock_prices",
        "tasks.calculate_volatility",
        "tasks.calculate_metrics",
        "tasks.analyze_portfolio",
        "tasks.daily_analysis",
        "tasks.sector_performance"
    ]
    
    found_tasks = {}
    for task in tasks:
        print(f"  [+] {task.name}")
        print(f"      Module: {task.module_path}")
        print(f"      Function: {task.function_name}")
        print(f"      Active: {task.is_active}")
        found_tasks[task.name] = task
    
    print(f"\nTotal tasks in DB: {len(tasks)}")
    
    print("\n2. Checking Expected Tasks:")
    print("-" * 60)
    
    missing = []
    for expected in expected_tasks:
        if expected in found_tasks:
            print(f"  [OK] {expected}")
        else:
            print(f"  [MISSING] {expected}")
            missing.append(expected)
    
    print("\n3. Checking Dagster Assets:")
    print("-" * 60)
    
    assets = session.scalars(select(Asset).where(Asset.is_active == True)).all()
    
    for asset in assets:
        print(f"  [+] {asset.name}")
        print(f"      Celery Task: {asset.celery_task_name}")
        print(f"      Dependencies: {asset.dependencies}")
        
        if asset.celery_task_name not in found_tasks:
            print(f"      [WARNING] Task '{asset.celery_task_name}' not found in Celery tasks!")
    
    print(f"\nTotal assets in DB: {len(assets)}")
    
    print("\n4. Task Function Verification:")
    print("-" * 60)
    
    sys.path.insert(0, str(Path(__file__).parent / "business-tasks"))
    
    for task_name, task in found_tasks.items():
        try:
            import importlib
            module = importlib.import_module(task.module_path)
            func = getattr(module, task.function_name)
            print(f"  [OK] {task_name} -> {task.module_path}.{task.function_name}")
        except Exception as e:
            print(f"  [ERROR] {task_name}: {e}")
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Tasks in DB: {len(tasks)}")
    print(f"Expected tasks: {len(expected_tasks)}")
    print(f"Missing tasks: {len(missing)}")
    print(f"Assets in DB: {len(assets)}")
    
    if missing:
        print("\n[ACTION REQUIRED] Run task generator to add missing tasks:")
        print("  cd grepx-task-generator-server")
        print("  ./run.sh")
    else:
        print("\n[SUCCESS] All tasks properly configured!")
    
    session.close()

if __name__ == "__main__":
    main()

