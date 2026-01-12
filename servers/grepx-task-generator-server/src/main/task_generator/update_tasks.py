#!/usr/bin/env python3
"""
Update Task Module Paths
=========================
Updates existing Celery tasks in database with new module paths from YAML
"""
import sys
from pathlib import Path
import yaml

# Get project root
script_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(script_dir / "main"))

from task_generator.database import DatabaseManager
from grepx_models import CeleryTask


def load_tasks_file(tasks_file_path: Path) -> dict:
    """Load tasks from YAML file"""
    with open(tasks_file_path, 'r') as f:
        return yaml.safe_load(f)


def update_celery_tasks(db_manager: DatabaseManager, tasks: list):
    """Update existing Celery tasks with new module paths"""
    count = 0
    with db_manager.get_session() as session:
        for task_data in tasks:
            try:
                # Find existing task by name
                existing = session.query(CeleryTask).filter_by(name=task_data['name']).first()
                if existing:
                    # Update module_path and function_name
                    existing.module_path = task_data['module_path']
                    existing.function_name = task_data['function_name']
                    existing.description = task_data.get('description', existing.description)
                    existing.tags = task_data.get('tags', existing.tags)
                    existing.options = task_data.get('options', existing.options)
                    existing.retry_policy = task_data.get('retry_policy', existing.retry_policy)
                    existing.timeout = task_data.get('timeout', existing.timeout)
                    existing.is_active = task_data.get('is_active', existing.is_active)
                    
                    count += 1
                    print(f"  [✓] Updated: {task_data['name']}")
                    print(f"      module_path: {task_data['module_path']}")
                    print(f"      function_name: {task_data['function_name']}")
                else:
                    print(f"  [!] Not found: {task_data['name']} - will be created on next generate")
                    
            except Exception as e:
                print(f"  [✗] Failed to update '{task_data['name']}': {e}")
                session.rollback()
        
        session.commit()
    return count


def main():
    """Main execution"""
    main_dir = Path(__file__).parent.parent
    
    # Load task config
    config_path = main_dir / "resources" / "task-config.yaml"
    if not config_path.exists():
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Get database URL and tasks file
    db_url = config['database']['db_url']
    tasks_file = config.get('tasks_file', 'resources/tasks.yaml')
    
    if tasks_file.startswith('resources/'):
        tasks_file_path = main_dir / tasks_file
    else:
        tasks_file_path = main_dir / "resources" / tasks_file
    
    if not tasks_file_path.exists():
        print(f"ERROR: Tasks file not found: {tasks_file_path}")
        sys.exit(1)
    
    # Connect to database
    print(f"Connecting to database: {db_url}")
    db_manager = DatabaseManager(db_url=db_url)
    
    # Load tasks from YAML
    print(f"Loading tasks from: {tasks_file_path}")
    tasks_data = load_tasks_file(tasks_file_path)
    
    # Update tasks
    print("\n=== Updating Celery Tasks ===")
    if 'celery_tasks' in tasks_data:
        count = update_celery_tasks(db_manager, tasks_data['celery_tasks'])
        print(f"\n✓ Updated {count} tasks")
    else:
        print("No celery_tasks found in YAML")
    
    print("\nDone! Restart Celery server to load updated tasks.")


if __name__ == '__main__':
    main()

