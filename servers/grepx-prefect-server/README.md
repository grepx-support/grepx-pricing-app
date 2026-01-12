# Grepx Prefect Server

Dynamic workflow orchestration server using Prefect. This server reads flow, task, deployment, and work pool configurations from the `grepx-master.db` database and dynamically registers them with Prefect.

## Architecture Overview

The Prefect server follows the same architecture as the Dagster server but uses Prefect for workflow orchestration:

- **Database-Driven Configuration**: All workflow definitions are stored in `grepx-master.db`
- **Dynamic Flow Creation**: Flows and tasks are created programmatically at startup
- **Celery Integration**: Tasks execute Celery tasks via the task queue (same as Dagster)
- **Self-Hosted**: Runs a local Prefect server with UI (no cloud dependencies)

## Components

### Core Components

- `config_loader.py` - Loads configuration from YAML with environment variable substitution
- `database_manager.py` - Read-only database access using raw SQL (no ORM dependencies)
- `task_client.py` - Celery client for submitting tasks to the worker queue

### Factories

- `task_factory.py` - Creates Prefect `@task` decorated functions that execute Celery tasks
- `flow_factory.py` - Creates Prefect `@flow` decorated functions that orchestrate tasks with dependencies

### Builders

- `deployment_builder.py` - Creates Prefect deployments with schedules
- `work_pool_manager.py` - Manages Prefect work pools for worker assignment

### Main

- `main.py` - Orchestrator that ties everything together

## Database Tables

The server reads from these tables in `grepx-master.db`:

- `prefect_flows` - Flow definitions
- `prefect_tasks` - Task definitions (linked to flows, execute Celery tasks)
- `prefect_deployments` - Deployment definitions (schedules, work pools)
- `prefect_work_pools` - Work pool configurations

## Installation

```bash
# Run setup script
./setup.sh
```

This will:
1. Create a Python virtual environment
2. Install all dependencies including Prefect
3. Install the shared models package

## Configuration

Configuration is stored in `src/main/prefect_app/resources/config.yaml`:

```yaml
app:
  name: grepx-prefect-server
  version: 1.0.0

prefect:
  api_url: ${PREFECT_API_URL:http://127.0.0.1:4200/api}
  home: ${PREFECT_HOME:./.prefect}

database:
  db_url: ${GREPX_MASTER_DB_URL:sqlite:///../../../data/grepx-master.db}

celery:
  broker_url: ${CELERY_BROKER_URL:redis://localhost:6379/0}
  result_backend: ${CELERY_RESULT_BACKEND:redis://localhost:6379/0}
```

Environment variables are loaded from `../../env.common`.

## Usage

### Starting the Server

```bash
# Start Prefect server and register flows/deployments
./start.sh

# Or use run.sh
./run.sh start
```

This will:
1. Start the Prefect API server on http://localhost:4200
2. Create work pools from database
3. Register flows and tasks
4. Create deployments with schedules

### Accessing the UI

Open http://localhost:4200 in your browser to access the Prefect UI.

### Starting a Worker

To execute flows, you need to start a Prefect worker:

```bash
# Activate virtual environment
source venv/bin/activate

# Set Prefect API URL
export PREFECT_API_URL=http://127.0.0.1:4200/api

# Start a worker for a specific work pool
prefect worker start --pool default-process-pool
```

### Running a Flow

Flows can be triggered in several ways:

1. **Via UI**: Use the Prefect UI to trigger deployments manually
2. **Via CLI**: Use the Prefect CLI to run deployments
3. **Scheduled**: Deployments with schedules run automatically

```bash
# Run a deployment via CLI
prefect deployment run "flow-name/deployment-name"
```

### Stopping the Server

```bash
# Stop Prefect server
./stop.sh

# Or use run.sh
./run.sh stop
```

### Checking Status

```bash
./run.sh status
```

## Defining Flows

Flows are defined in YAML files (e.g., `prefect_test_flows.yaml`) and loaded into the database via the task-generator-server.

### Example Flow Definition

```yaml
# Work pools
prefect_work_pools:
  - name: default-process-pool
    description: Default process-based work pool
    pool_type: process
    concurrency_limit: 10
    is_active: true

# Flows
prefect_flows:
  - name: stock_data_pipeline
    description: Flow for downloading and processing stock data
    flow_type: data_pipeline
    tags:
      - stocks
      - data
    is_active: true

# Tasks
prefect_tasks:
  - name: download_stock_data_task
    flow_name: stock_data_pipeline
    description: Download historical stock data
    celery_task_name: data.download_historical
    task_kwargs:
      ticker: "AAPL"
      period: "1mo"
    timeout_seconds: 300
    retry_config:
      max_retries: 3
      retry_delay_seconds: 30
    is_active: true

  - name: calculate_indicators_task
    flow_name: stock_data_pipeline
    description: Calculate technical indicators
    celery_task_name: indicators.calculate_rsi
    task_kwargs:
      ticker: "AAPL"
    depends_on:
      - download_stock_data_task
    timeout_seconds: 180
    is_active: true

# Deployments
prefect_deployments:
  - name: stock_data_pipeline_daily
    flow_name: stock_data_pipeline
    description: Daily stock data pipeline
    work_pool_name: default-process-pool
    work_queue_name: default
    schedule_type: cron
    schedule_config:
      cron_string: "30 15 * * 1-5"  # 3:30 PM on weekdays
      timezone: "Asia/Kolkata"
    is_active: true
```

## Task Dependencies

Tasks can depend on other tasks using the `depends_on` field:

```yaml
prefect_tasks:
  - name: task_a
    flow_name: my_flow
    celery_task_name: tasks.task_a

  - name: task_b
    flow_name: my_flow
    celery_task_name: tasks.task_b
    depends_on:
      - task_a  # task_b runs after task_a completes
```

The flow factory automatically handles task execution order based on dependencies.

## Schedules

Prefect supports three schedule types:

### Cron Schedule

```yaml
schedule_type: cron
schedule_config:
  cron_string: "0 9 * * *"  # Every day at 9 AM
  timezone: "Asia/Kolkata"
```

### Interval Schedule

```yaml
schedule_type: interval
schedule_config:
  interval_seconds: 3600  # Every hour
  timezone: "Asia/Kolkata"
```

### RRule Schedule

```yaml
schedule_type: rrule
schedule_config:
  rrule: "FREQ=DAILY;BYDAY=MO,TU,WE,TH,FR"  # Weekdays only
  timezone: "Asia/Kolkata"
```

## Comparison with Dagster Server

| Feature | Dagster | Prefect |
|---------|---------|---------|
| Orchestrator | Dagster | Prefect |
| Task Execution | Celery | Celery |
| Configuration | Database-driven | Database-driven |
| UI | Dagster UI | Prefect UI |
| Scheduling | Schedules & Sensors | Deployments with Schedules |
| Workers | No separate workers needed | Requires Prefect workers |

## Workflow

1. **Define**: Define flows, tasks, and deployments in YAML
2. **Generate**: Run task-generator-server to populate database
3. **Register**: Start Prefect server to register flows and deployments
4. **Execute**: Start workers to execute flows
5. **Monitor**: Use Prefect UI to monitor execution

## Troubleshooting

### Prefect server won't start

Check if port 4200 is already in use:
```bash
lsof -i :4200
```

### Flows not showing up

1. Check if flows are in the database:
```bash
sqlite3 ../../data/grepx-master.db "SELECT * FROM prefect_flows;"
```

2. Check the server logs:
```bash
cat .prefect/prefect-server.log
```

### Worker not picking up tasks

1. Ensure worker is connected to the correct work pool
2. Check that the deployment is using the correct work pool
3. Verify Prefect API URL is set correctly

### Tasks failing

1. Ensure Celery workers are running
2. Check that the Celery task names match between Prefect tasks and Celery workers
3. Verify Redis is running for Celery broker

## Logs

- Prefect server logs: `.prefect/prefect-server.log`
- Prefect database: `.prefect/prefect.db`

## Directory Structure

```
grepx-prefect-server/
├── src/
│   └── main/
│       └── prefect_app/
│           ├── __init__.py
│           ├── main.py              # Main orchestrator
│           ├── config_loader.py      # Configuration management
│           ├── database_manager.py   # Database access
│           ├── task_client.py        # Celery client
│           ├── factories/
│           │   ├── flow_factory.py   # Flow creation
│           │   └── task_factory.py   # Task creation
│           ├── builders/
│           │   ├── deployment_builder.py  # Deployment creation
│           │   └── work_pool_manager.py   # Work pool management
│           └── resources/
│               └── config.yaml       # Configuration file
├── requirements.txt
├── setup.sh
├── start.sh
├── stop.sh
├── run.sh
└── README.md
```

## Contributing

When adding new features:

1. Update the database models in `grepx-shared-models` if needed
2. Update the YAML schema for flow/task definitions
3. Update the orchestrator to handle new features
4. Update this README

## Support

For issues or questions, please refer to the main project documentation or contact the development team.
