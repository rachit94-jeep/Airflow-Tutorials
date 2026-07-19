# Apache Airflow Tutorials

A hands-on learning project for Apache Airflow 3.x, running locally with Docker Compose (CeleryExecutor).

## Project Structure

```
.
├── dags/
│   ├── 001_dag.py           # Basic DAG with Python tasks and dependencies
│   ├── 002_dag_versioning.py # DAG versioning with mixed Python and Bash tasks
│   ├── 003_operator.py      # BashOperator example
│   └── 004_xcoms.py         # XComs for passing data between tasks
├── logs/                    # Airflow task logs (git-ignored)
├── plugins/                 # Custom Airflow plugins
├── config/                  # Airflow configuration
└── docker-compose.yaml      # Local Airflow stack (CeleryExecutor + Redis + PostgreSQL)
```

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- At least 4GB RAM and 2 CPUs allocated to Docker

## Getting Started

```bash
# Start the Airflow stack
docker compose up -d

# Access the Airflow UI
# URL: http://localhost:8080
# Username: airflow
# Password: airflow
```

## DAGs

### 001 — First DAG ([dags/001_dag.py](dags/001_dag.py))
Introduces the basic DAG structure using the `@dag` and `@task.python` decorators. Three Python tasks chained sequentially.

```
my_first_task >> my_second_task >> my_third_task
```

### 002 — DAG Versioning ([dags/002_dag_versioning.py](dags/002_dag_versioning.py))
Demonstrates DAG versioning by extending the first DAG with an additional `@task.bash` task at the end of the chain.

```
my_first_task >> my_second_task >> my_third_task >> bash_task
```

### 003 — Operators ([dags/003_operator.py](dags/003_operator.py))
Shows how to use the `BashOperator` (traditional operator style, as opposed to the `@task` decorator). Creates a file using a bash command.

```
create_file
```

### 004 — XComs ([dags/004_xcoms.py](dags/004_xcoms.py))
Demonstrates XComs — Airflow's mechanism for passing data between tasks. Implements a simple ETL pipeline where the output of one task is automatically passed as input to the next.

```
extract >> transform >> load
```

## Stack

| Service | Purpose |
|---|---|
| PostgreSQL 16 | Airflow metadata database |
| Redis 7.2 | Celery message broker |
| Airflow API Server | REST API + Web UI (port 8080) |
| Airflow Scheduler | DAG scheduling |
| Airflow Worker | Task execution (Celery) |
| Airflow Triggerer | Deferred task execution |
| Airflow DAG Processor | DAG file parsing |

## Stopping the Stack

```bash
docker compose down
```

To also remove volumes (wipes the database):

```bash
docker compose down -v
```

## References

- [Apache Airflow Tutorial — YouTube](https://www.youtube.com/watch?v=IiczxlbQb8s)

## Docker Setup

The stack is defined in [docker-compose.yaml](docker-compose.yaml) and uses the official `apache/airflow:3.3.0` image with **CeleryExecutor**.

### Volumes

The following project folders are mounted into every Airflow container:

| Host folder | Container path | Purpose |
|---|---|---|
| `./dags` | `/opt/airflow/dags` | DAG files |
| `./logs` | `/opt/airflow/logs` | Task logs |
| `./config` | `/opt/airflow/config` | `airflow.cfg` |
| `./plugins` | `/opt/airflow/plugins` | Custom plugins |

Any file written to these paths inside the container is immediately visible on your host machine and vice versa.

### Environment Variables

Sensitive values are loaded from a `.env` file in the project root. Key variables:

| Variable | Description |
|---|---|
| `FERNET_KEY` | Key used to encrypt connections/variables in the database |
| `AIRFLOW_UID` | UID of the user running Airflow inside the container (Linux only) |
| `AIRFLOW_IMAGE_NAME` | Override the default Airflow image |
| `AIRFLOW_PROJ_DIR` | Override the base path for volume mounts (default: `.`) |

### Useful Commands

```bash
# View running containers
docker ps

# Tail logs for a specific service
docker compose logs -f airflow-worker

# Open a shell inside the worker container
docker exec -it <container-name> bash

# Trigger a DAG from the CLI
docker exec -it <container-name> airflow dags trigger <dag_id>
```
