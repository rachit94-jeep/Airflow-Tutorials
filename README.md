# Apache Airflow Tutorials

A hands-on learning project for Apache Airflow 3.x, running locally with Docker Compose (CeleryExecutor).

## Project Structure

```
.
├── dags/
│   ├── 001_dag.py            # Basic DAG with Python tasks and dependencies
│   ├── 002_dag_versioning.py # DAG versioning with mixed Python and Bash tasks
│   ├── 003_operator.py       # BashOperator example
│   ├── 004_xcoms.py          # XComs for passing data between tasks (return-value style)
│   ├── 005_xcoms_kwargs.py   # XComs via **kwargs and TaskInstance (ti) object
│   └── 006_parallel_dag.py   # Parallel task execution with XComs and a custom trigger rule
├── logs/                     # Airflow task logs (git-ignored)
├── plugins/                  # Custom Airflow plugins
├── config/                   # Airflow configuration
├── .env                      # Environment variables (AIRFLOW_UID, FERNET_KEY, etc.)
└── docker-compose.yaml       # Local Airflow stack (CeleryExecutor + Redis + PostgreSQL)
```

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- At least 4GB RAM and 2 CPUs allocated to Docker
- At least 10GB free disk space

## Setup

### 1. Clone the repository

```bash
git clone <repo-url>
cd apache-airflow
```

### 2. Create the `.env` file

Create a `.env` file in the project root with the following content:

```env
AIRFLOW_UID=50000
```

> **Linux only:** run `echo "AIRFLOW_UID=$(id -u)" > .env` to use your actual user ID so mounted files are not owned by root.

### 3. Generate a Fernet key (optional but recommended)

The Fernet key encrypts sensitive values (connections, variables) stored in the database. If not set, Airflow will generate one automatically, but it will change on every restart.

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Add the result to your `.env`:

```env
FERNET_KEY=<your-generated-key>
```

### 4. Create required local directories

These folders are mounted into the containers and must exist before starting:

```bash
mkdir -p logs plugins config
```

> On Linux, also set the correct permissions: `chmod -R 777 logs plugins config`

### 5. Initialize the database and start the stack

On first run, `airflow-init` bootstraps the database, creates the admin user, and sets correct permissions on the mounted volumes:

```bash
docker compose up airflow-init
```

Wait for it to exit with code 0, then start all services:

```bash
docker compose up -d
```

### 6. Verify all services are healthy

```bash
docker compose ps
```

All services should show `healthy` or `running`. This may take 1–2 minutes on first start.

### 7. Access the Airflow UI

Open [http://localhost:8080](http://localhost:8080) in your browser.

| Field    | Value     |
|----------|-----------|
| Username | `airflow` |
| Password | `airflow` |

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
Demonstrates XComs — Airflow's mechanism for passing data between tasks. Implements a simple ETL pipeline where the output of one task is automatically passed as input to the next using the return-value style.

```
extract >> transform >> load
```

### 005 — XComs via kwargs ([dags/005_xcoms_kwargs.py](dags/005_xcoms_kwargs.py))
Same ETL pipeline as 004, but uses the `**kwargs` / `TaskInstance (ti)` approach — manually pushing and pulling data using `ti.xcom_push()` and `ti.xcom_pull()` with explicit keys instead of relying on return values.

```
extract >> transform >> load
```

### 006 — Parallel DAG ([dags/006_parallel_dag.py](dags/006_parallel_dag.py))
Demonstrates parallel task execution. A single `extract` task pushes two datasets (S3 and Snowflake) to XCom. Two `transform` tasks then run in parallel, each consuming one dataset. A `createDataframe` task merges both results into a pandas DataFrame, using `trigger_rule='none_failed_min_one_success'` so it runs even if one branch is skipped. Finally a `BashOperator` signals load completion.

```
extract >> [transform_s3, transform_sf] >> createDataframe >> run_this_bash
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
