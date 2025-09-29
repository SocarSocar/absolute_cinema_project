import os
from datetime import datetime, timedelta
import pendulum
import requests
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Réglages généraux
TZ = pendulum.timezone("UTC")
PROJECT_DIR = os.environ.get("PROJECT_DIR", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
ENV_PATH = os.path.join(PROJECT_DIR, ".env")  # on réutilise ton .env existant

# --- Check Snowflake ---
def check_snowflake():
    # charge .env (Snowflake creds) si présent
    try:
        from dotenv import load_dotenv
        if os.path.exists(ENV_PATH):
            load_dotenv(ENV_PATH)
    except Exception:
        pass

    import snowflake.connector

    required = ["SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise AirflowFailException(f"Missing Snowflake env: {missing}")

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "GOLD"),
    )
    try:
        cs = conn.cursor()

        # 1) fraicheur minimale FCT_CONTENT (max(content_date) >= current_date - 1)
        cs.execute("SELECT TO_DATE(MAX(content_date)) AS max_dt FROM GOLD.FCT_CONTENT;")
        row = cs.fetchone()
        if not row or not row[0]:
            raise AirflowFailException("GOLD.FCT_CONTENT vide ou sans content_date.")
        max_dt = row[0]
        # On tolère D-1 pour absorber fuseaux et cutoffs
        from datetime import date as _date, timedelta as _td
        if max_dt < (_date.today() - _td(days=1)):
            raise AirflowFailException(f"FCT_CONTENT pas fraîche (max={max_dt}).")

        # 2) DIM_LANGUAGE non vide
        cs.execute("SELECT COUNT(*) FROM GOLD.DIM_LANGUAGE;")
        cnt = cs.fetchone()[0]
        if cnt <= 0:
            raise AirflowFailException("GOLD.DIM_LANGUAGE est vide.")

    finally:
        try:
            cs.close()
        except Exception:
            pass
        conn.close()

# --- Ping API ---
def ping_api():
    # On ping l’API exposée par le service app sur l’hôte
    # (compose fait déjà le bind 8000:8000 dans ton projet)
    url = os.environ.get("API_HEALTH_URL", "http://localhost:8000/docs")
    r = requests.get(url, timeout=15)
    if r.status_code >= 400:
        raise AirflowFailException(f"API ping KO: {url} -> {r.status_code}")

default_args = {
    "owner": "absolute",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="daily_full_update",
    default_args=default_args,
    start_date=datetime(2025, 9, 1, tzinfo=TZ),
    schedule="0 9 * * *",   # 09:00 UTC quotidien
    catchup=False,
    tags=["absolute_cinema"],
) as dag:

    run_full_update = BashOperator(
        task_id="run_full_update",
        bash_command=f'cd "{PROJECT_DIR}" && docker compose run --rm full_update',
        env={"PYTHONUNBUFFERED": "1"},
    )

    sf_checks = PythonOperator(
        task_id="snowflake_light_checks",
        python_callable=check_snowflake,
    )

    api_ping = PythonOperator(
        task_id="api_ping",
        python_callable=ping_api,
    )

    run_full_update >> sf_checks >> api_ping
