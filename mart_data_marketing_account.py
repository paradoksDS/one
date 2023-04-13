from airflow import DAG
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta

# default arguments
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email': ['airflow@dags.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

airflow_DAG = DAG(
    'mart_data_marketing_account',
    default_args=default_args,
    catchup=False,
    schedule_interval="0 20 * * *",
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1)
)

def mart_data_marketing_account(**kwargs):
    with BigQueryHook(bigquery_conn_id='bigquery_default', delegate_to=None, use_legacy_sql=False).get_conn() as conn_bg:
        cur = conn_bg.cursor()

        delete_from_marketing_account = """
            DELETE FROM mart_data.marketing_account
            WHERE "Date" >= current_date - INTERVAL '30 d'
        """
        cur.execute(delete_from_marketing_account)

        insert_into_marketing_account = """
            INSERT INTO mart_data.marketing_account
            WITH crm_deals AS
            (
                SELECT
                    "Date Create",
                    "Account Name",
                    count(*) AS count_leads
                FROM
                    raw_data.crm_deals
                WHERE "Date Create" >= current_date - INTERVAL '30 d'
                GROUP BY 1, 2
            ),
            all_cost AS
            (
                SELECT
                    "Date",
                    "Account Name",
                    "Cost"
                FROM
                    raw_data.google_ads
                WHERE "Date" >= current_date - INTERVAL '30 d'
                UNION
                SELECT
                    "Date",
                    "Account Name",
                    "Cost"
                FROM
                    raw_data.yandex_direct
                WHERE "Date" >= current_date - INTERVAL '30 d'
            )
            SELECT
                all_cost."Date",
                all_cost."Account Name",
                all_cost."Cost",
                count_leads
            FROM
                all_cost
                LEFT JOIN crm_deals
                    ON crm_deals."Date Create" = all_cost."Date"
                    AND crm_deals."Account Name" = all_cost."Account Name";
        """
        cur.execute(insert_into_marketing_account)

        conn_bg.commit()
        cur.close()

with airflow_DAG:

    udapte_marketing_account = PythonOperator(task_id="mart_data_marketing_account", python_callable=mart_data_marketing_account)
