"""
Логистические витрины
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

LOG_RAW_SCHEMA = "log_raw"
LOG_MARTS_SCHEMA = "log_marts"


with DAG(
    dag_id="logistics_analytics",
    start_date=datetime(2026, 3, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args={"owner": "xxx", "depends_on_past": False, "retries": 1},
    max_active_runs=1,
    tags=["logistics", "analytics", "marts"],
) as dag:
    ddl_delivery_activity = PostgresOperator(
        task_id="ddl_delivery_activity",
        postgres_conn_id="postgres_analytics",
        sql="""
        CREATE SCHEMA IF NOT EXISTS log_marts;
        CREATE TABLE IF NOT EXISTS log_marts.delivery_activity_daily (
            activity_date DATE NOT NULL, user_id TEXT NOT NULL, deliveries_count INTEGER NOT NULL,
            avg_delivery_duration_minutes NUMERIC(12, 2), delayed_or_failed_deliveries INTEGER,
            night_deliveries_count INTEGER, PRIMARY KEY (activity_date, user_id));
        """,
    )
    refresh_delivery_activity = PostgresOperator(
        task_id="refresh_delivery_activity",
        postgres_conn_id="postgres_analytics",
        sql="""
        DELETE FROM log_marts.delivery_activity_daily WHERE activity_date = '{{ ds }}'::date;
        INSERT INTO log_marts.delivery_activity_daily (
            activity_date, user_id, deliveries_count, avg_delivery_duration_minutes,
            delayed_or_failed_deliveries, night_deliveries_count)
        SELECT s.start_time::date, s.user_id, COUNT(*),
            AVG(s.session_duration_seconds)::NUMERIC(12, 2) / 60.0,
            COALESCE(SUM(CASE WHEN e.event_type IN ('error', 'failed', 'delayed') THEN 1 ELSE 0 END), 0),
            SUM(CASE WHEN EXTRACT(HOUR FROM s.start_time) BETWEEN 22 AND 23 OR EXTRACT(HOUR FROM s.start_time) BETWEEN 0 AND 5 THEN 1 ELSE 0 END)
        FROM log_raw.delivery_sessions_raw s
        LEFT JOIN log_raw.delivery_events_raw e ON e.timestamp::date = s.start_time::date AND e.event_type IN ('error', 'failed', 'delayed')
        WHERE s.start_time::date = '{{ ds }}'::date
        GROUP BY s.start_time::date, s.user_id;
        """,
    )
    ddl_ticket_flow = PostgresOperator(
        task_id="ddl_ticket_flow",
        postgres_conn_id="postgres_analytics",
        sql="""
        CREATE SCHEMA IF NOT EXISTS log_marts;
        CREATE TABLE IF NOT EXISTS log_marts.ticket_flow_daily (
            snapshot_date DATE NOT NULL, issue_type TEXT NOT NULL, status TEXT NOT NULL,
            tickets_count INTEGER NOT NULL, avg_resolution_time_hours NUMERIC(12, 2),
            open_tickets_count INTEGER, reopened_tickets_count INTEGER,
            PRIMARY KEY (snapshot_date, issue_type, status));
        """,
    )
    refresh_ticket_flow = PostgresOperator(
        task_id="refresh_ticket_flow",
        postgres_conn_id="postgres_analytics",
        sql="""
        DELETE FROM log_marts.ticket_flow_daily WHERE snapshot_date = '{{ ds }}'::date;
        INSERT INTO log_marts.ticket_flow_daily (
            snapshot_date, issue_type, status, tickets_count, avg_resolution_time_hours,
            open_tickets_count, reopened_tickets_count)
        SELECT '{{ ds }}'::date, dt.issue_type, dt.status, COUNT(*),
            AVG(CASE WHEN dt.resolution_time_seconds IS NOT NULL THEN dt.resolution_time_seconds / 3600.0 END)::NUMERIC(12, 2),
            SUM(CASE WHEN dt.status IN ('open', 'in_progress') THEN 1 ELSE 0 END),
            SUM(CASE WHEN dt.status = 'closed' AND EXISTS (SELECT 1 FROM jsonb_array_elements(dt.messages) AS m(elem) WHERE m.elem->>'sender' = 'user' AND (m.elem->>'message') ILIKE '%again%') THEN 1 ELSE 0 END)
        FROM log_raw.delivery_tickets_raw dt
        WHERE dt.created_at::date <= '{{ ds }}'::date
        GROUP BY dt.issue_type, dt.status;
        """,
    )
    ddl_delivery_activity >> refresh_delivery_activity >> ddl_ticket_flow >> refresh_ticket_flow
