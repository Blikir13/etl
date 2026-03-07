"""
Cинхронизация данных из MongoDB в PostgreSQL
"""
import json
import os
from datetime import datetime
from typing import Any, Dict, List
import pandas as pd

from pymongo import MongoClient

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "logistics_final")
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_analytics")
RAW_SCHEMA = os.getenv("LOGISTICS_RAW_SCHEMA", "log_raw")


def _mongo_coll(name: str):
    return MongoClient(MONGO_URI)[MONGO_DB][name]


def _pg_hook():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def init_schema(**kwargs):
    hook = _pg_hook()
    for sql in [
        f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};",
        f"""CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.delivery_sessions_raw (
            session_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, start_time TIMESTAMPTZ NOT NULL,
            end_time TIMESTAMPTZ NOT NULL, session_duration_seconds INTEGER NOT NULL,
            route_points JSONB, device JSONB, driver_actions JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW());""",
        f"""CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.delivery_events_raw (
            event_id TEXT PRIMARY KEY, timestamp TIMESTAMPTZ NOT NULL, event_type TEXT NOT NULL,
            details JSONB, created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW());""",
        f"""CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.delivery_tickets_raw (
            ticket_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, status TEXT NOT NULL, issue_type TEXT NOT NULL,
            messages JSONB, created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
            resolution_time_seconds INTEGER, messages_count INTEGER);""",
        f"""CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.route_recommendations_raw (
            user_id TEXT PRIMARY KEY, recommended_routes JSONB, last_updated TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW());""",
        f"""CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.quality_checks_raw (
            review_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, object_id TEXT NOT NULL, review_text TEXT,
            rating INTEGER, moderation_status TEXT, flags JSONB, submitted_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW());""",
    ]:
        hook.run(sql)


def _norm(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    try:
        out = []
        for rec in records:
            r = dict(rec)
            for k, v in list(r.items()):
                if hasattr(v, "to_pydatetime"):
                    r[k] = v.to_pydatetime()
                elif v is not None and getattr(pd, "isna", lambda x: False)(v):
                    r[k] = None
            out.append(r)
        return out
    except ImportError:
        return records


def ingest_sessions(**kwargs):
    col = _mongo_coll("DeliverySessions")
    docs = [dict(d) for d in col.find({})]
    for d in docs:
        d.pop("_id", None)
    if not docs:
        return
    df = pd.DataFrame(docs)
    df["start_time"] = pd.to_datetime(df["start_time"], utc=True)
    df["end_time"] = pd.to_datetime(df["end_time"], utc=True)
    df["session_duration_seconds"] = (df["end_time"] - df["start_time"]).dt.total_seconds().astype("Int64")
    df = df.rename(columns={"pages_visited": "route_points", "actions": "driver_actions"})
    df = df.drop_duplicates(subset=["session_id"])
    for c in ["route_points", "device", "driver_actions"]:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: json.dumps(x) if x is not None else None)
    hook = _pg_hook()
    sql = f"""INSERT INTO {RAW_SCHEMA}.delivery_sessions_raw (
        session_id, user_id, start_time, end_time, session_duration_seconds,
        route_points, device, driver_actions, updated_at)
        VALUES (%(session_id)s, %(user_id)s, %(start_time)s, %(end_time)s, %(session_duration_seconds)s,
        %(route_points)s::jsonb, %(device)s::jsonb, %(driver_actions)s::jsonb, NOW())
        ON CONFLICT (session_id) DO UPDATE SET user_id=EXCLUDED.user_id, start_time=EXCLUDED.start_time,
        end_time=EXCLUDED.end_time, session_duration_seconds=EXCLUDED.session_duration_seconds,
        route_points=EXCLUDED.route_points, device=EXCLUDED.device, driver_actions=EXCLUDED.driver_actions, updated_at=NOW();"""
    for rec in _norm(df.to_dict(orient="records")):
        hook.run(sql, parameters=rec)


def ingest_events(**kwargs):
    col = _mongo_coll("DeliveryEvents")
    docs = [dict(d) for d in col.find({})]
    for d in docs:
        d.pop("_id", None)
    if not docs:
        return
    df = pd.DataFrame(docs)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.drop_duplicates(subset=["event_id"])
    if "details" in df.columns:
        df["details"] = df["details"].apply(lambda x: json.dumps(x) if x is not None else None)
    hook = _pg_hook()
    sql = f"""INSERT INTO {RAW_SCHEMA}.delivery_events_raw (event_id, timestamp, event_type, details, updated_at)
        VALUES (%(event_id)s, %(timestamp)s, %(event_type)s, %(details)s::jsonb, NOW())
        ON CONFLICT (event_id) DO UPDATE SET timestamp=EXCLUDED.timestamp, event_type=EXCLUDED.event_type, details=EXCLUDED.details, updated_at=NOW();"""
    for rec in _norm(df.to_dict(orient="records")):
        hook.run(sql, parameters=rec)


def ingest_tickets(**kwargs):
    col = _mongo_coll("DeliveryTickets")
    docs = [dict(d) for d in col.find({})]
    for d in docs:
        d.pop("_id", None)
    if not docs:
        return
    df = pd.DataFrame(docs)
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)
    closed = df["status"].isin(["resolved", "closed"])
    df["resolution_time_seconds"] = None
    df.loc[closed, "resolution_time_seconds"] = (df.loc[closed, "updated_at"] - df.loc[closed, "created_at"]).dt.total_seconds().astype("Int64")
    if "messages" in df.columns:
        df["messages_count"] = df["messages"].apply(lambda x: len(x) if isinstance(x, list) else 0)
        df["messages"] = df["messages"].apply(lambda x: json.dumps(x) if x is not None else None)
    else:
        df["messages_count"] = 0
    df = df.drop_duplicates(subset=["ticket_id"])
    hook = _pg_hook()
    sql = f"""INSERT INTO {RAW_SCHEMA}.delivery_tickets_raw (
        ticket_id, user_id, status, issue_type, messages, created_at, updated_at, resolution_time_seconds, messages_count)
        VALUES (%(ticket_id)s, %(user_id)s, %(status)s, %(issue_type)s, %(messages)s::jsonb, %(created_at)s, %(updated_at)s, %(resolution_time_seconds)s, %(messages_count)s)
        ON CONFLICT (ticket_id) DO UPDATE SET user_id=EXCLUDED.user_id, status=EXCLUDED.status, issue_type=EXCLUDED.issue_type, messages=EXCLUDED.messages, created_at=EXCLUDED.created_at, updated_at=NOW(), resolution_time_seconds=EXCLUDED.resolution_time_seconds, messages_count=EXCLUDED.messages_count;"""
    for rec in _norm(df.to_dict(orient="records")):
        hook.run(sql, parameters=rec)


def ingest_routes(**kwargs):
    col = _mongo_coll("RouteRecommendations")
    docs = [dict(d) for d in col.find({})]
    for d in docs:
        d.pop("_id", None)
    if not docs:
        return
    df = pd.DataFrame(docs)
    df["last_updated"] = pd.to_datetime(df["last_updated"], utc=True)
    if "recommended_products" in df.columns:
        df["recommended_routes"] = df["recommended_products"]
        df = df.drop(columns=["recommended_products"])
    if "recommended_routes" in df.columns:
        df["recommended_routes"] = df["recommended_routes"].apply(lambda x: json.dumps(x) if x is not None else None)
    df = df.drop_duplicates(subset=["user_id"])
    hook = _pg_hook()
    sql = f"""INSERT INTO {RAW_SCHEMA}.route_recommendations_raw (user_id, recommended_routes, last_updated, updated_at)
        VALUES (%(user_id)s, %(recommended_routes)s::jsonb, %(last_updated)s, NOW())
        ON CONFLICT (user_id) DO UPDATE SET recommended_routes=EXCLUDED.recommended_routes, last_updated=EXCLUDED.last_updated, updated_at=NOW();"""
    for rec in _norm(df.to_dict(orient="records")):
        hook.run(sql, parameters=rec)


def ingest_quality(**kwargs):
    col = _mongo_coll("QualityChecks")
    docs = [dict(d) for d in col.find({})]
    for d in docs:
        d.pop("_id", None)
    if not docs:
        return
    df = pd.DataFrame(docs)
    df["submitted_at"] = pd.to_datetime(df["submitted_at"], utc=True)
    if "flags" in df.columns:
        df["flags"] = df["flags"].apply(lambda x: json.dumps(x) if x is not None else None)
    if "product_id" in df.columns:
        df = df.rename(columns={"product_id": "object_id"})
    df = df.drop_duplicates(subset=["review_id"])
    hook = _pg_hook()
    sql = f"""INSERT INTO {RAW_SCHEMA}.quality_checks_raw (
        review_id, user_id, object_id, review_text, rating, moderation_status, flags, submitted_at, updated_at)
        VALUES (%(review_id)s, %(user_id)s, %(object_id)s, %(review_text)s, %(rating)s, %(moderation_status)s, %(flags)s::jsonb, %(submitted_at)s, NOW())
        ON CONFLICT (review_id) DO UPDATE SET user_id=EXCLUDED.user_id, object_id=EXCLUDED.object_id, review_text=EXCLUDED.review_text, rating=EXCLUDED.rating, moderation_status=EXCLUDED.moderation_status, flags=EXCLUDED.flags, submitted_at=EXCLUDED.submitted_at, updated_at=NOW();"""
    for rec in _norm(df.to_dict(orient="records")):
        hook.run(sql, parameters=rec)


with DAG(
    dag_id="logistics_sync",
    start_date=datetime(2026, 3, 1),
    schedule_interval="@hourly",
    catchup=False,
    default_args={"owner": "xxx", "depends_on_past": False, "retries": 1},
    max_active_runs=1,
    tags=["logistics", "sync", "mongo", "postgres"],
) as dag:
    t_init = PythonOperator(task_id="init_schema", python_callable=init_schema)
    t_sessions = PythonOperator(task_id="ingest_sessions", python_callable=ingest_sessions)
    t_events = PythonOperator(task_id="ingest_events", python_callable=ingest_events)
    t_tickets = PythonOperator(task_id="ingest_tickets", python_callable=ingest_tickets)
    t_routes = PythonOperator(task_id="ingest_routes", python_callable=ingest_routes)
    t_quality = PythonOperator(task_id="ingest_quality", python_callable=ingest_quality)
    t_init >> [t_sessions, t_events, t_tickets, t_routes, t_quality]
