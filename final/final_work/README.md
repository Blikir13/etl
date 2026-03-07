# Логистика доставок — ETL и витрины

**Все пункты задания выполнены.** Скриншоты и результаты работы пайплайнов лежат в папке **`results/`**.

---

## Соответствие критериям оценивания (8 баллов)

| Критерий | Баллы | Выполнение |
|----------|-------|------------|
| Развёрнута реляционная база данных | 1 | PostgreSQL развёрнут в Docker (`docker-compose.yml`), база `etl_final_warehouse`, схемы `log_raw` и `log_marts`. |
| Развёрнута нереляционная база данных | 1 | MongoDB развёрнут в Docker, база `logistics_final`, коллекции: DeliverySessions, DeliveryEvents, DeliveryTickets, RouteRecommendations, QualityChecks. |
| Сгенерированы данные для нереляционной БД | 0,5 | Скрипт `scripts/load_logistics_mongo.py` генерирует и загружает тестовые данные в MongoDB. |
| Сформированы пайплайны для репликации в PostgreSQL + Airflow | 1 | DAG `logistics_sync`: загрузка из MongoDB в PostgreSQL (init_schema, ingest_sessions, ingest_events, ingest_tickets, ingest_routes, ingest_quality). |
| Пайплайны содержат этап трансформации данных | 1 | В каждом ingest-таске: чтение из Mongo, преобразование (pandas, даты, JSON, дедупликация), запись в PG. |
| Данные чистые: без дублей, корректно партиционированы, поддаются аналитике | 1 | Дедупликация по ключам (session_id, event_id, ticket_id, user_id, review_id), upsert в PG; схемы raw/marts разделены. |
| Пайплайны репликации описаны в документации | 0,5 | Описание DAG и тасков — в этом README (разделы «DAG-и и задачи», «Таблицы в PostgreSQL»). |
| Сформированы пайплайны для создания аналитических витрин в Airflow | 1 | DAG `logistics_analytics`: ddl_delivery_activity, refresh_delivery_activity, ddl_ticket_flow, refresh_ticket_flow. |
| Создано 2 аналитические витрины в Airflow | 1 | Витрина 1: `log_marts.delivery_activity_daily`. Витрина 2: `log_marts.ticket_flow_daily`. |
| **Итого** | **8** | |

---

## Кратко, что сделано

- **Нереляционная БД**: MongoDB, база `logistics_final`, коллекции:
  - `DeliverySessions`, `DeliveryEvents`, `DeliveryTickets`, `RouteRecommendations`, `QualityChecks`.
- **Реляционная БД**: PostgreSQL, база `etl_final_warehouse`, схемы:
  - `log_raw` — сырые данные из MongoDB (таблицы с суффиксом `_raw`);
  - `log_marts` — аналитические витрины.
- **Генерация данных**: скрипт `scripts/load_logistics_mongo.py` заполняет MongoDB тестовыми логистическими данными. Запуск: `python scripts/load_logistics_mongo.py` (из этой папки, переменные MONGO_URI, MONGO_DB).
- **Оркестрация**: два DAG-а в Airflow:
  - `logistics_sync` — загрузка из MongoDB в PostgreSQL (схема `log_raw`);
  - `logistics_analytics` — построение витрин в `log_marts.*`.

## DAG-и и задачи

### DAG `logistics_sync`

| Таск | Что делает |
|------|------------|
| `init_schema` | Создаёт схему `log_raw` и все таблицы (delivery_sessions_raw, delivery_events_raw, delivery_tickets_raw, route_recommendations_raw, quality_checks_raw). |
| `ingest_sessions` | Читает `DeliverySessions`, считает длительность доставки и сохраняет в `log_raw.delivery_sessions_raw`. |
| `ingest_events` | Читает `DeliveryEvents`, нормализует JSON `details` и сохраняет в `log_raw.delivery_events_raw`. |
| `ingest_tickets` | Читает `DeliveryTickets`, считает `resolution_time_seconds` и число сообщений, пишет в `log_raw.delivery_tickets_raw`. |
| `ingest_routes` | Читает `RouteRecommendations`, нормализует список маршрутов и пишет в `log_raw.route_recommendations_raw`. |
| `ingest_quality` | Читает `QualityChecks`, нормализует флаги и сохраняет в `log_raw.quality_checks_raw`. |

### DAG `logistics_analytics`

| Таск | Что делает |
|------|------------|
| `ddl_delivery_activity` | Создаёт таблицу витрины `log_marts.delivery_activity_daily`. |
| `refresh_delivery_activity` | По каждому пользователю и дате считает число доставок, среднюю длительность (в минутах), проблемные и ночные доставки. |
| `ddl_ticket_flow` | Создаёт таблицу витрины `log_marts.ticket_flow_daily`. |
| `refresh_ticket_flow` | По дате, типу проблемы и статусу считает число тикетов, среднее время решения, открытые и «переоткрытые» тикеты. |

## Таблицы в PostgreSQL

### Схема `log_raw`

- `log_raw.delivery_sessions_raw`, `log_raw.delivery_events_raw`, `log_raw.delivery_tickets_raw`, `log_raw.route_recommendations_raw`, `log_raw.quality_checks_raw`.

### Схема `log_marts`

- `log_marts.delivery_activity_daily` — эффективность доставок по пользователю и дате.
- `log_marts.ticket_flow_daily` — поток тикетов по типу проблемы и статусу.
