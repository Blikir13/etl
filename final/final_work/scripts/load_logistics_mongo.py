"""
Загрузка тестовых данных логистического домена в MongoDB.
Запуск: из корня проекта (friend1): python scripts/load_logistics_mongo.py
Переменные: MONGO_URI, MONGO_DB (по умолчанию logistics_final).
"""
import os
import random
from datetime import timedelta, timezone

from faker import Faker
from pymongo import MongoClient


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "logistics_final")


def _client():
    return MongoClient(MONGO_URI)


def _gen_delivery_sessions(fake: Faker, n: int = 1000):
    out = []
    for i in range(n):
        user_id = f"user_{random.randint(1, 200)}"
        start = fake.date_time_between(start_date="-30d", end_date="now", tzinfo=timezone.utc)
        duration_minutes = random.randint(1, 120)
        end = start + timedelta(minutes=duration_minutes)
        pages_pool = ["/", "/home", "/products", "/products/42", "/cart", "/checkout", "/support"]
        pages_visited = random.sample(pages_pool, k=random.randint(2, min(5, len(pages_pool))))
        actions_pool = ["login", "view_product", "add_to_cart", "checkout_start", "checkout_finish", "logout"]
        actions = random.sample(actions_pool, k=random.randint(2, min(6, len(actions_pool))))
        device = random.choice([
            {"type": "mobile", "os": "iOS", "browser": "Safari"},
            {"type": "mobile", "os": "Android", "browser": "Chrome"},
            {"type": "desktop", "os": "Windows", "browser": "Chrome"},
            {"type": "desktop", "os": "macOS", "browser": "Safari"},
        ])
        out.append({
            "session_id": f"sess_{i+1:05d}",
            "user_id": user_id,
            "start_time": start.isoformat(),
            "end_time": end.isoformat(),
            "pages_visited": pages_visited,
            "device": device,
            "actions": actions,
        })
    return out


def _gen_delivery_events(fake: Faker, n: int = 2000):
    out = []
    event_types = ["click", "view", "scroll", "purchase", "error", "login", "logout"]
    for i in range(n):
        ts = fake.date_time_between(start_date="-30d", end_date="now", tzinfo=timezone.utc)
        out.append({
            "event_id": f"evt_{i+1:05d}",
            "timestamp": ts.isoformat(),
            "event_type": random.choice(event_types),
            "details": {
                "path": random.choice(["/", "/home", "/products", "/checkout", "/support"]),
                "info": fake.sentence(nb_words=6),
            },
        })
    return out


def _gen_delivery_tickets(fake: Faker, n: int = 500):
    out = []
    statuses = ["open", "in_progress", "resolved", "closed"]
    issue_types = ["payment", "delivery", "product_quality", "account", "other"]
    for i in range(n):
        user_id = f"user_{random.randint(1, 200)}"
        status = random.choice(statuses)
        issue_type = random.choice(issue_types)
        created_at = fake.date_time_between(start_date="-30d", end_date="-1d", tzinfo=timezone.utc)
        if status in ("resolved", "closed"):
            updated_at = created_at + timedelta(hours=random.randint(1, 72))
        else:
            updated_at = fake.date_time_between(start_date=created_at, end_date="now", tzinfo=timezone.utc)
        messages = []
        t = created_at
        for j in range(random.randint(1, 5)):
            sender = "user" if j % 2 == 0 else "support"
            t += timedelta(hours=random.randint(1, 12))
            messages.append({"sender": sender, "message": fake.sentence(nb_words=10), "timestamp": t.isoformat()})
        out.append({
            "ticket_id": f"ticket_{i+1:05d}",
            "user_id": user_id,
            "status": status,
            "issue_type": issue_type,
            "messages": messages,
            "created_at": created_at.isoformat(),
            "updated_at": updated_at.isoformat(),
        })
    return out


def _gen_route_recommendations(fake: Faker, n_users: int = 200):
    out = []
    for i in range(1, n_users + 1):
        n_prod = random.randint(3, 10)
        products = [f"prod_{random.randint(1, 500)}" for _ in range(n_prod)]
        last_updated = fake.date_time_between(start_date="-7d", end_date="now", tzinfo=timezone.utc)
        out.append({"user_id": f"user_{i}", "recommended_products": products, "last_updated": last_updated.isoformat()})
    return out


def _gen_quality_checks(fake: Faker, n: int = 500):
    out = []
    statuses = ["pending", "approved", "rejected"]
    flags_pool = ["contains_images", "contains_links", "toxicity_suspected"]
    for i in range(n):
        submitted_at = fake.date_time_between(start_date="-30d", end_date="now", tzinfo=timezone.utc)
        n_flags = random.choice([0, 1, 2])
        flags = random.sample(flags_pool, k=n_flags) if n_flags > 0 else []
        out.append({
            "review_id": f"rev_{i+1:05d}",
            "user_id": f"user_{random.randint(1, 200)}",
            "product_id": f"prod_{random.randint(1, 500)}",
            "review_text": fake.text(max_nb_chars=120),
            "rating": random.randint(1, 5),
            "moderation_status": random.choice(statuses),
            "flags": flags,
            "submitted_at": submitted_at.isoformat(),
        })
    return out


def run_load():
    fake = Faker()
    db = _client()[MONGO_DB]
    print(f"Logistics load: {MONGO_URI} db={MONGO_DB}")

    for name in ["DeliverySessions", "DeliveryEvents", "DeliveryTickets", "RouteRecommendations", "QualityChecks"]:
        db[name].delete_many({})

    db.DeliverySessions.insert_many(_gen_delivery_sessions(fake))
    db.DeliveryEvents.insert_many(_gen_delivery_events(fake))
    db.DeliveryTickets.insert_many(_gen_delivery_tickets(fake))
    db.RouteRecommendations.insert_many(_gen_route_recommendations(fake))
    db.QualityChecks.insert_many(_gen_quality_checks(fake))

    print("Done: DeliverySessions, DeliveryEvents, DeliveryTickets, RouteRecommendations, QualityChecks")


if __name__ == "__main__":
    run_load()
