"""
Retail event generator.

The generator emits realistic multi-country retail activity while intentionally
injecting messy records so the Spark pipeline can demonstrate validation,
cleaning, duplicate handling, and dead-letter processing.
"""

from __future__ import annotations

import copy
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from faker import Faker

from config.config import VALID_CATEGORIES, VALID_EVENT_TYPES, VALID_YEARS


fake = Faker()

COUNTRIES = {
    "United States": ["New York", "Chicago", "Austin", "Seattle"],
    "Egypt": ["Cairo", "Alexandria", "Giza", "Mansoura"],
    "Germany": ["Berlin", "Munich", "Hamburg", "Frankfurt"],
    "United Kingdom": ["London", "Manchester", "Bristol", "Leeds"],
    "United Arab Emirates": ["Dubai", "Abu Dhabi", "Sharjah", "Ajman"],
    "Saudi Arabia": ["Riyadh", "Jeddah", "Dammam", "Medina"],
}

BRANDS = ["Aster", "Nexa", "UrbanPeak", "Velora", "OmniGoods", "BrightBox"]
SUPPLIERS = ["Global Trade Co", "Northwind Supply", "Delta Wholesale", "Prime Source"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "cash", "wallet"]
DEVICES = ["desktop", "mobile", "tablet"]
BROWSERS = ["Chrome", "Edge", "Firefox", "Safari", "Opera"]
CHANNELS = ["organic", "paid_search", "social", "email", "affiliate", "direct"]
GENDERS = ["Female", "Male", "Non-binary"]

EVENT_WEIGHTS = [0.43, 0.21, 0.11, 0.14, 0.04, 0.07]


class RetailEventGenerator:
    """Stateful generator with reusable customers, products, stores, and sessions."""

    def __init__(self, bad_record_rate: float = 0.08, duplicate_rate: float = 0.02) -> None:
        self.bad_record_rate = bad_record_rate
        self.duplicate_rate = duplicate_rate
        self.customers = self._build_customers(1500)
        self.products = self._build_products(350)
        self.stores = self._build_stores(40)
        self.last_event: dict[str, Any] | None = None

    def generate_event(self) -> dict[str, Any]:
        if self.last_event and random.random() < self.duplicate_rate:
            return copy.deepcopy(self.last_event)

        customer = random.choice(self.customers)
        product = random.choice(self.products)
        store = random.choice(self.stores)
        event_type = random.choices(VALID_EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
        quantity = self._quantity_for(event_type)
        discount = self._discount_for(event_type)
        rating = random.randint(1, 5) if event_type == "review" else None
        review_text = fake.sentence(nb_words=12) if event_type == "review" else None

        event = {
            "event_id": str(uuid.uuid4()),
            "event_timestamp": self._event_timestamp().isoformat(),
            "customer_id": customer["customer_id"],
            "customer_name": customer["customer_name"],
            "customer_age": customer["customer_age"],
            "customer_gender": customer["customer_gender"],
            "customer_country": customer["customer_country"],
            "customer_city": customer["customer_city"],
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "product_category": product["product_category"],
            "brand": product["brand"],
            "supplier": product["supplier"],
            "store_id": store["store_id"],
            "store_name": store["store_name"],
            "store_country": store["store_country"],
            "store_city": store["store_city"],
            "price": product["price"],
            "discount": discount,
            "quantity": quantity,
            "payment_method": random.choice(PAYMENT_METHODS) if event_type in {"checkout", "purchase", "return"} else None,
            "event_type": event_type,
            "rating": rating,
            "review_text": review_text,
            "device_type": random.choice(DEVICES),
            "browser": random.choice(BROWSERS),
            "session_id": f"SES-{uuid.uuid4()}",
            "marketing_channel": random.choice(CHANNELS),
        }

        if random.random() < self.bad_record_rate:
            event = self._inject_quality_issue(event)

        self.last_event = copy.deepcopy(event)
        return event

    def _event_timestamp(self) -> datetime:
        year = random.choice(VALID_YEARS)
        start = datetime(year, 1, 1, tzinfo=timezone.utc)
        end = datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        seconds = int((end - start).total_seconds())
        return start + timedelta(seconds=random.randint(0, seconds))

    def _build_customers(self, count: int) -> list[dict[str, Any]]:
        customers = []
        for i in range(1, count + 1):
            country = random.choice(list(COUNTRIES))
            customers.append(
                {
                    "customer_id": f"CUST-{i:06d}",
                    "customer_name": fake.name(),
                    "customer_age": random.randint(16, 80),
                    "customer_gender": random.choice(GENDERS),
                    "customer_country": country,
                    "customer_city": random.choice(COUNTRIES[country]),
                }
            )
        return customers

    def _build_products(self, count: int) -> list[dict[str, Any]]:
        products = []
        for i in range(1, count + 1):
            category = random.choice(VALID_CATEGORIES)
            products.append(
                {
                    "product_id": f"PROD-{i:05d}",
                    "product_name": f"{fake.word().title()} {category[:-1] if category.endswith('s') else category}",
                    "product_category": category,
                    "brand": random.choice(BRANDS),
                    "supplier": random.choice(SUPPLIERS),
                    "price": round(random.uniform(5, 1500), 2),
                }
            )
        return products

    def _build_stores(self, count: int) -> list[dict[str, Any]]:
        stores = []
        for i in range(1, count + 1):
            country = random.choice(list(COUNTRIES))
            city = random.choice(COUNTRIES[country])
            stores.append(
                {
                    "store_id": f"STORE-{i:04d}",
                    "store_name": f"{city} Retail Hub {i}",
                    "store_country": country,
                    "store_city": city,
                }
            )
        return stores

    @staticmethod
    def _quantity_for(event_type: str) -> int:
        if event_type in {"product_view", "review"}:
            return 1
        return random.randint(1, 6)

    @staticmethod
    def _discount_for(event_type: str) -> float:
        if event_type in {"purchase", "checkout"}:
            return round(random.choice([0, 0.05, 0.1, 0.15, 0.2]), 2)
        return 0.0

    @staticmethod
    def _inject_quality_issue(event: dict[str, Any]) -> dict[str, Any]:
        issue = random.choice(
            [
                "null_customer",
                "null_product",
                "missing_category",
                "invalid_category",
                "negative_price",
                "malformed_record",
                "missing_customer_id",
                "missing_product_id",
                "corrupted_timestamp",
                "impossible_age",
                "bad_quantity",
            ]
        )
        damaged = copy.deepcopy(event)
        if issue == "null_customer":
            damaged["customer_name"] = None
        elif issue == "null_product":
            damaged["product_name"] = None
        elif issue == "missing_category":
            damaged["product_category"] = None
        elif issue == "invalid_category":
            damaged["product_category"] = "INVALID_CATEGORY"
        elif issue == "negative_price":
            damaged["price"] = -abs(float(damaged["price"]))
        elif issue == "malformed_record":
            damaged.pop("event_type", None)
            damaged["malformed_payload"] = "{not valid business event"
        elif issue == "missing_customer_id":
            damaged["customer_id"] = None
        elif issue == "missing_product_id":
            damaged["product_id"] = None
        elif issue == "corrupted_timestamp":
            damaged["event_timestamp"] = "2026-99-99T99:99:99"
        elif issue == "impossible_age":
            damaged["customer_age"] = random.choice([-5, 130])
        elif issue == "bad_quantity":
            damaged["quantity"] = random.choice([0, -3, None])
        return damaged
