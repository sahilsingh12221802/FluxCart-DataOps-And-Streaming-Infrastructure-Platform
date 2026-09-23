import random
import uuid
from datetime import datetime, timezone


EVENT_TYPES = [
    "USER_LOGIN",
    "PRODUCT_SEARCH",
    "PRODUCT_VIEW",
    "ADD_TO_CART",
    "REMOVE_FROM_CART",
    "CHECKOUT",
    "PAYMENT",
    "PURCHASE",
]


DEVICES = [
    "mobile",
    "desktop",
    "tablet",
]


LOCATIONS = [
    "Pune",
    "Mumbai",
    "Delhi",
    "Bengaluru",
    "Hyderabad",
    "Chennai",
    "Kolkata",
    "Ahmedabad",
]


PRODUCT_CATALOG = [
    {
        "product_id": "PROD_101",
        "product_name": "Apple iPhone 16",
        "category": "Electronics",
        "price": 79999,
    },
    {
        "product_id": "PROD_102",
        "product_name": "MacBook Air M3",
        "category": "Electronics",
        "price": 114999,
    },
    {
        "product_id": "PROD_103",
        "product_name": "Sony WH-1000XM5",
        "category": "Audio",
        "price": 29999,
    },
    {
        "product_id": "PROD_104",
        "product_name": "Samsung 55-inch QLED TV",
        "category": "Electronics",
        "price": 64999,
    },
    {
        "product_id": "PROD_105",
        "product_name": "Nike Air Max",
        "category": "Fashion",
        "price": 12999,
    },
    {
        "product_id": "PROD_106",
        "product_name": "Logitech MX Master 3S",
        "category": "Accessories",
        "price": 8999,
    },
    {
        "product_id": "PROD_107",
        "product_name": "Apple AirPods Pro",
        "category": "Audio",
        "price": 24999,
    },
    {
        "product_id": "PROD_108",
        "product_name": "Samsung Galaxy S25",
        "category": "Electronics",
        "price": 74999,
    },
    {
        "product_id": "PROD_109",
        "product_name": "Adidas Ultraboost",
        "category": "Fashion",
        "price": 14999,
    },
    {
        "product_id": "PROD_110",
        "product_name": "Kindle Paperwhite",
        "category": "Books & Devices",
        "price": 15999,
    },
]


SEARCH_TERMS = [
    "wireless headphones",
    "laptop",
    "smartphone",
    "running shoes",
    "smart watch",
    "gaming mouse",
    "television",
    "earbuds",
    "keyboard",
    "books",
]


def now():
    return datetime.now(timezone.utc).isoformat()


def create_user():
    return {
        "user_id": f"USR_{random.randint(1000, 9999)}",
        "session_id": f"SES_{uuid.uuid4().hex[:10]}",
        "device": random.choice(DEVICES),
        "location": random.choice(LOCATIONS),
    }


def choose_product():
    return random.choice(PRODUCT_CATALOG)


def create_event(
    event_type,
    user,
    product=None,
    amount=0.0,
    search_query=None,
):
    event = {
        "event_id": f"evt_{uuid.uuid4().hex[:12]}",
        "event_type": event_type,
        "user": {
            "user_id": user["user_id"],
            "session_id": user["session_id"],
        },
        "product": None,
        "transaction": {
            "amount": round(amount, 2),
            "currency": "INR",
        },
        "context": {
            "device": user["device"],
            "location": user["location"],
        },
        "timestamp": now(),
    }

    if product:
        event["product"] = {
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "category": product["category"],
            "price": product["price"],
        }

    if search_query:
        event["search"] = {
            "query": search_query,
        }

    return event


def generate_journey(anomaly_rate=0.01):
    user = create_user()
    product = choose_product()

    events = []

    # 1. User login
    events.append(
        create_event(
            event_type="USER_LOGIN",
            user=user,
        )
    )

    # 2. Product search
    events.append(
        create_event(
            event_type="PRODUCT_SEARCH",
            user=user,
            search_query=random.choice(SEARCH_TERMS),
        )
    )

    # 3. Product view
    events.append(
        create_event(
            event_type="PRODUCT_VIEW",
            user=user,
            product=product,
        )
    )

    # Some users abandon the session after viewing
    if random.random() < 0.20:
        return events

    # 4. Add to cart
    events.append(
        create_event(
            event_type="ADD_TO_CART",
            user=user,
            product=product,
        )
    )

    # Some users remove the item instead of purchasing
    if random.random() < 0.10:
        events.append(
            create_event(
                event_type="REMOVE_FROM_CART",
                user=user,
                product=product,
            )
        )
        return events

    # 5. Checkout
    amount = product["price"]

    events.append(
        create_event(
            event_type="CHECKOUT",
            user=user,
            product=product,
            amount=amount,
        )
    )

    # 6. Payment
    events.append(
        create_event(
            event_type="PAYMENT",
            user=user,
            product=product,
            amount=amount,
        )
    )

    # 7. Purchase
    purchase_amount = amount

    # Inject an intentionally abnormal transaction
    if random.random() < anomaly_rate:
        purchase_amount = round(random.uniform(75000, 150000), 2)

    events.append(
        create_event(
            event_type="PURCHASE",
            user=user,
            product=product,
            amount=purchase_amount,
        )
    )

    return events