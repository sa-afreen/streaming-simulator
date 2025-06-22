import random, time

INTERACTIONS = ["click", "view", "purchase"]

def generate_log():
    return {
        "user_id": f"user_{random.randint(1, 500)}",
        "item_id": f"item_{random.randint(1, 200)}",
        "interaction_type": random.choice(INTERACTIONS),
        "timestamp": time.time()
    }