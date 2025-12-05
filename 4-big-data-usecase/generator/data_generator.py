import uuid
import random
import csv
from datetime import datetime, timedelta
import json
from pathlib import Path

OUTPUT_DIR = Path("generator_output")
OUTPUT_DIR.mkdir(exist_ok=True)

N_TRANSACTIONS = 1000
N_CUSTOMERS = 100
DIRTY_RATIO = 0.05  # 5%

PAYMENT_METHODS = ["credit", "debit", "gopay", "ovo", "transfer"]
CITIES = ["Jakarta", "Bandung", "Surabaya", "Medan", "Makassar"]
COUNTRIES = ["Indonesia"]

def generate_customers():
    customers = []

    for i in range(1, N_CUSTOMERS + 1):
        user_id = f"user_{i:03d}"
        name = f"Customer {i}"
        city = random.choice(CITIES)
        country = "Indonesia"
        signup_date = (datetime.now() - timedelta(days=random.randint(30, 1000))).date()

        customers.append([user_id, name, city, country, signup_date])

    with open(OUTPUT_DIR / "customers.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "name", "city", "country", "signup_date"])
        writer.writerows(customers)

    return customers

def generate_transactions(customers):
    user_ids = [row[0] for row in customers]
    dirty_count = int(N_TRANSACTIONS * DIRTY_RATIO)
    
    dict_lists = []

    for i in range(N_TRANSACTIONS):
        transaction_id = str(uuid.uuid4())
        product_id = f"prod_{random.randint(1, 50)}"
        amount = round(random.uniform(10, 500), 2)
        timestamp = datetime.now() - timedelta(hours=random.randint(1, 200))
        payment_method = random.choice(PAYMENT_METHODS)
        user_id = random.choice(user_ids)

        record = {
            "transaction_id": transaction_id,
            "user_id": user_id,
            "product_id": product_id,
            "amount": amount,
            "timestamp": timestamp.isoformat(),
            "payment_method": payment_method
        }

        if i < dirty_count:
            choice = random.choice(["negative", "future", "null"])
            if choice == "negative":
                record["amount"] = -abs(record["amount"])
            elif choice == "future":
                record["timestamp"] = (datetime.now() + timedelta(days=random.randint(30, 60))).isoformat()
            elif choice == "null":
                record["user_id"] = None
                
        dict_lists.append(record)

    with open(OUTPUT_DIR / "transactions.json", "w") as f:
        f.write(json.dumps(dict_lists, indent=4) + "\n")
            
def main():
    print("Generating mock data...")
    customers = generate_customers()
    generate_transactions(customers)
    print("Done. Files written to:", OUTPUT_DIR)