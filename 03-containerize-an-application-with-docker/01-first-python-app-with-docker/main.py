import faker
import pandas as pd
import time
from sqlalchemy import create_engine
import pendulum
import logging

# Replace path of SQLite file if needed
engine = create_engine("sqlite:////mysqlite01.db")
fake = faker.Faker()


def generate_customers(num_customers):
    customers = []
    for _ in range(num_customers):
        customer = {
            "customer_id": fake.uuid4(),
            "name": fake.name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "address": fake.address(),
            "segment": fake.random_element(
                ["Retail", "Wholesale", "Online", "In-Store"]
            ),
            "join_date": pendulum.now("Asia/Bangkok"),
            "loyalty_points": fake.random_int(min=0, max=1000),
        }
        customers.append(customer)
    return customers


# สร้างข้อมูลลูกค้า
while True:
    customers = generate_customers(5)
    customer_df = pd.DataFrame(customers)
    customer_df.to_sql("customers", engine, if_exists="append", index=False)
    print(
        f"Created {len(customers)} customers to SQLite at {time.strftime('%H:%M:%S')}"
    )
    time.sleep(5)
