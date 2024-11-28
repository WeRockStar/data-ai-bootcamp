import time
import faker
import pendulum
import pandas as pd
from sqlalchemy import create_engine

# สร้าง Faker object สำหรับสร้างข้อมูลปลอม
fake = faker.Faker()

# สร้าง connection string สำหรับ PostgreSQL
engine = create_engine("postgresql://postgres:pg1234@localhost:5432/postgres")

# สร้างข้อมูลลูกค้า
customers = []
for _ in range(10):
    # สร้างข้อมูลลูกค้าเป็น dictionary
    customer = {
        "customer_id": fake.uuid4(),
        "name": fake.name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "address": fake.address(),
        "segment": fake.random_element(["Retail", "Wholesale", "Online", "In-Store"]),
        "join_date": pendulum.now("Asia/Bangkok").strftime("%Y-%m-%d %H:%M:%S"),
        "loyalty_points": fake.random_int(min=0, max=1000),
    }
    customers.append(customer)
# แปลง dictionary เป็น DataFrame
df = pd.DataFrame(customers)

# บันทึกข้อมูลลง PostgreSQL
df.to_sql("customers", con=engine, if_exists="append", index=False)
