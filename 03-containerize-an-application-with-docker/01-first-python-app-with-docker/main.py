import time
import faker
import pendulum

# สร้าง Faker object สำหรับสร้างข้อมูลปลอม
fake = faker.Faker()

# สร้างข้อมูลลูกค้า
while True:
    customer = {
        "customer_id": fake.uuid4(),
        "name": fake.name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "address": fake.address(),
        "segment": fake.random_element(["Retail", "Wholesale", "Online", "In-Store"]),
        "join_date": pendulum.now('Asia/Bangkok').strftime("%Y-%m-%d %H:%M:%S"),
        "loyalty_points": fake.random_int(min=0, max=1000),
    }
    print(customer)
    time.sleep(5)
