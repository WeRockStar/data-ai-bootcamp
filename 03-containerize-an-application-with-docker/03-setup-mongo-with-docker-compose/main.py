from pymongo import MongoClient
from faker import Faker
import random

# เชื่อมต่อ MongoDB
client = MongoClient('mongodb://admin:mongo1234@localhost:27017/')
db = client['retail']
products = db['product_attributes']

# สร้าง Faker object
fake = Faker()

# กำหนดโครงสร้างข้อมูลสินค้า
product_schema = {
    'name': str,           # ชื่อสินค้า
    'description': str,    # รายละเอียดสินค้า 
    'price': float,        # ราคา
    'category': ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports'],
    'condition': ['New', 'Used', 'Refurbished'],
    'stock': int,          # จำนวนในคลัง
    'manufacturer': str,   # ผู้ผลิต
    'sku': str,           # รหัสสินค้า
    'created_at': 'datetime', # วันที่สร้าง
    'rating': float,       # คะแนน
    'reviews_count': int   # จำนวนรีวิว
}

# สร้างและเพิ่มข้อมูลสินค้า 20 รายการ
for _ in range(20):
    product = {
        'name': fake.catch_phrase(),
        'description': fake.text(),
        'price': round(random.uniform(10.0, 1000.0), 2),
        'category': random.choice(product_schema['category']),
        'condition': random.choice(product_schema['condition']), 
        'stock': random.randint(0, 100),
        'manufacturer': fake.company(),
        'sku': fake.ean13(),
        'created_at': fake.date_time_this_year(),
        'rating': round(random.uniform(1, 5), 1),
        'reviews_count': random.randint(0, 1000)
    }
    
    try:
        # ตรวจสอบว่าข้อมูลตรงตาม schema
        for key, value_type in product_schema.items():
            if isinstance(value_type, list):
                assert product[key] in value_type, f"ค่า {key} ไม่ถูกต้องตาม schema"
            elif value_type == 'datetime':
                continue  # ข้ามการตรวจสอบ datetime
            else:
                assert isinstance(product[key], value_type), f"ชนิดข้อมูล {key} ไม่ถูกต้อง"
        
        products.insert_one(product)
        print(f"เพิ่มสินค้า: {product['name']}")
    except AssertionError as e:
        print(f"ข้อมูลไม่ตรงตาม schema: {e}")
    except Exception as e:
        print(f"เกิดข้อผิดพลาด: {e}")

print("เพิ่มข้อมูลสินค้าเสร็จสิ้น")
