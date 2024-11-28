# คู่มือการตั้งค่า MongoDB ด้วย Docker Compose

ผมจะแก้ไขไฟล์ README.md ให้ใช้ Docker Compose แทน โดยมีการเปลี่ยนแปลงหลักๆดังนี้:

````markdown:03-containerize-an-application-with-docker/03-setup-mongo/README.md
# MongoDB Data Setup Guide
## Step 1: Setup MongoDB Using Docker Compose

### 1.1 สร้างไฟล์ compose.yml
```yaml
version: '3.8'
services:
  mongodb:
    image: mongo:8.0
    container_name: mymongo
    environment:
      - MONGO_INITDB_ROOT_USERNAME=admin
      - MONGO_INITDB_ROOT_PASSWORD=mongo1234
    ports:
      - "27017:27017"
    volumes:
      - mongodb_data:/data/db

volumes:
  mongodb_data:
```

### 1.2 รันคำสั่งเพื่อสร้างและเริ่มต้น Container
```bash
docker compose up -d
```

## Step 2: เชื่อมต่อด้วย VSCode MongoDB Extension

### 2.1 ติดตั้ง MongoDB Extension
1. เปิด VSCode
2. ไปที่ Extensions (Ctrl+Shift+X)
3. ค้นหา "Database Client JDBC"
4. ติดตั้ง Extension "Database Client JDBC"

### 2.2 เชื่อมต่อฐานข้อมูล
1. เปิด "Database Client JDBC" extension ใน VSCode
2. คลิก "Connect"
3. ใส่ connection string:
   ```
   mongodb://mongo:mg1234@localhost:27017/
   ```

### 2.3 สร้าง Database และ Collection
1. คลิกขวาที่ CONNECTIONS เลือก "Create Database"
2. ตั้งชื่อ Database: `retail`
3. ตั้งชื่อ Collection: `product_attributes`


## Step 3: รันสคริปต์ Python เพื่อเพิ่มข้อมูล

### 3.1 ติดตั้ง Library ที่จำเป็น
```bash
pip install pymongo
```

### 3.2 รันสคริปต์ Python
```bash
python main.py
```

## การแก้ไขปัญหา
- ตรวจสอบว่า Docker กำลังทำงานอยู่
- เช็คสถานะ Container: `docker compose ps`
- ตรวจสอบการเชื่อมต่อเครือข่าย
- เช็คเวอร์ชันของ Python library

## แนวทางปฏิบัติที่ดี
- ใช้ environment variables สำหรับข้อมูลการเชื่อมต่อฐานข้อมูล
- ใส่ error handling ในสคริปต์
- สำรองข้อมูลอย่างสม่ำเสมอ
```

การเปลี่ยนแปลงหลักๆ คือ:
1. เปลี่ยนจากการใช้คำสั่ง `docker run` เป็นการใช้ `docker compose`
2. เพิ่ม volume เพื่อเก็บข้อมูลอย่างถาวร
3. เพิ่มขั้นตอนการสร้าง Database อย่างชัดเจน
4. ปรับปรุงการจัดรูปแบบและภาษาให้เข้าใจง่ายขึ้น