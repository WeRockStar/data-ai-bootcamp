# MongoDB with Docker Compose

## Step 1: Setup MongoDB Using Docker Compose

```bash
docker compose up -d
```

## Step 2: เชื่อมต่อด้วย VSCode MongoDB Extension

### 2.1 ติดตั้ง MongoDB Extension

1. เปิด VSCode
2. ไปที่ Extensions (Ctrl+Shift+X)
3. ค้นหา "Database Client"
4. ติดตั้ง Extension "Database Client"

### 2.2 เชื่อมต่อฐานข้อมูล

1. เปิด "Database Client" extension ใน VSCode
2. คลิก "Create Connection"
   - Name: mymongo
   - Server Type: MongoDB
   - Host: localhost
   - Username: mongo
   - Password: mg1234
   - Click Connect

## Step 3: ทดสอบรัน Local

```bash
pip install -r requirements.txt
python main.py
```

## Step 4: Down Docker Compose

```bash
docker-compose down
```

## Congratulation!!
