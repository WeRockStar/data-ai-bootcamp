# PostgreSQL Data Setup Guide
## Step 1: Setup PostgreSQL Using Docker

### 1.1 Install Docker
- Download and install Docker Desktop from the official website
- Verify installation by running `docker --version` in your terminal

### 1.2 Pull PostgreSQL Image
```bash
docker pull postgres:13
```

### 1.3 Create and Run PostgreSQL Container
```bash
docker run --name mypostgres -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=pg1234 -e POSTGRES_DB=postgres -p 5432:5432 -d postgres:13
```

## Step 2: Connect with VSCode PostgreSQL Extension

### 2.1 Install Extension
1. Open VSCode
2. Go to Extensions (Ctrl+Shift+X)
3. Search for "Database Client JDBC"
4. Install "Database Client JDBC"

### 2.2 Connect to Database
1. Open "Database Client JDBC" extension in VSCode
2. Click "Add Connection"
3. Select Server Type: PostgreSQL
4. Enter connection details:
   - Host: localhost
   - Port: 5432
   - Database: postgres
   - Username: postgres
   - Password: pg1234

### 2.3 Create Customer Table (init_pg.sql)
```sql
CREATE TABLE customers (
    customer_id UUID PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    address TEXT,
    segment VARCHAR(50),
    join_date TIMESTAMP,
    loyalty_points INTEGER
);
```

### 2.4 Insert Initial Data
```sql
INSERT INTO customers (
    customer_id, name, email, phone, 
    address, segment, join_date, loyalty_points
) VALUES (
    gen_random_uuid(), 
    'John Doe', 
    'john.doe@example.com', 
    '123-456-7890',
    '123 Main St, Anytown, USA',
    'Retail',
    CURRENT_TIMESTAMP,
    500
);
```

## Step 3: Python Script to Insert More Data

### 3.1 Install Required Libraries
```bash
pip install -r requirements.txt
```

### 3.2 Python Script (data_insertion.py)
```python
python main.py
```

## Troubleshooting
- Ensure Docker is running
- Check PostgreSQL container status: `docker ps`
- Verify network connectivity
- Check Python library versions

## Best Practices
- Use environment variables for database credentials
- Implement error handling in data insertion scripts
- Regularly backup your database