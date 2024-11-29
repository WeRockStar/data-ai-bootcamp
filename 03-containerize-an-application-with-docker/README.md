# 02-docker

## โจทย์
- app เราจะรันได้ทุกที่ไหม?
- libs จะ
  
## Concept Docker
- Dockerfile
- Image
- Container
- Docker Compose

## Docker Command
- docker build -t <image_name> .
- docker run -p <port>:<port> <image_name>
- docker compose up


# Setup Postgres using docker

## Container Concept
- Dockerfile
- Image
- Registry
- Container

## command
- build
- push
- pull
- run
- stop
- rm

```bash
cd /workspaces/live-demo-data-ai-boostcamp/01-docker-and-setup-postgres/
```

## Pull & Run container
- dockerhub postgres: [https://hub.docker.com/_/postgres](https://hub.docker.com/_/postgres)

```bash
# default docker run on web dockerhub
docker run --name some-postgres -e POSTGRES_PASSWORD=mysecretpassword -d postgres

# change name & add config
docker run --name mypostgres -p 5432:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=pg1234 -d postgres:15.9
```

## Connect postgres
1. Go to SQLTools Extension
2. Add new connection: postgre
    - Connection name: mypostgres
    - Server Address: localhost
    - Port: 5432
    - Database: postgres
    - Username: postgres
    - Password: pg1234
3. Test Conenction
4. Save Connection
5. New SQL file
6. Write your SQL code

## Basic CRUD SQL
```sql
-- Create customer table
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert customers (Create)
INSERT INTO customers (name, email, phone, address) VALUES 
    ('John Doe', 'john@example.com', '+1234567890', '123 Main St'),
    ('Jane Smith', 'jane@example.com', '+1987654321', '456 Oak Ave');

-- Select customers (Read)
SELECT * FROM customers;

SELECT * FROM customers WHERE email = 'john@example.com';

-- Update customers (Update)
UPDATE customers 
SET email = 'john.doe@example.com',
    phone = '+1234567891'
WHERE id = 1;



-- Delete customers (Delete)
DELETE FROM customers WHERE id = 1;

```

## Cleanup
```bash
# to stop container
docker stop mypostgres

# to remove container
docker rm mypostgres
```

## Reference
- https://docs.docker.com/get-started/docker-overview/