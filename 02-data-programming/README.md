# 01-basic-python-and-sql

## โจทย์
- ถ้าเราไม่มี data จะ mock ข้อมูลได้อย่างไร?
- มี Script SQL สามารถ Automate ได้อย่างไร?

## Install Library
```bash
# change directory
cd /workspaces/data-ai-bootcamp/02-data-programming

pip install -r requirements.txt
```

## Connect to SQLite Database
1. เปิด "Database Client" extension ใน VSCode
2. คลิก "Create Connection"
3. เลือก Server Type: SQLite
4. ใส่รายละเอียดการเชื่อมต่อ:
   - Name: mysqlite01
   - Database Path: /workspaces/data-ai-bootcamp/02-data-programming/mysqlite01.db
   - Connect
5. close all tabs
6. go to `01_python_fundamentals.ipynb`
   - Select Kernel
   - Python 3.12.1 (version อาจจะไม่ตรงได้ ไม่เป็นไร)
7. go to `02_sql_fundamentals.sql`
   - Active Conection
   - mysqlite01
8. go to `03_python_and_sql_for_data.ipynb`

## Congratulation!!
