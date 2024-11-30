# การใช้งาน BigQuery บน Google Cloud Platform

```bash
pip install -r requirements.txt
```

check region codespace: `gh api /user/codespaces/$CODESPACE_NAME --jq .location`

## Quick start

- go to: https://console.cloud.google.com/bigquery?project=dataaibootcamp
- project: `dataaibootcamp`
- create dataset: `dataai_<name>_<yy>`
  - Location Type: `Region`
  - Region: `asia-southeast1`
  - CREATE DATASET
- create table:
  - Create table from: `Upload`
  - Source format: `CSV`
  - Upload file: `house-price.csv`
  - Table: `house_price`
  - Schema: Auto detect
  - CREATE TABLE

## BigQuery คืออะไร

BigQuery เป็นบริการ Data Warehouse แบบ Serverless ของ Google Cloud Platform ที่ช่วยให้คุณสามารถวิเคราะห์ข้อมูลขนาดใหญ่ได้อย่างรวดเร็ว

## ข้อดีของ BigQuery

- รองรับข้อมูลขนาดใหญ่ (Petabyte-scale)
- ประมวลผลได้รวดเร็ว
- จ่ายตามการใช้งานจริง
- รองรับ SQL มาตรฐาน
- มีการรักษาความปลอดภัยสูง

## การเริ่มต้นใช้งาน

1. สร้าง Project บน Google Cloud Platform
2. เปิดใช้งาน BigQuery API
3. สร้าง Dataset และ Table

```sql
-- สร้าง Dataset
CREATE SCHEMA `project_id.dataset_name`
```

## คำสั่งพื้นฐาน

### การ Query ข้อมูล

```sql
SELECT *
FROM `project_id.dataset_name.table_name`
WHERE condition
LIMIT 1000
```

### การ Insert ข้อมูล

```sql
INSERT INTO `project_id.dataset_name.table_name`
(column1, column2)
VALUES
('value1', 'value2')
```

### การสร้าง Table

```sql
CREATE TABLE `project_id.dataset_name.table_name`
(
  column1 STRING,
  column2 INTEGER,
  column3 TIMESTAMP
)
```

## ตัวอย่างการใช้งาน

### การ Query ข้อมูลพร้อม Join

```sql
SELECT a.*, b.additional_column
FROM `project_id.dataset_name.table1` a
JOIN `project_id.dataset_name.table2` b
ON a.id = b.id
WHERE a.date >= '2024-01-01'
```

### การใช้ Window Functions

```sql
SELECT
  *,
  ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) as rank
FROM `project_id.dataset_name.sales`
```

## เครื่องมือที่เกี่ยวข้อง

- BigQuery Web UI
- bq command-line tool
- Client Libraries (Python, Java, Node.js)
- Data Studio สำหรับการทำ Visualization

## ข้อควรระวัง

1. ตรวจสอบ Query ก่อนรันเพื่อประหยัดค่าใช้จ่าย
2. ใช้ LIMIT ในการทดสอบ Query
3. ตั้งค่า Quotas เพื่อควบคุมการใช้งาน
4. ระวังการ Query ข้อมูลที่มีขนาดใหญ่

## แหล่งข้อมูลเพิ่มเติม

- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [SQL Reference](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)
- [Pricing](https://cloud.google.com/bigquery/pricing)
