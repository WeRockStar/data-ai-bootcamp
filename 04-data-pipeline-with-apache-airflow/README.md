# Apache Airflow 

```bash
cd /workspaces/data-ai-bootcamp/04-data-pipeline-with-apache-airflow
```

```sh
mkdir -p ./dags ./logs ./plugins ./tests ./cred
```
​
สำหรับเครื่องที่เป็น Linux เราจำเป็นที่จะต้องกำหนด Airflow user ก่อนด้วย เพื่อให้ Airflow user ที่อยู่ใน Docker container สามารถเขียนไฟล์ลงมาบนเครื่อง host ได้ เราจะใช้คำสั่ง

```sh
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

```sh
docker compose up -d --build
```

#### Docker Commands
- Build custom Airflow Image: `docker compose build`
- Spin Up Docker Containers: `docker compose up -d`
- Stop Docker Containers: docker `compose stop`
- Start stopped Docker Containers: `docker compose start`
- Destroy Docker Containers: `docker compose down --volumes --remove-orphans`

----


| Name | Description |
| - | - |
| `dags/` | โฟลเดอร์ที่เก็บโค้ด DAG หรือ Airflow Data Pipelines ที่เราสร้างจะใช้ใน workshop |
| `docker-compose.yaml` | ไฟล์ Docker Compose ที่ใช้รัน Airflow ขึ้นมาบนเครื่อง |
| `cred/` | โฟลเดอร์ที่เก็บไฟล์ Credential หรือ Configuration อย่างไฟล์ `sa.json` |
| `tests/` | โฟลเดอร์ที่เก็บไฟล์ unitest เพื่อทำการทดสอบ python code |


# Reference
- [Airflow 2.10 Documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
- [Airflow 2.10 Docker Compose File](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
