# Creating and Scheduling Data Pipelines

## Data model
![DataModel](Doc/data-model.png)
<br>

## Documentation
[Documentation](https://github.com/chin-lertvipada/swu-ds525/blob/bff9b3c9375882b2133aa06c9896775038fdf517/05-creating-and-scheduling-data-pipelines/Doc/Lab5%20-%20Airflow%20-%20Summary.pdf)
<br>
__________
<br>

## Project implementation instruction
<br>

### 1. change directory to project 04-building-a-data-lake:
```sh
$ cd 05-creating-and-scheduling-data-pipelines
```
<br>

### 2. prepare environment workspace by Docker:
- ถ้าใช้งานระบบที่เป็น Linux ให้เรารันคำสั่งด้านล่างนี้ก่อน

```sh
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

- จากนั้นให้รัน

```sh
docker-compose up
```
<br>

### 3. Prepare data:
- คัดลอกโฟลเดอร์ `data` ที่เตรียมไว้ด้านนอกสุด เข้ามาใส่ในโฟลเดอร์ `dags` เพื่อที่ Airflow จะได้เห็นไฟล์ข้อมูลเหล่านี้ 
<br>

**หมายเหตุ:** จริง ๆ แล้วสามารถเอาโฟลเดอร์ `data` ไว้ที่ไหนก็ได้ที่ Airflow ที่เรารันเข้าถึงได้ แต่เพื่อความสะดวกสำหรับโปรเจคนี้ จึงนำเอาโฟลเดอร์ `data` ไว้ในโฟลเดอร์ `dags`

<br>

### 4. Access Airflow thru web service:
- เข้าไปที่หน้า Airflow UI ได้ที่ port 8080 (localhost:8080)
<br><br>

### 5. Access Postgres thru web service:
- เข้าไปที่หน้า Postgres UI ผ่าน service Adminer ได้ที่ port 8088 (localhost:8088)
<br><br>

### 6. Setup Postgres parameter for Airflow:
![AirflowConnectionSetup](Doc/AirflowConnectionSetup.png)
![AirflowConnectPostgres](Doc/AirflowConnectPostgres.png)
<br><br>

### 7. Data validation:
- ตรวจสอบการทำงานของ Airflow schedule ที่ตั้งค่าไว้
![Airflow](Doc/Airflow.png)
<br><br>
- ตรวจสอบข้อมูลที่มีการ load เข้าสู่ tables ตาม schedule ที่กำหนดไว้
![Postgres](Doc/Postgres.png)