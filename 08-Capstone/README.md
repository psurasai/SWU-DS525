# Final Capstone project

# Documentation

## Dataset

### UK Car Accidents 2005-2015 : [link](https://www.kaggle.com/datasets/silicon99/dft-accident-data?select=Casualties0515.csv)

## Data Schema

![er](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/orginal_dm.jpeg)
<br>

## Problem & Target
- ตรวจสอบการกระจายตัวของพื้นที่ที่เกิดอุบัติเหตุทางท้องถนนของพื้นที่ศึกษา (ข้อมูลการเกิดเหตุ 2005 - 2015 ประเทศสหราชอาณาจักร) ตามช่วงเวลาที่ผ่านมาว่ามีจุดเกิดเหตุที่ใด เปลี่ยนแปลงไปอย่างไร แบ่งกลุ่มพื้นที่เกิดเหตุในพื้นที่ต่างๆ และทำความเข้าใจรูปแบบที่เกิดขึ้นบ่อยๆ เช่น รายละเอียดคนขับ รุ่นรถ ถนน สภาพอากาศ แสงสว่าง ทางแยก ความรุนแรง ของอุบัติเหตุเหล่านี้ สะท้อน insight ที่น่าสนใจ เพื่อเสนอแนะให้ทางหน่วยงานที่เกี่ยวข้องหาทางแก้ไขปัญหา หรือออกนโยบายที่ลดการเกิดอุบัติเหตุลง
<br>

## Data model

![er](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/final_dm.jpeg)
<br>


## Project implementation instruction
<br>

### 1. Change directory to project **"chin-capstone"**:
```sh
$ cd 08-Capstone
```
<br>

### 2. Prepare Cloud access (AWS):
- Retrieve credential thru AWS terminal
```sh
$ cat ~/.aws/credentials
```
![awsAccessKey](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/access.jpeg)

- Copy 3 following values to update the source codes<br>

*Values to copy:*
> - aws_access_key_id
> - aws_secret_access_key
> - aws_session_token

*Source code to update (1):*
> - /code/etl_datalake_s3.ipynb
![awsAccessKeyDL](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/lake_access.jpeg)

*Source code to update (2):*
> - /dags/etl_dwh_airflow.py
![awsAccessKeyDWH](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/dwh_access.jpeg)

<br>

### 3. Prepare Datalake storage (AWS S3):
- Create below S3 bucket with *"All public access"*
> - **uk-car-accidents**

- Create below repositories in the bucket
> - **cleaned** (store datalake or cleaned data)

- Place the raw data in **uk-car-accidents**

![rawData](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/raw_S3.jpeg)

<br>

### 4. Prepare Datawarehouse storage (AWS RedShift):
- Create Redshift cluster with following information

![redshift1](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/cluster1.jpeg)
![redshift2](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/cluster2.jpeg)

- Copy "**Endpoint**" and Cluster information to update Redshift credential

![redshiftEndpoint](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/endpoint.jpeg)


- Update the source code with Redshift credential
> - **dags/etl_dwh_airflow.py**
![redshiftCredential](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/dwh_dags.jpeg)

<br>

### 5. Create virtual environment named **"ENV"** (only 1st time):
```sh
$ python -m venv ENV
```
<br>

### 6. Activate the visual environment:
```sh
$ source ENV/bin/activate
```
<br>

### 7. Install needful libraries from configuration file (only 1st time):
```sh
$ pip install -r prerequisite/requirements.txt
```
<br>

### 8. Prepare environment workspace thru Docker:
- If Linux system, run 2 following commands (for Airflow usage)

```sh
mkdir -p ./dags ./logs ./plugins
```
```sh
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

- After that, run below command to start Docker

```sh
docker-compose up
```
<br>

### 9. Execute the **"Datalake"** process thru Web service:
- Access PySpark Notebook UI by port 8888 (localhost:8888)

![pyspark](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/S3_nb.jpeg)

- Run PySpark Notebook **"/code/etl_datalake_s3.ipynb"**

![runSpark](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/final_tb.jpeg)

- The cleaned data will be stored in S3 for entity
> - uk-car-accidents/cleaned/clubs/<br>

- Each entity is partitioned by **"Day_of_Week"**

![cleanedDataPart](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/cleaned.jpeg)
<br><br>

### 10. Execute the **"Datawarehouse"** process thru Airflow:
- Access Airflow UI by port 8080 (localhost:8080) with below credential
> - Username: "airflow"<br>
> - Password: "airflow"<br>

- The Datawarehouse script will be run follow the schedule configuration
> - Schedule: "Monthly" (1st of each month)<br>
> - Start date: "1st January 2005"

![airflowDWH](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/airflow.jpeg)
![airflowDWH](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/faf.jpeg)

- The Datawarehouse data will be loaded into Redshift (check by Query editor)
```sh

<br>

### 11. Dashboard creation thru Tableau:
- Connect Tableau Desktop to Redshift by following information

![tbConnect](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/tableau.jpeg)

- Load the data from Redshift to Tableau

![tbLoadData](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/tableau_2.jpeg)

- Create Dashboard to visualize the insight!

https://public.tableau.com/app/profile/peerawit.surasai/viz/Car_accidents_Capstone/UKAccidentDashboard?publish=yes
![dashboard](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/visual.jpeg)
<br>
__________
<br>

## Shutdown steps
##### 1. Stop services by shutdown Docker:
```sh
$ docker-compose down
```

##### 2. Deactivate the virtual environment:
```sh
$ deactivate
```