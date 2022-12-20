# Final Capstone project

# Documentation

## Dataset

### UK Car Accidents 2005-2015 : [link](https://www.kaggle.com/datasets/silicon99/dft-accident-data?select=Casualties0515.csv)

## Data Schema

![er](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/orginal_dm.jpeg)
<br>

## Problem & Target
Mainly foucsing on selling report which we would like to asking the following questions:
1. How is the total sales amount compare in term of product catagories and each state
2. Average sales per orders in each state 
3. Each payment type sales proportion 
<br>

## Data model

![er](https://github.com/psurasai/SWU-DS525/blob/main/08-Capstone/gallary/final_dm.jpeg)
<br>


## Project implementation - Preparation

### 1. Get started
```sh
$ cd Capstone_Project_Final
```
<br>

### 2. Applying code for saving jupyter lab (Any update on coding)

```sh
sudo chmod 777 .
```
<br>

### 3. Prepare environment workspace by Docker (Airflow):

```sh
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
<br>

### 4. create visual environment & install required libraries
```sh
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```
<br>

### 5. Start  environment

```sh
docker-compose up
```
<br>


## Starting AWS cloud service

### 1. Create AWS S3 bucket
<br>

### 2. Upload raw data into S3 bucket

![er](./Picture%20ref/Screenshot%202022-12-17%20130228.png)
<br>

Noted: From assumption, normally raw data (All tables) will be manually uploaded to S3 on monthly basis 
<br>

### 3. Get access key from AWS
```sh
cat ~/.aws/credentials
```
Then, copy these access key (Used to link with S3 and Redshift)
1) aws_access_key_id 
2) aws_secret_access_key
3) aws_session_token
<br>

## Project implementation - Transformation

By using pyspark

Target: To transform data from 8 tables into a final table that contains with all necessary features by following up data model
<br>

### 1. Access into working port 8888
![er](./Picture%20ref/Screenshot%202022-10-05%20220731.png)
<br>

### 2. Excute notebook "etl.datalake_S3.ipynb" Step by step

![er](./Picture%20ref/Screenshot%202022-12-17%20222417.png)
<br>

*Transform data into "final_table" before uploading result back to S3 by partition it with "year"

![er](./Picture%20ref/Screenshot%202022-12-17%20130251.png)
<br>

Code: [python_code_for_create_final_table](https://github.com/pongthanin/swu-ds525/blob/main/Capstone_Project_Final/etl_datalake_S3.ipynb)
<br>

### Result after merging table
![er](./Picture%20ref/Screenshot%202022-12-17%20223352.png)
<br>


## Project implementation - Connect S3 to Redshift