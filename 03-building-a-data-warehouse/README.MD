# Building a Data Warehouse

## Data model
![DataModel](Document/921D3921-40B6-43CF-9A28-98F6DA1DFB73.jpeg)
<br>

## Documentation
[Documentation]()
<br>
__________
<br>

## Project implementation instruction
<br>


### 1. change directory to project 03-data-warehouse:
```sh
$ cd 03-building-a-data-warehouse
```

### 2. create visual environment named 'ENV' (only 1st time):
```sh
$ python -m venv ENV
```

### 3. activate the visual environment:
```sh
$ source ENV/bin/activate
```

### 4. install required libraries from config file (only 1st time): 
```sh
$ pip install -r requirements.txt
```

### 5. Create AWS Redshift cluster (with following config):
```sh
- 'Cluster identification'  : redshift-cluster-1
- 'Cluster for'             : Production
- 'Node type'               : ra3.xlplus
- 'AQUA'                    : Turn off
- 'Number of nodes'         : 1
- 'Database username'       : awsuser
- 'Database password'       : awsPassword1
- 'Cluster permission'      : LabRole
- 'Remaining'               : keep as default
```
**!! Don't forget !! - Enable public access to AWS RedShift**
<br>

![Redshift](Document/redshift_1.jpeg.png)
<br>

<br>

### 6. Upload data file and manifest file to AWS S3:
&nbsp;&nbsp;&nbsp;a. Create AWS S3 bucket with ‘Full public access’ <br>
&nbsp;&nbsp;&nbsp;b.	Upload files <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Manifest file : events_json_path.json <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Data file : github_events_01.json

![S3](Document/S3_bucket.jpeg)
<br>

<br>

### 7. Config ‘etl.py’ to connect to AWS Redshift:
&nbsp;&nbsp;&nbsp;a. Host : copy from AWS Redshift endpoint <br>
&nbsp;&nbsp;&nbsp;b. Port : 5439 <br>
&nbsp;&nbsp;&nbsp;c. Dbname : dev <br>
&nbsp;&nbsp;&nbsp;d. User/Password : as define when create the cluster 
![Connection](Document/etl_path.jpeg)
<br>

<br>

### 8. Config ‘etl.py’ to copy the data from AWS S3 to AWS Redshift:
&nbsp;&nbsp;&nbsp;a. From : the URI to data file <br>
&nbsp;&nbsp;&nbsp;b. Credentials : the ARN of LabRole <br>
&nbsp;&nbsp;&nbsp;c. Json : the URI to manifest file <br>
![Copy](Document/etl_path2.jpeg)
<br>

### 9. Create tables, Inject data from S3 to Redshift, Insert data, Query data thru python script, named ‘etl.py’:
```sh
$ python etl.py
```

<br>

### 10.	Check the data in cluster by ‘query editor’ (exported as csv):
&nbsp;&nbsp;[staging_events]<br>
&nbsp;&nbsp;[events] <br>
&nbsp;&nbsp;[actors] <br>
&nbsp;&nbsp;[repos]

<br>

## Shutdown steps

### 11. deactivate the visual environment:
```sh
$ deactivate
```

<br>

### 12. Delete the AWS Redshift cluster

<br>

### 13. Delete the files and bucket in AWS S3