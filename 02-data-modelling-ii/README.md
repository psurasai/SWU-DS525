# Data Modeling II

## Data model and Documentation
[DataModel](https://github.com/psurasai/SWU-DS525/blob/main/02-data-modelling-ii/Week2_Documentation.pdf)
<br>
<br>

## Project implementation instruction

### Implementation steps

##### 1. change directory to project 02-data-modeling-ii:
```sh
$ cd 02-data-modeling-ii
```

##### 2. create visual environment named 'ENV' (only 1st time):
```sh
$ python -m venv ENV
```

##### 3. activate the visual environment:
```sh
$ source ENV/bin/activate
```

##### 4. install required libraries from config file (only 1st time): 
```sh
$ pip install -r requirements.txt
```

##### 5. start Cassandra service by start Docker:
```sh
$ docker-compose up
```

##### 6. create tables, insert data, query data by execute python script:
```sh
$ python etl.py
```

##### 7. check the data in Terminal

<br>

### Shutdown steps

##### 8. stop Cassandra service by shutdown Docker:
```sh
$ docker-compose down
```

##### 9. deactivate the visual environment:
```sh
$ deactivate
```