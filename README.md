
# Korean NLP Data ETL Demonstration

## Scope and objectives 

- This is a demonstration of data ETL (extraction, transforming, loading) process using AWS Cloud. 
- This is for loading and processing raw data to prepare the base daatabase for Korean NLP analysis (such as machine translation) 

## ETL process

THe ETL process is as below: 
1. Raw data files are placed in an AWS S3 Bucket 
2. Extract the data from the files into to tables of AWS Redshift data warehouse
3. Assess the data quality 
4. Clean the data  (NA rows are removied, out of schema data are removed)  
5. Assess the quality again

The ETL process above is managed by Apache Airflow 

## Raw data 

Following data are used and placed in the repository

-  Wikipedia Dumps (Korean, 20190401)  
      - https://dumps.wikimedia.org/kowiki/20190401/kowiki-20190401-langlinks.sql.gz  (used a part of the data (300K rows) )
      - https://dumps.wikimedia.org/kowiki/20190401/  kowiki-20190401-pages-articles-multistream-index.txt.bz2 (2 million rows)
-  Korean Hanjya List https://github.com/studioego/korean_hanja_info_python/blob/master/daebeobwon_hanja/data/hanja.txt

##Data Model 

Extracted data are loaded into following Redshift tables 

- 'Korean' table 
      data_id INT IDENTITY(1,1),
      edit_id VARCHAR (30),
      word_id VARCHAR (30),
      korean VARCHAR (500),
      PRIMARY KEY (data_id)
      
- 'Korean_Japanese' table     
      data_id INT IDENTITY(1,1),
      article_id VARCHAR (30),
      language VARCHAR(30),
      text VARCHAR (500),
      PRIMARY KEY (data_id)
      
- 'Korean_hanjya' table 
      data_id INT IDENTITY(1,1),
      korean VARCHAR (100),
      hanjya VARCHAR (100),
      examples VARCHAR (100),
      PRIMARY KEY (data_id)

- 'Korean_japanese_hanjya' table - this is combination of above

      data_id INT IDENTITY(1,1),
      word_id VARCHAR(30),
      korean VARCHAR(500),
      japanese VARCHAR (500),
      hanjya VARCHAR (500),
      PRIMARY KEY (data_id)
    
    
## How to run

1. Setup Airflow on localhost or on cloud
2. Input credentials of AWS, S3 Bucket, and Redshift, in Airflow's Connections and Variables fields 
3. Place kNLP-etl.py in following folder (~/airflow/etl/)
4. Run Aiflow (by 'airflow webserver' and 'airflow scheduler') 
5. Access localhost:8080  
6. Run 'kNLP' DAG, then all process will be run by Airflow


##Some evaluations of the data quality 

Two points are checked 
- Whether the data is not null (0 row)
- Whether word_id is too long (more than 13 digits) or not
 
There was no problem for both 

##Assumptions for further scenarios

- What if the data was increased by 100x? 
       - There is no problem for storage (because AWS has flexibility), cost will increase though
       - Use Spark will solve the problem of loading speed. However Redshift can have parallel loading function

- What if pipelines were run on daily basis by 7am? 
       - This is not required because this data wont be updated frequently, but this can be done by setting a parameter on Airflow

- What if the databaas needed to be acceessed by 100+ people?  
       - This is not likely scenario for this data. However, in that case, it is an option to load onto Cassandra distributed database


