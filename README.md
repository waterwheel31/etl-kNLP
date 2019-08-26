
# Korean NLP Data ETL Demonstration

## Scope and objectives 

This is a demonstration of data ETL (extraction, transforming, loading) process using AWS Cloud. 
This is for loading and processing raw data to prepare the base daatabase for Korean NLP analysis (such as machine translation) 

## ETL process

THe ETL process is as below: 
1. Raw data files are placed in an AWS S3 Bucket 
2. Extract the data from the files into to tables of AWS Redshift data warehouse
3. Assess the data quality 
4. Clean the data  (NA rows are removied, out of schema data are removed)  
5. Assess the quality again

The ETL process above is managed by Apache Airflow 

## Raw data 

Following data are used and placed in the S3 Bucket 

-  Wikipedia Dumps (Korean, 20190401)  https://dumps.wikimedia.org/kowiki/20190401/
-  Korean Hanjya List https://github.com/studioego/korean_hanja_info_python/blob/master/daebeobwon_hanja/data/hanja.txt

# Data Model 

Extracted data are loaded into following Redshift tables 

- 'Korean_Japanese' table 
      - Korean_word (key)
      - japanese_word 
- 'Korean_Hanjya' table 
      - Korean_word (key)
      - Hanjya
- 'Korean_Japanese_Hanjya' table ... this is combined the two tables above
      - Korean_word (key)
      - Hanjya
      - Japanese_word

## How to run

1. Setup Airflow on localhost or on cloud
2. Input credentials of AWS,S3, and Redshift (actually, this is not disclosed, so you cannot run this)


# Some evaluations of the data quality 

- Korean_Japanese table 
- Korean_Hanjya table 
- KOrean_Japanese_Hanjya table 

Those are cleaned by following steps

- X
- X
- X 

# Assumptions for further scenarios

- What if the data was increased by 100x? 
       - There is no problem for storage (because AWS has flexibility), cost will increase though
       - There is no problem for loading (already using Spark distribution system), cost will increase though

- What if pipelines were run on daily basis by 7am? 
       - This is not required because this data wont be updated frequently, but this can be done by setting a parameter on Airflow

- What if the databaas needed to be acceessed by 100+ people?  
       - This is not likely scenario for this data. However, in that case, it is an option to load onto Cassandra distributed database


