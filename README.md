# Setting up a Data Lake on S3 for Sparkify
#### Creation of the Data Lake on AWS S3 using Spark


### Purpose:
We will be creating a Data Lake on AWS S3 for Sparkify app using the Event Log and Song files stored on S3. The source files are in JSON format and we will read and process the files using Spark. The Data Lake will have a set of parquet tables having fact and dimension information. The tables will be useful for sparkify to analyze their data in a more effective way which will in turn help them to make better business decisions. It will help them to understand user's taste in songs, their choice and listening pattern and utlizing those analytics, Sparkiy will be able to provide more quality service and gain their user base.

### Source File Information:
There are 2 different types of Data (Event log files and Song files) that is available for the Sparkify music streaming application amd they are stored as JSON files. Following are the paths for the files:

**Song Data**
s3://udacity-dend/song_data

**Log Data** 
s3://udacity-dend/log_data

Following are the JSON file structures:


- Song Files: It has all Songs, Albums and Artist related details. Here is one sample row:
        {   "num_songs": 1, 
            "artist_id": "ARD7TVE1187B99BFB1", 
            "artist_latitude": null, 
            "artist_longitude": null, 
            "artist_location": "California - LA", 
            "artist_name": "Casual", 
            "song_id": "SOMZWCG12A8C13C480", 
            "title": "I Didn't Mean To", 
            "duration": 218.93179, 
            "year": 0
        }

- Log Files: It has the logs of the user's music listening activity on the app. Here is one sample row:
        {   "artist":null,
            "auth":"Logged In",
            "firstName":"Walter",
            "gender":"M",
            "itemInSession":0,
            "lastName":"Frye",
            "length":null,
            "level":"free",
            "location":"San Francisco-Oakland-Hayward, CA",
            "method":"GET",
            "page":"Home",
            "registration":1540919166796.0,
            "sessionId":38,
            "song":null,
            "status":200,
            "ts":1541105830796,
            "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"
        }
        
### Approach:
Here are steps we will be taking to create the data warehouse on Cloud. 

- Read the JSON files stored in S3 using Spark and store the data in Spark Dataframe. 
- Filter data from the dataframes and store them in staging dataframes.
- Finally write the dataframes as parquet files in a S3 bucket. 

### Technical Design:
Here are the steps taken to create the Sparkify data lake:

- Create a new IAM role
    - Create a new IAM role and assign AmazonS3FullAccess policy to the same. Get the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for this role and same the data data in dl.cfg.
- Create a new S3 bucket 
    - Create a S3 bucket in your profile which will be used to write back the parquet files. Give public access to the S3 bucket.
    
    <img src="/resource/S3Datalake.PNG">
    
- Data Flow:
  <img src="/resource/dataflow.jpg">
  
- Write a python file for ETL process
    - Create a Spark session
    - Read the JSON files from S3 bucket and save them in a spark dataframe. 
    - Extract columns to build Song, Artist, User dataframes. 
    - Convert the TS column to a timestamo column and extract different part of a timestamp, and save them to create time table. 
    - Join the Song file and log file dataframe and extract columns for Songplay dataframe. 
    - Sabe the newly created dataframes to another S3 bucket as parquet file.
- Execute the ETL Process
    - To execute the ETL process, open a console and run the command : python etl.py. 