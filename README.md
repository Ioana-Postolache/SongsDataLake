# SongplaysDataWarehouseRedshift
This project builds an ETL pipeline for a data lake hosted on S3. It loads data from S3, processes the data into analytics tables using Spark, and then loads them back into S3. 

The data comes from two datasets that reside in S3. Below are the S3 links for each:

- Song data: s3://udacity-dend/song_data - contains JSON metadata on the songs in the app
- Log data: s3://udacity-dend/log_data - contains JSON logs on user activity on the app
Log data json path: s3://udacity-dend/log_json_path.json

The data is loaded from S3 to staging tables on Redshift and SQL statements are executed to create the the tables below from these staging tables:
- Fact Table
* songplays - records in event data associated with song plays i.e. records with page NextSong; 
Columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
- Dimension Tables
* users - users in the app;
Columns: user_id, first_name, last_name, gender, level
* songs - songs in music database;
Columns: song_id, title, artist_id, year, duration
* artists - artists in music database;
Columns: artist_id, name, location, lattitude, longitude
* time - timestamps of records in songplays broken down into specific units;
Columns: start_time, hour, day, week, month, year, weekday

## How to run
The following steps were taken from a comment by user David L on the [Udacity Knowledge Forum] (https://knowledge.udacity.com/questions/50066) and describe the steps for Linux/Mac users:
### Start Up EMR Cluster:
Log into the AWS console for Oregon and navigate to EMR
Click "Create Cluster"
Select "Go to advanced options"
Under "Software Configuration", select Hadoop, Hive, and Spark
* Optional: Select Hue (to view HDFS) and Livy (for running a notebook)
5. Under "Edit software settings", enter the following configuration:
```
[{"classification":"spark", "properties":{"maximizeResourceAllocation":"true"}, "configurations":[]}]
```
6. Click "Next" at the bottom of the page to go to the "Hardware" page
7. I found some EC2 subnets do not work in the Oregon region (where the Udacity S3 data is) -  For example, us-west-2b works fine. us-west-2d does not work (so don't select that)
8. You should only need a couple of worker instances (in addition to the master) - m3.xlarge was sufficient for me when running against the larger song dataset
9. Click "Next" at the bottom of the page to go to the "General Options" page
10. Give your cluster a name and click "Next" at the bottom of the page
11. Pick your EC2 key pair in the drop-down. This is essential if you want to log onto the master node and set up a tunnel to it, etc.
12. Click "Create Cluster"
### Connect to Master Node from a BASH shell and update the spark-env.sh file:
On the main page for your cluster in the AWS console, click on SSH next to "Master public DNS"
On the Mac/Linux tab, copy the command to ssh into the master node. It should look roughly as follows:
ssh -i PATH_TO_MY_KEY_PAIR_FILE.pem hadoop@ec2-99-99-999-999.us-west-2.compute.amazonaws.com
3. Paste it and run it in a BASH shell window and type "yes" when prompted, NOTE: You may need to set up an automated SSH ping on your Linux machine to run every 30 seconds or so to keep the shell connection to EMR alive. 
4. Using sudo, append the following line to the /etc/spark/conf/spark-env.sh file:
```
export PYSPARK_PYTHON=/usr/bin/python3
```
Create a local tunnel to the EMR Spark History Server on your Linux machine:
Open up a new Bash shell and run the following command (using the proper IP for your master node):
```
ssh -i PATH_TO_MY_KEY_PAIR_FILE.pem -N -L 8157:ec2-99-99-999-999.us-west-2.compute.amazonaws.com:18080 hadoop@ec2-99-99-999-999.us-west-2.compute.amazonaws.com
```
NOTE: This establishes a tunnel between your local port 8157 and port 18080 on the master node.
You can pick a different unused number for your local port here.
The list of ports on the EMR side and what UIs they offer can be found at: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-web-interfaces.html
2. Go to localhost:8157 in a web browser on your local machine and you should see the Spark History Server UI
### Run your job
SFTP the dl.cfg and etl.py files to the hadoop account directory on EMR. NOTE: You can SFTP directly from a BASH shell or use an FTP tool (e.g., Cyberduck on Mac)
2. In your home directory on the EMR master node (/home/hadoop), run the following command:
```
spark-submit etl.py
```
3. After a couple of minutes your job should show up in the Spark History Server page in your browser.You should see the real-time logging output in your EMR bash shell window as well
### Links for Windows:
[Connect to EMR using ssh] (https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-ssh.html)
[Set up an ssh tunnel] (https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-ssh-tunnel.html)

## Song Dataset
The first dataset is a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.
- song_data/A/B/C/TRABCEI128F424C983.json
- song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
- {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

### Sample rows from the song files
```
+------------------+---------------+-----------------+----------------+----------------------------------------------------------------------------------------------+---------+---------+------------------+----------------------------------------------------+----+
|artist_id         |artist_latitude|artist_location  |artist_longitude|artist_name                                                                                   |duration |num_songs|song_id |title                                               |year|
+------------------+---------------+-----------------+----------------+----------------------------------------------------------------------------------------------+---------+---------+------------------+----------------------------------------------------+----+
|ARDR4AC1187FB371A1|null           |                 |null            |Montserrat Caballé;Placido Domingo;Vicente Sardinero;Judith Blegen;Sherrill Milnes;Georg Solti|511.16363|1        |SOBAYLL12A8C138AF9|Sono andati? Fingevo di dormire                     |0   |
|AREBBGV1187FB523D2|null           |Houston, TX      |null            |Mike Jones (Featuring CJ_ Mello & Lil' Bran)                                                  |173.66159|1        |SOOLYAZ12A6701F4A6|Laws Patrolling (Album Version)                     |0   |
|ARMAC4T1187FB3FA4C|40.82624       |Morris Plains, NJ|-74.47995       |The Dillinger Escape Plan                                                                     |207.77751|1        |SOBBUGU12A8C13E95D|Setting Fire to Sleeping Giants                     |2004|
|ARPBNLO1187FB3D52F|40.71455       |New York, NY     |-74.00712       |Tiny Tim                                                                                      |43.36281 |1        |SOAOIBZ12AB01815BE|I Hold Your Hand In Mine [Live At Royal Albert Hall]|2000|
|ARDNS031187B9924F0|32.67828       |Georgia          |-83.22295       |Tim Wilson                                                                                    |186.48771|1        |SONYPOM12A8C13B2D7|I Think My Wife Is Running Around On Me (Taco Hell) |2005|
+------------------+---------------+-----------------+----------------+----------------------------------------------------------------------------------------------+---------+---------+------------------+----------------------------------------------------+----+
```

## Log Dataset
The second dataset consists of log files in JSON format generated by [this event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset are partitioned by year and month.
### Sample rows from the log files
```
+-----------+---------+---------+------+-------------+--------+---------+-----+-------------------------------------+------+--------+-----------------+---------+---------------+------+-------------+-----------------------------------------------------------------------------------------------------------------------------------------+------+
|artist     |auth     |firstName|gender|itemInSession|lastName|length   |level|location                             |method|page    |registration     |sessionId|song           |status|ts           |userAgent                                                                                                                                |userId|
+-----------+---------+---------+------+-------------+--------+---------+-----+-------------------------------------+------+--------+-----------------+---------+---------------+------+-------------+-----------------------------------------------------------------------------------------------------------------------------------------+------+
|Harmonia   |Logged In|Ryan     |M     |0            |Smith   |655.77751|free |San Jose-Sunnyvale-Santa Clara, CA   |PUT   |NextSong|1.541016707796E12|583      |Sehr kosmisch  |200   |1542241826796|"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"|26    |
|The Prodigy|Logged In|Ryan     |M     |1            |Smith   |260.07465|free |San Jose-Sunnyvale-Santa Clara, CA   |PUT   |NextSong|1.541016707796E12|583      |The Big Gundown|200   |1542242481796|"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"|26    |
|Train      |Logged In|Ryan     |M     |2            |Smith   |205.45261|free |San Jose-Sunnyvale-Santa Clara, CA   |PUT   |NextSong|1.541016707796E12|583      |Marry Me       |200   |1542242741796|"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"|26    |
|null       |Logged In|Wyatt    |M     |0            |Scott   |null     |free |Eureka-Arcata-Fortuna, CA            |GET   |Home    |1.540872073796E12|563      |null           |200   |1542247071796|Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko                                                                     |9     |
|null       |Logged In|Austin   |M     |0            |Rosales |null     |free |New York-Newark-Jersey City, NY-NJ-PA|GET   |Home    |1.541059521796E12|521      |null           |200   |1542252577796|Mozilla/5.0 (Windows NT 6.1; rv:31.0) Gecko/20100101 Firefox/31.0                                                                        |12    |
+-----------+---------+---------+------+-------------+--------+---------+-----+-------------------------------------+------+--------+-----------------+---------+---------------+------+-------------+-----------------------------------------------------------------------------------------------------------------------------------------+------+
```

## Config file (dl.cfg) structure
```
[AWS]
AWS_ACCESS_KEY_ID=''
AWS_SECRET_ACCESS_KEY=''
```
