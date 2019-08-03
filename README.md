+------------------+---------------+-----------------+----------------+----------------------------------------------------------------------------------------------+---------+---------+------------------+----------------------------------------------------+----+
|artist_id         |artist_latitude|artist_location  |artist_longitude|artist_name                                                                                   |duration |num_songs|song_id |title                                               |year|
+------------------+---------------+-----------------+----------------+----------------------------------------------------------------------------------------------+---------+---------+------------------+----------------------------------------------------+----+
|ARDR4AC1187FB371A1|null           |                 |null            |Montserrat Caballé;Placido Domingo;Vicente Sardinero;Judith Blegen;Sherrill Milnes;Georg Solti|511.16363|1        |SOBAYLL12A8C138AF9|Sono andati? Fingevo di dormire                     |0   |
|AREBBGV1187FB523D2|null           |Houston, TX      |null            |Mike Jones (Featuring CJ_ Mello & Lil' Bran)                                                  |173.66159|1        |SOOLYAZ12A6701F4A6|Laws Patrolling (Album Version)                     |0   |
|ARMAC4T1187FB3FA4C|40.82624       |Morris Plains, NJ|-74.47995       |The Dillinger Escape Plan                                                                     |207.77751|1        |SOBBUGU12A8C13E95D|Setting Fire to Sleeping Giants                     |2004|
|ARPBNLO1187FB3D52F|40.71455       |New York, NY     |-74.00712       |Tiny Tim                                                                                      |43.36281 |1        |SOAOIBZ12AB01815BE|I Hold Your Hand In Mine [Live At Royal Albert Hall]|2000|
|ARDNS031187B9924F0|32.67828       |Georgia          |-83.22295       |Tim Wilson                                                                                    |186.48771|1        |SONYPOM12A8C13B2D7|I Think My Wife Is Running Around On Me (Taco Hell) |2005|
+------------------+---------------+-----------------+----------------+----------------------------------------------------------------------------------------------+---------+---------+------------------+----------------------------------------------------+----+
only showing top 5 rows

#  |-- artist_id:  (nullable = true)
#  |-- artist_latitude: double (nullable = true)
#  |-- artist_location: string (nullable = true)
#  |-- artist_longitude: double (nullable = true)
#  |-- artist_name: string (nullable = true)
#  |-- duration: double (nullable = true)
#  |-- num_songs: long (nullable = true)
#  |-- song_id: string (nullable = true)
#  |-- title: string (nullable = true)
#  |-- year: long (nullable = true)

+-----------+---------+---------+------+-------------+--------+---------+-----+-------------------------------------+------+--------+-----------------+---------+---------------+------+-------------+-----------------------------------------------------------------------------------------------------------------------------------------+------+
|artist     |auth     |firstName|gender|itemInSession|lastName|length   |level|location                             |method|page    |registration     |sessionId|song           |status|ts           |userAgent                                                                                                                                |userId|
+-----------+---------+---------+------+-------------+--------+---------+-----+-------------------------------------+------+--------+-----------------+---------+---------------+------+-------------+-----------------------------------------------------------------------------------------------------------------------------------------+------+
|Harmonia   |Logged In|Ryan     |M     |0            |Smith   |655.77751|free |San Jose-Sunnyvale-Santa Clara, CA   |PUT   |NextSong|1.541016707796E12|583      |Sehr kosmisch  |200   |1542241826796|"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"|26    |
|The Prodigy|Logged In|Ryan     |M     |1            |Smith   |260.07465|free |San Jose-Sunnyvale-Santa Clara, CA   |PUT   |NextSong|1.541016707796E12|583      |The Big Gundown|200   |1542242481796|"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"|26    |
|Train      |Logged In|Ryan     |M     |2            |Smith   |205.45261|free |San Jose-Sunnyvale-Santa Clara, CA   |PUT   |NextSong|1.541016707796E12|583      |Marry Me       |200   |1542242741796|"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"|26    |
|null       |Logged In|Wyatt    |M     |0            |Scott   |null     |free |Eureka-Arcata-Fortuna, CA            |GET   |Home    |1.540872073796E12|563      |null           |200   |1542247071796|Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko                                                                     |9     |
|null       |Logged In|Austin   |M     |0            |Rosales |null     |free |New York-Newark-Jersey City, NY-NJ-PA|GET   |Home    |1.541059521796E12|521      |null           |200   |1542252577796|Mozilla/5.0 (Windows NT 6.1; rv:31.0) Gecko/20100101 Firefox/31.0                                                                        |12    |
+-----------+---------+---------+------+-------------+--------+---------+-----+-------------------------------------+------+--------+-----------------+---------+---------------+------+-------------+-----------------------------------------------------------------------------------------------------------------------------------------+------+

     |-- artist: string (nullable = true)
 |-- auth: string (nullable = true)
 |-- firstName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- itemInSession: long (nullable = true)
 |-- lastName: string (nullable = true)
 |-- length: double (nullable = true)
 |-- level: string (nullable = true)
 |-- location: string (nullable = true)
 |-- method: string (nullable = true)
 |-- page: string (nullable = true)
 |-- registration: double (nullable = true)
 |-- sessionId: long (nullable = true)
 |-- song: string (nullable = true)
 |-- status: long (nullable = true)
 |-- ts: long (nullable = true)
 |-- userAgent: string (nullable = true)
 |-- userId: string (nullable = true)