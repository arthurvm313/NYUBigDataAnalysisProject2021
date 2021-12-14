This repository was created for the final project in the course **Processing Big Data for Analytics Applications** at New York University in the Fall2021 Semester

# Structure

The /ana_code directory contains our analytics code. The main part is the python script for checking correlation, but it also contains several mapReduce programs for finding minimum and maximum values. The results of these programs are used to normalize the taxi data.

The /data_ingest directory contains only the original weather dataset. See datasets section below for more information

The /etl_code directory contains three subfolders: one for each dataset and one (misc) for additional needed code and resources. We did not split it on team members, because we worked on everything together from the beginning.

The /profiling_code directory is mostly empty because we reused most of our profiling code for the analytics and put it in that directory, besides that we also combined some profiling with the cleaning process. It now contains a MapReduce program to determine the most used taxi zones based on the amount of rides that start in each zone.

The /screenshots directory contains screenshots of map reduce programs running and SQL queries
 

# CLEANING:
## Dataset 1 Taxi:
The taxi data set consists of millions of lines (over 305million) and was distributed over 36 files (one per month for 3 years) it can be manually downloaded from:
https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
(all 'Yellow Taxi Trip Records' files for each month from 2017, 2018 and 2019)
 
Or be accessed in our hdfs directory  PATH:  "/user/tk2909/project/taxi"   if access is granted.
 
It includes many columns and lots of empty fields.
 
Cleaning is done with the CleanTaxi Script in "etl_code/taxi/CleanTaxi.jar"
This removes faulty lines (too short, too many missing/faulty values, etc.)
 
This script combines rides that started in the same area, on the same day into single lines by aggregating the important data fields, (~250k lines)
 
There is also a MapReduceTask that combines it on the day only by aggregating the important data fields (~1000 lines)
"etl_code/taxi/CleanTaxiWithoutZones.jar"
 
The final Set has these columns:
location (where the ride started), 
amount (how many rides started there on that day) 
durationSum, distanceSum, passengersSum, priceSum and tipsSum (all totals over all rides starting in that area)
 
If a value was missing, we added it as 0 (1 for passengers count, as there has to be at least one passenger.)
This is no problem, as we can still show rises or falls in these values (just not rely on the exact number) 
and amount and duration, which are our main focus, can never be 0 



## Dataset 2 Weather
The Weather Data was granted upon request from here:
https://www.ncdc.noaa.gov/cdo-web/search

but can also be found here "data_ingest/2740761.csv"
or on HPC  "/user/tk2909/project/weather/"
In general, it had lots of different values from weather stations around New York City (up to 20 miles away)
So we decided to shorten it on Weather stations that are actually close to our taxi pickup locations.

To find the stations that matter, we run a python script "etl_code/misc/findNearestStations.py"
That takes the center points of the taxi locations  from this file: "TaxiLocationsMiddlePoints.csv" (see below how that one was created) and the weather dataset 

and then, for each taxi location, it finds the three nearest stations (this lookup map "TaxiWeatherLookup.csv" is later used in joining the datasets)
It also prints out the used and unused stations.  The used stations were (manually) saved in "misc/List.txt" 

This file is then used in another python script  "misc/deleteLines.py"   to delete unused weather stations.

Then we cleaned it using "CleanWeather.csv" reducing the columns.  It now includes information on the reporting weather station (ID, name, location, height) and the total amount of rain reported for that day. Every other information, for example the amount of sun, was not reported by every station and thus left out.

This gives us the weatherNYC dataset.  "etl_code/weather/weatherNYC.csv"



## Taxi Locations
The center points of each taxi location in file: TaxiLocationsMiddlePoints.csv are calculated using a MapReduce job  ("etl_code/misc/TaxiZones.jar") and a list of points extracted From: https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddgc
as input 
it can also be found on hdfs "/user/tk2909/project/taxiZonesInput"
This includes a list of points that outline each taxi location and takes the center by using a simple method of taking the middle point between the min and max x- and y-coordinate respectively.

This is a very rough estimate of the center points and sometimes they fall out of the location, but it is close enough and a compromise due to the difference in forms and size of each location.


## Combining dataset
The two main datasets  NormalisedTaxi (/user/av3073/project/final/NormaliseTaxi) and weatherNYC were put into hive and joined with the help of the taxi Lookup file "etl_code/misc/".
   
Screenshots of the describe-table are included in the screenshot folder.

The join sql statement is:

SELECT ABCD.day, ABCD.weekday, ABCD.zone, ABCD.count, ABCD.duration, ABCD.r1, ABCD.r2, E.rain as r3  
FROM (SELECT ABC.day, ABC.weekday, ABC.zone, ABC.count, ABC.duration, ABC.r1, D.rain as r2, ABC.w3 
FROM ( SELECT AB.day, AB.weekday, Ab.zone, AB.count, AB.duration, C.rain as r1, AB.w2, AB.w3 
FROM (SELECT A.day, A.weekday, A.zone, A.count, A.duration, B.w1, B.w2, B.w3 from taxinormalized as A LEFT JOIN lookup as B on A.zone == B.zone) 
as AB LEFT JOIN weatherNYC as C on AB.w1 == C.id AND Ab.day == C.day) 
as ABC LEFT JOIN weatherNYC as D on ABC.w2 == D.id AND ABC.day == D.day) 
as ABCD LEFT JOIN weatherNYC as E on ABCD.w3 == E.id AND ABCD.day == E.day  ;

it joins the taxi table with the nearest three taxi zones and then successively joins the rain data for each of the three weather stations.

the resulting dataset now contains the following columns:
date, day of the week, taxi zone, normalized count of rides and normalized total duration and the rain values of the three nearest stations (if they were reported otherwise NULL)

we also did a more basic combination of the datasets that are reduced to one line per day (/user/av3073/project/final/RainCount and /user/av3073/project/final/NormaliseTaxiWithoutZones) with a simple join to do some first visualizations.


# Analytics

First the total and average amount of rain per day was calculated using the RainCount MapReduce job. In this job the rainfall per location is summed up and an average is taken per day. The output file is a file consisting of the date of the day and the total and average amount of rain that day across NYC.
This MapReduce job runs on /user/tk2909/project/weatherNYC.

To account for the different days during the week (because we expected that there are in general more taxi rides during the weekend than during the week for example) we normalised the taxi rides count and duration per day. We did this by taking the maximum and minimum value of count and duration for each day. 
We split this process into 2 parts one where the zone in which the taxi ride was taken is accounted for and one for all zones combined. This was done to obtain a general insight for all NYC and a more precise one per zone, in which the weather station locations were also taken into account.

The files to calculate this are:

MaxMinTaxiRideDuration
MaxMinTaxiRidesCount
MaxMinTaxiRideDurationWithoutZones
MaxMinTaxiRidesCountWithoutZones

In these MapReduce job the mapper takes in the lines from the 
"/user/av3073/project/final/CleanTaxi/"  or  "/user/av3073/project/final/CleanTaxiWithoutZones"
It then extracts the specific day from the date numbered 0-6 where 0 corresponds to sunday and outputs a key value pair with as key the day and as value the amount of rides/ duration of the rides.
The reducer then calculates the maximum and minimum value for each day.

Then we normalized the count and duration of each line with the following formula (in which the max and min values are obtained from the previous MapReduce job) to obtain a value between 0 and 1:
(x-min)/(max-min).
The files which were used to run this are:

NormaliseTaxi
NormaliseTaxiWithoutZones

The input files are the same as the previous MapReduce job.
These normalized data sets are then joined on the date with the rainCount dataset to obtain a conclusion. 
The amount of rides and duration vs raincount per day are then plotted to obtain a conclusion about the impact of rain on the amount/duration of taxi rides in NYC.


After we created the final table in hive (see combining datasets)  we also ran basic analytics using sql (min, max, average, count etc.)

We used a python script to do a final analysis and visualization of the final dataset for correlation (/ana_code/correlation.py)
