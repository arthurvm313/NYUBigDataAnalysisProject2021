# NYUBigDataAnalysisProject2021
This is the repository for the NYU Big Data Analysis Project 2021

Our topic iss to analyze whether precipitation (rainfall) has an influence on taxi rides in NYC.
Everything was computed on the NYU peel Cluster.


## Datasets
The main two datasets we used are:

### Taxi
The data for yellow taxis from [this website](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) 
for the years 2017, 2018 and 2019

### Weather
Granted by [this website](https://www.ncdc.noaa.gov/cdo-web/search) upon request.

## 1. Cleaning
The datasets were cleaned first, using the two mapReduce scripts in the folder `clean`.
This included reducing everything to columns that were important for us, and aggregating the taxi trips by date and starting location.
Also the weather dataset was limited to stations that were (at least the in the the top 3) closest to at least one taxi location.

## 2. Analysis
We then analyzed the data with mapReduce jobs for finding average rainfall for each date in our data and finding the nearest weather station to each taxi location. `analyse`
