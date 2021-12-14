import csv
import math

#load in taxi center locations
with open('TaxiLocationsMiddlePoints.csv' , newline='') as file:
    taxiReader = csv.reader(file, delimiter=',')
    taxi = []
    for row in taxiReader:
        taxi.append(row)

    #load in weather station locations
with open('WeatherLocations.csv', newline='') as file2:
    weatherReader = csv.reader(file2, delimiter=',')
    used = {}

    weather = []
    for row in weatherReader:
        weather.append(row)
        used[row[0]] = 0


        #mydict.pop("key", None)
with open("ouputTop3.csv" , 'w', newline='') as file:
    outWriter = csv.writer(file, delimiter = ',')
    #compare locations
    for taxiRow in taxi:
        tLat = float(taxiRow[1])
        tLon = float(taxiRow[2])

        distances = []
        for wRow in weather:

            wLat = float(wRow[4])
            wLon = float(wRow[3])
            a = math.pow( tLat - wLat , 2 )
            b = math.pow( tLon - wLon , 2 )
            distances.append( (math.sqrt( a + b ), wRow[0]) )

        distancesTop3 = sorted(distances, key=lambda tup: tup[0])[:3]   #[:3]
        for e in distancesTop3:
            used[e[1]] += 1
        outWriter.writerow( [taxiRow[0] , distancesTop3[0][1] , distancesTop3[1][1]   , distancesTop3[2][1]] )

    countUnused = 0
    countUsed = 0
    for e in used:
        if used[e] == 0:
            countUnused += 1
        else:
            countUsed += 1
            print(e , used[e])

    print("unused: " + str(countUnused))
    print("used " + str(countUsed))



    #also print out unsused weatherstations?
