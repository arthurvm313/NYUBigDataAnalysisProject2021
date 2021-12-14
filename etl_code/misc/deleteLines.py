
with open('List.txt' , 'r') as file:
    weatherStations = file.read().splitlines()

with open( '2740761.csv' , 'r' ) as weather:
    with open('output.csv' , 'a') as output:

        line = weather.readline()
        countAll = 0
        countChosen = 0
        while line:
            countAll += 1
            if any(e in line for e in weatherStations):
                output.write(line)
                countChosen += 1
            line = weather.readline()

print('kept' , countChosen , 'from:' , countAll)


#read in relevant locations   as a map/ list?ß??

#open output file
#open weather file
#only copy lines that contain a string form the lookup file