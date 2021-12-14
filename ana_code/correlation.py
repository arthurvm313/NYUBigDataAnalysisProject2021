import datetime as dt
import matplotlib.dates as mdates
import numpy as np
import matplotlib.pyplot as plt
import pylab
from pylab import *

dates = []
counts = {}
durations = {}     # [[]]*265
rains = {}


currDate = "2017-01-01"

with open("joinedTables1NULLREMOVED.csv" , 'r' ) as file:
    line = file.readline()
    while line:
        #2017-01-01    0   105  0.0   1.0E-5    0.0   NULL   0.0
        #   0          1   2    3      4         5    6      7
        # date      day   zone count  duration  r1  r2     r3
        lineSplit = line.split(",")

        if  lineSplit[5] != "NULL":
            rain = float(lineSplit[5])
        elif lineSplit[6] != "NULL":
            rain = float(lineSplit[6])
        elif lineSplit[7] != "NULL":
            rain = float(lineSplit[7])
        else:
            line = file.readline()
            continue

        count = float(lineSplit[3])
        duration = float(lineSplit[4])
        zone = int(lineSplit[2])

        counts.setdefault(zone, []).append(count)
        durations.setdefault(zone, []).append(duration)
        rains.setdefault(zone, []).append(rain)

        line = file.readline()

print("done")

#for key in counts:
#    print(key , len(counts[key]))

#260 1095
#261 1095
#262 1095
#263 1095

#plt.figure(dpi=300)
#ax = plt.gca()
#plt.scatter(rains[262] , counts[260] , s=1);
#plt.show()


fig, axs = plt.subplots(2, 2)
axs[0, 0].scatter(rains[132] , counts[132] , s=1)
axs[0, 0].set_title('132 - jfk')
axs[0, 1].scatter(rains[170] , counts[170] , s=1)
axs[0, 1].set_title('170 - midtown')
axs[1, 0].scatter(rains[88] , counts[88] , s=1)
axs[1, 0].set_title('88 - financial district')
axs[1, 1].scatter(rains[114] , counts[114] , s=1)
axs[1, 1].set_title('114 - nyu')

for ax in axs.flat:
    ax.set(xlabel='rain', ylabel='number of rides ')

# Hide x labels and tick labels for top plots and y ticks for right plots.
#for ax in axs.flat:
#    ax.label_outer()

plt.show()

for key in counts:
    print (np.corrcoef(rains[key] , counts[key] )[0 , 1] )



#print(duration)
#print(rains)

#plt.figure(dpi=300)
#ax = plt.gca()
#ax.set_yscale('log')
#ax.set_xscale('log')

#plt.scatter(rain, counts , s=1, alpha=0.1);
#plt.show()
#plt.savefig('duration.png', dpi=300)

#fig = plt.figure()
#fig.subplots_adjust(bottom=0.2)
#ax = fig.add_subplot(111)

#plt.scatter(counts,rain)
#plt.show()