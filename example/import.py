import csv
from random import random
print 'Generating some dummy data in data.csv...'
f = csv.writer(open('_data.csv', 'w'))
f.writerow(['x','y'])
for x in range(0,10000):
    f.writerow([x, x + 10000*random()])
