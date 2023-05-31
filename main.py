
import csv
import glob
import operator
from collections import defaultdict

#1. Split, formatting process. Divides the source file into small chunks of equal size.
def split(file):
    with open(file,'r')as f:
        reader=csv.reader(f)
        data=list(reader)
    #size is 5, divided into 5 split files
    size=len(data)//5
    for i in range(5):
        splitfile=data[i*size:(i+1)*size]
        with open(f'Split/split{i}.csv','w',newline='')as f:
            writer=csv.writer(f)
            writer.writerows(splitfile)

split('AComp_Passenger_data_no_error.csv')

#2. In the map stage, use the map function to generate a series of new key / value.
def map(file):
    new=defaultdict(int)
    with open(file,'r')as f:
        reader=csv.reader(f)
        for row in reader:
            user=row[0]
            new[user]=new[user]+1
    #Sort and reverse arrangement
    new = sorted(new.items(), key=lambda x: x[1], reverse=True)
    return new

#3.buffer in memory, spill stage
def spill(splitfile,i):
    buffer=[]
    num=0
    #Customize the number of buffers
    buffersize=10
    for j in splitfile:
        buffer += [j]
        buffer.sort(key=operator.itemgetter(1))
        #Check for overflow
        if len(buffer)>=buffersize:
            with open(f'buffer/split{i}_buffer{num}.csv', 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(buffer)
            buffer=[]
            num=num+1

for i in range(5):
    filename = f'Split/split{i}.csv'
    splitfile = map(filename)
    spill(splitfile, i)


#4.In the Merge phase, the Reduce Task merges files to prevent excessive use of memory or disk space.
bufferdata=[]
#read all files
buffer_files = glob.glob('buffer/split*_buffer*.csv')
#Merge files
for file in buffer_files:
    with open(file,'r')as f:
        reader=csv.reader(f)
        for row in reader:
            row[1] = int(row[1])
            bufferdata += [row]
#sort
bufferdata.sort(key=operator.itemgetter(1))

#5.In the Reduce phase, write the reduce() method to input data, and write the output key-value pairs to HDFS.
def reduce(file):
    count=defaultdict(int)
    for item in file:
        count[item[0]] += item[1]
    return count
result=reduce(bufferdata)
#output file after processing
with open('result.csv', 'w') as f:
    writer = csv.writer(f)
    for key, value in result.items():
        writer.writerow([key, value])

most_flights_passenger = max(result, key=result.get)
print(f'most user is: {most_flights_passenger}')





