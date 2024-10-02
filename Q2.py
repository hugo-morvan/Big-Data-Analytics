#!/usr/bin/env python3

from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings-small.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = ((year-month,station),temperature)
year_temperature = lines.map(lambda x: ((x[1][0:7],x[0]), float(x[3])))

#filter
year_temperature = year_temperature.filter(lambda x: int(x[0][0][0:4])>=1950 or int(x[0][0][0:4])<=2014)
year_temperature_10 = year_temperature.filter(lambda x: float(x[1])>10)

#Get max
#max_temperatures = year_temperature.reduceByKey(max)
#max_temperatures = max_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1][1])

#group
#year_temperature_group_station= year_temperature_10.reduceByKey(lambda x: x[0])
#map
year_temperature_group=year_temperature_10.map(lambda x: (x[0][0],1))

#year_temperature_group_2=year_temperature_group.map(lambda x: ((x[

#Get count
#day_count = year_temperature_group_2.map(lambda x: (x,1))
counts = year_temperature_group.reduceByKey(lambda x1,x2: x1 + x2)

#sorted_counts = counts.sortBy(ascending = False, keyfunc = lambda k: k[1])
#print(max_temperatures.collect())
#sort_cnt_notemp = sorted_counts.map(lambda x: (x[0], x[2]))

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
counts.saveAsTextFile("BDA/output")
