#!/usr/bin/env python

import pyspark
import sys

def preprocess(data):
 hr = data.split(' ')[1].split(':')[0]
 if int(hr) <= 6:
  return ('0-6', 1)
 elif int(hr) <= 12:
  return ('6-12', 1)
 elif int(hr) <= 18:
  return ('12-18', 1)
 else:
  return ('18-24', 1)

if len(sys.argv) != 3:
  raise Exception("Exactly 2 arguments are required: <inputUri> <outputUri>")

inputUri=sys.argv[1]
outputUri=sys.argv[2]

sc = pyspark.SparkContext()
lines = sc.textFile(sys.argv[1])
filter = lines.filter(lambda x: True if ':' in x else False)
map = filter.map(preprocess)
reduce = map.reduceByKey(lambda a, b: a + b)
reduce.saveAsTextFile(sys.argv[2])
#print(filter.take(1))
