#!/usr/bin/env python

import sys
import fileinput
import pickle

if len(sys.argv) != 4:
    print "We need 4 arguments (reduce.py input1 input2 output)"
    exit(1)

# read python dict back from the files
pkl_file = open(sys.argv[1], 'rb')
mydict1 = pickle.load(pkl_file)
pkl_file.close()

pkl_file = open(sys.argv[2], 'rb')
mydict2 = pickle.load(pkl_file)
pkl_file.close()

# d is the result dictionary
d=dict()
for k,v in mydict1.items():
       if mydict2.__contains__(k):
            d[k]=mydict1[k]+mydict2[k]
       else:
            d[k]=mydict1[k]
for k,v in mydict2.items():
       if  not(d.__contains__(k)):
            d[k]=mydict2[k]

# write python dict to a file
output = open(sys.argv[3], 'wb')
pickle.dump(d, output)
output.close()
