#!/usr/bin/env python

import sys
import fileinput
import pickle

if len(sys.argv) != 3:
    print "We need 3 arguments"
    exit(1)

d=dict()
# input comes from 
print "Input file: ",sys.argv[1]
print "Output file: ",sys.argv[2]
for line in fileinput.input(sys.argv[1]):
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split()
    # increase counters
    for word in words:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        if d.__contains__(word):
            d[word]=d[word]+1
            #print '%s\t%s' % (word, d[word])
        else:
            d[word]=1
# write python dict to a file
output = open(sys.argv[2], 'wb')
pickle.dump(d, output)
output.close()

