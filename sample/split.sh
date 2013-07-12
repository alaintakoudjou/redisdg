#!/bin/bash
# 
# Part of essaiMapReduce1.py
#
# Split the $1 file into 4 parts
#
if [ -f "$1" ]
then
    taille=`wc -l $1 | cut -d ' ' -f 1`
    taille=$((taille / 3))
    echo "$1 is split into chuncks of $taille lines"
    split -l $taille $1 /tmp/INPUT
else
    echo "Program \($1\) not found"
fi