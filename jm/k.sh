#!/bin/bash

process_list=`ps -ef | grep $1 | grep $2 | grep $3 | grep python | grep -v grep | grep -v $0 | awk '{print $2}'`
for p in $process_list; do
        echo kill -s 9 $p
        kill -s 9 $p
done;
