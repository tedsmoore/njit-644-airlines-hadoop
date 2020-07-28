#!/bin/bash

yarn jar njit-644-airlines.jar TaxiTime /$1 /unsort_temp
yarn jar njit-644-airlines.jar SortDescending /unsort_temp /$2
hdfs dfs -rm -r /unsort_temp
