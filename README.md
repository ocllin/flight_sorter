# MapReduce Joins
---

## Overview
This assignment introduced me to using MapReduce in Python to manipulate data on the Penn State Hadoop fileserver. 
The data was in the following format:

Destination Country | Origin Country | count
------------ | -------------- | -----
Canada | United States | 2
Mexico | United States | 5

THe goal of this assignment were to check how many ways there are of going from destination A to destination B in exactly two flights. Such as:
>2 flights from United States to Mexico and 3 flights from Mexico to Germany, giving 2x3=6 choices

Example output is provided.

## Prerequisites
1. Python3
1. MRJob package
1. Access to the proper filesystem provided by Penn State
