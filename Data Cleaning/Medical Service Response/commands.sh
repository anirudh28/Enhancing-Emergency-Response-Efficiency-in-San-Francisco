#!/bin/bash

# Compile and run Data Cleaning
javac -classpath `hadoop classpath` *.java
jar cvf dataCleaning.jar *.class
# Create a directory in HDFS for the project
hadoop fs -mkdir rc5397

# Upload the dataset to the project directory in HDFS
hadoop fs -put datasnippet.csv rc5397

# Run code
hadoop jar dataCleaning.jar DataCleaning rc5397/datasnippet.csv rc5397/cleaned_output

hadoop jar dataCleaning.jar DataProfiling rc5397/cleaned_output/part-r-00000 rc5397/profiling_output

# See output
hadoop fs -tail rc5397/cleaned_output/part-r-00000

hadoop fs -cat rc5397/profiling_output/part-r-00000
