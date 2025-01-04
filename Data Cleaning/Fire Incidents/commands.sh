#!/bin/bash


# Create directory and upload data to hdfs
hadoop fs -mkdir project
hadoop fs -put Fire_Incidents_20241117.csv project

# Compile Java code to get .class files and create the jar file
javac -classpath `hadoop classpath` *.java
jar cvf datacleaner.jar *.class

# Execute the map-reduce job for cleaning unnecessary columns
hadoop jar datacleaner.jar FireIncidentCleaning project/Fire_Incidents_20241117.csv project/output

# Get the output file from hdfs
hadoop fs -get project/output/part-r-00000

# Put the output file in hdfs
hadoop fs -put part-r-00000 project/cleaned_columns


# Execute the map-reduce job for cleaning unnecessary rows
javac -classpath `hadoop classpath` *.java
jar cvf datefiltering.jar *.class
hadoop jar datefiltering.jar FireIncidentDateFiltering project/cleaned_columns project/output2

# Get the output file from hdfs
hadoop fs -getmerge project/output2 project/finaldatasetcleanedrows.csv

# Put the output file in hdfs
hadoop fs -put finaldatasetcleanedrows.csv project/finaldatasetcleanedrows.csv


# Injuries / Fatalities Profiling
javac -classpath `hadoop classpath` *.java
jar cvf profiling.jar *.class
hadoop jar profiling.jar FireIncidentsInjuredDeathProfiling project/finaldatasetcleanedrows.csv project/output3

# Get the output file from hdfs
hadoop fs -get project/output3 project/injuries_fatalities_profiling


# Time Profiling
javac -classpath `hadoop classpath` *.java
jar cvf timeprofiling.jar *.class
hadoop jar timeprofiling.jar FireIncidentTimeProfiling project/finaldatasetcleanedrows.csv project/output4

# Get the output file from hdfs
hadoop fs -get project/output4 project/time_profiling_output