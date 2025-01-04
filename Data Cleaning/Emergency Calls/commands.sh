vi FilterColumns.java
vi FilterColumnsMapper.java
vi FilterColumnsReducer.java
javac -classpath `hadoop classpath` *.java
jar cvf filterColumns.jar *.class
vi FilterAndTransform.java
vi FilterAndTransformMapper.java
vi FilterAndTransformReducer.java
javac -classpath `hadoop classpath` FilterAndTransform*
jar cvf filterAndTransform.jar FilterAndTransform*.class
vi ValueCounts.java
vi ValueCountsMapper.java
vi ValueCountsReducer.java
javac -classpath `hadoop classpath` ValueCounts*
jar cvf valueCounts.jar ValueCounts*.class
hadoop fs -mkdir rbda_project
hadoop distcp gs://nyu-dataproc-hdfs-ingest/fire_data.csv /user/ag9563_nyu_edu/rbda_project
hadoop jar filterColumns.jar FilterColumns rbda_project/fire_data.csv rbda_project/output
hadoop fs -mv rbda_project/output/part-r-00001 rbda_project/filtered_columns_data.csv
hadoop fs -rm -r rbda_project/output
hadoop jar filterAndTransform.jar FilterAndTransform rbda_project/filtered_columns_data.csv rbda_project/output
hadoop fs -cat rbda_project/output/part-r-00000 rbda_project/output/part-r-00001 > data.csv
mv data.csv filtered_transformed_data.csv
hadoop fs -put filtered_transformed_data.csv rbda_project
hadoop fs -rm -r rbda_project/output
hadoop jar valueCounts.jar ValueCounts rbda_project/filtered_transformed_data.csv rbda_project/call_type 'Call Type'
hadoop fs -cat rbda_project/call_type/part-r-00000









