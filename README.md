# Spark-Mini-Project
Automobile post-sales report (Redesign)

# steps
1- Filter out accident incidents with make and year

   by split the line
   
   
2- Read the input data CSV file

   by sc.textFile()
   
   
3- Perform map operation

   use function map and iterate through the line
   
   
4-  Perform group aggregation to populate make and year to all the records

    by groupby.flatmap()
    
    
5- Count number of occurrence for accidents for the vehicle make and year

   1.Perform map operation
   
   
   2.Aggregate the key and count the number of records in total per key
   
     by reduceby()
     
     
6- Save the result to HDFS as text
