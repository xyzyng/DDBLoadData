# DDBLoadData

Load data to DynamoDB

## How to build

> $ cd DDBLoadData  
> $ mvn package  

## How to run

Generate data and send directly to DynamoDB, e.g.
> $ java -jar target/DDBLoadData-1.0-SNAPSHOT.jar -tableName DDBApp2 -primaryKey customerid -attrPrefix product -numAttr 30 -numThread 40 -numMB 200

or generate data, save a copy to local disk (in /var/tmp/DDB-app), then send to DynamoDB, e.g. 

> $ java -jar target/DDBLoadData-1.0-SNAPSHOT.jar -tableName DDBApp2 -primaryKey customerid -attrPrefix product -numAttr 30 -numThread 40 -numMB 200 -saveToDisk

"-tableName"    DynamoDB table name  
"-primaryKey"   Primary key  
"-attrPrefix"   Attribute prefix  
"-numAttr"      Number of attributes  
"-numThread"    Number of threds to generate data  
"-numMB"        Total data size in MegaBytes  
"-saveToDisk"   Save data to local disk (optional)  
