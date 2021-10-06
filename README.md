# Cloud Assignment
### Problem Statement:
Implementation of SQL queries and executing them using three different frameworks of parallel processing:
- Spark
- Hadoop
- Storm

Detailed Problem Statement can be found here [insert link]
<hr>

### Query Input:
We are receiving the queries from a single input text file. Following are the general query formats:

Here[INSERT QUERY FORMAT IMAGE FROM THE DOC]

Example queries: <br>
- ```SELECT * FROM users WHERE gender = "M";```
- ```SELECT * FROM users INNER JOIN rating ON users.userid = rating.userid WHERE gender = "F";```
- ```SELECT occupation, COUNT(userid) FROM users WHERE gender = "M" GROUP BY occupation HAVING COUNT(userid) > 10;```

We will be testing our code on these queries and displaying the respective outputs of all the three frameworks.
<hr>

### Approach:
There are four input tables: users, movies, zipcodes, rating.

[INSERT SCHEMA]

For Spark:

Based on the input query, we read the relevant tables from their respective queries.

For Storm:  

Storm version used- 2.2.0  
Zookeeper version used- 3.6.3  

Trident Topology is used for processing SQL queries captured in JSON objects. The topology is built on the fly depending on the operations present in the SQL JSON object.
A typical topology consists of Spouts and Bolts. A Spout is used for ingesting data into the topology while Bolts are used for processing the ingested data. Data moves along the topology in the form of Tuple stream.  

For our use case, a Spout is created for each table, i.e., 4 Spouts in total for reading data from the csv file and converting it into Tuple Stream. Bolts or Operations in Trident Topology are created for different keywords like WHERE, GROUPBY, HAVING, JOIN and SELECT. Finally, the Tuple Stream coming out of the SELECT Bolt is final output of the query and is written to the final output file along with total execution time.
