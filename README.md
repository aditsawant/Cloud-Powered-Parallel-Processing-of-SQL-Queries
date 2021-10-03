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
There are four input tables: users, movies, zipcodes, rating
