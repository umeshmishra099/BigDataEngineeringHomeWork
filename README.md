# For Emp.txt file cleansing
* Import as maven project
* run SparkFileCleansing
* Cleansed record will be available in resources/output/part-00000-6f48bd71-4710-4fef-8af8-d583a6723aa1-c000.csv
* Failed record will be available in resources/quar/part-00000


# For Spark sql queries
* run SparkSQL
* All results will be available on console as below. 
  * (1. For sql statements to insert data in table refer resources/sql/statements.txt )
  * (2. For queries refer resources/sql/query.txt )
* Write a query and compute average salary (sal) of employees distributed by location (loc). Output shouldn't show any locations which don't have any employees.
```
+--------+------------------+
|     loc|          avg(sal)|
+--------+------------------+
|  DALLAS|            2175.0|
| CHICAGO|1566.6666666666667|
|NEW YORK|2916.6666666666665|
+--------+------------------+
```

* Write a query and compute average salary (sal) of employees located in NEW YORK excluding PRESIDENT
```
+--------+--------+
|     loc|avg(sal)|
+--------+--------+
|NEW YORK|  1875.0|
+--------+--------+
```

* Write a query and compute average salary (sal) of four most recently hired employees
```
+--------+
|avg(sal)|
+--------+
|  2100.0|
+--------+
```

* Write a query and compute minimum salary paid for different kinds of jobs in DALLAS
```
+-------+--------+
|    job|min(sal)|
+-------+--------+
|ANALYST|    3000|
|  CLERK|     800|
|MANAGER|    2975|
+-------+--------+
```
