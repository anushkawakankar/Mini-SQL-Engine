# Mini SQL Engine

An SQL engine using Python3.

## Dataset

1. csv files for tables.
a. If a file is : ​ File1.csv ​ , the table name would be File1
b. There will be no tab ​ separation or space ​ separation, so you are not required
to handle it but you have to make sure to take care of both csv file type
cases: the one where values are in double quotes and the one where values
are without quotes.
2. All the elements in files would be ​ only INTEGERS
3. A file named: ​ metadata.txt​ (note the extension) would be given to you which will
have the following structure for each table:
<begin_table>
<table_name>
<attribute1>
....
<attributeN>
<end_table>

## Usage

```bash
python3 engine.py "{SQL command};"
```

## Supported Commands
1) Select * from table_name;
2) Select max(col1) from table1;
3) Select col1, col2 from table_name;
4) Select distinct col1, col2 from table_name;
5) Select col1,col2 from table1,table2 where col1 = 10 AND col2 = 20;
    
   a) In the where queries, there would be a maximum of one AND/OR operator
with no NOT operators.

   b) Relational operators that are to be handled in the assignment, the operators
include "< , >, <=, >=, =".
6) Select * from table1, table2 where table1.col1=table2.col2;
7) Select col1, col2 from table1,table2 where table1.col1=table2.col2;


Pull requests are welcome. 

