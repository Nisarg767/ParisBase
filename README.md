DavisBase supports only one database and one user which are used by default. User can create multiple tables in the database. Supported commands are as follows.

SHOW TABLES;                                               Display all the tables in the database.
CREATE TABLE table_name (column_name datatype);            Create a new table in the database.
INSERT INTO table_name VALUES (value1,value2,..);          Insert a new record into the table.
DELETE FROM TABLE table_name WHERE row_id = value;         Delete a record from the table whose rowid is <value>.
UPDATE table_name SET column_name = value WHERE condition; Modifies the records in the table.
CREATE INDEX ON table_name (column_name);                  Create index for the specified column in the table.
SELECT * FROM table_name;                                  Display all records in the table.
SELECT * FROM table_name WHERE column_name operator value; Display records in the table where the given condition is satisfied.
DROP TABLE table_name;                                     Remove table data and its schema.
VERSION;                                                   Show the program version.
HELP;                                                      Show this help information.
EXIT;                                                      Exit DavisBase.

All the commands are case insensitive. For demonstration, load the folder ParisBase and run the file "DavisBase.java" as a Java application.

Team Member Contribution:-

Tirth Mehta:-
Buffer.java
DavisBase.java

Nisarg Patel:-
CreateTable.java
DropTable.java

Sai Teja Guduru:-
Index.java
Table.java
UpdateTable.java

Sahil Prashant Kirpekar:-
DeleteTable.java
Page.java

Devang Vamja:-
ShowTables.java
Init.java
Insert.java
