Topic:

Effective use of temporary tables


Abstract:

Every developer eventually comes against business logic that can’t be handled with a single simple or complex query. TSQL provides the developer with several constructs that can store temporary result sets that are passed to the next query in the script.

I will be covering these various TSQL techniques with examples using the [AdventureWorks] database as well as a toy database called AUTOS. The pros and cons of each construct will be examined. Advanced options like enabling trace flag 1118 will be explored.


Coverage:

1 - Derived tables.
2 - Local temporary tables.
3 - Global temporary tables.
4 - Table variables.
5 - User tables in [tempdb].
6 - Common table expressions (CTE).
7 - Trace flag 1118.


Details:

presentation bundle - Included