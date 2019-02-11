Topic:

Full Text Indexing Basics


Abstract:

Today’s large data fields (LDF) are full of unstructured information stored in varchar, text, varbinary or xml data types. How do you write an application to search the column for patterns? Traditional SQL techniques using a column INDEX and LIKE operator result in a query plan that contains a full table scan.

In this presentation, I will be introducing the brother’s grimm database that has the full text of each fairy tale. I will create a full text catalog / index, select a change tracking strategy, define optional stop list / thesaurus file, and then populate the index. I will use CONTAINS and FREETEXT operators in SELECT queries to leverage the FTI. This resulting query plan performs index seek.


Coverage:

1 – Creating a database from scratch.
2 – Creating a table with LOB field.
3 – Loading files via BULK INSERT.
4 – Performance via traditional techniques.
5 – Creating a full text index.
6 – Performance with the full text index.


Details:

presentation bundle - Included

 