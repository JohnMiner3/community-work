Topic:

How isolated are your sessions?


Abstract:

Have you ever executed a T-SQL program that crashed due to an integrity error? Upon inspecting the job history, the error code states you have duplicate key values. However; re-running the job does not reproduce the error. You probably had an transaction isolation level issue without knowing it.

I will be covering the following topics in this presentation.


Coverage:

1 – Maintaining the ACID quality of transactions.
2 – How SQL Server implements transaction durability?
3 – System versus User transactions
4 – Transaction basics
5 – Exploring the various transaction modes
6 – Exclusive versus Shared locks
7 – Blocking versus Deadlocks
8 – How to detect them with my free code.
9 – How Isolation levels affect transaction behavior.
10 – What is a dirty read versus a phantom read?

At the end of the talk, you will know how to fix the above scenario by changing the isolation level.


Details:

presentation bundle - included