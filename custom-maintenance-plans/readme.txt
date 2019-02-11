Topic:

Designing Custom Maintenance Plans using the Ola Hallengren Scripts


Abstract:

Have you ever received a call from a client asking for help when their SQL Server database is corrupted? You suggest that they restore the database from the last good backup.

There is a long pause on the phone as the client states that the only backups/tapes they have are bad. Do not let this situation happen to you!

We will investigate how to build a custom maintenance plans from the ground up using Ola’s scripts as a starting point. What are the best practices for daily, weekly and monthly tasks?

This presentation includes tape rotation schemes and restoring those backups to make sure they really work. Some topics will be covered in depth while others will be given as homework.


Coverage:

1 – How to install the Ola Hallengren scripts.
2 – Identifying system versus user databases.
3 – Making a backup schedule (full, diff, log) based on size and business requirements.
4 – Various backup options (verify, checksum, compression, cleanup, copy only, etc).
5 – Backup read/write file groups only for data warehouse systems.
6 – Advance options to speed up the backup (file striping, buffer count, max transfer size).
7 – One missing option I would like to see added is ‘MIRROR TO’.
8 – Why backup to disk then swipe to tape?
9 – Tape rotations such as ‘Tower of Hanoi’ versus ‘Grandfather-Father-Son’.
10 – Checking the integrity of your database.
11 – Intelligent Index Maintenance; Reorganize versus Rebuild.
12 – Updating statistics on your table.
13 – Using RESTORE for FILE LIST ONLY (information) and VERIFY ONLY (integrity).
14 – How do I restore my database?
15 – Testing your backups via monthly restores.
16 – What is a tail backup? When is it used?
17 – How to do a point in time recovery?
18 – What are the best practices for daily, weekly and monthly tasks?



Details:

presentation bundle - included

