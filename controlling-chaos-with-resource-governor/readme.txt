Topic:

Controlling Chaos with the Resource Governor


Abstract:

Every database administrator has come across a rogue user that executes unbounded reporting queries against the OLTP database during critical business hours. This issue might show up as a sluggish system to the end users and front end applications such as point of sale system could be affected.

How can we prevent reporting activities from impacting the sales process?

One potential solution to this problem is to deploy and configure the resource governor.

The first version of the resource governor was introduced in the 2008 version of the engine. This enterprise only feature allows you to manage SQL Server resource consumption by specify limits on the amount of CPU, physical IO, and memory that incoming application request can use. There are three key concepts with this technology: a resource pool is carved out portion of the physical resources; a work group is a collection of requests of similar priorities assigned to a resource pool, and a classification function maps a new application session to a work group.

The concepts behind the resource governor seem to be quite simple; However, there are simple guidelines that will help you from getting in trouble. For instance, allocating to many resources to one work group (set of applications) might starve another work group.

At the end of this talk, you will have a firm understand of how to start using the resource governor in your own environment.


Coverage:

1 - Why use the resource governor?
2 - Architectural Overview.
3 - Resource Pools.
4 - Work Groups.
5 - Classification Function. 
6 - Monitoring work groups.


Details:

presentation bundle - included

