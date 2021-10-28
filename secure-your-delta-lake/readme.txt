Topic:

Securing your delta lake with table access controls


Abstract:

Many companies are starting to use spark to process big data in the cloud. How can we secure the data lake to provide the correct level of access to the end users?

In this presentation, I will cover why mounting has its advantages over AD credential pass thru when working remote storage.

For this design pattern to be functional, one must create a high concurrency cluster and enable table access control in Azure Databricks (ADB)

The remaining topics will cover creating ADB users, assigning ADB rights, creating Hive tables, creating Hive views and granting access to Hive objects.

Finally, a theoretical discussion on data encryption and masking will be covered for companies that have stringent laws on private information.

At the end of the presentation, the user will be able to create a delta lake ecosystem that has a security layer for the end users.



Hash Tags:

#spark #sql #python #azure #databricks #remote_storage #mounting #adpassthru #clusters #users #table_access #data_masking #data_encryption #speaker #john_miner