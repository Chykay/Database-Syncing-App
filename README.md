# Database-Syncing-App

A java application to synch two databases together using SQL Server
This code was written with java and uses microsoft SQL Server database

It is a synching application used to sync to tables in different(two) together.
In this application, i considered a scenario whereby i am synching a local datbase in a local server to an online database in an online server

The following should be noted about this application 
* It assumes that all tables with primary keys are (auto increment) or (identity)
* it uses local ID(localID) - Primary Key of records in the local table - and company organisation ID(Org_ID) on the online database to differenciate each record
* It also assumes and expects that the number of columns in the local table is equal to and same as the number of columns in the online database
