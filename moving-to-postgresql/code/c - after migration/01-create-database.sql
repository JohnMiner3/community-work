/******************************************************
 *
 * Name:         01-create-database.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     01-01-2024
 *     Purpose:  Create pubs database
 * 
 ******************************************************/


/*  
	Create a database
*/

-- delete existing database
drop database if exists "pubs" with (force);

-- add new database
create database "pubs" owner "postgres";
