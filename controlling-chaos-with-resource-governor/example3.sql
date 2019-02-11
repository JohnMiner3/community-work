/******************************************************
 *
 * Name:         example-3.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     11-07-2017
 *     Purpose:  Create logins for 2 hospital databases.
 * 
 ******************************************************/

-- Use system database
USE [master];
GO

--
-- Create server logins - 2 logins w/weak passwords
--

-- Delete existing login.
IF  EXISTS (SELECT * FROM sys.server_principals WHERE name = N'hospital1')
DROP LOGIN [hospital1]
GO
 
-- Add new login.
CREATE LOGIN [hospital1]
   WITH PASSWORD = 'P@ssw0rd1', CHECK_POLICY = OFF;
   

-- Delete existing login.
IF  EXISTS (SELECT * FROM sys.server_principals WHERE name = N'hospital2')
DROP LOGIN [hospital2]
GO
 
-- Add new login.
CREATE LOGIN [hospital2]
   WITH PASSWORD = 'P@ssw0rd2', CHECK_POLICY = OFF;


--
-- Hospital database 1 
--

-- Which database to use?
USE [HOSPITAL1];
GO

-- Create login
CREATE USER hospital1 FOR LOGIN hospital1;
GO

-- Grant control
GRANT CONTROL ON SCHEMA::ACTIVE TO hospital1;
GO


--
-- Hospital database 2 
--

-- Which database to use?
USE [HOSPITAL2];
GO

-- Create login
CREATE USER hospital2 FOR LOGIN hospital2;
GO

-- Grant control
GRANT CONTROL ON SCHEMA::ACTIVE TO hospital2;
GO