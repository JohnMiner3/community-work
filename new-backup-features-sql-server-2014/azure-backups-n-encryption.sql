/******************************************************
 *
 * Name:         azure-backups-n-encryption.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     12-09-2015
 *     Purpose:  New options for backup up 2 the cloud
 *               and encrypting backups.
 * 
 ******************************************************/

/*
    Step 1 - Create credential for azure blob storage
*/

-- Change database to full recovery
ALTER DATABASE [AdventureWorks2012] SET RECOVERY SIMPLE;
GO


-- Drop existing credential
IF EXISTS (SELECT * FROM sys.credentials WHERE name = 'ci_AzureBackups')
DROP CREDENTIAL ci_AzureBackups
GO


-- Create new credential
CREATE CREDENTIAL ci_AzureBackups 
WITH IDENTITY = 'sa4rissug',
SECRET = 'gLv40oINQNZuHXNEqhT8BHJTfW5oSIry7yFsXd6Z+0oAkvW1jTenStuvHUEMCAyoaIQtMZAC7Y40sHUv05k/4w==' ;
GO


/*
    Step 2 - Create full backup, verify backup, and read header info
*/

-- Create a full backup
BACKUP DATABASE [AdventureWorks2012]
    TO URL = 'https://sa4rissug.blob.core.windows.net/cn4backups/AdventureWorks2012-A.bak' 
    WITH CREDENTIAL = 'ci_AzureBackups' 
  , COMPRESSION
  , STATS = 5;
GO 


-- Get a file listing
RESTORE FILELISTONLY 
    FROM URL = 'https://sa4rissug.blob.core.windows.net/cn4backups/AdventureWorks2012-A.bak' 
    WITH CREDENTIAL = 'ci_AzureBackups' ;


-- Verify the backup
RESTORE VERIFYONLY 
    FROM URL = 'https://sa4rissug.blob.core.windows.net/cn4backups/AdventureWorks2012-A.bak' 
    WITH CREDENTIAL = 'ci_AzureBackups' ;
	


/* 
    Step 3 - Add data, perform full & log backup
*/

-- Change database to full recovery
ALTER DATABASE [AdventureWorks2012] SET RECOVERY FULL;
GO

-- Change some data
INSERT INTO [AdventureWorks2012].[dbo].[ErrorLog] 
VALUES (GETDATE(), SUSER_NAME(), 16, 1, 1, 'Azure Backup Script', 7, 'Some Message');
GO 500


-- Create a full backup
BACKUP DATABASE [AdventureWorks2012]
    TO URL = 'https://sa4rissug.blob.core.windows.net/cn4backups/AdventureWorks2012-B.bak' 
    WITH CREDENTIAL = 'ci_AzureBackups' 
  , COMPRESSION
  , STATS = 5;
GO 

-- Create a log backup
BACKUP LOG [AdventureWorks2012]
    TO URL = 'https://sa4rissug.blob.core.windows.net/cn4backups/AdventureWorks2012-B.trn' 
    WITH CREDENTIAL = 'ci_AzureBackups' 
  , COMPRESSION
  , STATS = 5;
GO 


/* 
    Step 4 - Restore with standby
*/

-- Restore as new database
RESTORE DATABASE TestRestore 
    FROM URL = 'https://sa4rissug.blob.core.windows.net/cn4backups/AdventureWorks2012-B.bak' 
    WITH MOVE 'AdventureWorks2012_data' to 'C:\MSSQL\DATA\TestRestore.mdf'
  , MOVE 'AdventureWorks2012_log' to 'C:\MSSQL\LOG\TestRestore.ldf'
  , STANDBY = 'C:\MSSQL\DATA\StandBy4TestRestore.bak'
  , REPLACE
  , STATS = 5
  , CREDENTIAL = 'ci_AzureBackups';
GO 

-- Apply log file
RESTORE LOG [TestRestore]
 FROM URL = 'https://sa4rissug.blob.core.windows.net/cn4backups/AdventureWorks2012-B.trn' 
 WITH RECOVERY,
 CREDENTIAL = 'ci_AzureBackups';
GO


/* 
    Step 5 - Encrypt the backups
*/


-- Main catalog
USE MASTER
GO

-- Save service master key
BACKUP SERVICE MASTER KEY
TO FILE = 'C:\MSSQL\KEYS\service_master_key.key'
ENCRYPTION BY PASSWORD = 'a$tr0n9#!P@$$w0r2_f0rDBb@ckupEncryption';
GO

-- Create master key
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'tsfDQuIFEGSk3VrJ';
GO

-- Save master key
BACKUP MASTER KEY
TO FILE = 'C:\MSSQL\KEYS\master_key.key'
ENCRYPTION BY PASSWORD = 'a$tr0n9#!P@$$w0r2_f0rDBb@ckupEncryption';
GO

-- Location of keys
select * from sys.symmetric_keys 


-- SUBJECT can not have special characters!!

-- Create certificate
CREATE CERTIFICATE BackupEncryption2014
WITH SUBJECT = 'SQL Server 2014 Backup Encryption Certificate';
GO

-- Show certificates
select * from sys.certificates


-- Save certificate to disk
BACKUP CERTIFICATE BackupEncryption2014
TO FILE = 'C:\MSSQL\KEYS\backup_encryption.cer'
WITH PRIVATE KEY
(
    FILE = 'C:\MSSQL\KEYS\private_key.key'
  , ENCRYPTION BY PASSWORD = 'a$tr0n9#!P@$$w0r2_f0rDBb@ckupEncryption'

);
GO


-- Use the cert when backing up
BACKUP DATABASE [NORTHWIND]
TO DISK = N'C:\MSSQL\Backup\Northwind.bak'
WITH
  COMPRESSION,
  ENCRYPTION 
   (
   ALGORITHM = AES_256,
   SERVER CERTIFICATE = BackupEncryption2014
   ),
  STATS = 10
GO

-- Get a file listing
RESTORE FILELISTONLY 
    FROM DISK = N'C:\MSSQL\Backup\Northwind.bak';

-- Remove certificate
DROP CERTIFICATE BackupEncryption2014;

-- Restore the certificate
CREATE CERTIFICATE BackupEncryption2014 
    FROM FILE ='C:\MSSQL\KEYS\backup_encryption.cer'
    WITH PRIVATE KEY
    (
	   FILE='C:\MSSQL\KEYS\private_key.key', 
       DECRYPTION BY PASSWORD='a$tr0n9#!P@$$w0r2_f0rDBb@ckupEncryption'
	);


/*

--
-- Clean up before presenting
--

-- Remove backup certificate
DROP CERTIFICATE BackupEncryption2014;

-- Remove test data
TRUNCATE TABLE [AdventureWorks2012].[dbo].[ErrorLog] 


*/