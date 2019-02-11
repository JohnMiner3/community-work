--
-- Restore AdvWrks 2014
--

-- Verify backup contents
RESTORE VERIFYONLY 
    FROM DISK = 'C:\NE-DEV-UG\backup\AdventureWorks2014.bak';
GO

-- List backup contents
RESTORE FILELISTONLY 
    FROM DISK = 'C:\NE-DEV-UG\backup\AdventureWorks2014.bak';
GO

-- Restore and move files
RESTORE DATABASE AdventureWorks  
    FROM DISK = 'C:\NE-DEV-UG\backup\AdventureWorks2014.bak'
    WITH MOVE 'AdventureWorks2014_Data' TO 'C:\NE-DEV-UG\data\AdventureWorks2014_Data.mdf',  
    MOVE 'AdventureWorks2014_Log' TO 'C:\NE-DEV-UG\Log\AdventureWorks2014_Log.ldf',
	RECOVERY
GO 


--
-- Restore AdvWrks 2008 R2
--

-- Verify backup contents
RESTORE VERIFYONLY 
    FROM DISK = 'C:\NE-DEV-UG\backup\adventureworks2008.bak';
GO

-- List backup contents
RESTORE FILELISTONLY 
    FROM DISK = 'C:\NE-DEV-UG\backup\adventureworks2008.bak';
GO


-- Restore and move files
RESTORE DATABASE AdventureWorks2008R2 
    FROM DISK = 'C:\NE-DEV-UG\backup\adventureworks2008.bak'
    WITH MOVE 'AdventureWorks2008R2_Data' TO 'C:\NE-DEV-UG\data\AdventureWorks2008R2_Data.mdf',  
    MOVE 'AdventureWorks2008R2_Log' TO 'C:\NE-DEV-UG\Log\AdventureWorks2008R2_Log.ldf',
	RECOVERY
GO 