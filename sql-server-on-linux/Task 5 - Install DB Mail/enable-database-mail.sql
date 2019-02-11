/******************************************************
 *
 * Name:         enable-database-mail.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Blog:     www.craftydba.com
 *
 *     Version:  1.1
 *     Date:     08-01-2014
 *     Purpose:  Enable database mail (profiles & accounts).
 *
 *     Notes:    Need to update with correct email settings.
 *
 ******************************************************/

-- Select the correct database
USE [msdb]
GO


/*
   Turn on database mail
*/

-- Just shows standard options
sp_configure
GO

-- Turn on advance options
sp_configure 'show advanced options', 1;
GO

-- Reconfigure server
RECONFIGURE;
GO

-- Turn on database xp's
sp_configure 'Database Mail XPs', 1;
GO

-- Reconfigure server
RECONFIGURE
GO



/*
  Stopping and starting mail service
*/

-- See if database mail (DM) is started
EXEC msdb.dbo.sysmail_help_status_sp
GO

-- Stop the service
EXEC msdb.dbo.sysmail_stop_sp
GO

-- See if database mail (DM) is stopped
EXEC msdb.dbo.sysmail_help_status_sp
GO

-- Start the service
EXEC msdb.dbo.sysmail_start_sp
GO

-- See if database mail (DM) is started
EXEC msdb.dbo.sysmail_help_status_sp
GO



/*
   Creating two mail accounts with different SMTP servers
*/

-- not supplying @username, @password - defaults to anonymous connection

-- Create a Database Mail account 1
EXEC msdb.dbo.sysmail_add_account_sp
@account_name = 'act_Default_Email',
@description = 'Mail account for use by all database users.',
@email_address = 'Silence.Dogood@craftydba.com',
@replyto_address = 'Silence.Dogood@craftydba.com',
@display_name = 'SQL SERVER (sql4linux1)',
@mailserver_name = 'smtp.1and1.com',
@port = 587, 
@enable_ssl = 1, 
@username = 'Silence.Dogood@craftydba.com', 
@password = 'ARZ5K3xVQuE298QB'
GO

-- Show the new mail accounts
EXEC msdb.dbo.sysmail_help_account_sp;
GO



/*
   Creating a mail profile
*/

-- Create a Database Mail profile
EXEC msdb.dbo.sysmail_add_profile_sp
@profile_name = 'prf_Default_Email',
@description = 'Profile used for administrative mail.' ;
GO

-- Show the new mail profile
EXEC msdb.dbo.sysmail_help_profile_sp;
GO



/*
  Linking the mail profile to the accounts
*/

-- Add the account 1 to the profile
EXEC msdb.dbo.sysmail_add_profileaccount_sp
@profile_name = 'prf_Default_Email',
@account_name = 'act_Default_Email',
@sequence_number = 1 ;
GO

-- Show the link between profile and accounts
EXEC msdb.dbo.sysmail_help_profileaccount_sp @profile_name = 'prf_Default_Email';



/*
   Given public access to profile
*/

-- Grant access to the profile to all users in the msdb database
EXEC msdb.dbo.sysmail_add_principalprofile_sp
@profile_name = 'prf_Default_Email',
@principal_name = 'public',
@is_default = 1 ;

-- Show the new default profile
EXEC msdb.dbo.sysmail_help_principalprofile_sp



/*
   Send test message
*/

-- Plain text message
EXEC msdb.dbo.sp_send_dbmail
@profile_name = 'prf_Default_Email',
@recipients = 'jominer@microsoft.com',
@body = 'The stored procedure finished successfully.',
@subject = 'Automated Success Message' ;
GO

-- The mail queue
EXEC msdb.dbo.sysmail_help_queue_sp
GO


/*

-- Sql Agent Mail Profile (does not work)
EXEC msdb.dbo.sp_set_sqlagent_properties @email_save_in_sent_folder=1, 
        @databasemail_profile=N'prf_Default_Email', 
        @use_databasemail=1;
GO

-- Failsafe operator (does not work)
EXEC master.dbo.sp_MSsetalertinfo @failsafeoperator=N'Basic Monitoring', @notificationmethod = 1;
GO


-- Only unix cmds work
sudo /opt/mssql/bin/mssql-conf set sqlagent.databasemailprofile prf_Default_Email

systemctl restart mssql-server.service

*/