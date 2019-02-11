rem ******************************************************
rem *
rem * Name:         install.bat
rem *     
rem * Design Phase:
rem *     Author:   John Miner
rem *     Date:     07-31-2014
rem *     Purpose:  Install the maintenance programs.
rem * 
rem ******************************************************


cd c:\temp

sqlcmd -S (local) -d msdb -E -i ola-hallengren-script.sql

sqlcmd -S (local) -d msdb -E -i usp_maintain_msdb.sql

sqlcmd -S (local) -d msdb -E -i usp_monitor_dbsize.sql

sqlcmd -S (local) -d msdb -E -i usp_monitor_vlfs.sql

sqlcmd -S (local) -d msdb -E -i job-monitor-server-database-size.sql

sqlcmd -S (local) -d msdb -E -i job-monitor-server-vlf-fragmentation.sql

sqlcmd -S (local) -d msdb -E -i job-msdb-database-cleanup-history.sql

sqlcmd -S (local) -d msdb -E -i job-system-databases-check-integrity.sql

sqlcmd -S (local) -d msdb -E -i job-system-databases-full-backup.sql

sqlcmd -S (local) -d msdb -E -i job-system-databases-log-backup.sql

sqlcmd -S (local) -d msdb -E -i job-system-databases-optimize-indexes.sql

sqlcmd -S (local) -d msdb -E -i job-user-databases-check-integrity.sql

sqlcmd -S (local) -d msdb -E -i job-user-databases-full-backup.sql

sqlcmd -S (local) -d msdb -E -i job-user-databases-log-backup.sql

sqlcmd -S (local) -d msdb -E -i job-user-databases-optimize-indexes.sql




