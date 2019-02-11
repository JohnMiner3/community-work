# ******************************************************
# *
# * Name:         step06-create-azure-sql-dw.ps1
# *     
# * Design Phase:
# *     Author:   John Miner
# *     Date:     08-08-2017
# *     Purpose:  Create azure sql database dw 
# *               using resource manager.
# * 
# ******************************************************


#
# Azure Subscriptions 
#

# Clear the screen
Clear-Host

# Import security credentials
Import-AzureRmContext -Path "C:\COMMUNITY WORK\BCC30\STAGING\ATS\AZURE-CREDS.JSON" 


#
# Data Centers with my service
#

# Clear the screen
Clear-Host

# Data centers with Azure SQL database
$AzureSQLLocations = (Get-AzureRmResourceProvider -ListAvailable | 
    Where-Object {$_.ProviderNamespace -eq 'Microsoft.Sql'}).Locations
$AzureSQLLocations




#
# Create a resource group
#

# Clear the screen
Clear-Host

# List resource groups
Get-AzureRmResourceGroup -Name "rg4stg4bcc30"



#
# Create a new server
#

# Clear the screen
Clear-Host

# List Sql Servers
Get-AzureRmSqlServer -ResourceGroupName "rg4stg4bcc30"



#
# Add laptop ip to firewall (todo = code to figure out ip since I am behind a router)
#

# Clear the screen
Clear-Host

# List firewall rules
Get-AzureRmSqlServerFirewallRule -ResourceGroupName "rg4stg4bcc30" -ServerName "svr4stocks"



#
# Make the new database
#

# Give database size quota
$TenTb = (1024 * 1024 * 1024 * 1024 * 10)
$TenTb

# Create a new database
New-AzureRmSqlDatabase -RequestedServiceObjectiveName "DW100" -DatabaseName "dw4stocks" `
    -ServerName "svr4stocks" -ResourceGroupName "rg4stg4bcc30" -Edition "DataWarehouse" `
    -CollationName "SQL_Latin1_General_CP1_CI_AS" -MaxSizeBytes $TenTb

# Clear the screen
Clear-Host

# List the new database
Get-AzureRmSqlDatabase -ResourceGroupName "rg4stg4bcc30" -ServerName "dw4stocks"



#
# Change the database size (DW1000)
#

# Log into Azure
Login-AzureRmAccount

# Clear the screen
Clear-Host

# Give database size quota
Set-AzureRmSqlDatabase -ResourceGroupName "rg4stg4bcc30" -ServerName "svr4adf18"  `
  -DatabaseName "dw4stocks" -Edition "DataWarehouse" -RequestedServiceObjectiveName "DW1000"



#
# Pause the compute charges
#


$dbinfo = Get-AzureRmSqlDatabase –ResourceGroupName "rg4stg4bcc30" –ServerName "svr4adf18" –DatabaseName "dw4stocks"
$result = $dbinfo | Suspend-AzureRmSqlDatabase
$result



#
# Resume the compute charges
#


$dbinfo = Get-AzureRmSqlDatabase –ResourceGroupName "rg4stg4bcc30" –ServerName "svr4adf18" –DatabaseName "dw4stocks"
$result = $dbinfo | Resume-AzureRmSqlDatabase
$result