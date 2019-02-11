# ******************************************************
# *
# * Name:         step03-create-azure-sql-db.ps1
# *     
# * Design Phase:
# *     Author:   John Miner
# *     Date:     04-01-2018
# *     Purpose:  Create azure sql database using resource manager.
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

# Create a credential
$User = "jminer"
$Pword = ConvertTo-SecureString –String 'Bm#StG?2018' –AsPlainText -Force
$Credential = New-Object –TypeName System.Management.Automation.PSCredential –ArgumentList $User, $Pword

# New sql server
New-AzureRmSqlServer -ResourceGroupName "rg4stg4bcc30" -ServerName "svr4stocks" -Location "East US" -SqlAdministratorCredentials $Credential

# Clear the screen
Clear-Host

# List Sql Servers
Get-AzureRmSqlServer -ResourceGroupName "rg4stg4bcc30"

# Remove the sql server
#Remove-AzureRmSqlServer -ResourceGroupName "rg4stg4bcc30" -ServerName "svr4stocks" 



#
# Add laptop ip to firewall (todo = code to figure out ip since I am behind a router)
#

# External ip
$MyIp = (Invoke-WebRequest ifconfig.me/ip).Content -replace "`n","" 
$MyIp

# Create new firewall rule
New-AzureRmSqlServerFirewallRule -ResourceGroupName "rg4stg4bcc30" -ServerName "svr4stocks" -FirewallRuleName "fr4laptop" -StartIpAddress "$MyIp" -EndIpAddress "$MyIp"

# Clear the screen
# Clear-Host

# List firewall rules
# Get-AzureRmSqlServerFirewallRule -ResourceGroupName "rg4stg4bcc30" -ServerName "svr4stocks"


# Remove firewall rule
# Remove-AzureRmSqlServerFirewallRule -ResourceGroupName "rg4stg4bcc30" -ServerName "svr4stocks" -FirewallRuleName "fr4laptop"



#
# Make the new database
#

# Create a new database
New-AzureRmSqlDatabase -ResourceGroupName "rg4stg4bcc30" -ServerName "svr4stocks" -DatabaseName "db4stocks"

# Clear the screen
# Clear-Host

# List the new database
# Get-AzureRmSqlDatabase -ResourceGroupName "rg4stg4bcc30" -ServerName "svr4stocks"


# Remove the database
# Remove-AzureRmSqlDatabase -ResourceGroupName "rg4stg4bcc30" -ServerName "svr4stocks" -DatabaseName "db4stocks"



#
# Change the database size (s0)
#

# Login-AzureRmAccount

# Clear the screen
# Clear-Host

# Give database size quota
$TwoGb = (1024 * 1024 * 1024 * 2)
Set-AzureRmSqlDatabase -ResourceGroupName "rg4stg4bcc30" -ServerName "svr4stocks" -DatabaseName "db4stocks" -Edition "Standard" -RequestedServiceObjectiveName "S0" -MaxSizeBytes $TwoGb


#
# Change the database size (b)
#

# Set-AzureRmSqlDatabase -ResourceGroupName "rg4stg4bcc30" -ServerName "svr4stocks" -DatabaseName "db4stocks" -Edition "Basic" 




