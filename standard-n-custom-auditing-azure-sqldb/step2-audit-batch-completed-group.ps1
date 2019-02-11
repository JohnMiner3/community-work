# ******************************************************
# *
# * Name:         step2-audit-batch-completed-group.ps1
# *     
# * Design Phase:
# *     Author:   John Miner
# *     Date:     11-29-2017
# *     Purpose:  Create blob storage and define a
# *               database audit
# * 
# ******************************************************

#
# Azure Subscriptions 
#

# Prompts you for azure credentials
# Add-AzureRmAccount

# Clear the screen
# Clear-Host

# Save security credentials
#Save-AzureRmContext -Path "C:\MSSQLTIPS\MINER2018\ARTICLE-2018-09-BATCH-AUTOMATION\AZURE-CREDS.JSON" -Force

# Import security credentials
Import-AzureRmContext -Path "C:\MSSQLTIPS\MINER2018\ARTICLE-2018-09-BATCH-AUTOMATION\AZURE-CREDS.JSON" 


#
# Create a storage account
#

# Create new storage account 
<#

New-AzureRmStorageAccount –StorageAccountName "sa4tips18" `
  -ResourceGroupName "rg4tips18"  -Location "East US" `
  -Type "Standard_LRS"

#> 

# Show the account
Get-AzureRmStorageAccount -ResourceGroupName "rg4tips18"

# Delete storage account
# Remove-AzureRmStorageAccount -ResourceGroupName "rg4tips18" -Name "sa4tips18"


#
# View current database auditing
#

# Clear the screen
Clear-Host

# Show database auditing
Get-AzureRmSqlDatabaseAuditing `
  -ServerName "svr4tips18" `
  -DatabaseName "db4hippa" `
  -ResourceGroupName "rg4tips18"


#
# Enable database auditing
#

# Clear the screen
Clear-Host

# Enable auditing (has to be Generic storage)
Set-AzureRmSqlDatabaseAuditing `
    -State Enabled `
    -ResourceGroupName "rg4tips18" `
    -ServerName "svr4tips18" `
    -StorageAccountName "sa4tips18a" `
    -DatabaseName "db4hippa"


#
# Disable database auditing
#

# Clear the screen
Clear-Host

# Enable auditing (has to be Generic storage)
Set-AzureRmSqlDatabaseAuditing `
    -State Disable `
    -ResourceGroupName "rg4tips18" `
    -ServerName "svr4tips18" `
    -StorageAccountName "sa4tips18a" `
    -DatabaseName "db4hippa"


# Remove database auditing
Remove-AzureRmSqlDatabaseAuditing `
    -ResourceGroupName "rg4tips18" `
    -ServerName "svr4tips18" `
    -StorageAccountName "sa4tips18a" `
    -DatabaseName "db4hippa"
