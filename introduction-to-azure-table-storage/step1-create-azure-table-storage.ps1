# ******************************************************
# *
# * Name:         step1-create-azure-table-storage.ps1
# *     
# * Design Phase:
# *     Author:   John Miner
# *     Date:     04-01-2018
# *     Purpose:  Sample use of Azure Table Storage.
# * 
# ******************************************************


#
# Required Azure Module
#

# Make sure we have the module
Install-Module AzureRmStorageTable

# List cmdlets
Get-Command | Where-Object { $_.Source -match 'Storage' -and $_.Name -match 'StorageTableRow' } 



#
# Azure Subscriptions 
#

# Prompts you for azure credentials
#Add-AzureRmAccount

# Clear the screen
#Clear-Host

# List my subscriptions
#Get-AzureRmSubscription

# Save security credentials
#Save-AzureRmContext -Path "C:\COMMUNITY WORK\STAGING\ATS\AZURE-CREDS.JSON" -Force

# Import security credentials
Import-AzureRmContext -Path "C:\COMMUNITY WORK\STAGING\ATS\AZURE-CREDS.JSON"



#
# Create a resource group
#

# New resource group
# New-AzureRmResourceGroup -Name "rg4ts18" -Location "East US 2"

# Clear the screen
Clear-Host

# List resource groups
Get-AzureRmResourceGroup -Name "rg4ts18"

# Delete resource group
# Remove-AzureRmResourceGroup -Name "rg4ts18" -Force



#
# Data Centers with my service
#

# Clear the screen
Clear-Host

# White space
Write-Host ""

# Data centers with table storage
$TableStorageLocations = (Get-AzureRmResourceProvider -ListAvailable | 
    Where-Object {$_.ResourceTypes.ResourceTypeName -eq 'storageAccounts/tableServices'}).Locations 

# Us locations
$TableStorageLocations |where {$_ -match " US"}

# White space
Write-Host ""



#
# Create a storage account
#

# Create new storage account (lowercase)
New-AzureRmStorageAccount –StorageAccountName 'sa4ts18' -ResourceGroupName "rg4ts18" -Location "East US 2" -Type "Standard_GRS"

# Clear the screen
Clear-Host


# White space
Write-Host ""

# Show the account
$List = Get-AzureRmStorageAccount | Select-Object ResourceGroupName, StorageAccountName, Location
$List 

# White space
Write-Host ""


# Delete storage account
#Remove-AzureRmStorageAccount -ResourceGroupName "rg4ts18" -Name 'sa4ts18' 



#
# Create storage table - 4 stocks
#

# Grab storage context - work around for RM
$StorageContext = Get-AzureRmStorageAccount -Name 'sa4ts18' -ResourceGroupName "rg4ts18" 

# Create a storage table
#$StorageContext | New-AzureStorageTable –Name "ts4stocks"


# White space
Write-Host ""

# List the storage table
$StorageTable = $StorageContext | Get-AzureStorageTable –Name "ts4stocks"
$StorageTable

# White space
Write-Host ""

# Remove the storage table
# $StorageContext | Remove-AzureStorageTable –Name "ts4stocks"



#
# Create storage table - tests
#

# Grab storage context - work around for RM
$StorageContext = Get-AzureRmStorageAccount -Name 'sa4ts18' -ResourceGroupName "rg4ts18" 

# Create a storage table
$StorageContext | New-AzureStorageTable –Name "ts4tests"


# White space
Write-Host ""

# List the storage table
$StorageTable = $StorageContext | Get-AzureStorageTable –Name "ts4tests"
$StorageTable

# White space
Write-Host ""


#
# Create storage table - Big Jon's Investments
#

# Grab storage context - work around for RM
$StorageContext = Get-AzureRmStorageAccount -Name 'sa4ts18' -ResourceGroupName "rg4ts18" 

# Create a storage table
$StorageContext | New-AzureStorageTable –Name "ts4invest"


# White space
Write-Host ""

# List the storage table
$StorageTable = $StorageContext | Get-AzureStorageTable –Name "ts4invest"
$StorageTable

# White space
Write-Host ""