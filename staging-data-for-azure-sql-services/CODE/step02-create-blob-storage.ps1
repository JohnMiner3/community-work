# ******************************************************
# *
# * Name:         step02-create-blob-storage.ps1
# *     
# * Design Phase:
# *     Author:   John Miner
# *     Date:     01-10-2018
# *     Purpose:  Create two containers and load sample
# *               stock data.  Create a shared 
# *               access signature for use later.
# * 
# ******************************************************


#
# Azure Subscriptions 
#

# Prompts you for azure credentials
#Add-AzureRmAccount

# List my subscriptions
#Get-AzureRmSubscription


# Save security credentials
#Save-AzureRmContext -Path "C:\COMMUNITY WORK\BCC30\STAGING\ATS\AZURE-CREDS.JSON" -Force

# Clear the screen
# Clear-Host

# Import security credentials
Import-AzureRmContext -Path "C:\COMMUNITY WORK\BCC30\STAGING\ATS\AZURE-CREDS.JSON"




#
# Create a resource group
#

# New resource group
New-AzureRmResourceGroup -Name "rg4stg4bcc30" -Location "East US"

# Clear the screen
Clear-Host

# List resource groups
Get-AzureRmResourceGroup -Name "rg4stg4bcc30"

# Delete resource group
# Remove-AzureRmResourceGroup -Name "rg4stg4bcc30" -Force



#
# Create a storage account
#

# Create new storage account (lowercase)
New-AzureRmStorageAccount –StorageAccountName 'sa4stg4bcc30' -ResourceGroupName "rg4stg4bcc30" -Location "East US" -Type "Standard_LRS" 

# Clear the screen
Clear-Host

# Show the account
$A = Get-AzureRmStorageAccount -ResourceGroupName "rg4stg4bcc30"
$A.Sku
$A.Type

# Delete storage account
#Remove-AzureRmStorageAccount -ResourceGroupName "rg4stg4bcc30" -Name 'sa4stg4bcc30' 



#
# Create a storage container(s)
#

# Grab storage context - work around for RM
$StorageContext = Get-AzureRmStorageAccount -Name 'sa4stg4bcc30' -ResourceGroupName "rg4stg4bcc30" 

# Create new containers
$StorageContext | New-AzureStorageContainer -Name "sc4inbox" -Permission Off
$StorageContext | New-AzureStorageContainer -Name 'sc4archive' -Permission Off

# Clear the screen
Clear-Host

# Show the container
$StorageContext | Get-AzureStorageContainer 
 
# Remove the container
# $StorageContext | Remove-AzureStorageContainer -Name "sc4inbox" -Force
# $StorageContext | Remove-AzureStorageContainer -Name 'sc4archive' -Force



#
# Upload files to azure (stocks)
#

# Grab storage context - work around for RM
$StorageContext = Get-AzureRmStorageAccount -Name 'sa4stg4bcc30' -ResourceGroupName "rg4stg4bcc30" 

# Set source path
$srcpath = "C:\COMMUNITY WORK\BCC30\STAGING\DATA\S&P-2017\"

# Set counter
$cnt = 0

# Local file path
$list = Get-ChildItem $srcpath -Filter *.csv 

# For each stock
foreach ($file in $list)
{
    # file names
    $localname = $srcpath + $file.Name
    $remotename = "NEW/" + $file.name

    # Upload file to azure
    $StorageContext | Set-AzureStorageBlobContent -Container "sc4inbox" -File $localname -Blob $remotename -Force

    # Increment count
    Write-Host $srcfull
    Write-Host $cnt
    $cnt = $cnt + 1
}



#
# Upload master stock list
#

# Grab storage context - work around for RM
$StorageContext = Get-AzureRmStorageAccount -Name 'sa4stg4bcc30' -ResourceGroupName "rg4stg4bcc30" 

# Set source path
$srcpath = "C:\COMMUNITY WORK\BCC30\STAGING\DATA\S&P-2017\"
$filename = "PACKING-LIST.TXT"

# File names
$localname = $srcpath + $filename
$remotename = "NEW/" + $filename

# Upload file to azure
$StorageContext | Set-AzureStorageBlobContent -Container "sc4inbox" -File $localname -Blob $remotename -Force



#
# List all blobs in the container
#


# List all blobs in a container.
$StorageContext | Get-AzureStorageBlob -Container "sc4inbox" 



#
# Download file from azure
#

$StorageContext | Get-AzureStorageBlob -Container "sc4inbox" -Blob "NEW\PACKING-LIST.TXT" | Get-AzureStorageBlobContent -Destination "c:\temp\"



#
# Create Shared Access Signature (SAS)
#

# Grab storage context - work around for RM
$StorageContext = Get-AzureRmStorageAccount -Name "sa4stg4bcc30" -ResourceGroupName "rg4stg4bcc30" 

# Sets up a Stored Access Policy and a Shared Access Signature for the new container  
$Policy = $StorageContext | New-AzureStorageContainerStoredAccessPolicy -Container "sc4inbox" -Policy "pol4inbox" `
  -StartTime $(Get-Date).ToUniversalTime().AddMinutes(-5) -ExpiryTime $(Get-Date).ToUniversalTime().AddYears(10) -Permission rwld

# Gets the Shared Access Signature for the policy  
$SAS = $StorageContext | New-AzureStorageContainerSASToken -name "sc4inbox" -Policy "pol4inbox" 
Write-Host 'Shared Access Signature= '$($SAS.Substring(1))''  

<#

sv=2017-07-29&sr=c&si=pol4inbox&sig=MJBICz66qeCuw%2Bq%2FQaBkOw1tSmrKuEgpFIECW3EjoMg%3D 

#>


#
# Grab key (0 | 1)
#

$StorageKey1 = `
    (Get-AzureRmStorageAccountKey `
    -ResourceGroupName "rg4stg4bcc30"  `
    -Name 'sa4stg4bcc30').Value[0]
$StorageKey1

<#

2iRgxGQZThH3Q8Kn9MK7e2rQceZ+bGF6G7GwEx+yoEMDsg37dAWn7qdi5hBGQiKvDrImE7PBopg+bE1X+5iC0A==

#>