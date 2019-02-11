<# 
 ******************************************************
 *
 * Name:         Make-Azure-Storage.ps1
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     12-09-2015
 *     Purpose:  
 *               Create blob storage account and 
 *               containers for backups.
 *
 *               Send test file up azure and down
 *               to hard drive.
 * 
 ******************************************************
#>

# Load module
Import-Module Azure

# Subscription name
$SubscriptionName="Visual Studio Ultimate with MSDN"

# Storage account name
$StorageAccountName="sa4rissug"

# Data center location
$Location = "East US 2"

# Three containers please
$ContainerName = "cn4backups"

# File 2 upload
$FileToUpload = "C:\backups\input\hello-world.txt"

# Destination of download
$DestFolder = "C:\backups\output"

# Add your Azure account to the local PowerShell environment.
Add-AzureAccount

# Set a default subscription
Select-AzureSubscription -SubscriptionName $SubscriptionName –Default

# Create a new storage account
New-AzureStorageAccount –StorageAccountName $StorageAccountName -Location $Location

# Set a default storage account.
Set-AzureSubscription -CurrentStorageAccountName $StorageAccountName -SubscriptionName $SubscriptionName

# Create a new container.
New-AzureStorageContainer -Name $ContainerName -Permission Off

# Upload a blob into a container.
Set-AzureStorageBlobContent -Container $ContainerName -File $FileToUpload

# List all blobs in a container.
Get-AzureStorageBlob -Container $ContainerName

# Download blobs from the container:
# Get a reference to a list of all blobs in a container.
$blobs = Get-AzureStorageBlob -Container $ContainerName

# Create the destination directory.
# New-Item -Path $DestFolder -ItemType Directory -Force  

# Download blobs into the local destination directory.
$blobs | Get-AzureStorageBlobContent –Destination $DestFolder


<#

# Show accounts
Get-AzureAccount

# Must have a subscription
Get-AzureSubscription

# Show locations
Get-AzureLocation | Where-Object {$_.Name -like "East US*"}

# Show storage
Get-AzureStorageAccount | Where-Object {$_.Label -like "sa4rissug*"}

#>
