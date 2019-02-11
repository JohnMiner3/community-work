# ******************************************************
# *
# * Name:         step5-mixing-entity-properties.ps1
# *     
# * Design Phase:
# *     Author:   John Miner
# *     Date:     04-01-2018
# *     Purpose:  Different data in same partition.
# * 
# ******************************************************



#
# Azure Subscriptions 
#

# Import security credentials
Import-AzureRmContext -Path "C:\COMMUNITY WORK\STAGING\ATS\AZURE-CREDS.JSON" 



#
# Want fast http calls
#

# Turn off this algorithm
[System.Net.ServicePointManager]::UseNagleAlgorithm = $false



# Grab storage context - work around for RM
$StorageContext = Get-AzureRmStorageAccount -Name 'sa4ts18' -ResourceGroupName "rg4ts18" 
$StorageTable = $StorageContext | Get-AzureStorageTable –Name "ts4invest"


# Customer record
$PartKey = 'BigJons-PVD'
$RowKey = 'C-001'
$RowData = @{'FirstName'="James"; 'LastName' = "Woods"; 'Street'="273 Chatham Circle";'City'="Warwick";'State'="RI";'ZipCode'='02886'}
Add-StorageTableRow -table $StorageTable -partitionKey $PartKey -rowKey $RowKey -property $RowData | Out-Null


# Portfolio record 1
$PartKey = 'BigJons-PVD'
$RowKey = 'P-001'
$RowData = @{'CustomerId'="C-001";'Stock'='MSFT';'Date'= "2018-02-05";'Price'=89.25;'Quantity'=250}
Add-StorageTableRow -table $StorageTable -partitionKey $PartKey -rowKey $RowKey -property $RowData | Out-Null


# Portfolio record 2
$PartKey = 'BigJons-PVD'
$RowKey = 'P-002'
$RowData = @{'CustomerId'="C-001";'Stock'='AMZN';'Date'= "2018-02-05";'Price'=1385;'Quantity'=100}
Add-StorageTableRow -table $StorageTable -partitionKey $PartKey -rowKey $RowKey -property $RowData | Out-Null


# Portfolio record 3
$PartKey = 'BigJons-PVD'
$RowKey = 'P-003'
$RowData = @{'CustomerId'="C-001";'Stock'='PRNYX';'Date'= "2018-02-05";'Price'=11.48;'Quantity'=3400}
Add-StorageTableRow -table $StorageTable -partitionKey $PartKey -rowKey $RowKey -property $RowData | Out-Null

