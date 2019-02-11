# ******************************************************
# *
# * Name:         step4-sample-crud-operations.ps1
# *     
# * Design Phase:
# *     Author:   John Miner
# *     Date:     04-01-2018
# *     Purpose:  Sample crud operations.
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



#
# Custom-Log-Object - Custom stop watch 
#

function Custom-Log-Object {
    [CmdletBinding()] 
    param(
        [Parameter(Mandatory = $true)]
        [String] $Action,

        [Parameter(Mandatory = $true)]
        [String] $FileName,

        [Parameter(Mandatory = $true)]
        [String] $RecCnt,

        [Parameter(Mandatory = $true)]
        [String] $TimeStamp
    )

    # Create time span object
    $Span = New-TimeSpan -Start $TimeStamp

    # Create properties
    $Properties = @{'Action'="$Action"; 'File' = "$FileName"; 'Recs'="$RecCnt";'Total'=$Span.TotalMilliseconds}

    # Create object
    $Object = New-Object –TypeName PSObject –Prop $Properties

    # Return object
    Return $Object
}



#
# Test 1 - Select by column
#

# Capture total time
$Time2 = @()

# Mark start ~ query
$Start = Get-Date -Format o
$Time2 += Custom-Log-Object -Action 'Start Select By Column' -FileName "GE" -RecCnt "0" -TimeStamp $Start

# Get objects
$StorageContext = Get-AzureRmStorageAccount -Name 'sa4ts18' -ResourceGroupName "rg4ts18" 
$StorageTable = $StorageContext | Get-AzureStorageTable –Name "ts4tests"

# Just msft rows
$DataSet = Get-AzureStorageTableRowByColumnName -table $StorageTable `
    -columnName "symbol" `
    -value "GE" `
    -operator Equal

# Data set size
$Recs = $DataSet.Length

# Mark end ~ query
$Time2 += Custom-Log-Object -Action 'Start Select By Column' -FileName "GE" -RecCnt "$Recs" -TimeStamp $Start

# Timing
$Time2

# Show the data set
$DataSet



#
# Test 2 - Select by partition
#


# Capture total time
$Time2 = @()

# Mark start ~ query
$Start = Get-Date -Format o
$Time2 += Custom-Log-Object -Action 'Start Select By Partition Key' -FileName "G-STOCKS" -RecCnt "0" -TimeStamp $Start

# Get objects
$StorageContext = Get-AzureRmStorageAccount -Name 'sa4ts18' -ResourceGroupName "rg4ts18" 
$StorageTable = $StorageContext | Get-AzureStorageTable –Name "ts4tests"

# Just msft rows
$DataSet = Get-AzureStorageTableRowByPartitionKey -table $StorageTable -partitionKey "G-STOCKS"

# Data set size
$Recs = $DataSet.Length

# Mark end ~ query
$Time2 += Custom-Log-Object -Action 'Start Select By Partition Key' -FileName "G-STOCKS" -RecCnt "$Recs" -TimeStamp $Start

# Timing
$Time2

# Show the data set
$DataSet



#
# Test 3 - Select by row key
#

# Capture total time
$Time2 = @()

# Mark start ~ query
$Start = Get-Date -Format o
$Time2 += Custom-Log-Object -Action 'Start Select By RowKey' -FileName "GE" -RecCnt "0" -TimeStamp $Start

# Get objects
$StorageContext = Get-AzureRmStorageAccount -Name 'sa4ts18' -ResourceGroupName "rg4ts18" 
$StorageTable = $StorageContext | Get-AzureStorageTable –Name "ts4tests"

# Find my record
$OneRec = Get-AzureStorageTableRowByCustomFilter `
    -table $StorageTable `
    -customFilter "(RowKey eq 'GE-12182013') and (PartitionKey eq 'G-STOCKS')"
$OneRec
#    -customFilter "(PartitionKey eq 'G-STOCKS')"

# Data set size
#$Recs = $OneRec.Length
$Recs = 1

# Mark end ~ query
$Time2 += Custom-Log-Object -Action 'Start Select By RowKey' -FileName "GE" -RecCnt "$Recs" -TimeStamp $Start

# Timing
$Time2



#
# Test 4 - Update one record
#

# Capture total time
$Time2 = @()

# Mark start ~ query
$Start = Get-Date -Format o
$Time2 += Custom-Log-Object -Action 'Start Update One Rec' -FileName "GE" -RecCnt "0" -TimeStamp $Start

# Get objects
$StorageContext = Get-AzureRmStorageAccount -Name 'sa4ts18' -ResourceGroupName "rg4ts18" 
$StorageTable = $StorageContext | Get-AzureStorageTable –Name "ts4tests"

# Before update
$OneRec = Get-AzureStorageTableRowByCustomFilter `
    -table $StorageTable `
    -customFilter "(RowKey eq 'GE-10252013') and (PartitionKey eq 'G-STOCKS')"
$OneRec

# Change entity
$OneRec.low = 25.81

# Pipe entity to update cmdlet
$OneRec | Update-AzureStorageTableRow -table $StorageTable 

# Mark end ~ query
$Time2 += Custom-Log-Object -Action 'Start Update One Rec' -FileName "GE" -RecCnt "1" -TimeStamp $Start

# Timing
$Time2


# After update
$OneRec = Get-AzureStorageTableRowByCustomFilter `
    -table $StorageTable `
    -customFilter "(RowKey eq 'GE-10252013') and (PartitionKey eq 'G-STOCKS')"
$OneRec



#
# Test 5 - Delete one record
#


# Capture total time
$Time2 = @()

# Mark start ~ query
$Start = Get-Date -Format o
$Time2 += Custom-Log-Object -Action 'Start Delete One Rec' -FileName "GE" -RecCnt "0" -TimeStamp $Start

# Get objects
$StorageContext = Get-AzureRmStorageAccount -Name 'sa4ts18' -ResourceGroupName "rg4ts18" 
$StorageTable = $StorageContext | Get-AzureStorageTable –Name "ts4tests"

# Before delete
$OneRec = Get-AzureStorageTableRowByCustomFilter `
    -table $StorageTable `
    -customFilter "(RowKey eq 'GE-10252013') and (PartitionKey eq 'G-STOCKS')"
$OneRec


# Pipe entity to delete cmdlet
$OneRec | Remove-AzureStorageTableRow -table $StorageTable 


# Mark end ~ query
$Time2 += Custom-Log-Object -Action 'Start Delete One Rec' -FileName "GE" -RecCnt "1" -TimeStamp $Start

# Timing
$Time2

# After delete
$OneRec = Get-AzureStorageTableRowByCustomFilter `
    -table $StorageTable `
    -customFilter "(RowKey eq 'GE-10252013') and (PartitionKey eq 'G-STOCKS')"
$OneRec







# Grab storage context - work around for RM
$StorageContext = Get-AzureRmStorageAccount -Name 'sa4ts18' -ResourceGroupName "rg4ts18" 
$StorageTable = $StorageContext | Get-AzureStorageTable –Name "ts4invest"


# Customer record
$PartKey = 'BigJons'
$RowKey = 'C-1'
$RowData = @{'FirstName'="James"; 'LastName' = "Woods"; 'Street'="273 Chatham Circle";'City'="Warwick";'State'="RI";'ZipCode'='02886'}
Add-StorageTableRow -table $StorageTable -partitionKey $PartKey -rowKey $RowKey -property $RowData | Out-Null


# Portfolio record 1
$PartKey = 'BigJons'
$RowKey = 'P-1'
$RowData = @{'CustomerId'="C-1";'Stock'='MSFT';'Date'= "2018-02-05";'Price'=89.25;'Quantity'=250}
Add-StorageTableRow -table $StorageTable -partitionKey $PartKey -rowKey $RowKey -property $RowData | Out-Null


# Portfolio record 2
$PartKey = 'BigJons'
$RowKey = 'P-2'
$RowData = @{'CustomerId'="C-1";'Stock'='AMZN';'Date'= "2018-02-05";'Price'=1385;'Quantity'=100}
Add-StorageTableRow -table $StorageTable -partitionKey $PartKey -rowKey $RowKey -property $RowData | Out-Null


# T. Rowe Price New York Tax-Free Bond Fund

# Portfolio record 3
$PartKey = 'BigJons'
$RowKey = 'P-3'
$RowData = @{'CustomerId'="C-1";'Stock'='PRNYX';'Date'= "2018-02-05";'Price'=11.48;'Quantity'=3400}
Add-StorageTableRow -table $StorageTable -partitionKey $PartKey -rowKey $RowKey -property $RowData | Out-Null

