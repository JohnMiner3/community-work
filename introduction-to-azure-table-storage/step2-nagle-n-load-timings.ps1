# ******************************************************
# *
# * Name:         step2-nagle-n-load-timings.ps1
# *     
# * Design Phase:
# *     Author:   John Miner
# *     Date:     04-01-2018
# *     Purpose:  Sample use of Azure Table Storage.
# * 
# ******************************************************

#
# Custom-Log-Object() - Custom stop watch / log object
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
# Sample call
#

# Call at start of event
#$Start = Get-Date -Format o

# Call at both start and stop
#Custom-Log-Object -Action 'Start' -FileName 'John' -RecCount '314' -TimeStamp $Start



#
# Load-Stocks-2-TableStorage() - parse stock files into Azure table storage rows
#

function Load-Stocks-2-TableStorage {
    [CmdletBinding()] 
    param(
        [Parameter(Mandatory = $true)]
        [String] $ResourceGrp,

		[Parameter(Mandatory = $true)]
        [String] $StorageAct,

		[Parameter(Mandatory = $true)]
        [String] $StorageTbl,

		[Parameter(Mandatory = $true)]
        [String] $RootPath,

		[Parameter(Mandatory = $true)]
        [String] $FileExt,

		[Parameter(Mandatory = $true)]
        [String] $FileStart	
    )
	
    # Optional message
    Write-Verbose "Start - Load-Stocks-2-TableStorage()"
		
    # 1.0 - Get list of files
    $List = @()
    Get-ChildItem $RootPath -recurse | Where-Object {$_.extension -eq "$FileExt" -and $_.Name -match "^$FileStart.*"} | Foreach-Object {
        $List += $_.FullName
    }

	# 2.0 - Grab storage table
    $Context = Get-AzureRmStorageAccount -Name $StorageAct -ResourceGroupName $ResourceGrp
	$Table =  $Context | Get-AzureStorageTable –Name $StorageTbl

    # Capture total time
    $Time2 = @()

    # Debugging - load just 2 files
    $List = $List | Select-Object -First 3
    # $List = $List | Select-Object -Last 2
     
    # 3.0 - For each file
    $List | Foreach-Object {
    
    # Initialize counter
    $Cnt = 0

	# Grab file name
    $FileName = $_
	
    # 3.1 - Mark start ~ File process
    $Start = Get-Date -Format o
    $Time2 += Custom-Log-Object -Action 'Start Processing' -FileName "$FileName" -RecCnt "$Cnt" -TimeStamp $Start

	# Optional message
    Write-Verbose "File - $FileName"

    # 3.2 Read in csv file
    $Data = Import-Csv $FileName 

    # 3.3 - For each row
    $Data | Foreach-Object {
    
        # Row Details - Hash table from psobject
        $RowData = @{"symbol"=[string]$_.symbol; 
           "date" = [datetime]$_.date; 
           "open" = [float]$_.open;
           "high" = [float]$_.high;
           "low" = [float]$_.low;
           "close" = [float]$_.close;
           "adjclose" = [float]$_.adjclose;
           "volume" = [long]$_."volume"}


        # Partition Key - First letter in stock symbol
        $First = (Split-Path -Path $FileName -Leaf).Substring(0, 1)
        $PartKey = $First + "-STOCKS"

        # Row key - symbol + date
        $RowKey = $_.symbol + "-" + $_.date  
        $RowKey = $RowKey -replace "/", ""


        # Ignore dividend zero records
        If ($_.open -ne 0 -and $_.close -ne 0)
        {
            # Add to table storage
            Add-StorageTableRow -table $Table -partitionKey $PartKey -rowKey $RowKey -property $RowData | Out-Null
        }

        # Increment counter
        $Cnt += 1
        
        # Show counter every 25
        If ($Cnt % 25 -eq 0) 
        {
            Write-Verbose "Processed $Cnt records."
        }

    }

    # Final record count
    Write-Verbose "Processed $Cnt records."
	Write-Verbose " "

    # 3.3 - Mark end ~ File process
    $Time2 += Custom-Log-Object -Action 'End Processing' -FileName "$FileName" -RecCnt "$Cnt" -TimeStamp $Start

    # Reset counter
    $Cnt = 0
	
    }

	# Optional message
    Write-Verbose "End Load-Stocks-2-TableStorage()"

    # Return processing data
    Write-Output ($Time2)
}


#
# Main Program
#   - load a given table partition
#


# Turn off this algorithm
#[System.Net.ServicePointManager]::UseNagleAlgorithm = $false

# Command line arg
$Letter=$args[0]

# Get saved security credentials
Import-AzureRmContext -Path "C:\COMMUNITY WORK\STAGING\ATS\azure-creds.json" 

# Load files that start with letter
$Result = Load-Stocks-2-TableStorage -ResourceGrp "rg4ts18" -StorageAct "sa4ts18" `
-StorageTbl "ts4tests" -RootPath "C:\COMMUNITY WORK\STAGING\DATA\" `
-FileExt ".csv" -FileStart $Letter -Verbose

# Export Timings
$Path = "C:\Community Work\BCC29\TS\TIMINGS\$Letter-Timings.csv"
$Result | Export-Csv $Path


