# ******************************************************
# *
# * Name:         convert-csv-rows-2-json-data-files.ps1
# *     
# * Design Phase:
# *     Author:   John Miner
# *     Date:     02-22-2018
# *     Purpose:  Turn a few csv files into many json files.
# *     Algorithm:
# *  
# *       1 - Create sub-directory for stock symbol
# *       2 - Read in csv yearly file.
# *       3 - For each row, create a json file.
# * 
# ******************************************************

# Optional message
Write-Host "Start - Convert Csv 2 Json"

# Set global variables
$SrcPath = "C:\COMMUNITY WORK\STAGING\DATA\"
$DstPath = "C:\JSON DATA\"
$FileExt = ".CSV"

		
# Get list of files
$List = @()
Get-ChildItem $SrcPath -recurse | Where-Object { $_.extension -eq "$FileExt" } | Foreach-Object {
    $List += $_.FullName
}

# Debugging - run just first file
# $List = $List | Select-Object -First 1
  
# For each file
$List | Foreach-Object {

    # Change the path
    Set-Location -Path $DstPath
    
    # Initialize counter
    $Cnt = 0

	# Grab file name
    $FileName = $_
	
	# Optional message
    Write-Host "File - $FileName"

    # Read in csv file
    $Data = Import-Csv $FileName 

    # For each row
    $Data | Foreach-Object {
    
        # Create directory
        $path = $DstPath + $_.symbol;
        If(!(test-path $path))
        {
            New-Item -ItemType Directory -Force -Path $path
        }

        # Searchable date str
        $DateStr =  $_.date.Substring(6, 4) + '-' + $_.date.Substring(0, 2) + '-' + $_.date.Substring(3, 2);

        # New file name
        $NewFile = ".\" + $_.symbol + "\" + $_.symbol + '-' + $DateStr + '.JSON';

        # Unique Key
        $UniqueKey = $DateStr -replace '-', ''
        $UniqueKey = $_.symbol + '-' + $UniqueKey

        # Row Details - Hash table from psobject
        $RowData = @{"symbol"=[string]$_.symbol; 
           "date" = [string]$DateStr;
           "open" = [float]$_.open;
           "high" = [float]$_.high;
           "low" = [float]$_.low;
           "close" = [float]$_.close;
           "adjclose" = [float]$_.adjclose;
           "volume" = [long]$_."volume";
           "docid" = [string]$UniqueKey;
           "docver" = [float]1.1;
           "moduser" = "system";
           "moddate" = [string](get-date).ToString("yyyy-MM-dd");}


        # Ignore dividend zero records
        If ($_.open -ne 0 -and $_.close -ne 0)
        {
            # Add to table storage
            $RowData | ConvertTo-Json | Out-File -filepath $NewFile
        }

        # Increment counter
        $Cnt += 1
        
        # Show counter every 25
        If ($Cnt % 25 -eq 0) 
        {
            Write-Host "Processed $Cnt records."
        }

    }

    # Final record count
    Write-Host "Processed $Cnt records."
	Write-Host " "

}

# Optional message
Write-Host "End - Convert Csv 2 Json"