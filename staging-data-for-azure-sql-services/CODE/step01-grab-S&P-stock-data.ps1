# ******************************************************
# *
# * Name:         step01-grab-S&P-stock-data.ps1
# *     
# * Design Phase:
# *     Author:   John Miner
# *     Date:     04-01-2018
# *     Purpose:  Grab stock data from yahoo financials.
# *
# ******************************************************


#
# Name:      Get-WebRequest-Table
# Purpose:   Grab data from internet.
#

# http://www.leeholmes.com/blog/2015/01/05/extracting-tables-from-powershells-invoke-webrequest/

function Get-WebRequest-Table {
    [CmdletBinding()] 
    param(
        [Parameter(Mandatory = $true)]
        [String] $Url,

        [Parameter(Mandatory = $true)]
        [int] $TableNumber
    )

    # Make are web request
    $request = Invoke-WebRequest $url 

    # Extract the tables 
    $tables = @($request.ParsedHtml.getElementsByTagName("TABLE"))

    # Pick which table #?
    $table = $tables[$TableNumber]

    # Header data
    $titles = @()

    # Row data
    $rows = @($table.Rows)

    # Go through all of the rows in the table
    foreach($row in $rows)
    {

        # Get the cells
        $cells = @($row.Cells)
          

        # Found table header, remember titles
        if($cells[0].tagName -eq "TH")
        {
            $titles = @($cells | % { ("" + $_.InnerText).Trim() })
            continue
        }

        # No table header, make up names "P1", "P2", ...
        if(-not $titles)
        {
            $titles = @(1..($cells.Count + 2) | % { "P$_" })
        }

        # Ordered hash table output
        $output = [Ordered] @{}

        # Process each cell
        for($counter = 0; $counter -lt $cells.Count; $counter++)
        {
            # Grab a title
            $title = $titles[$counter]

            # Just incase we have an empty cell
            if(-not $title) { continue }

            # Title to value mapping
            $output[$title] = ("" + $cells[$counter].InnerText).Trim()
        }

        # Finally cast that hashtable to an object
        [PSCustomObject] $output

    }
}



#
#  1 - Getting historical data
#


# Set working directory
Set-Location "c:\stocks"

# Read csv file
$list = Import-Csv -Path "S&P-500.csv" -Delimiter ',' -Encoding ascii | Sort-Object -Property symbol

$list = $list | select -First 10


# Period 1
$TimeSpan1 = [DateTime]'2013-01-01' - [DateTime]'1970-01-01';

# Period 2
$TimeSpan2 = [DateTime]'2014-01-01' - [DateTime]'1970-01-01';


# For each stock
[int]$cnt = 0
foreach ($stock in $list)
{

    # Create variables used by proceess
    $symbol = $stock.symbol
    $site = 'http://finance.yahoo.com/quote/' + $symbol +'/history?period1=' + $TimeSpan1.TotalSeconds + '&period2=' + $TimeSpan2.TotalSeconds + '&interval=1d&filter=history&frequency=1d'

    $html = 'c:\stocks\inbound\' + $symbol + '-FY2018.HTML'
    $csv = 'c:\stocks\outbound\' + $symbol + '-FY2018.CSV'
    $json = 'c:\stocks\inbound\' + $symbol + '-FY2018.JSON'

    # Save web page 2 html file
    $response = (New-Object System.Net.WebClient).DownloadString($site)
    $response | Out-File -FilePath $html

    # Find outer document (json)
    $content = Get-Content $html
    foreach ($line1 in $content)
    {
        if ($line1 -match 'root.App.main')
        {
        $main = $line1
        }
    }

    # Remove java script tags
    $main = $main.Substring(16, $main.Length-17)

    # Find inner document (json)
    $start = $main.indexof('"HistoricalPriceStore"')
    $end = $main.indexof('"NavServiceStore"')
    $len = $end-$start-24
    $prices = $main.Substring($start+23, $len)

    # Write json document
    $prices | Out-File $json

    # Convert to powershell object
    $data = $prices | ConvertFrom-Json
    
    # Reformat data & remove dividends
    $output = $data.prices | 

    Select-Object @{Name="symbol";Expression={[string]$symbol}}, 
           @{Name="date";Expression={([datetime]"1/1/1970").AddSeconds($_."date").ToString("MM/dd/yyyy")}},
           @{Name="open";Expression={[float]$_."open"}}, 
           @{Name="high";Expression={[float]$_."high"}},
           @{Name="low";Expression={[float]$_."low"}},
           @{Name="close";Expression={[float]$_."close"}},
           @{Name="adjclose";Expression={[float]$_."adjclose"}},
           @{Name="volume";Expression={[uint64]$_."volume"}} |

    Where-Object {-not [string]::IsNullOrEmpty($_.volume)}


    # Open file
    $stream = [System.IO.StreamWriter]::new( $csv )

    # Write header
    $line2 = '"symbol","date","open","high","low","close","adjclose","volume"'
    $stream.WriteLine($line2) 

    # For each line, format data
    $output | ForEach-Object { 
        $line2 = [char]34 + $_.symbol + [char]34 + "," + [char]34 + $_.date + [char]34 + "," + 
            $_.open + "," + $_.high + "," + $_.low + "," + $_.close + "," + $_.adjclose + "," + $_.volume
        $stream.WriteLine($line2) 
    }

    # Close file
    $stream.close()


    # Status line
    $cnt = $cnt + 1
    Write-Host $cnt
    Write-Host $csv
}


