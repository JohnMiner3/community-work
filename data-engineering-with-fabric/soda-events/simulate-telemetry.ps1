# ******************************************************
# *
# * Name:         simulate-telemetry-b.ps1
# *     
# * Design Phase:
# *     Author:   John Miner
# *     Date:     12-25-2019
# *     Purpose:  Read in supporting files.
# *               Post simulated telemetry on event hub.
# *               Package 45 messages into one call.
# * 
# ******************************************************


#
# Get-Vending-Grid() - For a given pattern, create a vending grid
#

function Get-Vending-Grid {
    [CmdletBinding()] 
    param(
        [Parameter(Mandatory = $true)]
        [Int] $minval,

        [Parameter(Mandatory = $true)]
        [Int] $maxval

    )

    # Total vends this hour
    $total = Get-Random -Minimum $minval -Maximum $maxval;

    # Debug
    $m = [string]::Format("Total Vends This Hour {0}.", $total)
    Write-Verbose $m

    # Empty array of 0's
    $array = @(0) * 45

    # Make a sale
    for ($i = 1; $i -le $total; $i++) 
    {
        $idx = Get-Random -Minimum 1 -Maximum 45;
        $array[$idx-1] += 1;
    }

    # Return object
    Return $array;
}


#
# Post-EventHub-Message() - Post a message on an event hub
#

function Post-EventHub-Message {
    [CmdletBinding()] 
    param(
        [Parameter(Mandatory = $true)]
        [string] $namespace,

        [Parameter(Mandatory = $true)]
        [string] $hubname,

        [Parameter(Mandatory = $true)]
        [string] $keyname,

        [Parameter(Mandatory = $true)]
        [string] $keyval,

        [Parameter(Mandatory = $true)]
        [string] $msg
    )


    # Load assembly
    [Reflection.Assembly]::LoadWithPartialName("System.Web") | Out-Null

    # Get address
    $uri = "{0}.servicebus.windows.net/{1}" -f @($namespace, $hubname)

    # Encode address
    $encodeduri = [System.Web.HttpUtility]::UrlEncode($uri)

    # Make expiration time
    $expiry = [string](([DateTimeOffset]::Now.ToUnixTimeSeconds())+3600)

    # Create the signature
    $strtosign = [System.Web.HttpUtility]::UrlEncode($uri) + "`n" + $expiry

    # Encode the key
    $hmacsha = New-Object System.Security.Cryptography.HMACSHA256
    $hmacsha.key = [Text.Encoding]::ASCII.GetBytes($keyval)

    # Encode signature
    $signature = $hmacsha.ComputeHash([Text.Encoding]::ASCII.GetBytes($strtosign))
    $signature = [System.Web.HttpUtility]::UrlEncode([Convert]::ToBase64String($signature))

    # Create header
    $headers = @{
            "Authorization"="SharedAccessSignature sr=" + $encodeduri + "&sig=" + $signature + "&se=" + $expiry + "&skn=" + $keyname;
            "Content-Type"="application/atom+xml;type=entry;charset=utf-8";  
            "Content-Length" = ("{0}" -f ($msg.Length))
            }

    # Use post method
    $method = "POST"

    # Rest api destination
    $dest = 'https://' +$uri  +'/messages?timeout=60&api-version=2014-01'

    # Call rest api
    Invoke-RestMethod -Uri $dest -Method $method -Headers $headers -Body $msg | Out-Null
}


#
# Start exec script
# 

# Set the path
Set-Location "C:\soda\"

# Logging
Write-Host "Starting vending simulator script at $(Get-Date -Format u)."



#
# Read program configuration from JSON file
# 

# read config file
$config = Get-Content -Raw -Path ".\simulate-telemetry.json" | ConvertFrom-Json

# Logging
Write-Host "Convert json [program configuration] file to ps object"


#
# Read machine list from JSON file
# 

# read config file
$machines = Get-Content -Raw -Path ".\machines.json" | ConvertFrom-Json

# Logging
Write-Host "Convert json [machine list] file to ps object"


#
# Read vending pattern from JSON file
#

# read config file
$vending = Get-Content -Raw -Path ".\vending.json" | ConvertFrom-Json

# Logging
Write-Host "Convert json [vending pattern] file to ps object"



#
# Read slot 2 product JSON file
#

# read config file
$slots = Get-Content -Raw -Path ".\slots.json" | ConvertFrom-Json

# Logging
Write-Host "Convert json [slot 2 product] file to ps object"



#
# Allow small packets to go
#

# Turn off this algorithm 
[System.Net.ServicePointManager]::UseNagleAlgorithm = $false 


#
# Create telemetry for each machine
#

# Get utc time
$u = [System.TimeZoneInfo]::ConvertTimeBySystemTimeZoneId((Get-Date), 'Greenwich Standard Time')

# Execute each tsql stmt
for ($i = 0; $i -lt $machines.Count; $i++)
{

    # Processing machine x
    $m = [string]::Format("Machine {0} is located in {1} with offset {2}.", $machines[$i].Machine_Id.ToString(), $machines[$i].State_Code.ToString(), $machines[$i].Utc_Offset.ToString())
    Write-Host $m

    # Get offset
    $o = $machines[$i].Utc_Offset

    # Get local time
    $l = $u.AddHours($o)

    # Remove minutes and seconds)
    $l = ($l.ToShortDateString() + ' ' + $l.Hour.ToString() + ':00:00')

    # Get hour index
    $h = $u.AddHours($o).hour

    # Smallest sale
    $minval = $vending[$h].Min_Sale

    # Largest sale
    $maxval = $vending[$h].Max_Sale

    # Random vend sales grid
    $salesgrid = Get-Vending-Grid -minval $minval -maxval $maxval 

    # Array of messages (1 .. 45)
    $msgary = @()

    # For each slot
    for ($j = 1; $j -lt 46; $j++)
    {

        # Create variables
        $p1 = $machines[$i].Machine_Id
        $p2 = $j
        $p3 = $slots[$j-1].Product_Id
        $p4 = $l
        $p5 = $salesgrid[$j-1]

        # Create property string
        $properties = @{'Machine_Id'="$p1"; 'Slot_Id'="$p2"; 'Product_Id'="$p3"; 'Date_Time'= "$p4"; 'Vend_Count'="$p5"}

        # Create ps object
        $object = New-Object -TypeName PSObject -Property $properties

        # Create array of objects
        $msgary += $object 
    }

    # Convert to Json
    $msg = $msgary | ConvertTo-Json

    # Post data to hub
    Post-EventHub-Message -namespace $config.eventhub.namespace -hubname $config.eventhub.hubname -keyname $config.eventhub.keyname -keyval $config.eventhub.keyval -msg $msg

}


#
# End exec script
# 

# Logging
Write-Host "Ending vending simulator script at $(Get-Date -Format u)." 

