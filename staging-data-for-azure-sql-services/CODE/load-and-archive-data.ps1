# ******************************************************
# *
# * Name:         load-and-archive-data.ps1
# *     
# * Design Phase:
# *     Author:   John Miner
# *     Date:     01-10-2018
# *     Purpose:  Use OpenRowSet to load files from Blob Storage.
# *               Copy files from inbox to archive container.
# *               Remove files from inbox.
# * 
# ******************************************************


workflow load-and-archive-data
{

    InlineScript 
	{ 
        try
        {

		#
		# Name:      Get-DataSet-SqlDb
		# Purpose:   Retrieve data from SELECT query.
		#

		function Get-DataSet-SqlDb {
			[CmdletBinding()] 

			param(
				[Parameter(Mandatory = $true)]
				[String] $ConnStr,

				[Parameter(Mandatory = $true)]
				[string] $SqlQry,

				[Parameter(Mandatory=$false)] 
				[ValidateSet("DataSet", "DataTable", "DataRow")] 
				[string]$As="DataRow" 
			)

			# Create connection object
			$Conn = New-Object -TypeName System.Data.SqlClient.SqlConnection
			$Conn.ConnectionString = $ConnStr

			# Create command 
			$Cmd = $Conn.CreateCommand()
			$Cmd.CommandText = $SqlQry

			# Create adapter & dataset objects
			$Adapter = New-Object -TypeName System.Data.SqlClient.SqlDataAdapter $Cmd
			$DataSet = New-Object -TypeName System.Data.DataSet

			# Fill dataset and return as result
			$Adapter.Fill($DataSet)

			# Close the session
			$Conn.Close()

			# Return the result
			Switch ($As) 
			{ 
				'DataSet'   { Write-Output ($DataSet) } 
				'DataTable' { Write-Output ($DataSet.Tables) } 
				'DataRow'   { Write-Output ($DataSet.Tables[0]) } 
			} 
		 
		}


		#
		# Name:      Exec-NonQuery-SqlDb
		# Purpose:   Execute a DELETE, INSERT, UPDATE or DDL statement.
		#

		function Exec-NonQuery-SqlDb {
			[CmdletBinding()] 

			param(
				[Parameter(Mandatory = $true)]
				[String] $ConnStr,

				[Parameter(Mandatory = $true)]
				[string] $SqlQry
			)

			# Create & open connection object
			$Conn = New-Object -TypeName System.Data.SqlClient.SqlConnection
			$Conn.ConnectionString = $ConnStr
			$Conn.Open()

			# Create command object
			$Cmd = $Conn.CreateCommand()
			$Cmd.CommandTimeout = 300
			$Cmd.CommandText = $SqlQry

			# Execute query with no return sets
			$Result = $Cmd.ExecuteNonQuery()

			# Close the session
			$Conn.Close()

			# Return the result
			return $Result
		}

		
		#
		# Name:      Send-Grid-Mail 
		# Purpose:   This is a specific function for SendGrid
		#

		function Send-Grid-Mail {
			[CmdletBinding()] 

			param(
				[Parameter(Mandatory = $true)]
				[String] $ToList,

				[Parameter(Mandatory = $true)]
				[string] $FromList,

				[Parameter(Mandatory = $true)]
				[String] $SubjText,

				[Parameter(Mandatory = $true)]
				[string] $BodyText,

				[Parameter(Mandatory=$true)] 
				[System.Management.Automation.PSCredential] $Credential

			)

            # Split into string array
            $ToArray = $ToList.Split(';', [StringSplitOptions]'RemoveEmptyEntries')

			# Send out the mail
			 Send-MailMessage -To $ToArray -From $FromList -Subject $SubjText -Body $BodyText -BodyAsHtml  -SmtpServer 'smtp.sendgrid.net' -Credential $Credential
		   
		}


		#
		# Name:      Main
		# Purpose:   This is the main section of code.
		#
		
        # Test or Prod
        $MyEnv = "test"

		# Messaging
		Write-Output ("Start - Load and archive data.");		

		

        <#
            Step 1
        #>

        Write-Output ("Step 1 - Get database credential.");

		# Get credential  
        if ($MyEnv -ne "test")
        {
            $Credential = Get-AutomationPSCredential -Name "SqlCredential"
            $User = $Credential.GetNetworkCredential().UserName
            $Password = $Credential.GetNetworkCredential().Password
        }
        else
        {
            $User = "jminer"
            $Password = 'MS#tIpS$2018'
        }



        <#
            Step 2
        #>

		Write-Output ("Step 2 - Get and show packing list.");

		# Set connection string
		[string]$ConnStr = 'Server=tcp:svr4stocks.database.windows.net;Database=db4stocks;Uid=' + $User + ';Pwd=' +  $Password + ';'

		# Make TSQL stmt
		[string]$SqlQry = "SELECT CAST(LIST_DATA.VALUE AS VARCHAR(256)) AS PKG_LIST
        FROM OPENROWSET
        (
            BULK 'NEW/PACKING-LIST.TXT',
            DATA_SOURCE = 'EDS_AZURE_4_STOCKS', 
            SINGLE_CLOB
        ) AS RAW_DATA
        CROSS APPLY STRING_SPLIT(REPLACE(REPLACE(RAW_DATA.BulkColumn, CHAR(10), 'þ'), CHAR(13), ''), 'þ') AS LIST_DATA;"
	
		# Grab file list 
		$SqlData = Get-DataSet-SqlDb -ConnStr $ConnStr -SqlQry $SqlQry | Where-Object { $_.PKG_LIST }


        Exit


        <#
            Step 3
        #>

		Write-Output ("Step 3 - Load data from blob storage.");

		# Make TSQL stmt
        $SqlQry = "EXEC [ACTIVE].[LOAD_FROM_BLOB_STORAGE] @VAR_VERBOSE_FLAG = 'N';"

        # Call stored procedure
        $Count = Exec-NonQuery-SqlDb -ConnStr $ConnStr -SqlQry $SqlQry

        # Return value
        $Count



        <#
            Step 4
        #>

		Write-Output ("Step 4 - Move from stage to active.");

		# Make TSQL stmt
        $SqlQry = "INSERT INTO [ACTIVE].[STOCKS] SELECT * FROM [STAGE].[STOCKS];"

        # Call stored procedure
        $Count = Exec-NonQuery-SqlDb -ConnStr $ConnStr -SqlQry $SqlQry

        # Return value
        $Count



        <#
            Step 5
        #>

		Write-Output ("Step 5 - Log into azure with credentials.");		

        if ($MyEnv -ne "test")
        {		
            $Conn = Get-AutomationConnection -Name AzureRunAsConnection
            Add-AzureRMAccount -ServicePrincipal -Tenant $Conn.TenantID `
            -ApplicationId $Conn.ApplicationID -CertificateThumbprint $Conn.CertificateThumbprint
		}
        else
        {
            Import-AzureRmContext -Path "C:\MSSQLTIPS\MINER2018\ARTICLE-2018-09-BATCH-AUTOMATION\AZURE-CREDS.JSON" 
		}



        <#
            Step 6
        #>

		Write-Output ("Step 6 - Copy files from inbox to archive.");

        # Grab storage context
        $StorageCtx = Get-AzureRmStorageAccount -Name "sa4adf18" -ResourceGroupName "rg4adf18" 

        # Get a list of all files in the container.
        $FileList = $StorageCtx | Get-AzureStorageBlob -Container "sc4inbox" | Select-Object Name

        ForEach ($SrcFile in $FileList)
        {
            # Remove 'NEW/' from name
            $SplitName = $SrcFile.Name.Split("/")

            # Create new destination name
            $DstFile = "OLD/" + $SplitName[1]

            # Start Copy            
            $StorageContext | Start-AzureStorageBlobCopy -SrcBlob $SrcFile.Name `
                -SrcContainer "sc4inbox" `
                -DestContainer "sc4archive" `
                -DestBlob $DstFile -Force       
                
        }



        <#
            Step 7
        #>

		Write-Output ("Step 7 - Remove files from inbox.");

        # Grab storage context
        $StorageCtx = Get-AzureRmStorageAccount -Name "sa4ssug" -ResourceGroupName "rg4ssug" 

        # Get a list of all files in the container.
        $FileList = $StorageContext | Get-AzureStorageBlob -Container "sc4inbox" | Select-Object Name

        # Remove each file
        ForEach ($SrcFile in $FileList)
        {
            $StorageContext | Remove-AzureStorageBlob -Blob $SrcFile.Name -Container "sc4inbox"                        
        }

	

		# Messaging
		Write-Output ("Stop - Load and archive data.");
		

        }
        catch 
        {
            Write-Error -Message $_.Exception;
            throw $_.Exception;
        }
	}
}

# Call the workflow
load-and-archive-data