function Break-Blob-Lease {

<#
 	.SYNOPSIS
       This script can be break the lease if the specified blob with locked lease status.
    .DESCRIPTION
       This script can be break the lease if the specified blob with locked lease status.
    .PARAMETER  ContainerName
		Specifies the name of container.
	.PARAMETER	BlobName
		Specifies the name of blob.
	.PARAMETER  StorageAccountName
		Specifies the name of the storage account to be connected.
	.EXAMPLE
        C:\PS> C:\Script\BreakBlobLease.ps1 -StorageAccountName "storageaccount0102" -ContainerName "vhd" -BlobName "p4j2014032006.vhd"

        Successfully broken lease on 'p4j2014032006.vhd' blob.

		This example shows how to break lease on specified blob.
#>



[CmdletBinding(SupportsShouldProcess = $true)]

Param
(
    [Parameter(Mandatory=$true)]
    [Alias('SN')]
    [String]$StorageAccountName,
    [Parameter(Mandatory=$true)]
    [Alias('CN')]
    [String]$ContainerName,
    [Parameter(Mandatory=$true)]
    [Alias('BN')]
    [String]$BlobName
)

#Check if Windows Azure PowerShell Module is avaliable
If((Get-Module -ListAvailable Azure) -eq $null)
{
    Write-Warning "Windows Azure PowerShell module not found! Please install from http://www.windowsazure.com/en-us/downloads/#cmd-line-tools"
}
Else
{
    If($StorageAccountName)
    {
        Get-AzureStorageAccount -StorageAccountName $StorageAccountName -ErrorAction SilentlyContinue `
        -ErrorVariable IsExistStorageError | Out-Null

        #Check if storage account is exist
        If($IsExistStorageError.Exception -eq $null)
        {
            If($ContainerName)
            {
                Get-AzureStorageContainer -Name $ContainerName -ErrorAction SilentlyContinue `
                -ErrorVariable IsExistContainerError | Out-Null

                #Check if container is exist
                If($IsExistContainerError.Exception -eq $null)
                {
                    If($BlobName)
                    {
                        Get-AzureStorageBlob -Container $ContainerName -Blob $BlobName -ErrorAction SilentlyContinue `
                        -ErrorVariable IsExistBlobError | Out-Null

                        #Check if blob is exist
                        If($IsExistBlobError.Exception -eq $null)
                        {
                            #Specify a Windows Azure Storage Library path
                            $StorageLibraryPath = "$env:SystemDrive\Program Files\Microsoft SDKs\Windows Azure\.NET SDK\v2.2\ref\Microsoft.WindowsAzure.Storage.dll"

                            #Getting Azure storage account key
                            $Keys = Get-AzureStorageKey -StorageAccountName $StorageAccountName
                            $StorageAccountKey = $Keys[0].Primary

                            #Loading Windows Azure Storage Library for .NET.
                            Write-Verbose -Message "Loading Windows Azure Storage Library from $StorageLibraryPath"
                            [Reflection.Assembly]::LoadFile("$StorageLibraryPath") | Out-Null

                            $Creds = New-Object Microsoft.WindowsAzure.Storage.Auth.StorageCredentials("$StorageAccountName","$StorageAccountKey")
                            $CloudStorageAccount = New-Object Microsoft.WindowsAzure.Storage.CloudStorageAccount($creds, $true)
                            $CloudBlobClient = $CloudStorageAccount.CreateCloudBlobClient()

                            Write-Verbose "Getting the container object named $ContainerName."
                            $BlobContainer = $CloudBlobClient.GetContainerReference($ContainerName)

                            $Blob = $BlobContainer.ListBlobs() | Where{$_.Name -eq $BlobName}

                            If($Blob.Properties.LeaseStatus -eq "Locked")
                            {
                                Try
                                {
                                    Write-Verbose "Breaking leases on '$BlobName' blob."
                                    $Blob.BreakLease($(New-TimeSpan), $null, $null, $null) | Out-Null
                                    Write-Host "Successfully broken lease on '$BlobName' blob."
                                }
                                Catch
                                {
                                    Write-Host "Failed to break lease on '$BlobName' blob." -ForegroundColor Red
                                }
                            }
                            Else
                            {
                                Write-Host "The '$BlobName' blob's lease status is unlocked."
                            }
                        }
                        Else
                        {
                            Write-Warning "Cannot find blob '$BlobName' because it does not exist. Please make sure thar the name of blob is correct."
                        }
                    }
                }
                Else
                {
                    Write-Warning "Cannot find container '$ContainerName' because it does not exist. Please make sure thar the name of container is correct."
                }
            }
        }
        Else
        {
            Write-Warning "Cannot find storage account '$StorageAccountName' because it does not exist. Please make sure thar the name of storage is correct."
        }
    }
}

}


Break-Blob-Lease -StorageAccountName "sa4rissug" -ContainerName "cn4backups" -BlobName "AdventureWorks2012-A.bak"