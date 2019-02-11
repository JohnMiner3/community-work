# ******************************************************
# *
# * Name:         step09-azure-automation-objects.ps1
# *     
# * Design Phase:
# *     Author:   John Miner
# *     Date:     01-10-2018
# *     Purpose:  Automate the load and archive data workflow.
# *
# *     Note:     Manually punch firewall hole for azure svcs.
# * 
# ******************************************************

<#
    Azure Automation Cmdlets
    https://msdn.microsoft.com/en-us/library/mt244122.aspx
#>


#
# Name:      Create-PSCredential
# Purpose:   Create a secure credential.
#

Function Create-PSCredential { 
 
    [CmdletBinding()] 
    Param( 
 
        [Parameter(Mandatory = $true)] 
        [string]$Username, 
 
        [Parameter(Mandatory = $true)] 
        [string]$Password  
 
    )   
 
    $SecurePassword = ConvertTo-SecureString $Password -AsPlainText -Force 
    $Credential = New-Object System.Management.Automation.PSCredential ($Username, $SecurePassword) 
    Return $Credential 
} 
 


#
# Azure Subscriptions 
#

# Prompts you for azure credentials
#Add-AzureRmAccount

# Clear the screen
#Clear-Host

# List my subscriptions
#Get-AzureRmSubscription

# Pick my internal subscription
#$SubscriptionId = 'cdfb69bf-3533-4c89-9684-1ba6ba81b226'
#Set-AzureRmContext -SubscriptionId $SubscriptionId


# Clear the screen
Clear-Host

# Import security credentials
Import-AzureRmContext -Path "C:\COMMUNITY WORK\STAGING\ATS\AZURE-CREDS.JSON" 



#
# Create automation account
#

# Create account
New-AzureRMAutomationAccount -Name "aa4adf18" -Location "East US 2" -ResourceGroupName "rg4adf18"

# List automation accounts
# Get-AzureRMAutomationAccount 

# Delete automation accounts
# Remove-AzureRMAutomationAccount -Name "aa4adf18"



#
# Create credential for sqldb
#

$Credential = Create-PSCredential -Username "jminer" -Password 'Bm#StG?2018'
New-AzureRMAutomationCredential -AutomationAccountName "aa4adf18" -Name "SqlCredential" -Value $Credential -ResourceGroupName "rg4adf18"

# List saved credential
Get-AzureRMAutomationCredential -AutomationAccountName "aa4adf18" -ResourceGroupName "rg4adf18"

# Delete saved credential
# Remove-AzureRMAutomationCredential -AutomationAccountName "aa4adf18" -Name "SqlCredential" -ResourceGroupName "rg4adf18"


<#
  $Credential.UserName
  $Credential.Password
  $Credential.GetNetworkCredential().Password
#>



#
# Create runbook from ps workflow
#


# Add local workflow
$Path = "C:\Community Work\STAGING\CODE\load-and-archive-data.ps1"
Import-AzureRmAutomationRunbook -Path $Path -ResourceGroup "rg4adf18" -AutomationAccountName "aa4adf18"  -Type PowerShell 


# Publish work flow
$RunBook = "load-and-archive-data"
Publish-AzureRMAutomationRunbook -AutomationAccountName "aa4adf18" -Name $RunBook -ResourceGroupName "rg4adf18" 


# List saved runbook
# Get-AzureRMAutomationRunbook -AutomationAccountName "aa4adf18" -Name $RunBook -ResourceGroupName "rg4adf18"

# Delete save runbook
# Remove-AzureRMAutomationRunbook -AutomationAccountName "aa4adf18" -Name $RunBook -ResourceGroupName "rg4adf18"



#
# Create a schedule (asset)
#

$Schedule = "schd-load-and-archive-data"
New-AzureAutomationSchedule -AutomationAccountName "aa4adf18" -Name $Schedule -DayInterval 1 -StartTime  "20:00:00"

# List saved schedule
# Get-AzureAutomationSchedule -AutomationAccountName "aa4adf18" -Name $Schedule 

# Delete saved schedule
# Remove-AzureAutomationSchedule -AutomationAccountName "aa4adf18" -Name $Schedule



#
# Link schedule to runbook (bug with MSFT!!)
#

#  Add schedule to runbook
$Account = "aa4adf18"
$RunBook = "load-and-archive-data"
$Schedule = "schd-load-and-archive-data"
Register-AzureAutomationScheduledRunbook –AutomationAccountName $Account –Name $RunBook –ScheduleName $Schedule


# Account is linked
Get-AzureAutomationScheduledRunbook -AutomationAccountName $Account


#
# Disable schedule
#

Set-AzureAutomationSchedule –AutomationAccountName $Account –Name $Schedule –IsEnabled $false