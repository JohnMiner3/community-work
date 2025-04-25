#******************************************************
#*
#* Name:     05-make-new-warehouse-v2.py
#*   
#* Design Phase:
#*   Author:  John Miner
#*   Date:    03-15-2024
#*   Purpose: Test database sensitivity.
#* 
#******************************************************

# libraries
from azure.identity import ClientSecretCredential
import requests
import json

# variables for svc account - dlpsvcprn01
tenant_id = ""
client_id = ""
client_secret = ""

# get bearer token
scope = 'https://api.fabric.microsoft.com/.default'
credential_class = ClientSecretCredential(tenant_id, client_id, client_secret)
token_class = credential_class.get_token(scope)
token_string = token_class.token


#
# T1 - list workspaces
#

# show workspaces
header = {'Content-Type':'application/json','Authorization': f'Bearer {token_string}'}
response = requests.get(url='https://api.fabric.microsoft.com/v1/workspaces', headers=header)

# show response
print("\nstep 1 - list workspaces")
print("       - status = {0}\n".format(response.status_code))
print(json.dumps(response.json(), indent=4, separators=(',', ': ')))


#
# T2 - list warehouses
#

# show workspaces
header = {'Content-Type':'application/json','Authorization': f'Bearer {token_string}'}
workspaceId = "397a3457-9e41-4fbe-96c8-c33c3659bd61"
response = requests.get(url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/warehouses', headers=header)

# show response
print("\nstep 2 - list warehouses")
print("       - status = {0}\n".format(response.status_code))
print(json.dumps(response.json(), indent=4, separators=(',', ': ')))


#
# T3 - make case insensitive warehouse
#

# make api call
header = {'Content-Type':'application/json', 'Authorization': f'Bearer {token_string}'}
workspaceId = '397a3457-9e41-4fbe-96c8-c33c3659bd61'
body = {
   'type': 'Warehouse', 
   'displayName': 'wh_pubs2', 
   'description': 'Create warehouse with insensitive collation', 
   'creationPayload': { 
     'defaultCollation': 'Latin1_General_100_CI_AS_KS_WS_SC_UTF8' 
   } 
 }
print(body)
response = requests.post(url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items', headers=header, json=body)

# show response
print("\nstep 3 - create case insenstive warehouses")
print("       - status = {0}\n".format(response.status_code))
print(json.dumps(response.json(), indent=4, separators=(',', ': ')))