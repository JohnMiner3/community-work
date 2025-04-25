#******************************************************
#*
#* Name:     04-make-new-warehouse-v1.py
#*   
#* Design Phase:
#*   Author:  John Miner
#*   Date:    03-15-2024
#*   Purpose: Test database sensitivity.
#* 
#******************************************************

# ~ Install ~
# pip install msfabricpysdkcore

# ~ docs ~
# https://github.com/DaSenf1860/ms-fabric-sdk-core/tree/main?tab=readme-ov-file


# use library
from msfabricpysdkcore import FabricClientCore


#
#  T1 - create a connection
#

# variables for svc account - dlpsvcprn01
tenant_id = ""
client_id = ""
client_secret = ""

#  create a connection
fc = FabricClientCore(tenant_id, client_id, client_secret)


#
#  T2 - list workspaces
#

# List workspaces
result = fc.list_workspaces()
for ws in result:
    print(ws)


#
#  T3 - list warehouses in a workspace
#

# get workspace id
workspace = fc.get_workspace_by_name("ws-fabric-bites")
workspace_id = workspace.id

# List Warehouses
result = fc.list_warehouses(workspace_id)
for wh in result:
    print(wh)

#
#  T4 - create case insensitive warehouse
#


# Create Warehouse
#warehouse = fc.create_warehouse(workspace_id, display_name="ws-pubs-sdk")


#
#  T5 - delete semantic model
#

Remove = False
if Remove:
    # list semantic models
    semantic_models = fc.list_semantic_models(workspace_id)
    print(semantic_models)

    # Delete Semantic Model
    #fc.delete_semantic_model(workspace_id, semantic_model_id="bdd9ae42-9294-4739-b3d5-53e9d8f6f276")


