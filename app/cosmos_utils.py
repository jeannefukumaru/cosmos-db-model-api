from symbol import parameters
from azure.cosmos.aio import CosmosClient as cosmos_client
from azure.cosmos import PartitionKey, exceptions
import asyncio
import os 
from dotenv import load_dotenv
import configparser
import pandas as pd

config = configparser.ConfigParser()
config.read("../config.ini")
load_dotenv()

async def get_db(client, database_name):
    try:
        database_obj  = client.get_database_client(database_name)
        await database_obj.read()
        return database_obj
    except exceptions.CosmosResourceNotFoundError:
        print("Cosmos resource not found")
    except exceptions.CosmosHttpResponseError:
        raise

async def get_container(database_obj, container_name):
    try:        
        container_obj = database_obj.get_container_client(container_name)
        return container_obj
    except exceptions.CosmosResourceNotFoundError:
        print("no container found")
    except exceptions.CosmosHttpResponseError:
        raise

async def query_items(container_obj, query_text, id):
    # enable_cross_partition_query should be set to True as the container is partitioned
    # In this case, we do have to await the asynchronous iterator object since logic
    # within the query_items() method makes network calls to verify the partition key
    # definition in the container
    query_items_response = container_obj.query_items(
        query=query_text,
        enable_cross_partition_query=True,
        parameters=[dict(name="@id", value=id)]
    ) 
    request_charge = container_obj.client_connection.last_response_headers['x-ms-request-charge']
    items = [item async for item in query_items_response]
    print('Query returned {0} items. Operation consumed {1} request units'.format(len(items), request_charge))
    return items

async def read_items(container_obj, items_to_read):
    # Read items (key value lookups by partition key and id, aka point reads)
    # <read_item>
    for id in items_to_read:
        item_response = await container_obj.read_item(item="[0]", partition_key="_feature_store_internal__primary_keys")
        request_charge = container_obj.client_connection.last_response_headers['x-ms-request-charge']
        print('Read item with id {0}. Operation consumed {1} request units'.format(item_response['id'], (request_charge)))

async def query_cosmos(ids):
    async def query_cosmos(ids):
        endpoint = os.environ["COSMOS_ENDPOINT"]
        key = os.environ["COSMOS_KEY"]
        database_name = config["COSMOS"]["database_name"]
        container_name = config["COSMOS"]["container_name"]
        async with cosmos_client(endpoint, credential = key) as client:
            try:
                dbs = [x async for x in client.list_databases()]
                print(dbs)
                database_obj = await get_db(client, database_name)
                container_obj = await get_container(database_obj, container_name)
                # TODO: make this batch query with IN
                results = []
                for id in ids:
                    query_text = "SELECT * FROM feature_store_online_wine_features f WHERE f._feature_store_internal__primary_keys = @id"
                    res = await query_items(container_obj, query_text, id)
                    results.append(res)
                results = [r for res in results for r in res]
                print(results)
                return pd.DataFrame(results)

                # await read_items(container_obj, ids)
            except exceptions.CosmosResourceNotFoundError:
                print("Cosmos resource not found")
    # loop = asyncio.new_event_loop()
    ids = ["[0]", "[1]", "[2]"]
    results = await query_cosmos(ids)
    return results

    

# if __name__=="__main__":
#     loop = asyncio.get_event_loop()
#     ids = ["[0]", "[1]", "[2]"]
#     loop.run_until_complete(query_cosmos(ids))