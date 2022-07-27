#!/usr/bin/env python3

import sys
import json
import time
import requests
import pandas as pd


# You can omit this script arguments and replace your variables (instead of sys.argv[n])
metdata_API_key = sys.argv[1]
shroom_key = sys.argv[2]
query_id = sys.argv[3]
contract = '0x903e2f5d42ee23156d548dd46bb84b7873789e44'
TTL_MINUTES = 15
tic = time.time()
SQL_QUERY = f'''
WITH variables AS (
	SELECT '{contract}' AS godmode
), mints AS (
  SELECT
  	TOKENID AS ID,
  	NFT_TO_ADDRESS AS summoner,
  	TX_HASH AS summon_txn,
  	BLOCK_TIMESTAMP AS summon_date
  FROM ethereum.core.ez_nft_mints
  WHERE NFT_ADDRESS=(SELECT godmode FROM variables)
)

SELECT * FROM mints
ORDER BY CAST(ID AS INT) ASC
'''

# functions
def pull_metadata(id):
    URL = f'https://eth-mainnet.alchemyapi.io/nft/v2/{metdata_API_key}/getNFTMetadata'\
            + f'?contractAddress={contract}&tokenId={id}&tokenType=erc721'
    metadata = requests.get(
        URL
    ).text
    if (id%100 == 0):
        print(f'Metadata of Godmdoes {id}-{id+100} retrieved.')
    return(metadata)

def create_query():
    r = requests.post(
        'https://node-api.flipsidecrypto.com/queries', 
        data=json.dumps({
            "sql": SQL_QUERY,
            "ttlMinutes": TTL_MINUTES
        }),
        headers={"Accept": "application/json", "Content-Type": "application/json", "x-api-key": shroom_key},
    )
    if r.status_code != 200:
        raise Exception("Error creating query, got response: " + r.text + "with status code: " + str(r.status_code))
    
    return json.loads(r.text)    

def get_query_results(token):
    r = requests.get(
        'https://node-api.flipsidecrypto.com/queries/' + token, 
        headers={"Accept": "application/json", "Content-Type": "application/json", "x-api-key": shroom_key}
    )
    if r.status_code != 200:
        raise Exception("Error getting query results, got response: " + r.text + "with status code: " + str(r.status_code))
    
    data = json.loads(r.text)
    if data['status'] == 'running':
        time.sleep(10)
        return get_query_results(token)

    return data

def run():
    query = create_query()
    token = query.get('token')
    data = get_query_results(token)
    
    df = pd.DataFrame(data.get('results'))
    header = pd.Series(data.get('columnLabels'))
    df = df.rename(columns=header)
    return df

# main
try:
    raw_data = run()
    print(f'Request method: RestAPI with 200 Status code (~{round(time.time()-tic,3)}s)\n')
except Exception as e:
    print(f'RestAPI request failed with an error.\n"{e}"\n')
    print("Alternative request method: OpenAPI\n")
    raw_data = f'https://node-api.flipsidecrypto.com/api/v2/queries/{query_id}/data/latest'

if not isinstance(raw_data, pd.DataFrame):
    raw_data = requests.get(raw_data).json()
    raw_data = pd.DataFrame.from_dict(raw_data)

GodModes_hardcopy = pd.DataFrame.from_dict(raw_data)

GodModes_hardcopy = GodModes_hardcopy[:]
GodModes_hardcopy.ID = GodModes_hardcopy.ID.apply(int)

GodModes_hardcopy['Metadata'] = GodModes_hardcopy.ID.apply(lambda x: pull_metadata(x))

GodModes_hardcopy.to_csv(f'Godmodes-{int(time.time())}.csv')

toc = time.time()
elapsed_time = toc - tic
print(f'\nTotal elapsed time: {elapsed_time}s')