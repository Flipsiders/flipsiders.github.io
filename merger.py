#!/usr/bin/env python3

import sys
import json
import time
import glob
import requests
import pandas as pd

# You can omit this script arguments and replace your variables (instead of sys.argv[n])
type = sys.argv[1]
shroom_key = sys.argv[2]
query_id = sys.argv[3]
contract = "'0x903e2f5d42ee23156d548dd46bb84b7873789e44'"
TTL_MINUTES = 15
tic = time.time()
with open('query.sql', 'r') as f:
    SQL_QUERY = f.read()

SQL_QUERY = SQL_QUERY.format(contract=contract)

def create_query(SQL_QUERY):
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

def run(SQL_QUERY):
    query = create_query(SQL_QUERY)
    token = query.get('token')
    print(f'Query_token: {token[:12]}***{token[-4:]}')
    data = get_query_results(token)
    
    df = pd.DataFrame(data.get('results'))
    header = pd.Series(data.get('columnLabels'))
    df = df.rename(columns=header)
    return df

# main
try:
    raw_data = run(SQL_QUERY)
    print(f'Request method: RestAPI with 200 Status code (~{round(time.time()-tic,3)}s)\n')
except Exception as e:
    print(f'RestAPI request failed with an error.\n"{e}"\n')
    print("Alternative request method: OpenAPI\n")
    raw_data = f'https://node-api.flipsidecrypto.com/api/v2/queries/{query_id}/data/latest'

if not isinstance(raw_data, pd.DataFrame):
    raw_data = requests.get(raw_data).json()
    raw_data = pd.DataFrame.from_dict(raw_data)

daily = pd.DataFrame.from_dict(raw_data)

if type == 'weekly':
    weekly = glob.glob('Godmodes-*.csv')[0]
    weekly = pd.read_csv(weekly)
    # weekly = weekly.rename(columns = {'ID':'TOKENID'})

    daily['TOKENID'] = daily['TOKENID'].astype(str)
    weekly['TOKENID'] = weekly['TOKENID'].astype(str)

    merged = pd.merge(daily, weekly, on='TOKENID')
    merged = merged.drop(columns=['Unnamed: 0'])
    merged.to_csv(f'Godmodes-{int(time.time())}.csv')
else:
    old = glob.glob('Godmodes-*.csv')[0]
    old = pd.read_csv(old)
    
    try:
        old.drop(
            old.columns.difference(
                [
                    'TOKENID',
                    'SUMMONER',
                    'SUMMON_TXN',
                    'SUMMON_DATE',
                    'Metadata'
                ]
            ),
            1,
            inplace=True
        )
        
    except Exception as e:
        print('Unknown error on merging process')
        print(e)
    
    daily['TOKENID'] = daily['TOKENID'].astype(str)
    old['TOKENID'] = old['TOKENID'].astype(str)
    
    merged = pd.merge(daily, old, on='TOKENID')
    merged.to_csv(f'Godmodes-{int(time.time())}.csv')

toc = time.time()
elapsed_time = toc - tic
elapsed_time = '{:.3f}'.format(elapsed_time)
print(f'The elapsed time of the merging process: ~{elapsed_time}s')
