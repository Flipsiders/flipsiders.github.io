#!/usr/bin/env python3

import requests
import pandas as pd
import sys
import json
import time
from bs4 import BeautifulSoup as bs
import glob
import datetime
import math

contract = '0x903e2f5d42ee23156d548dd46bb84b7873789e44'

tic = time.time()
def rank_calculator(trait, type):
    trait_dictionary = traits.get(type)

    try:
        PoO = trait_dictionary.get(trait)
        if PoO == None:
            PoO = trait_dictionary.get(trait.lower())
    except Exception as e:
        PoO = 0
    
    return PoO/total_supply

def rank_categorizer(rank):
    if rank <= 10:
        cat = 'Top 10 (1%)'
    elif 10 < rank and rank <= 25:
        cat = 'Top 25 (2.5%)'
    elif 25 < rank and rank <= 50:
        cat = 'Top 50 (5%)'
    elif 50 < rank and rank <= 100:
        cat = 'Top 100 (10%)'
    elif 100 < rank and rank <= 500:
        cat = 'Top 500 (50%)'
    else:
        cat = 'Bottom 500'
    
    return cat

def json_splitter(dataframe, column):
    temp = dataframe[:]
    ser = temp[column].apply(lambda s: pd.json_normalize(json.loads(s)))

    a = temp.drop(columns=[
        column
    ])

    b = pd.concat(list(ser), ignore_index=True)
    c = a.join(b)
    return c

def address_shortener(address):
    return address[:4] + '...' + address[-4:]


# Raw_data
# It could be optimized by additional columns like last price, transfers and etc.
GodModes_hardcopy = pd.read_csv(glob.glob('Godmodes-*.csv')[0])

# Pulling the collection's essential data from OpenSea
collection_info = requests.get(
    'https://api.opensea.io/api/v1/collection/godmode-by-flipsidecrypto'
).json().get('collection')

# Tables cleaning
GodModes = GodModes_hardcopy[:]

GodModes = json_splitter(GodModes, 'Metadata')

GodModes['media'] = GodModes['media'].apply(
    lambda x: pd.DataFrame.from_dict(x).to_json()
)
GodModes = json_splitter(GodModes, 'media')

GodModes['metadata.attributes'] = GodModes['metadata.attributes'].apply(
    lambda x: pd.DataFrame.from_dict(x).to_json()
)
GodModes = json_splitter(GodModes, 'metadata.attributes')

GodModes = GodModes.drop(columns=[
    'title', 'description',
    'timeLastUpdated', 'id.tokenId',
    'id.tokenMetadata.tokenType',
    'timeLastUpdated',
    'tokenUri.raw', 'contract.address',
    'tokenUri.gateway', 'metadata.background_color',
    'metadata.description', 'metadata.name',
    'metadata.image', 'format.0', 'trait_type.0',
    'trait_type.1', 'trait_type.2',
    'trait_type.3', 'trait_type.4',
])

GodModes = GodModes.rename(columns = {
    'SUMMONER': 'Summoner',
    'SUMMON_TXN': 'Summon TXN',
    'SUMMON_DATE': 'Mint date',
    'metadata.owner': 'Owner',
    'metadata.external_url': 'External URL',
    'raw.0': 'Full size image',
    'thumbnail.0': 'Thumbnail',
    'gateway.0': 'Gateway',
    'value.0': 'Back',
    'value.1': 'Front',
    'value.2': 'Under',
    'value.3': 'Spectrum',
    'value.4': 'Substance',
    'CURRENT_OWNER': 'Current owner',
    'PRICE': 'Price',
    'PRICE_USD': 'Price in USD',
    'LAST_TRANSFER_TIME': 'Last transfer',
    'TX_HASH': 'Last transfer TXN'
})

# Analyzing the tables
total_supply = collection_info.get('stats').setdefault('total_supply', 1000)
owners = collection_info.get('stats').setdefault('num_owners', 1000)
traits = collection_info.get('traits')

GodModes['Back_PoO'] = GodModes.apply(lambda x: rank_calculator(x['Back'], type='Back'), axis=1)
GodModes['Front_PoO'] = GodModes.apply(lambda x: rank_calculator(x['Front'], type='Front'), axis=1)
GodModes['Under_PoO'] = GodModes.apply(lambda x: rank_calculator(x['Under'], type='Under'), axis=1)
GodModes['Spectrum_PoO'] = GodModes.apply(lambda x: rank_calculator(x['Spectrum'], type='Spectrum'), axis=1)
GodModes['Substance_PoO'] = GodModes.apply(lambda x: rank_calculator(x['Substance'], type='Substance'), axis=1)

GodModes['PoO'] = GodModes.apply(
    lambda x: 1/x['Back_PoO'] + 1/x['Front_PoO'] + 1/x['Under_PoO'] + 1/x['Spectrum_PoO'] + 1/x['Substance_PoO'],
    axis=1
)

GodModes['PoO'] = GodModes.apply(
    lambda x: (x['Back_PoO'] + x['Front_PoO'] + x['Under_PoO'] + x['Spectrum_PoO'] + x['Substance_PoO'])/5 ,
    axis=1
)

GodModes['PoO'] = GodModes.apply(
    lambda x: x['Back_PoO'] * x['Front_PoO'] * x['Under_PoO'] * x['Spectrum_PoO'] * x['Substance_PoO'],
    axis=1
)

GodModes['Rank'] = GodModes['PoO'].rank(ascending=True, method='first')
GodModes = GodModes.sort_values('Rank')

GodModes['Last transfer'] = pd.to_datetime(GodModes['Last transfer'], format='%Y-%m-%d %H:%M:%S')

GodModes['Category'] = GodModes['Rank'].apply(rank_categorizer)

holders = list(GodModes['Current owner'])
holders = {holder: holders.count(holder) for holder in set(holders)}

# Initializing the HTML file
tbody = ''
for GodMode in GodModes.index:
    rank = int(GodModes['Rank'][GodMode])

    external_url = GodModes['External URL'][GodMode]

    image = GodModes['Thumbnail'][GodMode]
    if pd.isnull(image):
        image = GodModes['Gateway'][GodMode]
        image = image.replace('upload', 'upload/w_256,h_256')
        
    width = '200'
    height = '148'
    image.replace('w_256,h_256', f'w_{width},h_{height}')
    full_size = GodModes['Full size image'][GodMode]
    three_d_image = './Contents/3D.png'

    opensea_link = 'https://opensea.io/assets/ethereum/'
    opensea_link += '0x903e2f5d42ee23156d548dd46bb84b7873789e44'
    opensea_logo = './Contents/Logomark-Blue.png'
    opensea_query = 'https://opensea.io/collection/godmode-by-flipsidecrypto' \
    + '?search[stringTraits][0][name]={trait}' \
    + '&amp;search[stringTraits][0][values][0]={value}'

    summoner = GodModes['Summoner'][GodMode]
    summon_transaction = GodModes['Summon TXN'][GodMode]
    mint_date = GodModes['Mint date'][GodMode]

    owner = GodModes['Current owner'][GodMode]
    holds = holders.setdefault(owner, 1)
    holds = (f'Holds {holds} Godmodes', f'Holds {holds} Godmode')[holds == 1]

    Back = GodModes['Back'][GodMode]
    Front = GodModes['Front'][GodMode]
    Under = GodModes['Under'][GodMode]
    Spectrum = GodModes['Spectrum'][GodMode]
    Substance = GodModes['Substance'][GodMode]

    Back_PoO = GodModes['Back_PoO'][GodMode]
    Front_PoO = GodModes['Front_PoO'][GodMode]
    Under_PoO = GodModes['Under_PoO'][GodMode]
    Spectrum_PoO = GodModes['Spectrum_PoO'][GodMode]
    Substance_PoO = GodModes['Substance_PoO'][GodMode]

    poo = GodModes['PoO'][GodMode]

    price = GodModes['Price'][GodMode]
    usd_price = GodModes['Price in USD'][GodMode]
    last_transfer = GodModes['Last transfer'][GodMode]
    last_transfer = datetime.datetime.strftime(last_transfer, '%Y-%m-%d')
    last_transfer_transaction = GodModes['Last transfer TXN'][GodMode]

    if math.isnan(price):
        price = '-'
    else:
        price = f'''
            {price}Ξ (${usd_price} on {last_transfer})
            <br/>
            at <a href="https://etherscan.com/tx/{last_transfer_transaction}"
            target="blank_">
            {address_shortener(last_transfer_transaction)}
            </a>
        '''

    category = GodModes['Category'][GodMode]
    category_color = ''

    if category == 'Top 10 (1%)':
        category_color = 'linear-gradient(36deg, rgba(238,174,202,1) 0%,' \
        + 'rgba(171,225,171,0.38699229691876746) 75%, rgba(148,187,233,1) 100%);'
    if category == 'Top 25 (2.5%)':
        category_color = 'linear-gradient(36deg, rgba(238,174,202,1) 0%,' \
        + 'rgba(148,187,233,1) 100%);'
    if category == 'Top 50 (5%)':
        category_color = 'linear-gradient(21deg, rgba(131,58,180,1) 0%,' \
        + 'rgba(253,29,29,1) 50%, rgba(252,176,69,1) 100%);'
    if category == 'Top 100 (10%)':
        category_color = 'linear-gradient(21deg, rgba(173,173,173,1) 0%,' \
        + 'rgba(252,176,69,1) 100%);'


    row = f'''
        <td>
            {rank}
        </td>

        <td>
            <a href="{external_url}" target="blank_">
                <div class="try-me">
                    <img class="first-image" src="{image}"
                    width="{width}px" height="{height}px" alt="Try Me!" loading="lazy"/>
                    <img src="{three_d_image}" width='{width}px' alt="Try Me!" />
                </div>
            </a>

            <div style="height:5px;">
                <hr>
            </div>

            <a href="{full_size}" target="blank_">
            <b>View full-size image</b>
            </a>
            <b> | </b>
            <a href="{opensea_link}/{GodMode}" target="blank_">
            <b>View on OpenSea</b>
            <img src="{opensea_logo}" width="20px"/>
            </a>
        </td>

        <td>
            <a href="https://etherscan.io/token/{contract}?a={GodMode}"
            class="gmTooltip" data-tooltip="History of GodMode#{GodMode}"
            target="blank_">
            {GodMode}
            </a>
            <span style="display: none;"> #{GodMode} </span>
        </td>

        <td>
            <a href="https://etherscan.com/address/{summoner}"
            class="gmTooltip" data-tooltip="{summoner}"
            target="blank_">
            {address_shortener(summoner)}
            </a>
            at
            <a href="https://etherscan.com/tx/{summon_transaction}"
            class="gmTooltip" data-tooltip="{summon_transaction}"
            target="blank_">
            {address_shortener(summon_transaction)}
            </a>
            <br>
            {mint_date}
            <span style="display: none;">{summoner}</span>
            <span style="display: none;">{summon_transaction}</span>
        </td>

        <td>
            <a href="https://etherscan.io/token/{contract}?a={owner}"
            class="gmTooltip" data-tooltip="{owner}"
            target="blank_">
            {address_shortener(owner)}
            </a>
            <br/>{holds}
            <span style="display: none;">{owner}</span>
        </td>
        
        <td>
            Back:
            <a href={opensea_query.format(trait='Back', value=Back)}>
            {Back} 
            </a>
            |
            <u class="gmTooltip"
            data-tooltip="{int(Back_PoO*1000)} of 1000 GodModes ({Back_PoO*100}% have this trait)">
            {Back_PoO}</u>
            <br>
            Front:
            <a href={opensea_query.format(trait='Front', value=Front)}>
            {Front}
            </a>
            |
            <u class="gmTooltip"
            data-tooltip="{int(Front_PoO*1000)} of 1000 GodModes ({Front_PoO*100}% have this trait)">
            {Front_PoO}</u>
            <br>
            Under:
            <a href={opensea_query.format(trait='Under', value=Under)}>
            {Under}
            </a>
            |
            <u class="gmTooltip"
            data-tooltip="{int(Under_PoO*1000)} of 1000 GodModes ({Under_PoO*100}% have this trait)">
            {Under_PoO}</u>
            <br>
            Spectrum:
            <a href={opensea_query.format(trait='Spectrum', value=Spectrum)}>
            {Spectrum}
            </a>
            |
            <u class="gmTooltip"
            data-tooltip="{int(Spectrum_PoO*1000)} of 1000 GodModes ({Spectrum_PoO*100}% have this trait)">
            {Spectrum_PoO}</u>
            <br>
            Substance:
            <a href={opensea_query.format(trait='Substance', value=Substance)}>
            {Substance}
            </a>
            |
            <u class="gmTooltip"
            data-tooltip="{int(Substance_PoO*1000)} of 1000 GodModes ({Substance_PoO*100}% have this trait)">
            {Substance_PoO}</u>
            <br>
        </td>

        <td>
            <div class="gmTooltip" data-tooltip="Exact amount: {poo}">
                <u>
                    {"{}".format(poo)}
                    <br>
                    ({"{:.12f}".format(float("{}".format(poo))* 100)}%)
                </u>
            </div>
        </td>

        <td>
            {price}
        </td>

        <td style="background-image: {category_color}">
            {category}
        </td>
    '''

    tbody += f'<tr>{row}</tr>'

# Rendering the HTML file
with open('template.html', 'r') as f:
    html = f.read()

script = '''
    $(document).ready(function(){\n\t
    $('#godmodeTable').DataTable();\n\t
});
'''
thead = '''
    <tr>
        <th>Rank</th>
        <th>Godmode (3D brain available)</th>
        <th>TokenID</th>
        <th>Summoner</th>
        <th>Current owner</th>
        <th>Attributes</th>
        <th>Probability of Occurrence</th>
        <th>Last price</th>
        <th>Category</th>
    </tr>
'''

ts = datetime.datetime.fromtimestamp(int(time.time())).strftime('%Y-%m-%d %H:%M:%S')

html = html.format(
    table_script=script,
    last_update_timestamp=ts,
    table_thead=thead,
    table_tbody=tbody,
)

html = bs(html, 'lxml')
prettyHTML = html.prettify()
prettyHTML = prettyHTML.replace('&amp;', '&')

with open('godmodes.html', 'w') as f:
    f.write(prettyHTML)

toc = time.time()
elapsed_time = toc - tic
elapsed_time = '{:.3f}'.format(elapsed_time)
print(f'The elapsed time of the rendering process: ~{elapsed_time}s')