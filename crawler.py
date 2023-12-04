# -*- coding: utf-8 -*-

import time
import logging
import requests
import warnings

import pandas as pd
import numpy as np

logging.basicConfig(filename='crawler.log', encoding='utf-8', level=logging.DEBUG)

warnings.filterwarnings("ignore")


def get_startups_from_abstartups():
    default_url = 'https://wabi-brazil-south-api.analysis.windows.net/public/reports/querydata'
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.6',
        'ActivityId': '7ee02cbe-3469-f4a7-42d1-d9572c4557c5',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json;charset=UTF-8',
        'Origin': 'https://app.powerbi.com',
        'Pragma': 'no-cache',
        'Referer': 'https://app.powerbi.com/',
        'RequestId': '5c7467c9-e010-735b-697a-47547cc9d052',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'Sec-GPC': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'X-PowerBI-ResourceKey': '2d1592b0-f51d-41d9-b999-0a55418e961e',
        'sec-ch-ua': '"Brave";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
    }

    params = {
        'synchronous': 'true',
    }

    json_data = {
        'version': '1.0.0',
        'queries': [
            {
                'Query': {
                    'Commands': [
                        {
                            'SemanticQueryDataShapeCommand': {
                                'Query': {
                                    'Version': 2,
                                    'From': [
                                        {
                                            'Name': 'd',
                                            'Entity': 'dStartup',
                                            'Type': 0,
                                        },
                                        {
                                            'Name': 'd1',
                                            'Entity': 'dLocalização',
                                            'Type': 0,
                                        },
                                        {
                                            'Name': 'd2',
                                            'Entity': 'dCalendário',
                                            'Type': 0,
                                        },
                                    ],
                                    'Select': [
                                        {
                                            'Column': {
                                                'Expression': {
                                                    'SourceRef': {
                                                        'Source': 'd',
                                                    },
                                                },
                                                'Property': 'Nome_Startup',
                                            },
                                            'Name': 'dStartup.Nome_Startup',
                                        },
                                        {
                                            'Column': {
                                                'Expression': {
                                                    'SourceRef': {
                                                        'Source': 'd1',
                                                    },
                                                },
                                                'Property': 'Estado',
                                            },
                                            'Name': 'dLocalização.Estado',
                                        },
                                        {
                                            'Column': {
                                                'Expression': {
                                                    'SourceRef': {
                                                        'Source': 'd1',
                                                    },
                                                },
                                                'Property': 'Cidade',
                                            },
                                            'Name': 'dLocalização.Cidade',
                                        },
                                        {
                                            'Column': {
                                                'Expression': {
                                                    'SourceRef': {
                                                        'Source': 'd1',
                                                    },
                                                },
                                                'Property': 'UF',
                                            },
                                            'Name': 'dLocalização.UF',
                                        },
                                        {
                                            'Column': {
                                                'Expression': {
                                                    'SourceRef': {
                                                        'Source': 'd',
                                                    },
                                                },
                                                'Property': 'Segmento Atuação_Descrição',
                                            },
                                            'Name': 'dStartup.Segmento Atuação_Descrição',
                                        },
                                        {
                                            'Column': {
                                                'Expression': {
                                                    'SourceRef': {
                                                        'Source': 'd',
                                                    },
                                                },
                                                'Property': 'Link',
                                            },
                                            'Name': 'dStartup.Link',
                                        },
                                        {
                                            'Column': {
                                                'Expression': {
                                                    'SourceRef': {
                                                        'Source': 'd',
                                                    },
                                                },
                                                'Property': 'Público Alvo',
                                            },
                                            'Name': 'dStartup.Público Alvo',
                                        },
                                    ],
                                    'Where': [
                                        {
                                            'Condition': {
                                                'In': {
                                                    'Expressions': [
                                                        {
                                                            'Column': {
                                                                'Expression': {
                                                                    'SourceRef': {
                                                                        'Source': 'd2',
                                                                    },
                                                                },
                                                                'Property': 'Ano',
                                                            },
                                                        },
                                                    ],
                                                    'Values': [
                                                        [
                                                            {
                                                                'Literal': {
                                                                    'Value': '2023L',
                                                                },
                                                            },
                                                        ],
                                                    ],
                                                },
                                            },
                                        },
                                    ],
                                    'OrderBy': [
                                        {
                                            'Direction': 1,
                                            'Expression': {
                                                'Column': {
                                                    'Expression': {
                                                        'SourceRef': {
                                                            'Source': 'd',
                                                        },
                                                    },
                                                    'Property': 'Nome_Startup',
                                                },
                                            },
                                        },
                                    ],
                                },
                                'Binding': {
                                    'Primary': {
                                        'Groupings': [
                                            {
                                                'Projections': [
                                                    0,
                                                    1,
                                                    2,
                                                    3,
                                                    4,
                                                    5,
                                                    6,
                                                ],
                                                'Subtotal': 1,
                                            },
                                        ],
                                    },
                                    'DataReduction': {
                                        'DataVolume': 50000,
                                        'Primary': {
                                            'Window': {
                                                'Count': 50000,
                                            },
                                        },
                                    },
                                    'Version': 1,
                                },
                                'ExecutionMetricsKind': 1,
                            },
                        },
                    ],
                },
                'CacheKey': '{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"d","Entity":"dStartup","Type":0},{"Name":"d1","Entity":"dLocalização","Type":0},{"Name":"d2","Entity":"dCalendário","Type":0}],"Select":[{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Nome_Startup"},"Name":"dStartup.Nome_Startup"},{"Column":{"Expression":{"SourceRef":{"Source":"d1"}},"Property":"Estado"},"Name":"dLocalização.Estado"},{"Column":{"Expression":{"SourceRef":{"Source":"d1"}},"Property":"Cidade"},"Name":"dLocalização.Cidade"},{"Column":{"Expression":{"SourceRef":{"Source":"d1"}},"Property":"UF"},"Name":"dLocalização.UF"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Segmento Atuação_Descrição"},"Name":"dStartup.Segmento Atuação_Descrição"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Link"},"Name":"dStartup.Link"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Público Alvo"},"Name":"dStartup.Público Alvo"}],"Where":[{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"d2"}},"Property":"Ano"}}],"Values":[[{"Literal":{"Value":"2023L"}}]]}}}],"OrderBy":[{"Direction":1,"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Nome_Startup"}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0,1,2,3,4,5,6],"Subtotal":1}]},"DataReduction":{"DataVolume":3,"Primary":{"Window":{"Count":500}}},"Version":1},"ExecutionMetricsKind":1}}]}',
                'QueryId': '',
                'ApplicationContext': {
                    'DatasetId': 'efa862f8-ba39-4fd7-bca2-4eb430300764',
                    'Sources': [
                        {
                            'ReportId': '7edcafe8-bad0-49b6-9399-27c389628f40',
                            'VisualId': '1fc05ca20340ccb50557',
                        },
                    ],
                },
            },
        ],
        'cancelQueries': [],
        'modelId': 6573501,
    }

    logging.info(f'Crawling site from: {default_url}')
    response = requests.post(
        default_url,
        params=params,
        headers=headers,
        json=json_data,
    )

    # Coleta os dados do json
    data = response.json()['results'][0]['result']['data']['dsr']['DS'][0]['PH'][0]['DM0']

    # Coleta as referencias d0, d1, d2, d3, d4, d5, d6, d7
    reference = {}
    reference['Nome'] = response.json()['results'][0]['result']['data']['dsr']['DS'][0]['ValueDicts']['D0']
    reference['Estado'] = response.json()['results'][0]['result']['data']['dsr']['DS'][0]['ValueDicts']['D1']
    reference['Cidade'] = response.json()['results'][0]['result']['data']['dsr']['DS'][0]['ValueDicts']['D2']
    reference['UF'] = response.json()['results'][0]['result']['data']['dsr']['DS'][0]['ValueDicts']['D3']
    reference['Segmento'] = response.json()['results'][0]['result']['data']['dsr']['DS'][0]['ValueDicts']['D4']
    reference['URL'] = response.json()['results'][0]['result']['data']['dsr']['DS'][0]['ValueDicts']['D5']
    reference['Publico'] = response.json()['results'][0]['result']['data']['dsr']['DS'][0]['ValueDicts']['D6']

    # Cria o dataframe com as colunas de interesse
    columns = ['Nome', 'Estado', 'Cidade', 'UF', 'Segmento', 'URL', 'Publico']
    df = pd.DataFrame(columns=columns)

    logging.info(f'Parsing the data from: {default_url}')
    # Itera sobre os dados
    for d in data:
        row = {}

        # Verifica o valor 'R' (responsável por identificar quais valores se repetem entre linhas consecutivas)
        # Atribui o np.nan para as colunas repetidas (mais a frente vai ser utilizado o fillna com ffill para preencher os valores)
        if 'R' in d:
            repeat = d['R']
            if repeat == 64:
                row['Publico'] = np.nan
            elif repeat == 80:
                row['Publico'] = np.nan
                row['Segmento'] = np.nan
            elif repeat == 74:
                row['Estado'] = np.nan
                row['UF'] = np.nan
                row['Publico'] = np.nan
            elif repeat == 78:
                row['Estado'] = np.nan
                row['UF'] = np.nan
                row['Publico'] = np.nan
                row['Cidade'] = np.nan
            elif repeat == 14:
                row['Estado'] = np.nan
                row['UF'] = np.nan
                row['Cidade'] = np.nan
            elif repeat == 94:
                row['Estado'] = np.nan
                row['UF'] = np.nan
                row['Publico'] = np.nan
                row['Cidade'] = np.nan
                row['Segmento'] = np.nan
            elif repeat == 90:
                row['Estado'] = np.nan
                row['UF'] = np.nan
                row['Publico'] = np.nan
                row['Segmento'] = np.nan
            elif repeat == 10:
                row['Estado'] = np.nan
                row['UF'] = np.nan
            elif repeat == 16:
                row['Segmento'] = np.nan
            elif repeat == 26:
                row['Estado'] = np.nan
                row['UF'] = np.nan
                row['Segmento'] = np.nan
            elif repeat == 30:
                row['Estado'] = np.nan
                row['UF'] = np.nan
                row['Segmento'] = np.nan
                row['Cidade'] = np.nan
            else:
                print(f'achei um cara novo REPEATING: {repeat}')

        # Verifica o valor 'Ø' (responsável por identificar quais valores estão ausentes na linha)
        # Atribui - aos valores ausentes
        if 'Ø' in d:
            missing = d['Ø']
            if missing == 112:
                row['Segmento'] = '-'
                row['Publico'] = '-'
                row['URL'] = '-'
            elif missing == 64:
                row['Publico'] = '-'
            elif missing == 80:
                row['Publico'] = '-'
                row['Segmento'] = '-'
            elif missing == 96:
                row['Publico'] = '-'
                row['Segmento'] = '-'
                row['URL'] = '-'
            elif missing == 74:
                row['Estado'] = '-'
                row['UF'] = '-'
                row['Publico'] = '-'
            elif missing == 78:
                row['Estado'] = '-'
                row['UF'] = '-'
                row['Publico'] = '-'
                row['Cidade'] = '-'
            elif missing == 94:
                row['Estado'] = '-'
                row['UF'] = '-'
                row['Publico'] = '-'
                row['Cidade'] = '-'
                row['Segmento'] = '-'
            elif missing == 90:
                row['Estado'] = '-'
                row['UF'] = '-'
                row['Publico'] = '-'
                row['Segmento'] = '-'
            elif missing == 10:
                row['Estado'] = '-'
                row['UF'] = '-'
            elif missing == 16:
                row['Segmento'] = '-'
            elif missing == 32:
                row['URL'] = '-'
            elif missing == 26:
                row['Estado'] = '-'
                row['UF'] = '-'
                row['Segmento'] = '-'
            elif missing == 30:
                row['Estado'] = '-'
                row['UF'] = '-'
                row['Segmento'] = '-'
                row['Cidade'] = '-'
            elif missing == 14:
                row['Estado'] = '-'
                row['UF'] = '-'
                row['Cidade'] = '-'
            else:
                print(f'achei um cara novo MISSING: {missing}')

        # Itera sobre colunas do dataframe complementando os dados da linha com os dados lidos
        # Ao final tem-se a nova linha que deve ser acrescentada ao dataframe
        count = 0
        for c in columns:
            if c in row.keys():
                pass
            else:
                row[c] = d['C'][count]
                count = count + 1

        df = df.append(row, ignore_index=True)

    df = df.fillna(method='ffill')

    # Substitui os valores codificados por valores reais conforme as referencias
    for c in columns:
        df[c] = df[c].replace(dict(enumerate(reference[c])))

    df = df.rename(columns={'Segmento': 'Setor de atuação', 'Nome': 'Nome da startup', 'URL': 'Informações de contato'})
    df["Localização"] = df["Cidade"].str.cat(df["Estado"], sep=" - ")
    df = df.drop(['Publico', 'Estado', 'Cidade', 'UF'], axis=1)

    return df


def run_crawlers():
    df_abs_startups = get_startups_from_abstartups()
    return df_abs_startups


if __name__ == '__main__':
    start_time = time.time()
    logging.info("Crawling started")
    df = run_crawlers()
    total_time = "%s seconds" % (time.time() - start_time)
    total_items = len(df.index)
    df.to_excel('brazil_startups_list.xlsx', sheet_name='Startups', index=False)
    logging.info(f"Crawling finished\nCrawled {total_items} items in {total_time}")
