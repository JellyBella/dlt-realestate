import json
from google.auth.transport.requests import Request
import dlt
from google.cloud import secretmanager_v1
import google.auth
import requests
import os
import pandas as pd

def get_api_secret_key():
    return json.loads(access_secret_version("YOUR-GCP-PROJECT-ID", "ACCESS-KEY", version_id="1"))

def access_secret_version(project_id, secret_id, version_id):
    client = secretmanager_v1.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    request = secretmanager_v1.AccessSecretVersionRequest(name=secret_name)
    response = client.access_secret_version(request)
    return response.payload.data.decode('UTF-8')

# @dlt.source
# def property_analytics_source(suburb, api_secret_key=None):
    # api_secret_key = get_api_secret_key()
    # return realestate_analytics(suburb)

def _create_auth_headers(api_secret_key):
    try:
        credentials, _ = google.auth.default()
        auth_request = Request()
        credentials.refresh(auth_request)
        headers = {"Authorization": f"Bearer {credentials.token}"}
    except Exception as e:
        print(f"Error during authentication: {e}")

    headers = {"Authorization": f"Bearer {credentials.token}"}
    return headers

@dlt.resource(write_disposition="append")
def realestate_analytics(suburb):
    api_secret_key = get_api_secret_key()
    headers = _create_auth_headers(api_secret_key)
    # print(headers)

    url = "https://realty-in-au.p.rapidapi.com/properties/list"
    rapidapi_key = os.environ.get("RAPIDAPI_KEY")
    rapidapi_host = os.environ.get("RAPIDAPI_HOST")

    rapidapi_headers = {
        "X-RapidAPI-Key": rapidapi_key,
        "X-RapidAPI-Host": rapidapi_host
    }

    page = 1
    pagesize = 30
    # loaded_suburbs = {}
    data = []

    while True:
        querystring = {
            "channel": "sold",
            "searchLocation": suburb,
            "searchLocationSubtext": "Region",
            "type": "region",
            "maxSoldAge": "12",
            "sortType": "relevance",
            "page": page,
            "pageSize": pagesize,
            "surroundingSuburbs": False
        }

        try:
            response = requests.get(url, headers={**rapidapi_headers}, params=querystring, timeout=10)
            response.raise_for_status()

            page_json = response.json()
            page_len = len(page_json['tieredResults'][0]['results'])
            total_count = page_json['tieredResults'][0]['count']
            
            print(f'{suburb}: page number {page} with {page_len} records ')
            data.append(page_json)

            if page > 50 or page_len == 0 or page >= total_count/pagesize or int(response.headers['X-RateLimit-Requests-Remaining']) <= 6000:
                print(f'tier 1 results exhausted')
                break 
        except requests.exceptions.RequestException as e:
            print(f"Error during API request: {e}")
            break

        page += 1

    yield data

def realestate_run():

    pipeline = dlt.pipeline(
        pipeline_name='realestate_list',
        destination='bigquery', 
        staging='filesystem', #staging files in GCP buckets
        dataset_name='realestate_sydney'    
    )

    sydney_suburbs_df = pd.read_csv('sydney_suburbs.csv')
    for suburb in sydney_suburbs_df['Suburb']:
        load_info = pipeline.run(realestate_analytics(suburb))
        print(load_info)

if __name__ == '__main__':
    realestate_run()
