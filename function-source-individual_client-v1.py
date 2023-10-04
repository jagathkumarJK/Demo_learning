
import json
from googleapiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery,secretmanager

project_id = 'gsc-project-demo'         # Provide the project ID  in bigquery
dataset_name = 'gsc_data_extract_data'    # Provide the dataset name in bigquery

client_secret_path = "projects/**/secrets/**/versions/latest"     # Provide the service account key json path that is existed in secret manager 
site_name_list = ['example1' , 'example2']                         # Provide all the site/property namesof the client
site_url_list = ['https://example1.in/' , 'https://example2.in/']     # Provde the corresponding site/Property Urls for the site/property name previously described

scopes = ['https://www.googleapis.com/auth/webmasters.readonly']     # This scope is defualt for google search console API

# start_date = '2023-01-01'
# end_date = '2023-03-31' 
current_date = datetime.now()
three_days_before_date = current_date - timedelta(days=5)
start_date = three_days_before_date.strftime('%Y-%m-%d')
end_date = three_days_before_date.strftime('%Y-%m-%d')
all_dimensions = {
                'date_dim' : ['date'],
                'date_device_country_dim' : ['date', 'device', 'country'],
                'date_page_device_country_dim' : ['date', 'page', 'device', 'country'],
                'date_query_device_country_dim' : ['date', 'query', 'device', 'country']
                }

def get_secret_of_client_service_account_json(client_secret_path):
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": client_secret_path})
    service_account_credentials_json  = response.payload.data.decode("UTF-8")
    service_account_credentials_json_final = json.loads(service_account_credentials_json)
    return service_account_credentials_json_final
    
def convert_response_to_df(service,start_date,end_date,dimension_type,site_url):
    all_responses = []
    
    startRow = 0
    
    while (startRow == 0) or (startRow%25000 == 0):
        request_body = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': dimension_type,
            'rowLimit': 25000,
            'dataState': 'final',
            'startRow' : startRow}
    
        # Make a request to the Google Search Console API for the current property
        response_data = service.searchanalytics().query(siteUrl=site_url, body=request_body).execute()
        response_data_df = pd.DataFrame(response_data['rows'])
        startRow = startRow + len(response_data_df.index)
    
        for row in response_data['rows']:
            all_responses.append(row)
    
    df = pd.DataFrame(all_responses)
    df[dimension_type] = pd.DataFrame(df['keys'].tolist(), index=df.index)
    df.drop(columns=['keys'], inplace=True)
    return df

def import_df_to_bq(project_id,dataset_name,site_name,dimension_group_name,df) :
    client = bigquery.Client()
    table_id = f"{project_id}.{dataset_name}_{site_name}.{site_name}_{dimension_group_name}"  #this should be the naming convention while creating table
    job_config = bigquery.LoadJobConfig()
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)  # Make an API request.
    job.result()  # Wait for the job to complete.
    table = client.get_table(table_id)  # Make an API request


def extract_data_gsc(request):
    try:
        service_account_file = get_secret_of_client_service_account_json(client_secret_path)

        credentials = service_account.Credentials.from_service_account_info(service_account_file, scopes=scopes)
        
        # Build the Google Search Console API service
        service = build('searchconsole', 'v1', credentials=credentials)

        # Iterate through each property for the client
        for site_url,site_name in zip(site_url_list,site_name_list):

            #Iterate though each dimension group
            for dimension_group_name , dimension_type in all_dimensions.items():
                try:
                    import_df_to_bq(project_id,dataset_name,site_name,dimension_group_name,convert_response_to_df(service,start_date,end_date,dimension_type,site_url))
                except Exception as e:   
                        print(f"Error processing {dimension_group_name} for {site_name}: {str(e)}")
                            
        response_data = {'message': 'Data extraction and loading completed successfully'}
        return response_data, 200
        
    except Exception as e:
        error_message = str(e)
        return {'error': error_message}, 500


