import requests
import time
from typing import Dict, Any
import boto3

def get_polygon_options_data(
    api_key: str, symbol: str, expiration_date: str, url: str = None
) -> Dict[str, Any]:
    """
    Fetch options contracts data from Polygon.
    If `url` is provided, it is used for pagination.
    Otherwise, the URL is built using the provided parameters.
    """
    if not url:
        url = (
            f"https://api.polygon.io/v3/reference/options/contracts"
            f"?underlying_ticker={symbol}"
            f"&expiration_date={expiration_date}"
            f"&apiKey={api_key}"
        )
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        return {}

def store_data_timestream(data: Dict[str, Any], db_name: str, table_name: str) -> None:
    """
    Stores aggregated options data in Amazon Timestream using batch inserts.
    
    Each record is written as a multi-measure record into the OptionsPrice table.
    We assume a Timestream schema where:
      - The primary dimension 'OptionID' (here we use the 'ticker') partitions the data.
      - The 'Time' field (epoch in seconds, as a string) is the timestamp.
      - MeasureValues include: price, volume, and implied_volatility.
      
    Note: The API response from Polygon may not include all measures (e.g. current price or volume),
    so adjust the mapping as needed.
    """
    client = boto3.client('timestream-write', region_name='eu-west-1')
    records = []
    options_list = data.get("results", [])
    
    for option in options_list:
        # Use the 'ticker' as the OptionID.
        option_id = option.get("ticker", "")
        # For demonstration, we use strike_price as a proxy for "price"
        price = option.get("strike_price", 0.0)
        # Use 0 if volume or implied volatility are not provided.
        volume = option.get("volume", 0)
        implied_vol = option.get("implied_volatility", 0.0)
        
        # For historical data, you would parse a timestamp from the API.
        # Here we simulate by using the current time.
        current_time = int(time.time())
        
        record = {
            'Dimensions': [
                {'Name': 'OptionID', 'Value': option_id},
                {'Name': 'Underlying', 'Value': option.get("underlying_ticker", "")},
                {'Name': 'ContractType', 'Value': option.get("contract_type", "")},
            ],
            'MeasureName': 'OptionMetrics',  # constant for multi-measure record
            'MeasureValues': [
                {'Name': 'price', 'Value': str(price), 'Type': 'DOUBLE'},
                {'Name': 'volume', 'Value': str(volume), 'Type': 'BIGINT'},
                {'Name': 'implied_volatility', 'Value': str(implied_vol), 'Type': 'DOUBLE'},
            ],
            'Time': str(current_time),
            'TimeUnit': 'SECONDS'
        }
        records.append(record)
        
        # Timestream batch API supports up to 100 records per request.
        if len(records) == 100:
            try:
                client.write_records(DatabaseName=db_name, TableName=table_name, Records=records)
                print(f"Inserted {len(records)} records to Timestream")
            except Exception as e:
                print(f"Error writing records: {e}")
            records = []

    # Write any remaining records.
    if records:
        try:
            client.write_records(DatabaseName=db_name, TableName=table_name, Records=records)
            print(f"Inserted final batch of {len(records)} records to Timestream")
        except Exception as e:
            print(f"Error writing final records: {e}")

def get_data(api_key: str, symbol: str, expiration_date: str) -> Dict[str, Any]:
    """
    Aggregates options data from Polygon across multiple pages for a given symbol.
    Returns a dictionary with a key "results" containing a list of option records.
    """
    aggregated_data: Dict[str, Any] = {"results": []}
    # Fetch the first page.
    data = get_polygon_options_data(api_key, symbol, expiration_date)
    aggregated_data["results"].extend(data.get("results", []))
    next_url = data.get("next_url")
    
    # Loop over subsequent pages if available.
    while next_url:
        data = get_polygon_options_data(api_key, symbol, expiration_date, url=next_url)
        aggregated_data["results"].extend(data.get("results", []))
        next_url = data.get("next_url")
    
    return aggregated_data
