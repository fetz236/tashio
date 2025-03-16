import requests
import time
from typing import Dict, Any
import boto3

def get_polygon_options_data(api_key: str, symbol: str, expiration_date: str, url: str = None) -> Dict[str, Any]:
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
    return response.json() if response.status_code == 200 else {}

def _create_timestream_record(option: Dict[str, Any], current_time: int) -> Dict[str, Any]:
    """Helper function to create a single Timestream record from an option."""
    return {
        'Dimensions': [
            {'Name': 'OptionID', 'Value': option.get("ticker", "")},
            {'Name': 'Underlying', 'Value': option.get("underlying_ticker", "")},
            {'Name': 'ContractType', 'Value': option.get("contract_type", "")},
        ],
        'MeasureName': 'OptionMetrics',
        'MeasureValues': [
            {'Name': 'price', 'Value': str(option.get("strike_price", 0.0)), 'Type': 'DOUBLE'},
            {'Name': 'volume', 'Value': str(option.get("volume", 0)), 'Type': 'BIGINT'},
            {'Name': 'implied_volatility', 'Value': str(option.get("implied_volatility", 0.0)), 'Type': 'DOUBLE'},
        ],
        'Time': str(current_time),
        'TimeUnit': 'SECONDS'
    }

def _write_batch_to_timestream(client: Any, records: list, db_name: str, table_name: str) -> None:
    """Helper function to write a batch of records to Timestream."""
    try:
        client.write_records(DatabaseName=db_name, TableName=table_name, Records=records)
        print(f"Inserted {len(records)} records to Timestream")
    except Exception as e:
        print(f"Error writing records: {e}")

def store_data_timestream(data: Dict[str, Any], db_name: str, table_name: str) -> None:
    """
    Stores aggregated options data in Amazon Timestream using batch inserts.
    
    Each record is written as a multi-measure record with dimensions for OptionID,
    Underlying, and ContractType. MeasureValues include price, volume, and implied_volatility.
    """
    client = boto3.client('timestream-write', region_name='eu-west-1')
    records = []
    current_time = int(time.time())
    
    for option in data.get("results", []):
        records.append(_create_timestream_record(option, current_time))
        
        if len(records) == 100:  # Timestream batch API limit
            _write_batch_to_timestream(client, records, db_name, table_name)
            records = []

    if records:  # Write any remaining records
        _write_batch_to_timestream(client, records, db_name, table_name)

def get_data(api_key: str, symbol: str, expiration_date: str) -> Dict[str, Any]:
    """
    Aggregates options data from Polygon across multiple pages for a given symbol.
    Returns a dictionary with a key "results" containing a list of option records.
    """
    aggregated_data = {"results": []}
    next_url = None
    
    while True:
        data = get_polygon_options_data(api_key, symbol, expiration_date, url=next_url)
        aggregated_data["results"].extend(data.get("results", []))
        
        next_url = data.get("next_url")
        if not next_url:
            break
            
    return aggregated_data
