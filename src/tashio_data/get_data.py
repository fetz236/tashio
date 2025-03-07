import requests
import boto3
import os
from typing import Dict, Any

def get_polygon_options_data(api_key: str, symbol: str, expiration_date: str) -> Dict[str, Any]:
    """
    Fetch options contracts data from Polygon.
    Note: Adjust the URL and parameters according to the specific endpoint
    and your data needs (e.g., filtering by expiration date, strikes, etc.).
    """
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

def store_data_dynamodb(data: Dict[str, Any], table_name: str = "options_data") -> None:
    """
    Store options data in an AWS DynamoDB table.
    
    This example assumes the Polygon response includes a 'results' key with a list of option contracts.
    Adjust the keys according to the actual API response.
    
    IMPORTANT:
    - Ensure that the DynamoDB table exists and has the appropriate primary key configuration.
    - In this example, 'contract_ticker' is used as the primary key.
    """
    # Initialize the DynamoDB resource
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    options_list = data.get("results", [])
    
    # Insert each option record into the DynamoDB table.
    for option in options_list:
        contract_ticker = option.get("contract_ticker", "")
        strike_price = option.get("strike_price", 0.0)
        expiration_date = option.get("expiration_date", None)
        option_type = option.get("type", "")  # e.g., "call" or "put"
        
        # Construct the item to insert.
        item = {
            "contract_ticker": contract_ticker,
            "strike_price": strike_price,
            "expiration_date": expiration_date,
            "option_type": option_type
        }
        
        # Put the item into the DynamoDB table.
        table.put_item(Item=item)
    
    print("Stored options data in DynamoDB.")

def get_data() -> Dict[str, Any]:
    """
    Retrieves options data from Polygon and stores it in DynamoDB.
    Returns the raw data for further processing if needed.
    """
    # Ensure that your Polygon API key is set in the environment variable 'POLYGON_API_KEY'
    polygon_api_key = os.environ.get("POLYGON_API_KEY", "")
    if not polygon_api_key:
        raise ValueError("Please set your Polygon API key in the POLYGON_API_KEY environment variable.")
    
    # Define the underlying asset symbol and a sample expiration date.
    symbol = "AAPL"
    expiration_date = "2025-03-15"  # Adjust as needed
    
    # Fetch the data from Polygon.
    data = get_polygon_options_data(polygon_api_key, symbol, expiration_date)
    
    # Store the fetched data in DynamoDB.
    store_data_dynamodb(data)
    
    return data

