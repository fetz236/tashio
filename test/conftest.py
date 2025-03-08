import pytest
import boto3
from moto import mock_aws


@pytest.fixture()
def dynamodb_table():
    # Start the mock DynamoDB service.
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="eu-west-1")
        # Create the table matching your production schema.
        table = dynamodb.create_table(
            TableName="options_data",
            KeySchema=[{"AttributeName": "contract_ticker", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "contract_ticker", "AttributeType": "S"}
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        )
        table.wait_until_exists()
        yield table
        # The context manager will automatically tear down the mocked service.
