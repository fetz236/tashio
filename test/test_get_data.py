from tashio_data.get_data import get_polygon_options_data, store_data_timestream

def test_get_polygon_options_data(monkeypatch):
    sample_response = {
        "results": [
            {
                "ticker": "TEST",
                "strike_price": 100,
                "expiration_date": "2025-03-07",
                "contract_type": "call",
                "underlying_ticker": "TESTUNDERLYING"
            }
        ],
        "next_url": None,
    }

    class DummyResponse:
        def __init__(self, json_data, status_code):
            self._json = json_data
            self.status_code = status_code

        def json(self):
            return self._json

    def fake_get(url):
        return DummyResponse(sample_response, 200)

    monkeypatch.setattr("requests.get", fake_get)
    data = get_polygon_options_data("dummy_api_key", "TEST", "2025-03-07")
    assert data == sample_response


def test_store_data_timestream(monkeypatch):
    # Create a dummy Timestream client that records calls to write_records.
    class DummyTimestreamClient:
        def __init__(self):
            self.calls = []

        def write_records(self, DatabaseName, TableName, Records):
            self.calls.append({
                "DatabaseName": DatabaseName,
                "TableName": TableName,
                "Records": Records
            })
            return {}

    dummy_client = DummyTimestreamClient()

    def fake_boto3_client(service_name, region_name=None):
        if service_name == "timestream-write":
            return dummy_client
        raise Exception("Unexpected service name: " + service_name)

    monkeypatch.setattr("boto3.client", fake_boto3_client)

    sample_data = {
        "results": [
            {
                "ticker": "O:TEST1",
                "strike_price": 100,
                "expiration_date": "2025-03-07",
                "contract_type": "call",
                "underlying_ticker": "TESTUNDERLYING"
            },
            {
                "ticker": "O:TEST2",
                "strike_price": 110,
                "expiration_date": "2025-03-07",
                "contract_type": "call",
                "underlying_ticker": "TESTUNDERLYING"
            },
        ]
    }

    db_name = "TestDB"
    table_name = "OptionsPrice"
    store_data_timestream(sample_data, db_name=db_name, table_name=table_name)

    # Verify that one batch insert was made.
    assert len(dummy_client.calls) == 1, "Expected one batch insert call"
    call = dummy_client.calls[0]
    assert call["DatabaseName"] == db_name
    assert call["TableName"] == table_name

    records = call["Records"]
    assert isinstance(records, list)
    assert len(records) == 2, "Expected two records in the batch"

    # Check that each record has the expected dimensions (first dimension is OptionID).
    tickers = {record["Dimensions"][0]["Value"] for record in records}
    assert "O:TEST1" in tickers
    assert "O:TEST2" in tickers
