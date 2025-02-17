import polars as pl
import pytest
from datetime import datetime, timezone
from mock_alpaca_ws import from_messages, to_messages

@pytest.fixture
def sample_tabular_data():
    return pl.DataFrame({
        "Time": [datetime(2024, 7, 24, 7, 56, 53, 639713, tzinfo=timezone.utc)],
        "Event": ["T"],  # T for trade
        "Asset": ["FAKEPACA"],
        "Price": [133.85],
        "Size": [4.0]
    })

@pytest.fixture
def sample_alpaca_data():
    # Note: This represents a quote message that will be flattened into bid/ask rows
    # The from_messages function will need to convert this flat format back to Alpaca's nested format
    return pl.DataFrame({
        "Time": [datetime(2024, 7, 24, 7, 56, 53, 639713, tzinfo=timezone.utc)],
        "Event": ["B"],  # B for bid
        "Asset": ["FAKEPACA"],
        "Price": [133.85],
        "Size": [4.0]
    }).vstack(
        pl.DataFrame({
            "Time": [datetime(2024, 7, 24, 7, 56, 53, 639713, tzinfo=timezone.utc)],
            "Event": ["S"],  # S for ask (sell)
            "Asset": ["FAKEPACA"],
            "Price": [135.77],
            "Size": [5.0]
        })
    )

def test_from_messages_dataframe(sample_tabular_data):
    result = from_messages(sample_tabular_data)
    assert isinstance(result, pl.DataFrame)
    assert all(col in result.columns for col in ["T", "S", "t", "bp", "bs", "ap", "as"])

def test_from_messages_lazyframe(sample_tabular_data):
    lazy_df = sample_tabular_data.lazy()
    result = from_messages(lazy_df)
    assert isinstance(result, pl.LazyFrame)
    result_df = result.collect()
    assert all(col in result_df.columns for col in ["T", "S", "t", "bp", "bs", "ap", "as"])

def test_to_messages_dataframe(sample_alpaca_data):
    result = to_messages(sample_alpaca_data)
    assert isinstance(result, pl.DataFrame)
    assert all(col in result.columns for col in ["Time", "Event", "Asset", "Price", "Size"])

def test_to_messages_lazyframe(sample_alpaca_data):
    lazy_df = sample_alpaca_data.lazy()
    result = to_messages(lazy_df)
    assert isinstance(result, pl.LazyFrame)
    result_df = result.collect()
    assert all(col in result_df.columns for col in ["Time", "Event", "Asset", "Price", "Size"])

def test_roundtrip_conversion(sample_tabular_data):
    """Test that converting to Alpaca format and back preserves data"""
    alpaca_format = from_messages(sample_tabular_data)
    result = to_messages(alpaca_format)
    
    # Compare original and final dataframes
    pl.assert_frame_equal(result, sample_tabular_data)

def test_quote_flattening():
    """Test that quote messages are properly flattened into bid/ask rows"""
    # Create a quote message in Alpaca format
    quote_df = pl.DataFrame({
        "T": ["q"],
        "S": ["FAKEPACA"],
        "t": [datetime(2024, 7, 24, 7, 56, 53, 639713, tzinfo=timezone.utc)],
        "bp": [133.85],
        "bs": [4],
        "ap": [135.77],
        "as": [5],
        "bx": ["O"],
        "ax": ["R"],
        "c": [["R"]],
        "z": ["A"]
    })
    
    result = to_messages(quote_df)
    
    # Should produce two rows: one for bid (B) and one for ask (S)
    assert len(result) == 2
    assert set(result["Event"].to_list()) == {"B", "S"}
    
    # Check bid row
    bid_row = result.filter(pl.col("Event") == "B")
    assert bid_row["Price"][0] == 133.85
    assert bid_row["Size"][0] == 4.0
    
    # Check ask row
    ask_row = result.filter(pl.col("Event") == "S")
    assert ask_row["Price"][0] == 135.77
    assert ask_row["Size"][0] == 5.0