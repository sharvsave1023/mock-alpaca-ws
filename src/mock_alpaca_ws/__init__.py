import polars as pl

def from_messages(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    """
    Convert from tabular format (Time, Event, Asset, Price, Size) to Alpaca WebSocket format
    
    Args:
        df: Input DataFrame/LazyFrame with columns: Time, Event, Asset, Price, Size
        
    Returns:
        DataFrame/LazyFrame with columns matching Alpaca WebSocket JSON structure
    """
    raise NotImplementedError()

def to_messages(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    """
    Convert from Alpaca WebSocket format to tabular format
    
    Args:
        df: Input DataFrame/LazyFrame with Alpaca WebSocket structure
        
    Returns:
        DataFrame/LazyFrame with columns: Time, Event, Asset, Price, Size
    """
    raise NotImplementedError()