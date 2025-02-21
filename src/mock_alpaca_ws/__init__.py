import polars as pl
from datetime import datetime, timezone

def from_messages(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    """
    Convert from tabular format (Time, Event, Asset, Price, Size) to Alpaca WebSocket format
    
    Args:
        df: Input DataFrame/LazyFrame with columns: Time, Event, Asset, Price, Size
        
    Returns:
        DataFrame/LazyFrame with columns matching Alpaca WebSocket JSON structure
    """
    is_lazy = isinstance(df, pl.LazyFrame)
    if is_lazy:
        df = df.collect()
    trades = df.filter(pl.col("Event") == "T")
    bids = df.filter(pl.col("Event") == "B")
    asks = df.filter(pl.col("Event") == "S")

    result_cols = {
        "T": [],
        "S": [],
        "t": [],
        "p": [],  # Changed from bp to p for trades
        "s": [],  # Changed from bs to s for trades
        "bp": [],
        "bs": [],
        "ap": [],
        "as": []
    }
    
    if len(trades) > 0:
        trades = trades.with_columns([
            pl.lit("t").alias("T"),
            pl.col("Asset").alias("S"),
            pl.col("Time").alias("t"),
            pl.col("Price").alias("p"), # p price
            pl.col("Size").alias("s"),   # s size
            pl.lit(None).cast(pl.Float64).alias("bp"),
            pl.lit(None).cast(pl.Float64).alias("bs"),
            pl.lit(None).cast(pl.Float64).alias("ap"),
            pl.lit(None).cast(pl.Float64).alias("as")
        ]).select(["T", "S", "t", "p", "s", "bp", "bs", "ap", "as"])
    
    if len(bids) > 0 and len(asks) > 0:
        quotes = bids.join(
            asks,
            on=["Time", "Asset"],
            how="outer"
        ).with_columns([
            pl.lit("q").alias("T"),
            pl.col("Asset").alias("S"),
            pl.col("Time").alias("t"),
            pl.lit(None).cast(pl.Float64).alias("p"),
            pl.lit(None).cast(pl.Float64).alias("s"),
            pl.col("Price").alias("bp"),
            pl.col("Size").alias("bs"),
            pl.col("Price_right").alias("ap"),
            pl.col("Size_right").alias("as")
        ]).select(["T", "S", "t", "p", "s", "bp", "bs", "ap", "as"])
        #itr thru and pair up
        if len(trades) > 0:
            result = pl.concat([trades, quotes])
        else:
            result = quotes
    else:
        result = trades if len(trades) > 0 else pl.DataFrame(result_cols)
    
    if is_lazy:
        return result.lazy()
    else:
        return result

def to_messages(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    """
    Convert from Alpaca WebSocket format to tabular format
    
    Args:
        df: Input DataFrame/LazyFrame with Alpaca WebSocket structure
        
    Returns:
        DataFrame/LazyFrame with columns: Time, Event, Asset, Price, Size
    """
    is_lazy = isinstance(df, pl.LazyFrame)
    if is_lazy:
        df = df.collect()
    result_rows = []
    trades = df.filter(pl.col("T") == "t")
    if len(trades) > 0:
        trades = trades.with_columns([
            pl.col("t").alias("Time"),
            pl.lit("T").alias("Event"),
            pl.col("S").alias("Asset"),
            pl.col("p").alias("Price"),  # Changed from bp to p
            pl.col("s").alias("Size")    # Changed from bs to s
        ]).select(["Time", "Event", "Asset", "Price", "Size"])
        result_rows.append(trades)
    
    # Process quotes (T = "q")
    quotes = df.filter(pl.col("T") == "q")
    if len(quotes) > 0: # bid rows
        bids = quotes.with_columns([
            pl.col("t").alias("Time"),
            pl.lit("B").alias("Event"),
            pl.col("S").alias("Asset"),
            pl.col("bp").alias("Price"),
            pl.col("bs").alias("Size")
        ]).select(["Time", "Event", "Asset", "Price", "Size"])
        # ask rows
        asks = quotes.with_columns([
            pl.col("t").alias("Time"),
            pl.lit("S").alias("Event"),
            pl.col("S").alias("Asset"),
            pl.col("ap").alias("Price"),
            pl.col("as").alias("Size")
        ]).select(["Time", "Event", "Asset", "Price", "Size"])
        result_rows.extend([bids, asks])
    if result_rows:
        result = pl.concat(result_rows)
    else:
        result = pl.DataFrame({
            "Time": [],
            "Event": [],
            "Asset": [],
            "Price": [],
            "Size": []
        })
    if is_lazy:
        return result.lazy()
    else:
        return result