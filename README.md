# mock-alpaca-ws
helper library to translate data back and forth for trader agent

## Setup
To install dependencies and dev-deps:
```
uv sync
```

To run tests:
```
uv run pytest
```

## Background

Currently, the tabular format set forth by https://github.com/kavorite/unspool defines five fields:
* Time - Time of the event
* Event - Single character indicating the event type ('B' for Buy, 'S' for Sell, 'T' for trades)
* Asset - The trading symbol/ticker of the financial instrument
* Price - The price of the trade in decimal format, (0.0 if the applicable bid/ask is being canceled)
* Size - The quantity/volume of the trade in decimal format (0.0 when no trade occurs)

The goal is to translate to/from the serialization format defined by [Alpaca's WebSocket API](https://docs.alpaca.markets/docs/streaming-market-data#messages). See [`test_translate.py`](/tests/test_translate.py) for details.

## Requirements

This package will provide:
- a function `from_messages` able to take a [`polars`](https://docs.pola.rs/api/python/stable/reference/index.html) table and efficiently map the fields to Alpaca JSON by translating the dataframe in to a format that produces Alpaca JSON when [`pl.write_json`](https://docs.pola.rs/api/python/dev/reference/api/polars.DataFrame.write_json.html#polars.DataFrame.write_json) is called, and 
- a second function `to_messages` that produces the inverse result when run against data read with [`pl.read_json`](https://docs.pola.rs/api/python/dev/reference/api/polars.read_json.html). 

It should do that simply using SQLish polars aggregations, aliasing, and collection, in a way that all functionality can be expressed lazily for push-down optimization.

Only [Quote](https://docs.alpaca.markets/docs/real-time-stock-pricing-data#quotes) and [Trade](https://docs.alpaca.markets/docs/real-time-stock-pricing-data#trades) type message payloads need be supported by the system for now; all others can be safely ignored, as they will be removed from JSON streams during I/O by the agent.

Subject to this interface, the package will be able to generate minibatches of data that can be used to condition the model on-demand as new bursts of websocket events arrive, allowing stress-testing, risk analysis, and robustness/performance evaluation of the `trader-agent`.

## Helpful Links
- [Grouping and Aggregation](https://docs.pola.rs/user-guide/transformations/time-series/rolling/)
- [Nesting and Structures](https://docs.pola.rs/user-guide/expressions/structs/#using-multiple-columns-in-a-single-expression)
- [`pl.Expr.explode`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.explode.html#polars.Expr.explode)
- [`pl.Expr.struct.unnest`](https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.struct.unnest.html)