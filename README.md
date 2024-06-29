# SingleStore Bytewax Connector

A high-performance connector for integrating SingleStore with Bytewax dataflows.

## Features

- Seamless integration with SingleStore's CDC (Change Data Capture) functionality
- Support for multiple event types (Insert, Update, Delete, etc.)
- Stateful processing with resumable offsets
- Compatible with Bytewax's dataflow paradigm

## Installation

Install the connector using pip:

```
pip install singlestore-bytewax-connector
```

## Quick Start

Here's a simple example of how to use the SingleStore Bytewax Connector:

```python
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from singlestore_source import SingleStoreSource

flow = Dataflow("singlestore-cdc")

source = SingleStoreSource(
    host="127.0.0.1",
    port=3306,
    user="root",
    password="password",
    database="test",
    table="test",
    event_types=["Insert", "Update", "Delete"],
)

stream = op.input("input", flow, source)
op.output("output", stream, StdOutSink())
```

This example sets up a dataflow that captures CDC events from a SingleStore table and outputs them to stdout.

## Configuration

The `SingleStoreSource` class accepts the following parameters:

- `host`: SingleStore server hostname
- `port`: SingleStore server port
- `user`: Database username
- `password`: Database password
- `database`: Name of the database
- `table`: Name of the table to observe
- `event_types`: List of event types to capture (default includes all types)

## Development

To set up the development environment:

```
pip install -e .[dev]
```

Run tests using:

```
python -m tox
```

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.

## Acknowledgements

This project is built on top of [Bytewax](https://bytewax.io/) and integrates with [SingleStore](https://www.singlestore.com/). We're grateful for their excellent technologies that make this connector possible.
