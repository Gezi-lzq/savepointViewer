# Savepoint-Viewer

A simple CLI tool for viewing and modifying the contents of Flink savepoints.

## Features

- **View:** Inspect the state of individual operators within a Flink savepoint.
- **Remove:** Delete existing state entries.

## Usage

```
state-viewer [-hV] [COMMAND]

View, delete and update states in Flink Savepoint.

-h, --help      Show this help message and exit.
-V, --version   Print version information and exit.

Commands:

view             View the state of a specific operator in the Savepoint.
removeAndUpdate  Update the state of a specific operator in the Savepoint.
```

### Examples

**View the state of an operator:**

```
java -jar savepointViewer-1.0-SNAPSHOT.jar state-viewer view --savepoint-path /path/to/savepoint --operator-id <operator-id>
```

**Remove a state entry from an operator:**

```
java -jar savepointViewer-1.0-SNAPSHOT.jar state-viewer removeAndUpdate --savepoint-path /path/to/savepoint --operator-id <operator-id> --action remove --key <state-key>
```


## Installation

**Build from source:**
- Clone the repository.
- Run `mvn clean install`.
