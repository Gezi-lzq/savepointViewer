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
### view

```
Usage: state-viewer view [-uid=<operatorUid>] [-uidhash=<operatorUidHash>]
                         <savepointPath>
View the state of a specific operator in the Savepoint.
      <savepointPath>   Savepoint file path.
      -uid, --operator-uid=<operatorUid>
                        Operator UID.
      -uidhash, --operator-uidhash=<operatorUidHash>
                        Operator UID Hash.
```

### removeAndUpdate

```
Usage: state-viewer removeAndUpdate [-uid=<operatorUid>]
                                    [-uidhash=<operatorUidHash>] <savepointPath>
Update the state of a specific operator in the Savepoint.
      <savepointPath>   Savepoint file path.
      -uid, --operator-uid=<operatorUid>
                        Operator UID.
      -uidhash, --operator-uidhash=<operatorUidHash>
                        Operator UID Hash.
```

### Examples

**View the state of all operators:**

```
java -jar savepointViewer-1.0-SNAPSHOT.jar view /path/to/savepoint
```

**View the state of an operator:**

```
java -jar savepointViewer-1.0-SNAPSHOT.jar view /path/to/savepoint -uid <operator-id>
```

**Remove a state entry from an operator:**

```
java -jar savepointViewer-1.0-SNAPSHOT.jar removeAndUpdate /path/to/savepoint -uid <operator-id>
```


## Installation

**Build from source:**
- Clone the repository.
- Run `mvn clean install`.


