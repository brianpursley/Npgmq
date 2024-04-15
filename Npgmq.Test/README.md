# Npgmq.Test

This project contains the tests for the Npgmq library.
It spins up a PostgreSQL server in docker, installs the necessary extensions, creates a database, and runs the tests against it.

## Helper Scripts

### Start database, run tests, and stop database (All in one)

```bash
scripts/run-tests.sh
```

Or, to use a specific version of pgmq:
```bash
scripts/run-tests.sh 1.1.1
```

### Start Database Only

```bash
scripts/start-db.sh
```

Or, to use a specific version of pgmq:
```bash
scripts/start-db.sh 1.1.1
```

### Stop Database Only

```bash
scripts/stop-db.sh
```
