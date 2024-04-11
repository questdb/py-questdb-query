# Developer's Notes

## Running tests

Running tests requires an instance of QuestDB.

This repo reuses the some testing code from the `c-questdb-client` repo.
To use this code, sync the submodule in git:

```shell
git submodule update --init
```

The tests will automatically download and start a QuestDB instance, but you
also need to have a Java 11 runtime installed.

## Cutting a release

```shell
poetry export --output requirements.txt
```
