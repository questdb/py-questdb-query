# Developer's Notes

## Cloning and Running Tests

```shell
git clone https://github.com/questdb/py-questdb-query.git
cd py-questdb-query
git submodule update --init
poetry install
./test
```

The tests will automatically download and start a QuestDB instance, but you
also need to have a Java 11 runtime installed.

## Updating the dependencies

Tweak the `pyproject.toml` file (if needed) and then run:

```
poetry update
```

## Running a single test

```shell
poetry run python -m unittest tests.tests.TestModule.test_basic_aut
```

## Cutting a release

```shell
poetry export --output requirements.txt
```
