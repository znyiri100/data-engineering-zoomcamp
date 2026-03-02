# my-dlt-pipeline

A Python data pipeline that loads book data from the [Open Library API](https://openlibrary.org/developers/api) into a local DuckDB database using [dlt](https://dlthub.com/).

## Requirements

- Python 3.11.9+
- [uv](https://github.com/astral-sh/uv)

## Setup

```bash
uv sync
```

## Usage

Run the pipeline with `open_library_pipeline.py`. A search `value` is required. The optional `--type` flag controls which Open Library API endpoint is used.

```bash
uv run open_library_pipeline.py [--type search|bibkeys] <value>
```

### `--type search` (default)

Performs a full-text search using the [Open Library Search API](https://openlibrary.org/dev/docs/api#search) (`/search.json`). Results are paginated automatically using offset-based pagination driven by the `numFound` total in the response.

```bash
# These are equivalent – search is the default
uv run open_library_pipeline.py "harry potter"
uv run open_library_pipeline.py --type search "harry potter"
```

### `--type bibkeys`

Looks up specific editions by bibkey using the [Open Library Books API](https://openlibrary.org/dev/docs/api/books) (`/api/books`). Bibkeys are comma-separated identifiers of the form `ISBN:<value>`, `LCCN:<value>`, or `OLID:<value>`.

```bash
uv run open_library_pipeline.py --type bibkeys "ISBN:0451526538,LCCN:62019420,OLID:OL7353617M"
```

## Output

Data is loaded into a DuckDB file (`open_library_pipeline.duckdb`) under the `open_library` dataset in a `books` table.

Query results after a run:

```bash
duckdb open_library_pipeline.duckdb "SELECT * FROM open_library.books LIMIT 10;"

# row counts in books tables
BOOKS=$( duckdb -readonly -noheader -list open_library_pipeline.duckdb "SELECT string_agg('SELECT ''' || table_schema || ''' BATCH, count(*) CNT FROM \"' || table_schema || '\".\"books\"', ' UNION ALL ') FROM information_schema.tables WHERE table_name='books' AND table_type='BASE TABLE' AND table_schema NOT IN ('information_schema','pg_catalog');" ); duckdb -readonly open_library_pipeline.duckdb "$BOOKS"

# rows in last batch
BOOKS=$( duckdb -readonly -noheader -list open_library_pipeline.duckdb "SELECT 'SELECT * CNT FROM \"' || max(table_schema) || '\".\"books\" limit 3' FROM information_schema.tables WHERE table_name='books' AND table_type='BASE TABLE' AND table_schema NOT IN ('information_schema','pg_catalog');" ); duckdb -readonly open_library_pipeline.duckdb "$BOOKS"
```

## Project structure

| File | Description |
|---|---|
| [open_library_pipeline.py](open_library_pipeline.py) | Pipeline entry point |
| [pyproject.toml](pyproject.toml) | Project metadata and dependencies |
| [uv.lock](uv.lock) | Deterministic dependency lockfile |
| `open_library_pipeline.duckdb` | DuckDB output database (generated on first run) |
