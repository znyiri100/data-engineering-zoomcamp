# Copilot Instructions for my-dlt-pipeline

This project is a Python-based data pipeline using `uv` for dependency management.

## Environment & Tooling
- **Dependency Manager:** [uv](https://github.com/astral-sh/uv). Always use `uv sync` or `uv add <package>` for managing dependencies. 
- **Python Version:** 3.11.9+ (as specified in [.python-version](.python-version) and [pyproject.toml](pyproject.toml)).
- **Virtual Environment:** Located at [.venv/](.venv/). Use `source .venv/bin/activate` or `uv run` to execute scripts.

## Project Structure
- [main.py](main.py): Entry point for the pipeline.
- [pyproject.toml](pyproject.toml): Project metadata and dependencies.
- [uv.lock](uv.lock): Deterministic lockfile for dependencies (do not edit manually).

## Development Patterns
- **Pipeline Implementation:** Stick to `dlt` (data load tool) conventions if adding data extraction logic.
- **Modularity:** Separate source definitions (extraction), transformations, and destination configurations.
- **Naming:** Follow PEP 8 for Python code. Use descriptive names for pipeline resources and transformers.

## Workflow Commands
- `uv sync`: Synchronize the environment with [pyproject.toml](pyproject.toml).
- `uv run main.py`: Run the main pipeline entry point.
- `uv add <package>`: Add a new dependency to the project.
