# Quacky-DENUE

A production-ready Python ETL pipeline for ingesting and processing DENUE (Directorio Estadístico Nacional de Unidades Económicas) data using DuckDB and Prefect.

## Features

- **Modular Architecture**: Independent, reusable components for extraction, validation, transformation, and loading
- **Prefect Orchestration**: Lightweight workflow management with retry logic and monitoring
- **DuckDB Storage**: Efficient columnar storage and SQL analytics
- **Live Testing**: All tests run against real-world data sources (no mocking)
- **Cloud-Ready**: Designed for easy migration to cloud providers (AWS, GCP, Azure)

## Project Structure

```
Quacky-DENUE/
├── src/
│   ├── scian_fetcher.py      # SCIAN classification data fetcher (Prefect tasks)
│   ├── scian_parser.py       # SCIAN data parser and DuckDB importer (Prefect tasks)
│   └── scian_pipeline.py     # Main Prefect flows orchestrating the pipeline
├── tests/
│   ├── test_scian_fetcher.py # Live tests for fetcher
│   └── test_scian_parser.py  # Live tests for parser
├── cache/                     # Downloaded files cache
├── requirements.txt           # Python dependencies
└── README.md
```

## Installation

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Run the Complete SCIAN ETL Pipeline

```bash
# Using Prefect flow
python src/scian_pipeline.py
```

### Discover Available SCIAN Download Links

```python
from src.scian_pipeline import discover_scian_links

links = discover_scian_links()
```

### Run Individual Tasks

```python
from src.scian_fetcher import fetch_scian_page, parse_xlsx_links, download_scian_file
from src.scian_parser import import_scian_to_duckdb

# Fetch and parse
html = fetch_scian_page.fn()
links = parse_xlsx_links.fn(html)

# Download
file_path = download_scian_file.fn(links[0][1])

# Import to DuckDB
results = import_scian_to_duckdb.fn(file_path, "scian.duckdb")
```

## Testing

All tests use **live, real-world data** - no mocking allowed.

```bash
# Run all tests
pytest tests/ -v -s

# Run specific test suite
pytest tests/test_scian_fetcher.py -v -s
pytest tests/test_scian_parser.py -v -s
```

## SCIAN Classification

**SCIAN** (Sistema de Clasificación Industrial de América del Norte) is Mexico's standardized industry classification system with hierarchical levels:

- **Sector** (2 digits): e.g., `31-33` = Manufacturing
- **Subsector** (3 digits): e.g., `311` = Food Manufacturing
- **Rama** (4 digits): e.g., `3111` = Animal Food Manufacturing
- **Subrama** (5 digits): e.g., `31111` = Animal Food Manufacturing
- **Clase** (6 digits): e.g., `311111` = Dog and Cat Food Manufacturing

## Cloud Migration

The pipeline is designed for easy cloud deployment:

1. **Local Development**: Run Prefect locally (current setup)
2. **Prefect Cloud**: Use managed Prefect Cloud for orchestration
3. **Self-Hosted**: Deploy Prefect server on AWS/GCP/Azure
4. **Hybrid**: Orchestration in cloud, workers anywhere

## Dependencies

- **prefect**: Workflow orchestration
- **duckdb**: Analytical database
- **requests**: HTTP client
- **beautifulsoup4**: HTML parsing
- **openpyxl**: Excel file handling
- **pytest**: Testing framework

## Development Workflow

See `.windsurf/workflows/scian-development.md` for detailed development instructions.

## License

TBD
