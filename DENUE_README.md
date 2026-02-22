# DENUE Data Fetching Pipeline

Production-ready ETL pipeline for fetching, parsing, and ingesting DENUE (Directorio Estadístico Nacional de Unidades Económicas) data from INEGI.

## Features

✅ **Intelligent Caching**: Downloads and extractions are cached to avoid redundant network requests  
✅ **DuckDB Deduplication**: Tracks ingested datasets to prevent duplicate records  
✅ **Progress Tracking**: Real-time progress bars for sequential processing  
✅ **Retry Logic**: Automatic retries with exponential backoff for network failures  
✅ **Schema Validation**: Validates data against INEGI's data dictionary  
✅ **Comprehensive Reporting**: Detailed execution reports with success/failure metrics  
✅ **Live Testing**: All tests use real INEGI data (no mocking)

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium
```

## Quick Start

### Run the full pipeline

```bash
python src/denue_pipeline.py
```

### Run with custom options

```bash
python src/denue_pipeline.py \
  --database ./data/denue.duckdb \
  --cache ./cache/denue \
  --report ./reports/denue_report.json \
  --limit 5  # Process only first 5 datasets (for testing)
```

## Architecture

### Modules

1. **`denue_fetcher.py`**: Web scraping with Playwright
   - Fetches dataset metadata from INEGI download page
   - Extracts sector names, periods, and download URLs

2. **`denue_downloader.py`**: Download management with caching
   - Downloads ZIP files with retry logic
   - Caches downloads to avoid re-downloading
   - Extracts and validates ZIP structure

3. **`denue_parser.py`**: Data parsing and validation
   - Parses data dictionaries (schema definitions)
   - Converts column names to snake_case
   - Validates datasets against expected schema
   - Parses metadata from DCAT-compliant files

4. **`denue_ingestion.py`**: DuckDB storage with deduplication
   - Creates partitioned DuckDB tables
   - Tracks ingestion status to prevent duplicates
   - Generates compression statistics

5. **`denue_pipeline.py`**: Orchestration with progress tracking
   - Coordinates all modules
   - Displays progress bars for sequential processing
   - Generates execution reports

## Caching Strategy

### Download Cache
- **Location**: `./cache/denue/`
- **Key**: MD5 hash of (URL + sector + period)
- **Benefit**: Avoids re-downloading large ZIP files

### Extraction Cache
- **Location**: `./cache/denue/extracted/`
- **Benefit**: Skips extraction if files already exist

### DuckDB Ingestion Cache
- **Location**: `ingestion_log` table in DuckDB
- **Key**: (sector, periodo_consulta)
- **Benefit**: Prevents duplicate records on re-runs

## Testing

All tests use **live data** from INEGI (no mocking).

### Run all tests

```bash
pytest tests/ -v
```

### Run specific test modules

```bash
# Test web scraping
pytest tests/test_denue_fetcher.py -v

# Test downloads and caching
pytest tests/test_denue_downloader.py -v

# Test parsing and validation
pytest tests/test_denue_parser.py -v

# Test DuckDB ingestion
pytest tests/test_denue_ingestion.py -v

# Test full pipeline
pytest tests/test_denue_pipeline.py -v
```

### Test with limited datasets

```bash
# Process only 2 datasets (faster for testing)
pytest tests/test_denue_pipeline.py::TestDENUEPipeline::test_pipeline_multiple_datasets -v -s
```

## Output Files

### DuckDB Database
- **Path**: `./data/denue.duckdb`
- **Tables**:
  - `denue`: Main table with business records
  - `ingestion_log`: Tracks ingestion status per dataset

### Execution Report
- **Path**: `./reports/denue_report.json`
- **Contents**:
  ```json
  {
    "execution_timestamp": "2025-05-22T14:30:00Z",
    "total_datasets_detected": 20,
    "successful_downloads": 19,
    "failed_downloads": 1,
    "successful_ingestions": 18,
    "failed_ingestions": 1,
    "total_records_ingested": 5847293,
    "total_size_duckdb_mb": 89.3,
    "total_size_compressed_mb": 156.7,
    "compression_ratio": 0.57,
    "failed_datasets": [...]
  }
  ```

### Logs
- **Path**: `./logs/denue_pipeline.log`
- **Levels**: INFO, WARNING, ERROR, CRITICAL

## Query Examples

```python
import duckdb

conn = duckdb.connect('./data/denue.duckdb')

# Count total businesses
conn.execute("SELECT COUNT(*) FROM denue").fetchone()

# Businesses by state
conn.execute("""
    SELECT entidad, COUNT(*) as count 
    FROM denue 
    GROUP BY entidad 
    ORDER BY count DESC
""").fetchall()

# Businesses by economic sector
conn.execute("""
    SELECT nombre_act, COUNT(*) as count 
    FROM denue 
    GROUP BY nombre_act 
    ORDER BY count DESC 
    LIMIT 10
""").fetchall()

# Check ingestion status
conn.execute("""
    SELECT sector, periodo_consulta, record_count, status 
    FROM ingestion_log 
    ORDER BY ingestion_timestamp DESC
""").fetchall()
```

## Performance

- **Sequential Processing**: Processes one dataset at a time to minimize memory usage
- **Caching**: Dramatically speeds up re-runs (downloads/extractions skipped)
- **DuckDB Compression**: Typically achieves 40-60% compression vs raw CSV
- **Progress Tracking**: Real-time progress bars for monitoring

## Error Handling

- **Download failures**: Retry up to 3 times with exponential backoff
- **Parse failures**: Log error, continue to next dataset
- **Ingestion failures**: Log error, mark in `ingestion_log` table
- **Network timeouts**: 60 seconds per download, retry on timeout

## Future Enhancements

See workflow documentation for cloud migration strategy:
- Parallel processing with multiple Playwright instances
- Serverless deployment (AWS Lambda, GCP Cloud Functions)
- Cost-optimized batch processing
- Enhanced observability and monitoring

## Troubleshooting

### Playwright timeout
```bash
# Increase timeout
python src/denue_pipeline.py  # Default is 60 seconds
```

### Encoding issues
All files use **ISO-8859-3 (Latin-3)** encoding. This is handled automatically.

### Cache cleanup
```bash
# Clear download cache
rm -rf ./cache/denue/

# Clear DuckDB (start fresh)
rm -rf ./data/denue.duckdb
```

## License

See main repository LICENSE file.
