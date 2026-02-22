# DENUE Pipeline Testing Guide

## Test Philosophy

**All tests use LIVE data from INEGI** - no mocking, no fake data. This ensures:
- Real-world validation of the pipeline
- Detection of schema changes at INEGI
- Verification that external dependencies work
- Confidence in production deployment

## Test Coverage

### 1. Fetcher Tests (`test_denue_fetcher.py`)

**What it tests:**
- ✅ Live web scraping from INEGI download page
- ✅ Dataset metadata extraction (sector, period, URL, file size)
- ✅ Data structure validation
- ✅ URL format verification

**Run:**
```bash
pytest tests/test_denue_fetcher.py -v
```

**Expected behavior:**
- Fetches 20+ datasets from INEGI
- Validates each dataset has required fields
- Confirms URLs point to CSV ZIP files

---

### 2. Downloader Tests (`test_denue_downloader.py`)

**What it tests:**
- ✅ Live file downloads from INEGI
- ✅ Download caching (avoids re-downloading)
- ✅ ZIP extraction with validation
- ✅ Extraction caching
- ✅ Retry logic on failures

**Run:**
```bash
pytest tests/test_denue_downloader.py -v
```

**Expected behavior:**
- Downloads real ZIP file (~2-10 MB)
- Caches download for subsequent runs
- Extracts 3 required directories: `conjunto_de_datos`, `diccionario_de_datos`, `metadatos`
- Verifies file structure matches INEGI format

**Note:** First run will download files, subsequent runs use cache.

---

### 3. Parser Tests (`test_denue_parser.py`)

**What it tests:**
- ✅ Schema parsing from data dictionary
- ✅ Metadata extraction from DCAT files
- ✅ Dataset parsing with ISO-8859-3 encoding
- ✅ Column name normalization (snake_case)
- ✅ Schema validation

**Run:**
```bash
pytest tests/test_denue_parser.py -v
```

**Expected behavior:**
- Parses 40+ columns from schema
- Converts Spanish column names to snake_case
- Validates dataset structure
- Extracts DCAT-compliant metadata

---

### 4. Ingestion Tests (`test_denue_ingestion.py`)

**What it tests:**
- ✅ DuckDB database initialization
- ✅ Live data ingestion
- ✅ Ingestion caching (prevents duplicates)
- ✅ Record counting and statistics
- ✅ Failed dataset tracking

**Run:**
```bash
pytest tests/test_denue_ingestion.py -v
```

**Expected behavior:**
- Creates DuckDB tables (`denue`, `ingestion_log`)
- Ingests thousands of real business records
- Prevents duplicate ingestion on re-run
- Tracks success/failure status

---

### 5. Pipeline Tests (`test_denue_pipeline.py`)

**What it tests:**
- ✅ End-to-end pipeline execution
- ✅ Multi-dataset processing
- ✅ Caching across full pipeline
- ✅ Report generation
- ✅ Progress tracking

**Run:**
```bash
pytest tests/test_denue_pipeline.py -v
```

**Expected behavior:**
- Processes multiple datasets sequentially
- Generates execution report (JSON)
- Uses cache on re-runs
- Displays progress bars

---

## Running Tests

### Quick test (unit tests only)
```bash
pytest tests/test_denue_fetcher.py::TestDENUEFetcher::test_dataset_to_dict -v
```

### Single module test
```bash
pytest tests/test_denue_fetcher.py -v
```

### All tests
```bash
pytest tests/ -v
```

### All tests with output
```bash
pytest tests/ -v -s
```

### Specific test
```bash
pytest tests/test_denue_pipeline.py::TestDENUEPipeline::test_pipeline_single_dataset -v -s
```

---

## Test Execution Time

| Test Module | Approx. Time | Network Required |
|-------------|--------------|------------------|
| `test_denue_fetcher.py` | ~10-15 sec | Yes (web scraping) |
| `test_denue_downloader.py` | ~30-60 sec | Yes (file download) |
| `test_denue_parser.py` | ~30-60 sec | Yes (download + parse) |
| `test_denue_ingestion.py` | ~60-90 sec | Yes (full ETL) |
| `test_denue_pipeline.py` | ~60-120 sec | Yes (full pipeline) |

**Total:** ~5-10 minutes for full test suite (first run)  
**Cached:** ~2-3 minutes (subsequent runs with cache)

---

## Test Data Caching

Tests create temporary directories that are cleaned up after each test:
- `test_denue_cache_*`: Download cache
- `test_denue_db_*`: Temporary databases
- `test_denue_parser_*`: Parser cache
- `test_denue_pipeline_*`: Full pipeline workspace

**Cleanup:** Automatic via pytest fixtures

---

## Continuous Integration

For CI/CD pipelines:

```yaml
# Example GitHub Actions
- name: Install dependencies
  run: |
    pip install -r requirements.txt
    playwright install chromium

- name: Run tests
  run: pytest tests/ -v --tb=short
  timeout-minutes: 15
```

**Important:** CI needs:
- Network access to INEGI
- Playwright browser installation
- ~500 MB disk space for cache

---

## Troubleshooting

### Test timeout
```bash
# Increase pytest timeout
pytest tests/ -v --timeout=300
```

### Network issues
- Tests require internet connection to INEGI
- INEGI website must be accessible
- Firewall/proxy may block Playwright

### Playwright issues
```bash
# Reinstall browsers
playwright install chromium --force
```

### Cache issues
```bash
# Tests use temporary directories, but if issues persist:
rm -rf /tmp/test_denue_*
```

---

## Test Assertions

All tests verify:
1. **Data exists**: No empty datasets
2. **Structure valid**: Required fields present
3. **Types correct**: Proper data types
4. **Encoding handled**: ISO-8859-3 decoded correctly
5. **Caching works**: No redundant operations
6. **Errors logged**: Failures tracked properly

---

## Adding New Tests

When adding tests, follow these rules:

1. **Use live data** - Never mock INEGI responses
2. **Clean up** - Use pytest fixtures for temp files
3. **Log clearly** - Use logger for debugging
4. **Test caching** - Verify cache behavior
5. **Handle failures** - Test error conditions

Example:
```python
def test_new_feature(self, temp_cache_dir):
    # Use live data
    fetcher = DENUEFetcher()
    datasets = fetcher.fetch_datasets()
    
    # Test with real dataset
    result = my_new_function(datasets[0])
    
    # Verify
    assert result is not None
    logger.info(f"Test result: {result}")
```

---

## Performance Benchmarks

Expected performance on modern hardware:

- **Fetch datasets**: ~5-10 seconds
- **Download single ZIP**: ~10-30 seconds (depends on file size)
- **Parse dataset**: ~5-15 seconds (depends on row count)
- **Ingest to DuckDB**: ~10-30 seconds
- **Full pipeline (1 dataset)**: ~30-90 seconds

**Bottlenecks:**
1. Network speed (downloads)
2. Disk I/O (extraction)
3. Pandas parsing (large CSVs)

---

## Test Coverage Report

Generate coverage report:
```bash
pytest tests/ --cov=src --cov-report=html
open htmlcov/index.html
```

Target: >80% code coverage
