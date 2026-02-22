---
description: Fetch and ingest standardized DENUE data from INEGI
auto_execution_mode: 2
---

# DENUE Data Fetching Pipeline

## Context: What is DENUE?

**DENUE (Directorio Estadístico Nacional de Unidades Económicas)** is Mexico's National Statistical Directory of Economic Units maintained by INEGI. It provides:
- Identification and location data of active businesses in Mexico
- Economic activity classification (using SCIAN codes)
- Business size information
- Updated primarily for large establishments
- Annual updates (typically published monthly throughout the year)

**Data Source**: https://www.inegi.org.mx/app/descarga/?ti=6

## Pipeline Overview

This pipeline fetches DENUE datasets partitioned by economic activity sector, validates schemas, and ingests data into DuckDB for efficient storage and querying.

## Technical Requirements

### 1. Web Scraping with Playwright

**Why Playwright?** The INEGI download page uses dynamic JavaScript rendering. Static HTTP requests won't load the complete data table.

**Target Elements:**
- Container: `div[role="tabpanel"]#denue` (class: `tab-pane active`)
- Data rows: `tr[data-nivel="3"][data-agrupacion="denue"]`
- Each row contains:
  - **Sector name**: `td > div` (with margin-left indicating hierarchy)
  - **Period**: Second `td` (format: `MM/YYYY`, e.g., `05/2025`)
  - **CSV download link**: `a[href$="csv.zip"]` within third `td`

**Row Structure Example:**
```html
<tr data-titulopadre="Otros|DENUE|Actividad económica" 
    data-titulo="Otros|DENUE|Actividad económica|Construcción" 
    data-nivel="3" 
    data-tipoinfo="Otros" 
    data-agrupacion="denue">
  <td><div style="margin-left:64px">Construcción</div></td>
  <td>05/2025</td>
  <td>
    <a href="/contenidos/masiva/denue/denue_00_23_csv.zip" download="">
      <img src="/img/ico/ico_csv.png" alt="">
      <span>2.22 MB</span>
    </a>
  </td>
</tr>
```

### 2. Downloaded ZIP Structure

Each `denue_XX_YY_csv.zip` contains:

```
root/
├── conjunto_de_datos/     # Main dataset (CSV with business records)
├── diccionario_de_datos/  # Data dictionary (CSV, schema definition)
└── metadatos/             # Metadata (TXT, dataset description)
```

**Important Notes:**
- **Diccionario de datos**: First row contains date header (e.g., `DICCIONARIO DE DATOS DENUE (22/05/2025)`), actual schema starts at row 2
- **Column mapping**: Use "Nombre del Atributo en csv" column for field names
- **Metadatos**: Contains DCAT-compliant metadata (Identifier, Title, Description, Modified, etc.)
- **Encoding**: All files use **ISO-8859-3 (Latin-3)** encoding

### 3. Data Ingestion Strategy

**Storage**: DuckDB (for compression and efficient querying)

**Partitioning Columns:**
- `codigo_act` (SCIAN economic activity code)
- `cod_postal` (postal code)
- `cve_ent` (state code)
- `cve_mun` (municipality code)
- `cve_loc` (locality code)
- `ageb` (basic geostatistical area)
- `periodo_consulta` (query period, extracted from web page as `MM/YYYY`)

**Column Naming**: Convert all column names to `snake_case`

**Schema Validation**: 
- Parse `diccionario_de_datos/*.csv` (skip row 1, read from row 2)
- Validate that `conjunto_de_datos/*.csv` columns match expected schema
- Report any schema mismatches or missing columns

### 4. Metadata Storage

Store metadata as JSON files with structure:
```json
{
  "identifier": "MEX-INEGI.EEC2.05-DENUE-2025",
  "title": "Directorio Estadístico Nacional de Unidades Económicas (DENUE) 05_2025",
  "description": "...",
  "modified": "2025-05-22",
  "publisher": "Instituto Nacional de Estadística y Geografía, INEGI",
  "temporal": "2025-05-22",
  "spatial": "Estados Unidos Mexicanos",
  "accrual_periodicity": "ANUAL",
  "sector": "Construcción",
  "periodo_consulta": "05/2025",
  "file_size_mb": 2.22,
  "download_url": "/contenidos/masiva/denue/denue_00_23_csv.zip"
}
```

### 5. Error Handling & Retry Logic

**Tracking Requirements:**
- Total datasets detected (count of `tr[data-nivel="3"]`)
- Successfully downloaded datasets
- Successfully parsed datasets
- Successfully ingested datasets
- Failed datasets (with error details)

**Retry Strategy:**
- Download failures: Retry up to 3 times with exponential backoff
- Parse failures: Log error, continue to next dataset
- Ingestion failures: Log error, save raw data for manual review
- Network timeouts: 60 seconds per download, retry on timeout

**Logging Levels:**
- INFO: Dataset detection, download start/complete, ingestion complete
- WARNING: Retry attempts, schema validation warnings
- ERROR: Download failures, parse errors, ingestion failures
- CRITICAL: Complete pipeline failure

### 6. Completeness Reporting

Generate a summary report after pipeline execution:
```json
{
  "execution_timestamp": "2025-05-22T14:30:00Z",
  "total_datasets_detected": 20,
  "successful_downloads": 19,
  "failed_downloads": 1,
  "successful_ingestions": 18,
  "failed_ingestions": 1,
  "total_records_ingested": 5847293,
  "total_size_compressed_mb": 156.7,
  "total_size_duckdb_mb": 89.3,
  "compression_ratio": 0.57,
  "failed_datasets": [
    {
      "sector": "Minería",
      "periodo": "05/2025",
      "error": "Download timeout after 3 retries",
      "url": "/contenidos/masiva/denue/denue_00_11_csv.zip"
    }
  ]
}
```

## Implementation Steps

### Step 1: Set up Playwright browser automation
```bash
pip install playwright pandas duckdb
playwright install chromium
```

### Step 2: Create DENUE fetcher module (`src/denue_fetcher.py`)
- Initialize Playwright browser (headless mode)
- Navigate to https://www.inegi.org.mx/app/descarga/?ti=6
- Wait for `#denue` tab to load
- Extract all `tr[data-nivel="3"]` rows
- Parse sector name, period, and CSV download URL
- Return list of dataset metadata

### Step 3: Create DENUE downloader module (`src/denue_downloader.py`)
- Download ZIP files with retry logic
- Extract ZIP contents to temporary directory
- Validate folder structure (conjunto_de_datos, diccionario_de_datos, metadatos)
- Return paths to extracted files

### Step 4: Create DENUE parser module (`src/denue_parser.py`)
- Parse `diccionario_de_datos/*.csv` (encoding: ISO-8859-3, skip row 1)
- Extract schema from "Nombre del Atributo en csv" column
- Convert column names to snake_case
- Parse `metadatos/*.txt` into structured JSON
- Validate `conjunto_de_datos/*.csv` against schema
- Return validated DataFrame and metadata

### Step 5: Create DENUE ingestion module (`src/denue_ingestion.py`)
- Initialize DuckDB database
- Create partitioned table with schema
- Insert data with `periodo_consulta` from web scraping
- Verify record counts
- Generate compression statistics

### Step 6: Create DENUE pipeline orchestrator (`src/denue_pipeline.py`)
- Coordinate all modules
- Implement retry logic
- Track success/failure metrics
- Generate completeness report
- Save report as JSON

### Step 7: Create live integration tests (`tests/test_denue_pipeline.py`)
- Test Playwright scraping (fetch at least 1 real dataset)
- Test ZIP download and extraction
- Test schema parsing and validation
- Test DuckDB ingestion
- Verify data integrity
- **CRITICAL**: Use live data, no mocking

### Step 8: Run full pipeline
```bash
python src/denue_pipeline.py --output denue.duckdb --report report.json
```

### Step 9: Verify results
```bash
# Query DuckDB to verify ingestion
python -c "import duckdb; conn = duckdb.connect('denue.duckdb'); print(conn.execute('SELECT COUNT(*) FROM denue').fetchone())"

# Check report
cat report.json | jq '.'
```

## Configuration Options

Create `config/denue_config.yaml`:
```yaml
source:
  url: "https://www.inegi.org.mx/app/descarga/?ti=6"
  encoding: "ISO-8859-3"
  
download:
  timeout_seconds: 60
  max_retries: 3
  backoff_factor: 2
  cache_dir: "./cache/denue"
  
ingestion:
  database_path: "./data/denue.duckdb"
  partition_columns:
    - codigo_act
    - cod_postal
    - cve_ent
    - cve_mun
    - cve_loc
    - ageb
    - periodo_consulta
  
logging:
  level: "INFO"
  file: "./logs/denue_pipeline.log"
  
reporting:
  output_path: "./reports/denue_report.json"
```

## Expected Output

- **DuckDB database**: `data/denue.duckdb` (compressed, partitioned data)
- **Metadata JSON**: `data/metadata/` (one JSON per sector)
- **Execution report**: `reports/denue_report.json`
- **Logs**: `logs/denue_pipeline.log`

## Performance Considerations

- **Memory**: Process datasets one at a time to avoid memory overflow
- **Disk I/O**: Use temporary directory for extraction, clean up after ingestion
- **Network**: Implement rate limiting to be respectful to INEGI servers
- **Compression**: DuckDB typically achieves 40-60% compression vs raw CSV
- **Parallelization**: Consider parallel downloads (max 3 concurrent) for faster execution

## Maintenance Notes

- INEGI updates DENUE data monthly
- Schema may change between releases (validate with diccionario_de_datos)
- Website structure may change (monitor Playwright selectors)
- Keep encoding as ISO-8859-3 unless INEGI changes format
- Archive old reports for historical tracking
