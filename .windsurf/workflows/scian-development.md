---
description: Fetch and parse SCIAN classification XLSX from INEGI
---

# SCIAN Classification Workflow

## Context: What is SCIAN?

**SCIAN** (Sistema de Clasificación Industrial de América del Norte / North American Industry Classification System) is Mexico's standardized industry classification system. It categorizes economic activities hierarchically:

- **Sector** (2 digits): Broadest category (e.g., `31-33` = Manufacturing)
- **Subsector** (3 digits): Subdivision of sector (e.g., `311` = Food Manufacturing)
- **Rama** (4 digits): Branch within subsector (e.g., `3111` = Animal Food Manufacturing)
- **Subrama** (5 digits): Sub-branch (e.g., `31111` = Animal Food Manufacturing)
- **Clase** (6 digits): Most specific classification (e.g., `311111` = Dog and Cat Food Manufacturing)

**Code Structure**: Each digit level represents a hierarchical refinement of the economic activity classification.

## Objective

Fetch the official SCIAN 2023 classification XLSX file from INEGI's website and parse it into DuckDB tables for analysis and validation.

## Source Information

- **Website**: https://www.inegi.org.mx/scian/#
- **Target File Pattern**: Look for `.xlsx` download button (typically named `scian_2023_categorias_y_productos.xlsx`)
- **Current URL Pattern**: `/contenidos/app/scian/scian_2023_categorias_y_productos.xlsx?v=YYYYMMDD`

⚠️ **Note**: The exact URL may change over time. The workflow includes validation to detect available download links.

## File Structure

### First Tab (Main Classification)
**Columns**:
- `Código` - SCIAN code (2-6 digits)
- `Título` - Title/name of the category
- `Descripción` - Description of the category
- `Incluye` - What is included
- `Excluye` - What is excluded
- `Índice de bienes y servicios comprendidos en las categorías del SCIAN México 2023` - Index of goods and services

**Header Row**: Row 2 (data starts at row 3)

### Other Tabs
**Columns**:
- `Código` - SCIAN code
- `Título` - Title
- `Descripción` - Description
- `Incluye` - What is included
- `Excluye` - What is excluded

**Header Row**: Row 2 (data starts at row 3)

## Implementation Steps

### 1. Create the SCIAN fetcher module

Create `src/scian_fetcher.py` with functions to:
- Fetch the INEGI SCIAN page
- Parse HTML to find all `.xlsx` download links
- Validate the file extension is `.xlsx`
- Download the file to a local cache directory

### 2. Create the SCIAN parser module

Create `src/scian_parser.py` with functions to:
- Use DuckDB's `st_read()` function to read XLSX files
- Import each sheet/tab into separate DuckDB tables
- Convert sheet names to snake_case for table names
- Handle the different column structures (first tab vs. other tabs)
- Set header row to row 2 for all sheets


### 3. Create live tests

Create `tests/test_scian_fetcher.py`:
- Test that INEGI page is accessible
- Test that at least one `.xlsx` link is found
- Test that download completes successfully
- **Report back to user**: List all found download URLs with `.xlsx` extension
- Validate file size is reasonable (> 1 MB)

Create `tests/test_scian_parser.py`:
- Test that DuckDB can read the downloaded XLSX
- Test that all sheets are imported
- Test that table names are properly snake_cased
- Test that first tab has 6 columns
- Test that other tabs have 5 columns
- Test that data starts at row 3 (after header at row 2)
- Validate SCIAN code format (2-6 digits)

### 4. Example DuckDB Implementation

```python
import duckdb
import re

def to_snake_case(name: str) -> str:
    """Convert sheet name to snake_case for table naming."""
    name = re.sub(r'[^\w\s-]', '', name)
    name = re.sub(r'[-\s]+', '_', name)
    return name.lower().strip('_')

def import_scian_to_duckdb(xlsx_path: str, db_path: str = ':memory:'):
    """Import SCIAN XLSX file into DuckDB tables."""
    conn = duckdb.connect(db_path)
    
    # Get all sheet names
    sheets_query = f"SELECT * FROM st_read('{xlsx_path}', layer='') LIMIT 0"
    # Use spatial extension or appropriate method to list sheets
    
    # For each sheet, import with header at row 2
    # Example for first sheet:
    table_name = to_snake_case("Clasificación")
    conn.execute(f"""
        CREATE TABLE {table_name} AS 
        SELECT * FROM st_read(
            '{xlsx_path}',
            sheet='Sheet1',
            header=true,
            skip_rows=1
        )
    """)
    
    return conn
```

### 5. Validation Requirements

✅ **Must validate**:
- File URL ends with `.xlsx`
- File downloads successfully
- File size > 1 MB
- All sheets can be read by DuckDB
- Column counts match expected (6 for first tab, 5 for others)
- SCIAN codes follow proper format
- No empty required columns (Código, Título)

❌ **Report to user if**:
- No `.xlsx` links found on page
- File extension is not `.xlsx`
- Download fails
- File is corrupted or unreadable
- Schema doesn't match expected structure

### 6. Run the workflow

// turbo
```bash
# Install dependencies
pip install duckdb requests beautifulsoup4 lxml openpyxl pytest
```

// turbo
```bash
# Run the fetcher test to discover available files
pytest tests/test_scian_fetcher.py -v -s
```

```bash
# Run the parser test to validate structure
pytest tests/test_scian_parser.py -v -s
```

## Expected Output

After running tests, you should see:
- List of all `.xlsx` download URLs found on INEGI page
- Confirmation that file was downloaded successfully
- List of all sheets/tabs imported into DuckDB
- Table names (snake_cased) and row counts
- Validation results for schema and data quality

## Notes

- Use **live tests only** - no mocking
- DuckDB's XLSX reading: https://duckdb.org/docs/stable/guides/file_formats/excel_import
- Cache downloaded files to avoid repeated downloads during testing
- Consider rate limiting when fetching from INEGI website
