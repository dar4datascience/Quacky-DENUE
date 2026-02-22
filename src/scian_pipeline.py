from pathlib import Path
from typing import Dict
from prefect import flow
from scian_fetcher import (
    fetch_scian_page,
    parse_xlsx_links,
    download_scian_file,
    validate_xlsx_file,
    INEGI_SCIAN_URL
)
from scian_parser import (
    import_scian_to_duckdb,
    validate_scian_schema,
    validate_scian_codes,
    get_sheet_names,
    to_snake_case
)


@flow(name="SCIAN ETL Pipeline", log_prints=True)
def scian_etl_pipeline(
    url: str = INEGI_SCIAN_URL,
    db_path: str = "cache/scian.duckdb",
    use_first_link: bool = True
) -> Dict[str, int]:
    """
    Complete SCIAN ETL pipeline using Prefect.
    
    This flow:
    1. Fetches the INEGI SCIAN page
    2. Parses and discovers .xlsx download links
    3. Downloads the SCIAN classification file
    4. Validates the downloaded file
    5. Imports all sheets into DuckDB
    6. Validates schema and data quality
    
    Args:
        url: URL of the INEGI SCIAN page
        db_path: Path for DuckDB database (use ':memory:' for in-memory)
        use_first_link: If True, use first .xlsx link found; otherwise prompt user
        
    Returns:
        Dictionary mapping table names to row counts
    """
    print("="*80)
    print("SCIAN ETL PIPELINE - START")
    print("="*80)
    
    print("\n[1/6] Fetching INEGI SCIAN page...")
    html_content = fetch_scian_page(url)
    print(f"✓ Fetched {len(html_content):,} bytes of HTML")
    
    print("\n[2/6] Discovering .xlsx download links...")
    xlsx_links = parse_xlsx_links(html_content)
    print(f"✓ Found {len(xlsx_links)} .xlsx link(s):")
    for i, (link_text, link_url) in enumerate(xlsx_links, 1):
        print(f"  {i}. [{link_text}]")
        print(f"     {link_url}")
    
    if not xlsx_links:
        raise ValueError("No .xlsx links found on INEGI SCIAN page")
    
    selected_url = xlsx_links[0][1] if use_first_link else xlsx_links[0][1]
    print(f"\n[3/6] Downloading SCIAN file...")
    print(f"  URL: {selected_url}")
    
    file_path = download_scian_file(selected_url)
    print(f"✓ Downloaded to: {file_path}")
    print(f"  Size: {file_path.stat().st_size / 1_000_000:.2f} MB")
    
    print("\n[4/6] Validating downloaded file...")
    validate_xlsx_file(file_path)
    print("✓ File validation passed")
    
    print("\n[5/6] Importing sheets to DuckDB...")
    results = import_scian_to_duckdb(file_path, db_path)
    print(f"✓ Imported {len(results)} table(s):")
    for table_name, row_count in results.items():
        print(f"  - {table_name}: {row_count:,} rows")
    
    print("\n[6/6] Validating data quality...")
    sheet_names = get_sheet_names(file_path)
    first_table = to_snake_case(sheet_names[0])
    
    validate_scian_schema(db_path, first_table, ["Código", "Título"])
    print(f"✓ Schema validation passed for '{first_table}'")
    
    validate_scian_codes(db_path, first_table)
    print(f"✓ SCIAN code format validation passed")
    
    print("\n" + "="*80)
    print("SCIAN ETL PIPELINE - COMPLETED SUCCESSFULLY")
    print("="*80)
    print(f"\nDatabase location: {db_path}")
    print(f"Total tables: {len(results)}")
    print(f"Total rows: {sum(results.values()):,}")
    
    return results


@flow(name="SCIAN Discovery Flow", log_prints=True)
def discover_scian_links(url: str = INEGI_SCIAN_URL):
    """
    Lightweight flow to discover available SCIAN download links.
    
    Args:
        url: URL of the INEGI SCIAN page
        
    Returns:
        List of tuples (link_text, url)
    """
    print("Discovering SCIAN download links...")
    html_content = fetch_scian_page(url)
    xlsx_links = parse_xlsx_links(html_content)
    
    print(f"\nFound {len(xlsx_links)} .xlsx link(s):")
    for i, (link_text, link_url) in enumerate(xlsx_links, 1):
        print(f"\n{i}. {link_text}")
        print(f"   URL: {link_url}")
    
    return xlsx_links


if __name__ == "__main__":
    scian_etl_pipeline()
