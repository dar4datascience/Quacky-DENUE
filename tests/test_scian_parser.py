import pytest
from pathlib import Path
import sys
import duckdb

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.scian_fetcher import (
    fetch_scian_page,
    parse_xlsx_links,
    download_scian_file,
    INEGI_SCIAN_URL
)
from src.scian_parser import (
    to_snake_case,
    get_sheet_names,
    import_sheet_to_duckdb,
    import_scian_to_duckdb,
    validate_scian_schema,
    validate_scian_codes
)


@pytest.fixture(scope="module")
def scian_file():
    """Download SCIAN file once for all tests."""
    html_content = fetch_scian_page.fn(INEGI_SCIAN_URL)
    xlsx_links = parse_xlsx_links.fn(html_content)
    
    if len(xlsx_links) == 0:
        pytest.skip("No .xlsx links found on INEGI page")
    
    file_path = download_scian_file.fn(xlsx_links[0][1])
    return file_path


class TestScianParser:
    """Live tests for SCIAN parser - no mocking allowed."""
    
    def test_snake_case_conversion(self):
        """Test sheet name to snake_case conversion."""
        assert to_snake_case.fn("Clasificación Principal") == "clasificacion_principal"
        assert to_snake_case.fn("SCIAN 2023") == "scian_2023"
        assert to_snake_case.fn("Índice de Bienes") == "indice_de_bienes"
    
    def test_get_sheet_names(self, scian_file):
        """Test getting all sheet names from XLSX."""
        sheet_names = get_sheet_names.fn(scian_file)
        
        print("\n" + "="*80)
        print("FOUND SHEETS IN XLSX:")
        print("="*80)
        for i, sheet_name in enumerate(sheet_names, 1):
            print(f"  {i}. {sheet_name}")
        print("="*80)
        
        assert len(sheet_names) > 0
        assert all(isinstance(name, str) for name in sheet_names)
    
    def test_import_single_sheet(self, scian_file):
        """Test importing a single sheet to DuckDB."""
        conn = duckdb.connect(":memory:")
        
        sheet_names = get_sheet_names.fn(scian_file)
        first_sheet = sheet_names[0]
        table_name = to_snake_case.fn(first_sheet)
        
        row_count = import_sheet_to_duckdb.fn(conn, scian_file, first_sheet, table_name)
        
        print(f"\nImported sheet '{first_sheet}' as table '{table_name}'")
        print(f"Row count: {row_count}")
        
        assert row_count > 0
        
        columns = conn.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
        """).fetchall()
        
        print(f"Columns: {[col[0] for col in columns]}")
        
        conn.close()
    
    def test_import_all_sheets(self, scian_file):
        """Test importing all sheets to DuckDB."""
        results = import_scian_to_duckdb.fn(scian_file, ":memory:")
        
        print("\n" + "="*80)
        print("IMPORTED TABLES:")
        print("="*80)
        for table_name, row_count in results.items():
            print(f"  {table_name}: {row_count:,} rows")
        print("="*80)
        
        assert len(results) > 0
        assert all(count > 0 for count in results.values())
    
    def test_validate_schema(self, scian_file):
        """Test schema validation for SCIAN tables."""
        db_path = ":memory:"
        conn = duckdb.connect(db_path)
        
        sheet_names = get_sheet_names.fn(scian_file)
        first_sheet = sheet_names[0]
        table_name = to_snake_case.fn(first_sheet)
        
        import_sheet_to_duckdb.fn(conn, scian_file, first_sheet, table_name)
        conn.close()
        
        expected_columns = ["Código", "Título"]
        is_valid = validate_scian_schema.fn(db_path, table_name, expected_columns)
        
        assert is_valid is True
    
    def test_validate_scian_codes_format(self, scian_file):
        """Test SCIAN code format validation (2-6 digits)."""
        db_path = ":memory:"
        conn = duckdb.connect(db_path)
        
        sheet_names = get_sheet_names.fn(scian_file)
        first_sheet = sheet_names[0]
        table_name = to_snake_case.fn(first_sheet)
        
        import_sheet_to_duckdb.fn(conn, scian_file, first_sheet, table_name)
        
        sample_codes = conn.execute(f"""
            SELECT "Código" 
            FROM {table_name}
            WHERE "Código" IS NOT NULL
            LIMIT 10
        """).fetchall()
        
        print("\n" + "="*80)
        print("SAMPLE SCIAN CODES:")
        print("="*80)
        for code in sample_codes:
            print(f"  {code[0]}")
        print("="*80)
        
        conn.close()
        
        is_valid = validate_scian_codes.fn(db_path, table_name)
        assert is_valid is True
    
    def test_no_empty_required_columns(self, scian_file):
        """Test that required columns (Código, Título) are not empty."""
        db_path = ":memory:"
        conn = duckdb.connect(db_path)
        
        sheet_names = get_sheet_names.fn(scian_file)
        first_sheet = sheet_names[0]
        table_name = to_snake_case.fn(first_sheet)
        
        import_sheet_to_duckdb.fn(conn, scian_file, first_sheet, table_name)
        
        empty_codigo = conn.execute(f"""
            SELECT COUNT(*) 
            FROM {table_name}
            WHERE "Código" IS NULL OR "Código" = ''
        """).fetchone()[0]
        
        empty_titulo = conn.execute(f"""
            SELECT COUNT(*) 
            FROM {table_name}
            WHERE "Título" IS NULL OR "Título" = ''
        """).fetchone()[0]
        
        print(f"\nEmpty 'Código' count: {empty_codigo}")
        print(f"Empty 'Título' count: {empty_titulo}")
        
        conn.close()
        
        assert empty_codigo == 0, f"Found {empty_codigo} empty Código values"
        assert empty_titulo == 0, f"Found {empty_titulo} empty Título values"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
