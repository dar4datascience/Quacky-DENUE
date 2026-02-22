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
from src.scian_utils import (
    to_snake_case,
    get_sheet_names,
    import_sheet_to_duckdb,
    import_scian_to_duckdb
)


@pytest.fixture(scope="module")
def scian_file():
    """Download main SCIAN classification file once for all tests."""
    html_content = fetch_scian_page.fn(INEGI_SCIAN_URL)
    xlsx_links = parse_xlsx_links.fn(html_content)
    
    if len(xlsx_links) == 0:
        pytest.skip("No .xlsx links found on INEGI page")
    
    url = xlsx_links[0][1]
    file_path = download_scian_file.fn(url)
    return file_path


class TestScianParser:
    """Live tests for SCIAN parser - no mocking allowed."""
    
    def test_snake_case_conversion(self):
        """Test sheet name to snake_case conversion."""
        assert to_snake_case("Clasificación Principal") == "clasificacion_principal"
        assert to_snake_case("SCIAN 2023") == "scian_2023"
        assert to_snake_case("Índice de Bienes") == "indice_de_bienes"
    
    def test_get_sheet_names(self, scian_file):
        """Test getting all sheet names from XLSX."""
        sheet_names = get_sheet_names(scian_file)
        
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
        
        sheet_names = get_sheet_names(scian_file)
        first_sheet = sheet_names[0]
        table_name = to_snake_case(first_sheet)
        
        row_count = import_sheet_to_duckdb(conn, scian_file, first_sheet, table_name)
        
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
        results = import_scian_to_duckdb(scian_file, ":memory:")
        
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
        conn = duckdb.connect(":memory:")
        
        sheet_names = get_sheet_names(scian_file)
        first_sheet = sheet_names[0]
        table_name = to_snake_case(first_sheet)
        
        import_sheet_to_duckdb(conn, scian_file, first_sheet, table_name)
        
        columns = conn.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """).fetchall()
        
        actual_columns = [col[0] for col in columns]
        
        print(f"\nTable '{table_name}' columns: {actual_columns}")
        
        assert "Código" in actual_columns
        assert "Título" in actual_columns
        
        conn.close()
    
    def test_validate_scian_codes_format(self, scian_file):
        """Test SCIAN code format validation (2-6 digits)."""
        conn = duckdb.connect(":memory:")
        
        sheet_names = get_sheet_names(scian_file)
        first_sheet = sheet_names[0]
        table_name = to_snake_case(first_sheet)
        
        import_sheet_to_duckdb(conn, scian_file, first_sheet, table_name)
        
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
        
        invalid_codes = conn.execute(f"""
            SELECT "Código" 
            FROM {table_name}
            WHERE "Código" IS NOT NULL 
            AND NOT regexp_matches("Código", '^[0-9]{{2,6}}$')
            LIMIT 10
        """).fetchall()
        
        conn.close()
        
        assert len(invalid_codes) == 0, f"Found invalid SCIAN codes: {invalid_codes}"
    
    def test_codigo_column_populated(self, scian_file):
        """Test that Código column has values (Título can be empty due to merged cells)."""
        conn = duckdb.connect(":memory:")
        
        sheet_names = get_sheet_names(scian_file)
        first_sheet = sheet_names[0]
        table_name = to_snake_case(first_sheet)
        
        import_sheet_to_duckdb(conn, scian_file, first_sheet, table_name)
        
        total_rows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        empty_codigo = conn.execute(f"""
            SELECT COUNT(*) 
            FROM {table_name}
            WHERE "Código" IS NULL OR "Código" = ''
        """).fetchone()[0]
        
        non_empty_titulo = conn.execute(f"""
            SELECT COUNT(*) 
            FROM {table_name}
            WHERE "Título" IS NOT NULL AND "Título" != ''
        """).fetchone()[0]
        
        print(f"\nTotal rows: {total_rows}")
        print(f"Empty 'Código' count: {empty_codigo}")
        print(f"Non-empty 'Título' count: {non_empty_titulo}")
        
        conn.close()
        
        assert empty_codigo == 0, f"Found {empty_codigo} empty Código values"
        assert non_empty_titulo > 0, "No Título values found"
        assert total_rows > 1000, f"Expected > 1000 rows, got {total_rows}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
