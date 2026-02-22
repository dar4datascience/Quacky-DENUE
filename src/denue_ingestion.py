import logging
import json
from pathlib import Path
from typing import Dict, Optional, List
import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


class DENUEIngestion:
    def __init__(self, database_path: str = "./data/denue.duckdb"):
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = None
        
    def __enter__(self):
        self.conn = duckdb.connect(str(self.database_path))
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
    
    def initialize_database(self):
        logger.info("Initializing DENUE database...")
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS denue (
                id VARCHAR,
                nom_estab VARCHAR,
                raz_social VARCHAR,
                codigo_act VARCHAR,
                nombre_act VARCHAR,
                per_ocu VARCHAR,
                tipo_vial VARCHAR,
                nom_vial VARCHAR,
                tipo_v_e_1 VARCHAR,
                nom_v_e_1 VARCHAR,
                tipo_v_e_2 VARCHAR,
                nom_v_e_2 VARCHAR,
                tipo_v_e_3 VARCHAR,
                nom_v_e_3 VARCHAR,
                numero_ext VARCHAR,
                letra_ext VARCHAR,
                edificio VARCHAR,
                edificio_e VARCHAR,
                numero_int VARCHAR,
                letra_int VARCHAR,
                tipo_asent VARCHAR,
                nomb_asent VARCHAR,
                tipoCenCom VARCHAR,
                nom_CenCom VARCHAR,
                num_local VARCHAR,
                cod_postal VARCHAR,
                cve_ent VARCHAR,
                entidad VARCHAR,
                cve_mun VARCHAR,
                municipio VARCHAR,
                cve_loc VARCHAR,
                localidad VARCHAR,
                ageb VARCHAR,
                manzana VARCHAR,
                telefono VARCHAR,
                correoelec VARCHAR,
                www VARCHAR,
                tipoUniEco VARCHAR,
                latitud DOUBLE,
                longitud DOUBLE,
                fecha_alta VARCHAR,
                periodo_consulta VARCHAR,
                PRIMARY KEY (id, periodo_consulta)
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ingestion_log (
                sector VARCHAR,
                periodo_consulta VARCHAR,
                ingestion_timestamp TIMESTAMP,
                record_count INTEGER,
                file_size_mb DOUBLE,
                status VARCHAR,
                error_message VARCHAR,
                PRIMARY KEY (sector, periodo_consulta)
            )
        """)
        
        logger.info("Database initialized successfully")
    
    def is_dataset_ingested(self, sector: str, period: str) -> bool:
        result = self.conn.execute("""
            SELECT COUNT(*) FROM ingestion_log 
            WHERE sector = ? AND periodo_consulta = ? AND status = 'success'
        """, [sector, period]).fetchone()
        
        return result[0] > 0 if result else False
    
    def ingest_dataset(self, df: pd.DataFrame, metadata: Dict, sector: str, period: str) -> bool:
        try:
            if self.is_dataset_ingested(sector, period):
                logger.info(f"Dataset {sector} ({period}) already ingested, skipping...")
                return True
            
            logger.info(f"Ingesting dataset {sector} ({period}) with {len(df)} records")
            
            df['periodo_consulta'] = period
            
            required_cols = ['id', 'periodo_consulta']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing required columns: {missing_cols}")
                self._log_ingestion(sector, period, 0, 0, 'failed', 
                                   f"Missing required columns: {missing_cols}")
                return False
            
            table_cols = [row[0] for row in self.conn.execute("DESCRIBE denue").fetchall()]
            df_cols_in_table = [col for col in df.columns if col in table_cols]
            
            logger.debug(f"Inserting {len(df_cols_in_table)}/{len(df.columns)} matching columns using BY NAME")
            
            initial_count = self.conn.execute("SELECT COUNT(*) FROM denue").fetchone()[0]
            
            df_subset = df[df_cols_in_table]
            
            update_cols = [col for col in df_cols_in_table if col not in ['id', 'periodo_consulta']]
            update_list = ', '.join([f"{col} = excluded.{col}" for col in update_cols])
            
            self.conn.execute(f"""
                INSERT INTO denue BY NAME
                SELECT * FROM df_subset
                ON CONFLICT (id, periodo_consulta) 
                DO UPDATE SET {update_list}
            """)
            
            final_count = self.conn.execute("SELECT COUNT(*) FROM denue").fetchone()[0]
            records_added = final_count - initial_count
            
            file_size_mb = float(metadata.get('file_size', '0').replace('MB', '').strip())
            
            self._log_ingestion(sector, period, len(df), file_size_mb, 'success', None)
            
            logger.info(f"Successfully ingested {records_added} records from {sector} ({period})")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting dataset {sector} ({period}): {e}")
            self._log_ingestion(sector, period, 0, 0, 'failed', str(e))
            return False
    
    def _log_ingestion(self, sector: str, period: str, record_count: int, 
                       file_size_mb: float, status: str, error_message: Optional[str]):
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO ingestion_log 
                (sector, periodo_consulta, ingestion_timestamp, record_count, 
                 file_size_mb, status, error_message)
                VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
            """, [sector, period, record_count, file_size_mb, status, error_message])
        except Exception as e:
            logger.error(f"Error logging ingestion: {e}")
    
    def get_statistics(self) -> Dict:
        try:
            total_records = self.conn.execute("SELECT COUNT(*) FROM denue").fetchone()[0]
            
            total_datasets = self.conn.execute("""
                SELECT COUNT(*) FROM ingestion_log WHERE status = 'success'
            """).fetchone()[0]
            
            failed_datasets = self.conn.execute("""
                SELECT COUNT(*) FROM ingestion_log WHERE status = 'failed'
            """).fetchone()[0]
            
            db_size_mb = self.database_path.stat().st_size / (1024 * 1024)
            
            total_file_size = self.conn.execute("""
                SELECT SUM(file_size_mb) FROM ingestion_log WHERE status = 'success'
            """).fetchone()[0] or 0
            
            compression_ratio = db_size_mb / total_file_size if total_file_size > 0 else 0
            
            return {
                'total_records_ingested': total_records,
                'successful_ingestions': total_datasets,
                'failed_ingestions': failed_datasets,
                'total_size_duckdb_mb': round(db_size_mb, 2),
                'total_size_compressed_mb': round(total_file_size, 2),
                'compression_ratio': round(compression_ratio, 2)
            }
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {}
    
    def get_failed_datasets(self) -> List[Dict]:
        try:
            result = self.conn.execute("""
                SELECT sector, periodo_consulta, error_message
                FROM ingestion_log
                WHERE status = 'failed'
            """).fetchall()
            
            return [
                {
                    'sector': row[0],
                    'periodo': row[1],
                    'error': row[2]
                }
                for row in result
            ]
        except Exception as e:
            logger.error(f"Error getting failed datasets: {e}")
            return []
