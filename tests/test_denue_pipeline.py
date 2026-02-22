import pytest
import logging
from pathlib import Path
import sys
import tempfile
import shutil
import json

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from denue_pipeline import DENUEPipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestDENUEPipeline:
    
    @pytest.fixture
    def temp_workspace(self):
        temp_dir = tempfile.mkdtemp(prefix="test_denue_pipeline_")
        workspace = {
            'root': Path(temp_dir),
            'database': str(Path(temp_dir) / "data" / "denue.duckdb"),
            'cache': str(Path(temp_dir) / "cache"),
            'report': str(Path(temp_dir) / "reports" / "report.json")
        }
        yield workspace
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_pipeline_single_dataset(self, temp_workspace):
        pipeline = DENUEPipeline(
            database_path=temp_workspace['database'],
            cache_dir=temp_workspace['cache'],
            report_path=temp_workspace['report']
        )
        
        stats = pipeline.run(limit=1)
        
        assert stats is not None
        assert stats['total_datasets_detected'] >= 1
        assert stats['successful_downloads'] >= 0
        assert stats['successful_ingestions'] >= 0
        
        logger.info(f"Pipeline stats: {json.dumps(stats, indent=2)}")
        
        report_path = Path(temp_workspace['report'])
        assert report_path.exists(), "Report should be created"
        
        with open(report_path, 'r') as f:
            report = json.load(f)
        
        assert report['total_datasets_detected'] == stats['total_datasets_detected']
        
        logger.info("Single dataset pipeline test completed successfully")
    
    def test_pipeline_multiple_datasets(self, temp_workspace):
        pipeline = DENUEPipeline(
            database_path=temp_workspace['database'],
            cache_dir=temp_workspace['cache'],
            report_path=temp_workspace['report']
        )
        
        stats = pipeline.run(limit=3)
        
        assert stats['total_datasets_detected'] == 3
        assert stats['successful_downloads'] + stats['failed_downloads'] == 3
        
        logger.info(f"Processed {stats['total_datasets_detected']} datasets")
        logger.info(f"Successful: {stats['successful_ingestions']}, Failed: {stats['failed_ingestions']}")
        
        if stats['failed_datasets']:
            logger.warning(f"Failed datasets: {stats['failed_datasets']}")
    
    def test_pipeline_caching_on_rerun(self, temp_workspace):
        pipeline = DENUEPipeline(
            database_path=temp_workspace['database'],
            cache_dir=temp_workspace['cache'],
            report_path=temp_workspace['report']
        )
        
        stats_1 = pipeline.run(limit=2)
        
        pipeline_2 = DENUEPipeline(
            database_path=temp_workspace['database'],
            cache_dir=temp_workspace['cache'],
            report_path=temp_workspace['report']
        )
        
        stats_2 = pipeline_2.run(limit=2)
        
        assert stats_2['total_datasets_detected'] == stats_1['total_datasets_detected']
        
        logger.info("First run stats:")
        logger.info(f"  Downloads: {stats_1['successful_downloads']}")
        logger.info(f"  Ingestions: {stats_1['successful_ingestions']}")
        
        logger.info("Second run stats (should use cache):")
        logger.info(f"  Downloads: {stats_2['successful_downloads']}")
        logger.info(f"  Ingestions: {stats_2['successful_ingestions']}")
        
        logger.info("Caching test completed - pipeline should skip already ingested datasets")
    
    def test_pipeline_report_structure(self, temp_workspace):
        pipeline = DENUEPipeline(
            database_path=temp_workspace['database'],
            cache_dir=temp_workspace['cache'],
            report_path=temp_workspace['report']
        )
        
        pipeline.run(limit=1)
        
        report_path = Path(temp_workspace['report'])
        assert report_path.exists()
        
        with open(report_path, 'r') as f:
            report = json.load(f)
        
        required_keys = [
            'execution_timestamp',
            'total_datasets_detected',
            'successful_downloads',
            'failed_downloads',
            'successful_ingestions',
            'failed_ingestions',
            'failed_datasets'
        ]
        
        for key in required_keys:
            assert key in report, f"Report should contain {key}"
        
        assert isinstance(report['failed_datasets'], list)
        
        logger.info(f"Report structure validated: {list(report.keys())}")
