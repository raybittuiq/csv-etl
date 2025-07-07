import logging

logging.basicConfig(filename='etl_pipeline.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)