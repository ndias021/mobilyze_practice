import logging

from src.core.base import Base
from src.core.raw import RawJob
from src.core.trusted import TrustedJob
from src.core.enriched import EnrichedJob


class EtlSlovakiaJob(Base):
    def run(self):
        to_raw = RawJob()
        to_trusted = TrustedJob()
        to_enriched = EnrichedJob()

        logging.info(f"Starting job on layer: {to_raw.layer}")
        to_raw.process()
        logging.info("-" * 20)

        logging.info(f"Starting job on layer: {to_trusted.layer}")
        to_trusted.process()
        logging.info("-" * 20)

        logging.info(f"Starting job on layer: {to_enriched.layer}")
        to_enriched.process()
        logging.info("-" * 20)


etl_job = EtlSlovakiaJob()
etl_job.run()
