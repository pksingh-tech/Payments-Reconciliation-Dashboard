import csv
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class MetadataManager:

    def __init__(self):

        self.start_time = datetime.now()

    def save_metrics(
            self,
            total_records,
            status
    ):

        end_time = datetime.now()

        metrics_path = (
            PROJECT_ROOT
            / "reports"
            / "pipeline_metrics"
        )

        metrics_path.mkdir(
            parents=True,
            exist_ok=True
        )

        with open(
            metrics_path / "metrics.csv",
            "w",
            newline=""
        ) as metrics_file:
            writer = csv.writer(metrics_file)
            writer.writerow(
                [
                    "start_time",
                    "end_time",
                    "records_processed",
                    "status"
                ]
            )
            writer.writerow(
                [
                    str(self.start_time),
                    str(end_time),
                    total_records,
                    status
                ]
            )
