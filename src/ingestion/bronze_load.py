import csv
import json
import os
import uuid
from datetime import date, datetime
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.functions import max as spark_max

from utils.config_reader import load_config


PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_ENTITIES = {
    "transactions": {
        "watermark_column": "date",
        "primary_key": "transaction_id"
    },
    "refunds": {
        "watermark_column": "date",
        "primary_key": "refund_id"
    },
    "chargebacks": {
        "watermark_column": "date",
        "primary_key": "chargeback_id"
    }
}


class BronzeLoader:

    def __init__(self, spark):

        self.spark = spark

        self.pipeline_run_id = str(
            uuid.uuid4()
        )

        self.config = load_config(
            PROJECT_ROOT / "configs" / "app_config.yaml"
        )

        self.bronze_path = self._project_path(
            self.config.get(
                "storage",
                {}
            ).get(
                "bronze_path",
                "data/bronze"
            )
        )

        self.incremental_config = self.config.get(
            "incremental",
            {}
        )

        self.incremental_enabled = self.incremental_config.get(
            "enabled",
            True
        )

        self.entities = self._build_entity_config()

        self.watermark_path = self._project_path(
            self.incremental_config.get(
                "watermark_path",
                "reports/watermarks/bronze_watermarks.json"
            )
        )

        self.watermark_state = self._load_watermark_state()
        self.state_dirty = False
        self.bronze_audit_rows = []
        self.audit_dir = PROJECT_ROOT / "reports" / "audit"
        self.audit_history_path = (
            self.audit_dir
            / "bronze_load_audit.csv"
        )
        self.audit_last_run_path = (
            self.audit_dir
            / "bronze_load_last_run.csv"
        )

        self.source_system = (
            self.config.get(
                "source",
                {}
            ).get(
                "system",
                "neon"
            ).upper()
        )

        self.jdbc_url = (
            f"jdbc:postgresql://"
            f"{os.getenv('NEON_HOST')}:"
            f"{os.getenv('NEON_PORT')}/"
            f"{os.getenv('NEON_DATABASE')}"
            f"?sslmode={os.getenv('NEON_SSLMODE', 'require')}"
        )

        self.connection_properties = {
            "user": os.getenv("NEON_USER"),
            "password": os.getenv("NEON_PASSWORD"),
            "driver": "org.postgresql.Driver"
        }

        missing_keys = [
            key
            for key, value in {
                "NEON_HOST": os.getenv("NEON_HOST"),
                "NEON_PORT": os.getenv("NEON_PORT"),
                "NEON_DATABASE": os.getenv("NEON_DATABASE"),
                "NEON_USER": os.getenv("NEON_USER"),
                "NEON_PASSWORD": os.getenv("NEON_PASSWORD")
            }.items()
            if value is None
        ]

        if missing_keys:
            raise RuntimeError(
                "Missing Neon environment values: "
                + ", ".join(missing_keys)
            )

    def _project_path(
            self,
            path
    ):

        resolved_path = Path(path)

        if resolved_path.is_absolute():
            return resolved_path

        return PROJECT_ROOT / resolved_path

    def _spark_path(
            self,
            path
    ):

        return Path(path).resolve().as_uri().replace("%3D", "=")

    def _build_entity_config(self):

        configured_entities = self.incremental_config.get(
            "entities",
            {}
        )

        entities = {}

        for entity_name, defaults in DEFAULT_ENTITIES.items():
            entity_config = configured_entities.get(
                entity_name,
                {}
            )

            entities[entity_name] = {
                "watermark_column": entity_config.get(
                    "watermark_column",
                    defaults["watermark_column"]
                ),
                "primary_key": entity_config.get(
                    "primary_key",
                    defaults["primary_key"]
                )
            }

        return entities

    def _load_watermark_state(self):

        if not self.watermark_path.exists():
            return {}

        with open(
            self.watermark_path,
            "r"
        ) as state_file:
            return json.load(state_file)

    def _save_watermark_state(self):

        self.watermark_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        with open(
            self.watermark_path,
            "w"
        ) as state_file:
            json.dump(
                self.watermark_state,
                state_file,
                indent=2,
                sort_keys=True
            )

    def _audit_fields(self):

        return [
            "run_timestamp",
            "pipeline_run_id",
            "source_system",
            "source_table",
            "bronze_table",
            "bronze_path",
            "records_inserted",
            "status",
            "message"
        ]

    def _build_bronze_audit_row(
            self,
            entity_name,
            records_inserted,
            status,
            message=""
    ):

        return {
            "run_timestamp": datetime.utcnow().isoformat(),
            "pipeline_run_id": self.pipeline_run_id,
            "source_system": self.source_system,
            "source_table": entity_name,
            "bronze_table": entity_name,
            "bronze_path": str(self.bronze_path / entity_name),
            "records_inserted": int(records_inserted),
            "status": status,
            "message": message
        }

    def _write_bronze_audit(self):

        if not self.bronze_audit_rows:
            return

        self.audit_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        fields = self._audit_fields()
        write_history_header = not self.audit_history_path.exists()

        with open(
            self.audit_history_path,
            "a",
            newline=""
        ) as audit_file:
            writer = csv.DictWriter(
                audit_file,
                fieldnames=fields
            )

            if write_history_header:
                writer.writeheader()

            writer.writerows(
                self.bronze_audit_rows
            )

        with open(
            self.audit_last_run_path,
            "w",
            newline=""
        ) as last_run_file:
            writer = csv.DictWriter(
                last_run_file,
                fieldnames=fields
            )
            writer.writeheader()
            writer.writerows(
                self.bronze_audit_rows
            )

    def _quote_identifier(
            self,
            identifier
    ):

        return '"' + identifier.replace('"', '""') + '"'

    def _quote_literal(
            self,
            value
    ):

        return "'" + str(value).replace("'", "''") + "'"

    def _format_watermark_value(
            self,
            value
    ):

        if value is None:
            return None

        if isinstance(
            value,
            datetime
        ):
            if value.time() == datetime.min.time():
                return value.date().isoformat()

            return value.isoformat()

        if isinstance(
            value,
            date
        ):
            return value.isoformat()

        return str(value)

    def _is_empty(
            self,
            df
    ):

        return df.limit(1).count() == 0

    def _checkpoint_from_dataframe(
            self,
            entity_name,
            df
    ):

        entity_config = self.entities[entity_name]
        watermark_column = entity_config["watermark_column"]
        primary_key = entity_config["primary_key"]

        if self._is_empty(df):
            return None

        max_watermark = (
            df
            .select(
                spark_max(
                    col(watermark_column)
                ).alias("last_watermark")
            )
            .collect()[0]["last_watermark"]
        )

        if max_watermark is None:
            return None

        max_key = (
            df
            .filter(
                col(watermark_column) == lit(max_watermark)
            )
            .select(
                spark_max(
                    col(primary_key).cast("string")
                ).alias("last_key")
            )
            .collect()[0]["last_key"]
        )

        return {
            "watermark_column": watermark_column,
            "primary_key": primary_key,
            "last_watermark": self._format_watermark_value(
                max_watermark
            ),
            "last_key": max_key,
            "updated_at": datetime.utcnow().isoformat(),
            "pipeline_run_id": self.pipeline_run_id
        }

    def _bootstrap_checkpoint_from_bronze(
            self,
            entity_name
    ):

        entity_path = self.bronze_path / entity_name

        if not entity_path.exists():
            return None

        parquet_files = list(
            entity_path.rglob("*.parquet")
        )

        if not parquet_files:
            return None

        df = (
            self.spark.read
            .option("recursiveFileLookup", "true")
            .parquet(
                self._spark_path(entity_path)
            )
        )

        checkpoint = self._checkpoint_from_dataframe(
            entity_name,
            df
        )

        if checkpoint:
            checkpoint["bootstrapped_from_bronze"] = True

        return checkpoint

    def _get_checkpoint(
            self,
            entity_name
    ):

        checkpoint = self.watermark_state.get(
            entity_name
        )

        if checkpoint:
            return checkpoint

        checkpoint = self._bootstrap_checkpoint_from_bronze(
            entity_name
        )

        if checkpoint:
            self.watermark_state[entity_name] = checkpoint
            self.state_dirty = True

        return checkpoint or {}

    def _build_dbtable(
            self,
            table_name,
            checkpoint
    ):

        entity_config = self.entities[table_name]
        watermark_column = entity_config["watermark_column"]
        primary_key = entity_config["primary_key"]

        table_identifier = self._quote_identifier(
            table_name
        )
        watermark_identifier = self._quote_identifier(
            watermark_column
        )
        primary_key_identifier = self._quote_identifier(
            primary_key
        )

        where_clause = ""

        if (
                self.incremental_enabled
                and checkpoint.get("last_watermark")
        ):
            watermark_literal = self._quote_literal(
                checkpoint["last_watermark"]
            )

            if checkpoint.get("last_key"):
                key_literal = self._quote_literal(
                    checkpoint["last_key"]
                )
                where_clause = (
                    f" WHERE ({watermark_identifier} > {watermark_literal} "
                    f"OR ({watermark_identifier} = {watermark_literal} "
                    f"AND {primary_key_identifier} > {key_literal}))"
                )
            else:
                where_clause = (
                    f" WHERE {watermark_identifier} > {watermark_literal}"
                )

        query = (
            f"SELECT * FROM {table_identifier}"
            f"{where_clause}"
            f" ORDER BY {watermark_identifier}, {primary_key_identifier}"
        )

        return f"({query}) AS {table_name}_src"

    def read_table(
            self,
            table_name
    ):

        checkpoint = self._get_checkpoint(
            table_name
        )

        return (
            self.spark.read
            .format("jdbc")
            .option("url", self.jdbc_url)
            .option(
                "dbtable",
                self._build_dbtable(
                    table_name,
                    checkpoint
                )
            )
            .option("user", self.connection_properties["user"])
            .option("password", self.connection_properties["password"])
            .option("driver", self.connection_properties["driver"])
            .option("fetchsize", "10000")
            .load()
        )

    def add_audit_columns(
            self,
            df
    ):

        return (
            df
            .withColumn(
                "load_timestamp",
                current_timestamp()
            )
            .withColumn(
                "pipeline_run_id",
                lit(self.pipeline_run_id)
            )
            .withColumn(
                "source_system",
                lit(self.source_system)
            )
        )

    def load_transactions(self):

        return self.add_audit_columns(
            self.read_table("transactions")
        )

    def load_refunds(self):

        return self.add_audit_columns(
            self.read_table("refunds")
        )

    def load_chargebacks(self):

        return self.add_audit_columns(
            self.read_table("chargebacks")
        )

    def write_bronze(
            self,
            df,
            entity_name,
            records_inserted=None
    ):

        if records_inserted is None:
            records_inserted = df.count()

        if records_inserted == 0:
            return 0

        entity_path = self.bronze_path / entity_name

        (
            df
            .withColumn(
                "load_date",
                lit(
                    datetime.utcnow().strftime("%Y-%m-%d")
                )
            )
            .write
            .mode("append")
            .partitionBy("load_date")
            .parquet(
                self._spark_path(entity_path)
            )
        )

        return records_inserted

    def _update_checkpoint(
            self,
            entity_name,
            df,
            records_loaded=None
    ):

        if records_loaded == 0:
            return

        checkpoint = self._checkpoint_from_dataframe(
            entity_name,
            df
        )

        if not checkpoint:
            return

        if records_loaded is None:
            records_loaded = df.count()

        checkpoint["records_loaded"] = int(
            records_loaded
        )

        self.watermark_state[entity_name] = checkpoint
        self.state_dirty = True

    def _load_to_bronze(
            self,
            entity_name,
            df
    ):

        records_inserted = df.count()

        try:
            records_inserted = self.write_bronze(
                df,
                entity_name,
                records_inserted
            )
            self._update_checkpoint(
                entity_name,
                df,
                records_inserted
            )
            self.bronze_audit_rows.append(
                self._build_bronze_audit_row(
                    entity_name,
                    records_inserted,
                    "SUCCESS"
                )
            )
        except Exception as error:
            self.bronze_audit_rows.append(
                self._build_bronze_audit_row(
                    entity_name,
                    records_inserted,
                    "FAILED",
                    str(error)
                )
            )
            raise

    def execute(self):

        transactions = self.load_transactions()
        refunds = self.load_refunds()
        chargebacks = self.load_chargebacks()

        self._load_to_bronze(
            "transactions",
            transactions
        )

        self._load_to_bronze(
            "refunds",
            refunds
        )

        self._load_to_bronze(
            "chargebacks",
            chargebacks
        )

        if self.state_dirty:
            self._save_watermark_state()

        self._write_bronze_audit()

        return (
            transactions,
            refunds,
            chargebacks
        )
