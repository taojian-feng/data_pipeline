"""AWS Glue job: Iceberg table maintenance for every governed table.

Runs nightly (see infra/terraform/orchestration.tf). Amazon S3 Tables performs
compaction and snapshot expiry automatically; this job covers tables in the
general-purpose S3 warehouse registered in the Glue Data Catalog.
"""

import sys

from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["catalog", "warehouse", "tables", "retain_days"])
cat = args["catalog"]
spark = GlueContext(SparkContext()).spark_session
spark.conf.set(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set(f"spark.sql.catalog.{cat}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set(f"spark.sql.catalog.{cat}.warehouse", args["warehouse"])
spark.conf.set(
    "spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
)

for table in args["tables"].split(","):
    ns, name = table.split(".")
    ident = f"{ns}.{name}"
    # bin-pack small files written by micro-batches, sorted for partition pruning
    spark.sql(
        f"CALL {cat}.system.rewrite_data_files(table => '{ident}', options => map('min-input-files','5'))"
    )
    spark.sql(f"CALL {cat}.system.rewrite_position_delete_files(table => '{ident}')")
    spark.sql(
        f"CALL {cat}.system.expire_snapshots(table => '{ident}', "
        f"older_than => current_timestamp() - INTERVAL {int(args['retain_days'])} DAYS, retain_last => 10)"
    )
    spark.sql(f"CALL {cat}.system.remove_orphan_files(table => '{ident}')")
