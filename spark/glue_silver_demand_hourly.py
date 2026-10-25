"""AWS Glue 5.0 (Spark 3.5 + Iceberg) job: bronze.eia_region_data -> silver.demand_hourly.

The scale-out twin of ``gridlake.transform.silver.demand_hourly``: identical semantics
(latest ingest wins per series, pivot to one row per BA-hour, quarantine rows failing the
contract's error rules) expressed as Spark SQL with an Iceberg MERGE INTO.

Incremental reads use the Iceberg snapshot recorded on the target table, so each run only
scans bronze files committed since the last successful run.

Job parameters (set in infra/terraform/glue.tf):
  --catalog            Spark catalog name bound to Glue / S3 Tables (default: glue_catalog)
  --warehouse          s3://.../warehouse
"""

import sys

from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "catalog", "warehouse"])
cat = args["catalog"]

sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
spark.conf.set(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set(f"spark.sql.catalog.{cat}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set(f"spark.sql.catalog.{cat}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set(f"spark.sql.catalog.{cat}.warehouse", args["warehouse"])
spark.conf.set(
    "spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
)
spark.conf.set("spark.sql.session.timeZone", "UTC")
job = Job(glue)
job.init(args["JOB_NAME"], args)

SRC, DST = f"{cat}.bronze.eia_region_data", f"{cat}.silver.demand_hourly"
HWM_KEY = "gridlake.hwm-snapshot.bronze.eia_region_data"

props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {DST}").collect()}
last_snapshot = props.get(HWM_KEY)
current_snapshot = spark.sql(
    f"SELECT snapshot_id FROM {SRC}.snapshots ORDER BY committed_at DESC LIMIT 1"
).first()[0]

if last_snapshot is None:
    spark.table(SRC).createOrReplaceTempView("new_rows")
elif int(last_snapshot) == current_snapshot:
    job.commit()
    sys.exit(0)
else:
    (
        spark.read.format("iceberg")
        .option("start-snapshot-id", last_snapshot)
        .option("end-snapshot-id", str(current_snapshot))
        .load(SRC)
        .createOrReplaceTempView("new_rows")
    )

spark.sql(f"""
    WITH touched AS (SELECT DISTINCT period, respondent FROM new_rows),
    latest AS (
        SELECT b.*, row_number() OVER (PARTITION BY b.period, b.respondent, b.type ORDER BY b._ingest_ts DESC) AS rn
        FROM {SRC} b JOIN touched t ON b.period = t.period AND b.respondent = t.respondent
    )
    SELECT to_timestamp(period, "yyyy-MM-dd'T'HH") AS period,
           respondent AS ba_code,
           max(CASE WHEN type = 'D'  THEN try_cast(value AS DOUBLE) END) AS demand_mwh,
           max(CASE WHEN type = 'DF' THEN try_cast(value AS DOUBLE) END) AS forecast_mwh,
           max(CASE WHEN type = 'NG' THEN try_cast(value AS DOUBLE) END) AS net_generation_mwh,
           max(CASE WHEN type = 'TI' THEN try_cast(value AS DOUBLE) END) AS interchange_mwh,
           current_timestamp() AS _updated_ts
    FROM latest WHERE rn = 1
    GROUP BY 1, 2
""").createOrReplaceTempView("candidates")

# Contract error rules (contracts/silver.demand_hourly.yaml) as a single predicate.
spark.sql(f"""
    SELECT c.*,
           concat_ws('; ',
             CASE WHEN demand_mwh IS NULL THEN 'not_null(demand_mwh)' END,
             CASE WHEN demand_mwh < 0 OR demand_mwh > 250000 THEN 'range(demand_mwh:0..250000)' END,
             CASE WHEN d.ba_code IS NULL THEN 'referential(ba_code->silver.dim_balancing_authority.ba_code)' END
           ) AS failed_rules
    FROM candidates c LEFT JOIN {cat}.silver.dim_balancing_authority d USING (ba_code)
""").cache().createOrReplaceTempView("checked")

spark.sql(f"""
    INSERT INTO {cat}.quality.quarantine
    SELECT '{args["JOB_NAME"]}' AS run_id, 'silver.demand_hourly', failed_rules,
           to_json(struct(period, ba_code, demand_mwh, forecast_mwh, net_generation_mwh, interchange_mwh)),
           current_timestamp()
    FROM checked WHERE failed_rules <> ''
""")

spark.sql(f"""
    MERGE INTO {DST} t
    USING (SELECT * EXCEPT (failed_rules) FROM checked WHERE failed_rules = '') s
    ON t.period = s.period AND t.ba_code = s.ba_code
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

spark.sql(f"ALTER TABLE {DST} SET TBLPROPERTIES ('{HWM_KEY}' = '{current_snapshot}')")
job.commit()
