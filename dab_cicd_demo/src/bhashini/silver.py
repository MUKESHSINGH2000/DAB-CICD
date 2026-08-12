from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    trim,
    when,
    lit
)
from pyspark.sql.types import StructType, StructField, StringType, BooleanType


# =====================================================
# Spark Session
# =====================================================

spark = SparkSession.builder.getOrCreate()


# =====================================================
# Configuration
# =====================================================

BRONZE_TABLE = "workspace.bronze.bhashini_user_application_report"

SILVER_TABLE = "workspace.silver.bhashini_user_application_report"


# =====================================================
# Schema for org JSON
# =====================================================

org_schema = StructType([
    StructField("org_type", StringType(), True),
    StructField("org_name", StringType(), True),
    StructField("org_website", StringType(), True),
    StructField("org_address", StringType(), True),
    StructField("pincode", StringType(), True),
    StructField("state", StringType(), True),
    StructField("city", StringType(), True),
    StructField("industry_type", StringType(), True),
    StructField("is_startup", BooleanType(), True),
    StructField("is_dpiit_certified", BooleanType(), True),
    StructField("is_interested_in_api_integration", BooleanType(), True),
    StructField("gst", StringType(), True)
])


# =====================================================
# Schema for application_details JSON
# =====================================================

application_schema = StructType([
    StructField("application_name", StringType(), True),
    StructField("service_domain", StringType(), True),
    StructField("usage_channel", StringType(), True),
    StructField("use_case", StringType(), True)
])


# =====================================================
# Main Function
# =====================================================

def main():

    try:

        print(f"Reading Bronze table: {BRONZE_TABLE}")

        # =============================================
        # Read Bronze
        # =============================================

        bronze_df = spark.table(BRONZE_TABLE)

        print(f"Bronze Record Count: {bronze_df.count()}")

        # =============================================
        # Parse JSON Columns
        # =============================================

        silver_df = (
            bronze_df

            # Parse org JSON
            .withColumn(
                "org_json",
                from_json(col("org"), org_schema)
            )

            # Parse application_details JSON
            .withColumn(
                "application_json",
                from_json(
                    col("application_details"),
                    application_schema
                )
            )
        )

        # =============================================
        # Flatten org Fields
        # =============================================

        silver_df = (
            silver_df

            .withColumn("org_type", col("org_json.org_type"))
            .withColumn("org_name", col("org_json.org_name"))
            .withColumn("org_website", col("org_json.org_website"))
            .withColumn("org_address", col("org_json.org_address"))
            .withColumn("pincode", col("org_json.pincode"))
            .withColumn("state", col("org_json.state"))
            .withColumn("city", col("org_json.city"))
            .withColumn("industry_type", col("org_json.industry_type"))
            .withColumn("is_startup", col("org_json.is_startup"))
            .withColumn(
                "is_dpiit_certified",
                col("org_json.is_dpiit_certified")
            )
            .withColumn(
                "is_interested_in_api_integration",
                col("org_json.is_interested_in_api_integration")
            )
            .withColumn("gst", col("org_json.gst"))
        )

        # =============================================
        # Flatten Application Fields
        # =============================================

        silver_df = (
            silver_df

            .withColumn(
                "application_name",
                col("application_json.application_name")
            )
            .withColumn(
                "service_domain",
                col("application_json.service_domain")
            )
            .withColumn(
                "usage_channel",
                col("application_json.usage_channel")
            )
            .withColumn(
                "use_case",
                col("application_json.use_case")
            )
        )

        # =============================================
        # Data Cleaning
        # =============================================

        silver_df = (
            silver_df

            # Trim important string columns
            .withColumn("api_key_name", trim(col("api_key_name")))
            .withColumn("email", trim(col("email")))
            .withColumn("org_name", trim(col("org_name")))
            .withColumn(
                "application_name",
                trim(col("application_name"))
            )

            # Convert timestamps
            .withColumn(
                "created_on",
                to_timestamp(col("created_on"))
            )
            .withColumn(
                "updated_on",
                to_timestamp(col("updated_on"))
            )
        )

        # =============================================
        # Remove Duplicates
        # =============================================

        silver_df = silver_df.dropDuplicates(
            [
                "api_key_name",
                "email",
                "application_name"
            ]
        )

        # =============================================
        # Select Final Silver Columns
        # =============================================

        silver_df = silver_df.select(
            "api_key_name",
            "email",

            "application_name",
            "service_domain",
            "usage_channel",
            "use_case",

            "org_type",
            "org_name",
            "org_website",
            "org_address",
            "pincode",
            "state",
            "city",
            "industry_type",
            "is_startup",
            "is_dpiit_certified",
            "is_interested_in_api_integration",
            "gst",

            "created_on",
            "updated_on",

            "hackathon_details",
            "parikshan_application",
            "plugin_details",

            "_ingestion_timestamp"
        )

        # =============================================
        # Show Results
        # =============================================

        print(f"Silver Record Count: {silver_df.count()}")

        print("Silver DataFrame Schema:")
        silver_df.printSchema()

        # =============================================
        # Write Silver Delta Table
        # =============================================

        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(SILVER_TABLE)
        )

        print("Silver table created successfully.")
        print(f"Table: {SILVER_TABLE}")

    except Exception as e:

        print(f"Silver transformation failed: {str(e)}")
        raise


# =====================================================
# Execute
# =====================================================

if __name__ == "__main__":
    main()