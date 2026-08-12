from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    desc,
    date_format
)


# =====================================================
# Spark Session
# =====================================================

spark = SparkSession.builder.getOrCreate()


# =====================================================
# Configuration
# =====================================================

SILVER_TABLE = "workspace.silver.bhashini_user_application_report"


# =====================================================
# Gold Table Names
# =====================================================

GOLD_SERVICE_DOMAIN = (
    "workspace.gold.bhashini_service_domain_summary"
)

GOLD_STATE = (
    "workspace.gold.bhashini_state_summary"
)

GOLD_ORGANIZATION = (
    "workspace.gold.bhashini_organization_summary"
)

GOLD_USAGE_CHANNEL = (
    "workspace.gold.bhashini_usage_channel_summary"
)

GOLD_DAILY_TREND = (
    "workspace.gold.bhashini_daily_application_trend"
)


# =====================================================
# Main Function
# =====================================================

def main():

    try:

        # =============================================
        # Read Silver Table
        # =============================================

        print(f"Reading Silver table: {SILVER_TABLE}")

        silver_df = spark.table(SILVER_TABLE)

        print(
            f"Silver Record Count: {silver_df.count()}"
        )


        # =============================================
        # Gold 1 - Service Domain Summary
        # =============================================

        service_domain_df = (
            silver_df
            .groupBy("service_domain")
            .agg(
                count("*").alias("total_applications"),
                countDistinct("email").alias("unique_users"),
                countDistinct("org_name").alias(
                    "unique_organizations"
                )
            )
            .orderBy(desc("total_applications"))
        )

        (
            service_domain_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(GOLD_SERVICE_DOMAIN)
        )

        print(
            f"Created Gold table: {GOLD_SERVICE_DOMAIN}"
        )


        # =============================================
        # Gold 2 - State Summary
        # =============================================

        state_df = (
            silver_df
            .groupBy("state")
            .agg(
                count("*").alias("total_applications"),
                countDistinct("email").alias("unique_users"),
                countDistinct("org_name").alias(
                    "unique_organizations"
                )
            )
            .orderBy(desc("total_applications"))
        )

        (
            state_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(GOLD_STATE)
        )

        print(
            f"Created Gold table: {GOLD_STATE}"
        )


        # =============================================
        # Gold 3 - Organization Summary
        # =============================================

        organization_df = (
            silver_df
            .groupBy("org_name")
            .agg(
                count("*").alias("total_applications"),
                countDistinct("email").alias("unique_users"),
                countDistinct("service_domain").alias(
                    "service_domains_used"
                )
            )
            .orderBy(desc("total_applications"))
        )

        (
            organization_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(GOLD_ORGANIZATION)
        )

        print(
            f"Created Gold table: {GOLD_ORGANIZATION}"
        )


        # =============================================
        # Gold 4 - Usage Channel Summary
        # =============================================

        usage_channel_df = (
            silver_df
            .groupBy("usage_channel")
            .agg(
                count("*").alias("total_applications"),
                countDistinct("email").alias("unique_users")
            )
            .orderBy(desc("total_applications"))
        )

        (
            usage_channel_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(GOLD_USAGE_CHANNEL)
        )

        print(
            f"Created Gold table: {GOLD_USAGE_CHANNEL}"
        )


        # =============================================
        # Gold 5 - Daily Application Trend
        # =============================================

        daily_trend_df = (
            silver_df
            .filter(col("created_on").isNotNull())
            .withColumn(
                "application_date",
                date_format(
                    col("created_on"),
                    "yyyy-MM-dd"
                )
            )
            .groupBy("application_date")
            .agg(
                count("*").alias("total_applications"),
                countDistinct("email").alias("unique_users")
            )
            .orderBy("application_date")
        )

        (
            daily_trend_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(GOLD_DAILY_TREND)
        )

        print(
            f"Created Gold table: {GOLD_DAILY_TREND}"
        )


        # =============================================
        # Completion Message
        # =============================================

        print("Gold transformation completed successfully.")

    except Exception as e:

        print(f"Gold transformation failed: {str(e)}")
        raise


# =====================================================
# Execute
# =====================================================

if __name__ == "__main__":
    main()