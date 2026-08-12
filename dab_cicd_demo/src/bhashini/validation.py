from pyspark.sql import SparkSession


# =====================================================
# Spark Session
# =====================================================

spark = SparkSession.builder.getOrCreate()


# =====================================================
# Table Configuration
# =====================================================

BRONZE_TABLE = "workspace.bronze.bhashini_user_application_report"

SILVER_TABLE = "workspace.silver.bhashini_user_application_report"

GOLD_TABLES = [
    "workspace.gold.bhashini_service_domain_summary",
    "workspace.gold.bhashini_state_summary",
    "workspace.gold.bhashini_organization_summary",
    "workspace.gold.bhashini_usage_channel_summary",
    "workspace.gold.bhashini_daily_application_trend"
]


# =====================================================
# Helper Function - Check Table Exists
# =====================================================

def validate_table_exists(table_name):

    if not spark.catalog.tableExists(table_name):
        raise Exception(
            f"VALIDATION FAILED: Table does not exist: {table_name}"
        )

    print(f"VALIDATION PASSED: Table exists: {table_name}")


# =====================================================
# Helper Function - Check Record Count
# =====================================================

def get_record_count(table_name):

    count_value = spark.table(table_name).count()

    print(
        f"Record Count [{table_name}]: {count_value}"
    )

    if count_value == 0:
        raise Exception(
            f"VALIDATION FAILED: Table is empty: {table_name}"
        )

    return count_value


# =====================================================
# Main Validation Function
# =====================================================

def main():

    try:

        print("=" * 70)
        print("STARTING BHASHINI PIPELINE VALIDATION")
        print("=" * 70)

        # =================================================
        # Step 1 - Validate Bronze Table
        # =================================================

        print("\n[1] VALIDATING BRONZE TABLE")

        validate_table_exists(BRONZE_TABLE)

        bronze_count = get_record_count(BRONZE_TABLE)

        # =================================================
        # Step 2 - Validate Silver Table
        # =================================================

        print("\n[2] VALIDATING SILVER TABLE")

        validate_table_exists(SILVER_TABLE)

        silver_count = get_record_count(SILVER_TABLE)

        # =================================================
        # Step 3 - Validate Bronze to Silver Count
        # =================================================

        print("\n[3] VALIDATING BRONZE → SILVER RECORD COUNTS")

        if silver_count > bronze_count:
            raise Exception(
                "VALIDATION FAILED: Silver record count cannot "
                "be greater than Bronze record count"
            )

        print(
            f"VALIDATION PASSED: "
            f"Bronze Count = {bronze_count}, "
            f"Silver Count = {silver_count}"
        )

        print(
            f"Records Removed/Transformed = "
            f"{bronze_count - silver_count}"
        )

        # =================================================
        # Step 4 - Validate Gold Tables
        # =================================================

        print("\n[4] VALIDATING GOLD TABLES")

        gold_counts = {}

        for table_name in GOLD_TABLES:

            validate_table_exists(table_name)

            record_count = get_record_count(table_name)

            gold_counts[table_name] = record_count

        # =================================================
        # Step 5 - Final Summary
        # =================================================

        print("\n" + "=" * 70)
        print("BHASHINI PIPELINE VALIDATION SUCCESSFUL")
        print("=" * 70)

        print(f"Bronze Records : {bronze_count}")
        print(f"Silver Records : {silver_count}")

        print("\nGold Table Records:")

        for table_name, record_count in gold_counts.items():

            print(
                f"{table_name} : {record_count}"
            )

        print("=" * 70)

    except Exception as e:

        print("\n" + "=" * 70)
        print("BHASHINI PIPELINE VALIDATION FAILED")
        print("=" * 70)

        print(f"Error: {str(e)}")

        raise


# =====================================================
# Execute
# =====================================================

if __name__ == "__main__":
    main()