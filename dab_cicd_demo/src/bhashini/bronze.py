import requests
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


# =====================================================
# Spark Session
# =====================================================

spark = SparkSession.builder.getOrCreate()


# =====================================================
# Configuration
# =====================================================

API_URL = "https://dashboard-be.bhashini.co.in/v1/user_application_report"


# =====================================================
# Get API Key from Databricks Secret
# =====================================================

AUTH_KEY = dbutils.secrets.get(
    scope="bhashini-api",
    key="auth-key"
)

headers = {
    "Authorization": AUTH_KEY,
    "Accept": "application/json"
}


# =====================================================
# Normalize API Records
# =====================================================

def normalize_record(record):
    """
    Convert dictionaries and arrays into JSON strings.

    This prevents Spark schema inference errors when the
    same API field contains different data types in different
    records, for example StringType and ArrayType.
    """

    normalized = {}

    for key, value in record.items():

        if isinstance(value, (dict, list)):
            normalized[key] = json.dumps(value)

        else:
            normalized[key] = value

    return normalized


# =====================================================
# Main Function
# =====================================================

def main():

    try:

        print(f"Calling API: {API_URL}")

        # =============================================
        # Call API
        # =============================================

        response = requests.get(
            API_URL,
            headers=headers,
            timeout=60
        )

        print(f"Status Code: {response.status_code}")

        # Raise error if API call failed
        response.raise_for_status()

        # =============================================
        # Read JSON Response
        # =============================================

        data = response.json()

        print(f"Response Type: {type(data)}")
        print(f"Total API Records: {len(data)}")

        # =============================================
        # Check Empty Response
        # =============================================

        if not data:
            raise Exception("API returned an empty response")

        # =============================================
        # Normalize API Response
        # =============================================

        normalized_data = [
            normalize_record(record)
            for record in data
        ]

        # =============================================
        # Convert API Response to Spark DataFrame
        # =============================================

        df = spark.createDataFrame(normalized_data)

        print("API DataFrame Schema:")
        df.printSchema()

        # =============================================
        # Add Ingestion Timestamp
        # =============================================

        bronze_df = df.withColumn(
            "_ingestion_timestamp",
            current_timestamp()
        )

        # =============================================
        # Write to Bronze Delta Table
        # =============================================

        bronze_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(
                "workspace.bronze.bhashini_user_application_report"
            )

        print("Bronze table created successfully.")

        print(
            "Table: workspace.bronze.bhashini_user_application_report"
        )

    except Exception as e:

        print(f"Bronze ingestion failed: {str(e)}")

        raise e


# =====================================================
# Execute
# =====================================================

if __name__ == "__main__":
    main()