# INCREMENTAL LOAD

import pandas as pd
from psycopg2.extras import execute_values
from etl_script import getConnection

# =========================================================
# 1. GET DATA FROM CSV
# =========================================================

df = pd.read_csv(
    r"C:\Users\LENOVO\Desktop\Intern\etl\restaurants_clean.csv"
)
print(df.columns.tolist())
print(df.head(10))

# =========================================================
# 2. CREATE CONNECTION
# =========================================================

conn = getConnection()
cursor = conn.cursor()


# =========================================================
# 3. GET LAST INGESTION DATE FROM ETL LOG
# =========================================================

def get_last_ingestion_date(cursor):

    cursor.execute("""
        SELECT COALESCE(
            MAX(ingestion_date),
            '1900-01-01 00:00:00'::timestamp
        )
        FROM etl_log
        WHERE script_name = 'restaurant_staging_load';
    """)

    return cursor.fetchone()[0]


last_ingestion_date = get_last_ingestion_date(cursor)

print("Last ingestion date:", last_ingestion_date)


# =========================================================
# 4. CONVERT CREATED AND MODIFIED TO DATETIME
# =========================================================

# Using dayfirst=True and removing the strict format allows Pandas 
# to dynamically handle whatever format Excel saved the CSV in.

df["created"] = pd.to_datetime(
    df["created"].astype(str).str.strip(),
    dayfirst=True, 
    errors="coerce"
)

df["modified"] = pd.to_datetime(
    df["modified"].astype(str).str.strip(),
    dayfirst=True,
    errors="coerce"
)

# CRITICAL FIX: Remove timezone info from the Postgres timestamp 
# so it can be mathematically compared to the CSV dates.
last_ingestion_date = pd.Timestamp(last_ingestion_date).tz_localize(None)

# =========================================================
# 5. FILTER INCREMENTAL RECORDS
# =========================================================

incremental_df = df[
    (df["created"] >= last_ingestion_date) |
    (df["modified"] >= last_ingestion_date)
].copy()

print("Incremental rows:", len(incremental_df))
print("\nINCREMENTAL DATA:")
print(
    incremental_df[
        ["restaurant_id", "restaurant_name", "rating"]
    ]
)

# =========================================================
# 6. SELECT ONLY COLUMNS REQUIRED IN STAGING
# =========================================================

columns = [
    "restaurant_id",
    "restaurant_url",
    "restaurant_name",
    "address",
    "location",
    "phone",
    "rating"
]

incremental_df = incremental_df[columns]


# =========================================================
# 7. CONVERT DATAFRAME TO TUPLES
# =========================================================

values = list(
    incremental_df.itertuples(
        index=False,
        name=None
    )
)


# =========================================================
# 8. INSERT INTO STAGING
# =========================================================

# UPSERT COMMAND SCD TYPE1 
if values:

    query = """
        INSERT INTO restaurants_stag (
            restaurant_id,
            restaurant_url,
            restaurant_name,
            address,
            location,
            phone,
            rating
        )
        VALUES %s

        ON CONFLICT (restaurant_id)
        DO UPDATE SET
            restaurant_url = EXCLUDED.restaurant_url,
            restaurant_name = EXCLUDED.restaurant_name,
            address = EXCLUDED.address,
            location = EXCLUDED.location,
            phone = EXCLUDED.phone,
            rating = EXCLUDED.rating
    """

    execute_values(
        cursor,
        query,
        values
    )

    print(f"{len(values)} rows inserted into staging.")

else:

    print("No new or modified records found.")


# =========================================================
# 9. WRITE ETL LOG
# =========================================================
cursor.execute(
    """
    INSERT INTO etl_log (script_name)
    VALUES (%s)
    """,
    ("restaurant_staging_load",)
)

# =========================================================
# 10. COMMIT
# =========================================================

conn.commit()

print("ETL completed successfully!")


# =========================================================
# 11. CLOSE CONNECTION
# =========================================================

cursor.close()
conn.close()

print("Connection closed.")