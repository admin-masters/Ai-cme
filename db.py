from sqlalchemy import create_engine, text

DATABASE_URL = "mssql+pyodbc://new_root:japl%40bJBYV77@20.171.24.17/CME2?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no&TrustServerCertificate=yes"

engine = create_engine(DATABASE_URL)

with engine.connect() as conn:

    result = conn.execute(text("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
    """))

    tables = result.fetchall()

    with open("full_dump.sql", "w", encoding="utf-8") as f:

        for schema, table in tables:
            full_table = f"[{schema}].[{table}]"   # 🔥 FIX HERE
            print(f"Dumping {full_table}...")

            f.write(f"\n-- Data for {full_table}\n")

            rows = conn.execution_options(stream_results=True).execute(
                text(f"SELECT * FROM {full_table}")
            )

            columns = rows.keys()

            for row in rows:
                values = []

                for val in row:
                    if val is None:
                        values.append("NULL")
                    elif isinstance(val, (int, float)):
                        values.append(str(val))
                    elif isinstance(val, (bytes, bytearray)):
                        values.append(f"0x{val.hex()}")
                    else:
                        val_str = str(val).replace("'", "''")
                        values.append(f"'{val_str}'")

                insert = f"INSERT INTO {full_table} ({', '.join(columns)}) VALUES ({', '.join(values)});\n"
                f.write(insert)

print("✅ Full SQL dump created: full_dump.sql")