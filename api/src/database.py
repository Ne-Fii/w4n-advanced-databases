import psycopg2


def connect():
    DB_NAME = "postgres"
    DB_USER = "postgres"
    DB_PASS = "root"
    DB_HOST = "db"
    DB_PORT = "5432"

    conn = None
    curr = None
    try:
        conn = psycopg2.connect(
            database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
        )
        print("Database connected successfully")
    except:
        print("Database not connected successfully")
        exit(1)

    curr = conn.cursor()
    return conn, curr


def insert_many(conn, curr, data: list[dict], table: str):
    # INSERT
    columns = ", ".join(data[0].keys())
    values = ", ".join(["%s" for _ in data[0]])

    data = [tuple(data.values()) for data in data]
    curr.executemany(f"INSERT INTO {table} ({columns}) VALUES ({values});", data)
    conn.commit()
    print("Data inserted successfully")

def delete_from(conn, curr, table: str):
    # DELETE
    curr.execute(f"DELETE FROM {table};")
    conn.commit()
    print("Data deleted successfully")
