from db_connector import DbConnection


conn = DbConnection() if conn is None else conn

cur = conn.get_cursor()
query = """select ida,idb from links limit 20"""
cur.execute(query)
for row in cur:
    print row