from db_connector import DbConnection


conn = DbConnection()

cur = conn.get_cursor()
query = """select ida,idb from links limit 20"""
cur.execute(query)
for row in cur:
    print row