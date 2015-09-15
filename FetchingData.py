from db_connector import DbConnection
import snap

conn = DbConnection()

cur = conn.get_cursor()
query = """select ida,idb from links limit 20"""
cur.execute(query)

Twitter = snap.TNEANet.New()
for [ida, idb] in cur:
    print ida