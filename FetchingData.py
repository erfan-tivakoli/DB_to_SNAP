from db_connector import DbConnection

conn = DbConnection()
cur = conn.get_cursor()
query = """select ida,idb from links limit 10"""
cur.execute(query)

print "finished"
