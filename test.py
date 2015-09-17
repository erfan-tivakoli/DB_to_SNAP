import snap
import ctypes
import Queue
from db_connector import DbConnection

chunk_size = 10000
conn = DbConnection()
cursor = conn.get_cursor()
query = """select userid, tweettime from tweets where tweetid between %s and %s"""
cursor.execute(query, (0, 0 + chunk_size - 1))
cursor.fetchall()
for [userid, tweettime] in cursor:
    print("userid is %d and tweettime is %d" %(userid, tweettime))