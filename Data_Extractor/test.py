from Data_Extractor.db_connector import DbConnection

chunk_size = 100000
conn = DbConnection()
cursor = conn.get_cursor()
query = """select userid, tweettime from tweets where tweetid between %s and %s"""
cursor.execute(query, (0, 0 + chunk_size - 1))

for [userid, tweettime] in cursor:
    print("userid is %d and tweettime is %d" %(userid, long(tweettime.strftime('%s'))))