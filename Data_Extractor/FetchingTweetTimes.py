import threading
import Queue

import snap

from db_connector import DbConnection


FIn = snap.TFIn("../test-with-tweets.graph")
Twitter = snap.TNEANet.Load(FIn)
"""
:type Twitter:snap.TNEANet
"""
print("the graph was loaded")
# Twitter.AddStrAttrN("TweetsTime")


def extract_tweets(thread_id, cur):
    queueLock.acquire()
    start_point_id = start_points.get()
    queueLock.release()

    query = """select userid, tweettime from tweets where tweetid between %s and %s"""
    cur.execute(query, (start_point_id, start_point_id + chunk_size - 1))
    rows = cur.fetchall()
    print('thread %d has fetched data from %d to %d' % (thread_id, start_point_id, start_point_id + chunk_size -1))
    for [userid, tweettime] in rows:
        threadLock.acquire()
        if Twitter.IsNode(userid):
            node_tweet_times = Twitter.GetStrAttrDatN(userid, "TweetsTime")
            """
            :type node_tweet_times:string
            """
            new_tweet_time = str(long(tweettime.strftime('%s')))
            Twitter.AddStrAttrDatN(userid, node_tweet_times+","+new_tweet_time, "TweetsTime")
        threadLock.release()

    if start_point_id % 100000000 is 0 and (start_point_id != initial_value):
        print('========thread %d is saving the graph up to tweet_id %d==========' % (thread_id, start_point_id+ chunk_size))
        threadLock.acquire()
        fout = snap.TFOut("../test-with-tweets.graph")
        Twitter.Save(fout)
        fout.Flush()
        threadLock.release()
        print('saved')


class TweetsExtractor(threading.Thread):
    def __init__(self, thread_id, cur):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.cur = cur

    def run(self):
        while not start_points.empty():
            extract_tweets(self.thread_id, self.cur)

conn = DbConnection()
start_points = Queue.Queue(50000)

threadLock = threading.Lock()
queueLock = threading.Lock()
threads = []
number_of_threads = 20
threadID = 0
chunk_size = 1000000

start_point = 120000000
initial_value = start_point
# start_point = 1700000000
queueLock.acquire()
while start_point < 4382219473:
    start_points.put(start_point)
    start_point += chunk_size
queueLock.release()

for i in range(number_of_threads):
    threadID += 1
    cursor = conn.get_cursor()
    thread = TweetsExtractor(threadID, cursor)
    threads.append(thread)
    thread.start()


for t in threads:
    t.join()

print("final save")
Fout = snap.TFOut("../test-with-tweets.graph")
Twitter.Save(Fout)
Fout.Flush()
print("saved")

print("Tsucee")