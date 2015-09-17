import snap
import threading
import Queue
from db_connector import DbConnection

# FIn = snap.TFIn("test.graph")
# Twitter = snap.TNEANet.Load(FIn)
# """
# :type Twitter:snap.TNEANet
# """
# print("the graph was loaded")
# Twitter.AddStrAttrN("TweetsTime")


def extract_tweets(thread_id, cur):
    queueLock.acquire()
    start_point_id = start_points.get()
    queueLock.release()

    query = """select userid, tweettime from tweets where tweetid between %s and %s"""
    cur.execute(query, (start_point_id, start_point_id + chunk_size - 1))
    print('thread %d has fetched data from %d to %d' % (thread_id, start_point_id, start_point_id + chunk_size -1))
    cur.fetchall()
    print("what the hell is going wrong*******")
    for [userid, tweettime] in cur:
        print("userid is %d and tweettime is %d" %(userid, tweettime))
    # for [userid, tweettime] in cur:
    #     threadLock.acquire()
    #     if Twitter.IsNode(userid):
    #         node_tweet_times = Twitter.GetStrAttrDatN(userid, "TweetsTime")
    #         Twitter.AddStrAttrDatN(userid, node_tweet_times+","+tweettime, "TweetsTime")
    #         print("user id %d tweettimes %d" % (userid, Twitter.GetStrAttrDatN(userid, "TweetsTime")))
    #         threadLock.release()
    #     else:
    #         threadLock.release()
    #         print("missing userid %d" % userid)
    #     if start_point_id % 10000000 is 0:
    #         print('saving the graph=========================')
    #         threadLock.acquire()
    #         fout = snap.TFOut("test-with-tweets.graph")
    #         Twitter.Save(fout)
    #         fout.Flush()
    #         threadLock.release()
    #         print('saved')


class TweetsExtractor(threading.Thread):
    def __init__(self, thread_id, cur):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.cur = cur

    def run(self):
        while not start_points.empty():
            extract_tweets(self.thread_id, self.cur)

conn = DbConnection()
start_points = Queue.Queue(500000)

threadLock = threading.Lock()
queueLock = threading.Lock()
threads = []
number_of_threads = 1
threadID = 0
chunk_size = 10000

start_point = 0
queueLock.acquire()
while start_point < 4382219473:
    start_points.put(start_point)
    start_point += chunk_size
queueLock.release()

for i in range(number_of_threads):
    threadID += 1
    thread = TweetsExtractor(threadID, conn.get_cursor())
    threads.append(thread)
    print("thread %d was created" % threadID)
    thread.start()

for t in threads:
    t.join()
