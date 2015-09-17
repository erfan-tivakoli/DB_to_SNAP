import snap
import threading
import Queue
from db_connector import DbConnection

FIn = snap.TFIn("test.graph")
Twitter = snap.TNEANet.Load(FIn)
"""
:type Twitter:snap.TNEANet
"""
print("the graph was loaded")
Twitter.AddStrAttrN("TweetsTime")


def extract_tweets(thread_id, start_id, size):
    conn = DbConnection()
    cur = conn.get_cursor()
    query = """select userid, tweettime from tweets where tweetid between %s and %s"""
    cur.execute(query, (start_id, start_id + size - 1))
    print('\t[%d] query executed!' % start_id)
    cur.fetchall()
    for [userid, tweettime] in cur:
        if Twitter.IsNode(userid):
            node_tweet_times = Twitter.GetStrAttrDatN(userid, "TweetsTime")
            Twitter.AddStrAttrDatN(userid, node_tweet_times+","+tweettime, "TweetsTime")
            print("user id %d tweettimes %d" % (userid, Twitter.GetStrAttrDatN(userid, "TweetsTime")))
        else:
            print("missing userid %d" %userid)


class TweetsExtractor(threading.Thread):
    def __init__(self, thread_id, start_id, size):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.start_id = start_id
        self.size = size

    def run(self):
        extract_tweets(self.thread_id)


# threadLock = threading.Lock()
# threads = []
# counter = 0
#
#
# thread1 = MyThread(1)
# thread2 = MyThread(2)
#
# thread1.start()
# thread2.start()
#
# threads.append(thread1)
# threads.append(thread2)
#
# for t in threads:
#     t.join()
#
# print "Exiting Main Thread"

extract_tweets(1, 0, 1000)