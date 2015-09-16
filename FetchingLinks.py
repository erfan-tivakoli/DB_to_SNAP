from __future__ import division
from db_connector import DbConnection
import snap
import sys


def add_edge(graph, src_id, dst_id):
    """
    :type graph : snap.TNEANet
    :param graph:
    :param src_id:
    :param dst_id:
    :return:
    """
    if not graph.IsNode(src_id):
        graph.AddNode(src_id)
    if not graph.IsNode(dst_id):
        graph.AddNode(dst_id)
    try:
        graph.AddEdge(src_id, dst_id)
    except RuntimeError, e:
        sys.stderr.write('problem in adding %d -> %d' % (src_id, dst_id))
        sys.stderr.write(str(e))

conn = DbConnection()
cur = conn.get_cursor()
query = """select max(ida) from links"""
cur.execute(query)
max_ida = cur.fetchone()[0]
query = """select max(idb) from links"""
cur.execute(query)
max_idb = cur.fetchone()[0]
total_users = max(max_ida, max_idb)
print("total user is %s and max_ida is %s and max_idb is %s" % (total_users, max_ida, max_idb))

Twitter = snap.TNEANet.New()
"""
:type Twitter: snap.TNEANet
"""
cur.close()
cur = conn.get_cursor()

i = 0
chunk = 10000

while i < total_users:
    print('started fetching links from ida %s to %s' % (i, i+chunk))
    query = """select ida,idb from links where (ida>= %s and ida< %s)"""
    cur.execute(query, (i, i+chunk))
    i += chunk

    print("started to add into graph")
    for [ida, idb] in cur:
        add_edge(Twitter, ida, idb)
    print('edges added to graph')

    if i % 1000000 is 0:
        print('saving the graph')
        FOut = snap.TFOut("test.graph")
        Twitter.Save(FOut)
        FOut.Flush()
        print('saved')
