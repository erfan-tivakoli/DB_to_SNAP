from db_connector import DbConnection
import snap
from __future__ import division



def add_edge(Graph, src_id, dst_id):
    """
    :type Graph : snap.PNEANet
    :param Graph:
    :param src_id:
    :param dst_id:
    :return:
    """
    if not Graph.IsNode(src_id):
        Graph.AddNode(src_id)
    if not Graph.IsNode(dst_id):
        Graph.AddNode(dst_id)
    Graph.AddEdge(src_id, dst_id)


conn = DbConnection()
cur = conn.get_cursor()
query = """select max(ida) from links"""
cur.execute(query)
max_ida = cur.fetchone()
query = """select max(idb) from links"""
cur.execute(query)
max_idb = cur.fetchone()
total_users = max(max_ida, max_idb)
print("total user is %s and max_ida is %s and max_idb is %s" % (total_users, max_ida, max_idb))

Twitter = snap.TNEANet.New()
"""
:type Twitter: snap.TNEANet
"""

i = 0
chunk = 10000
progress = (i/total_users)*100
while i < total_users:
    print('\r current progress is %.2f' % progress)
    print('started fetching links from ida %s to %s' % (i, i+chunk))
    query = """select ida,idb from links where (ida>= i and ida< i+chunk)"""
    cur.execute(query)
    i += chunk

    for [ida, idb] in cur:
        add_edge(Twitter, ida, idb)
    print('edges added to graph')

print('saving the graph')
FOut = snap.TFOut("test.graph")
Twitter.Save(FOut)
FOut.Flush()
