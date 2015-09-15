from db_connector import DbConnection
import snap



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


Twitter = snap.TNEANet.New()
"""
:type Twitter: snap.TNEANet
"""

conn = DbConnection()
cur = conn.get_cursor()
query = """select max(ida) from links"""
cur.execute(query)
max_ida = cur.fetchone()
query = """select max(idb) from links"""
cur.execute(query)
max_idb = cur.fetchone()
total_users = max(max_ida, max_idb)
print("total user is %d and max_ida is %d and max_idb is %d",(total_users, max_ida, max_idb))


# for i in range(1,1000):
query = """select ida,idb from links where ida<10000"""
cur.execute(query)

counter = 0
for [ida, idb] in cur:
    add_edge(Twitter, ida, idb)
    counter += 1
    print('\r counter is %d' %counter)

FOut = snap.TFOut("test.graph")
Twitter.Save(FOut)
FOut.Flush()

# for NI in Twitter.Nodes():
#     print "node: %d, out-degree %d, in-degree %d" % ( NI.GetId(), NI.GetOutDeg(), NI.GetInDeg())
#
# for EI in Twitter.Edges():
#     print "edge (%d, %d)" % (EI.GetSrcNId(), EI.GetDstNId())