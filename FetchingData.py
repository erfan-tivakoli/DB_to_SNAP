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
:type Twitter: snap.PNEANet
"""

conn = DbConnection()
cur = conn.get_cursor()
query = """select ida,idb from links limit 20"""
cur.execute(query)


for [ida, idb] in cur:
    add_edge(Twitter, ida, idb)

for NI in Twitter.Nodes():
    print "node: %d, out-degree %d, in-degree %d" % ( NI.GetId(), NI.GetOutDeg(), NI.GetInDeg())

for EI in Twitter.Edges():
    print "edge (%d, %d)" % (EI.GetSrcNId(), EI.GetDstNId())