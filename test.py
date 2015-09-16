import snap
import ctypes


FIn = snap.TFIn("test.graph")
G = snap.TNEANet.Load(FIn)
"""
:type G:snap.TNEANet
"""
print("loaded")

edge_number = G.GetEdges()
node_number = G.GetNodes()
"""
:type Node:snap.TNEANetNodeI
"""
print("%d edges and %d nodes" % (edge_number, node_number))
