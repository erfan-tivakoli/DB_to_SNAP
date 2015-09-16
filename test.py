import snap

FIn = snap.TFIn("test.graph")
G = snap.TNEANet.Load(FIn)

"""
:type G:snap.TNEANet
"""

Node = G.GetNI(50698488)
"""
:type Node:snap.TNEANetNodeI
"""
print("in degree is %s and out degree is %s" % (Node.GetInDeg(), Node.GetOutDeg()))