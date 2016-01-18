from __future__ import division

import snap
import sys

class GraphStructureBuilder:
    def __init__(self, conn):
        self._conn = conn
        self._graph = snap.TNEANet.New()
        self._links = []
        self._missed_links = 0
        """
        :type graph: snap.TNEANet
        """


    def fetch(self):
        counter = 0
        cur = self._conn.get_cursor()
        max_ida = cur.execute("""select max(ida) from links""").fetchone()[0]
        max_idb = cur.execute("""select max(idb) from links""").fetchone()[0]

        total_users = max(max_ida, max_idb)
        print("total user is %s " % (total_users))

        chunk = 1
        from_id = 0

        while from_id < total_users:
            self._links += cur.execute('select ida,idb from li.links where ida = ?',
                                       (from_id,)).fetchall()
            if from_id % 100000 == 0:
                print 'started fetching for %d ' % (from_id)
                print 'now links size is : %d' % len(self._links)
            from_id += chunk

        cur.close()


    def build_links(self):

        counter = 0
        print 'started to build the links'
        for ida, idb in self._links:
            self.add_edge(ida, idb)
            counter += 1
            if counter % 100000 is 0:
                print counter


    def add_edge(self, src_id, dst_id):
        """
        :param src_id:
        :param dst_id:
        :return:
        """
        if not self._graph.IsNode(src_id):
            self._graph.AddNode(src_id)
        if not self._graph.IsNode(dst_id):
            self._graph.AddNode(dst_id)
        try:
            self._graph.AddEdge(src_id, dst_id)
        except RuntimeError, e:
            sys.stderr.write(e.message)
            sys.stderr.write('\n')
            sys.stderr.write('problem in adding %d -> %d \n' % (src_id, dst_id))
            self._missed_links += 1
            sys.stderr.write('number of missed links %d' %self._missed_links)
            sys.stderr.write('============ \n')

    def get_graph(self):
        return self._graph

    def get_links(self):
        return self._links