from __future__ import division

import snap
import sys


class GraphStructureBuilder:
    def __init__(self, conn):
        self._conn = conn
        self._graph = snap.TNEANet.New()
        """
        :type graph: snap.TNEANet
        """


    def fetch_add_links(self):
        counter = 0
        cur = self._conn.get_cursor()

        max_ida = cur.execute("""select max(ida) from links""").fetchone()[0]
        max_idb = cur.execute("""select max(idb) from links""").fetchone()[0]

        total_users = max(max_ida, max_idb)
        print("total user is %s " % (total_users))

        chunk = int(total_users / 10)
        from_id = 0

        links = []

        while from_id < total_users:
            print 'started fetching from %d to %d' % (from_id, from_id + chunk)
            links += cur.execute('select ida,idb from li.links where ? <= ida < ?',
                                 (from_id, from_id + chunk)).fetchall()
            print 'now links size is : %d' % len(links)
            from_id += chunk

        print 'started to build the links'
        for ida, idb in links:
            self.add_edge(ida, idb)
            counter += 1
            if counter % 1000000 is 0:
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
            if not self._graph.IsEdge(src_id, dst_id):
                self._graph.AddEdge(src_id, dst_id)
        except RuntimeError, e:
            sys.stderr.write('problem in adding %d -> %d' % (src_id, dst_id))

    def get_graph(self):
        return self._graph