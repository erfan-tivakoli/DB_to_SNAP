__author__ = 'Rfun'
from FetchingLinks import GraphStructureBuilder
from db_connector import DbConnection
import snap


def main():
    conn = DbConnection()
    graph_builder = GraphStructureBuilder(conn)
    graph_builder.fetch()
    graph_builder.build_links()
    structured_graph = graph_builder.get_graph()

    # saving the graph to a file
    FOut = snap.TFOut("structured_graph.graph")
    structured_graph.save(FOut)
    FOut.Flush()

    print 'structred graph was saved'


if __name__ == '__main__':
    main()