import logging
import networkx as nx
from dateutil import parser


EDGE_DEPENDENCY = 'p'
EDGE_DEV_DEPENDENCY = 'd'

EVENT_ADD = "a"
EVENT_DELETE = "d"


class PackagesGraph:
    def __init__(self, events_source, G=None, directed=True):

        if isinstance(G, (nx.Graph, nx.DiGraph)):
            self.G = G
        elif G is None:
            self.G = nx.DiGraph() if directed else nx.Graph()
        else:
            raise Exception('Invalid G parameter')

        self.events_source = events_source

    def get_isolated_nodes(self):
        return nx.isolates(self.G)

    def get_connected_graph_view(self):
        graph_size = len(self.G.nodes)
        min_size = graph_size * .9

        views = nx.connected_component_subgraphs(self.G, False)

        largest_size = 0
        largest_view = None

        for view in views:
            view_size = len(view.nodes)

            if view_size > min_size:
                return view

            logging.warning('view of', view_size,
                            'nodes is smaller then the minimum size:', min_size)

            if view_size > largest_size:
                largest_size = view_size
                largest_view = view

        return largest_view

    def add_event(self, event):

        pkg_name, _, _, event_type, edge_type, target = event

        u = pkg_name
        v = target

        if event_type == EVENT_ADD:
            if edge_type == EDGE_DEPENDENCY:
                self.G.add_edge(u, v, prod=True)
            elif edge_type == EDGE_DEV_DEPENDENCY:
                self.G.add_edge(u, v, dev=True)
            else:
                self.G.add_edge(u, v)

        elif event_type == EVENT_DELETE:
            edge_data = self.G.get_edge_data(u, v)
            if edge_data is None:
                logging.error('network_delete', "edge not exist", event_type)
            elif edge_type == EDGE_DEPENDENCY and edge_data.get("dev"):
                edge_data["prod"] = False
            elif edge_type == EDGE_DEV_DEPENDENCY and edge_data.get("prod"):
                edge_data["dev"] = False
            else:
                self.G.remove_edge(u, v)

    def build_graph_until(self, time_scope):
        stop_time = time_scope.strftime("%Y-%m-01")

        logging.info('Building graph until: %s', stop_time)

        if hasattr(self, 'last_event'):
            if self.last_event.date[:10] >= stop_time:
                raise Exception("No events to cover the target time scope")

            self.add_event(self.last_event)

        for event in self.events_source:
            if event.date[:10] >= stop_time:
                self.last_event = event
                return True

            self.add_event(event)

        return False

    def get_pagerank(self, sort=False):
        return self._get_metrics(nx.pagerank_scipy, sort)

    def get_in_degree_centrality(self, sort=False):
        return self._get_metrics(nx.in_degree_centrality, sort)

    def get_in_degree(self, sort=False):

        def in_degree(G):
            return dict(G.in_degree())

        return self._get_metrics(in_degree, sort)

    def get_out_degree(self, sort=False):
        return self._get_metrics(nx.out_degree_centrality, sort)

    def _get_metrics(self, metrics_function, sort=False):
        metrics_result = metrics_function(self.G)

        if not sort:
            return metrics_result

        sorted_list = sorted(metrics_result.items(),
                             key=lambda kv: kv[1], reverse=True)

        dict_result = {}

        index = 0
        top_list = 10
        for name, value in sorted_list:
            index += 1

            if index / top_list > 1:
                top_list *= 10

            dict_result[name] = (value, index, top_list)

        return dict_result
