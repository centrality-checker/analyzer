import networkx as nx
import re
from requests.utils import quote
from enum import Enum
from dateutil import parser


class NodeType(Enum):
    PACKAGE = 1
    DEVELOPER = 3
    KEYWORD = 4
    LICENSE = 5


class EdgeType(Enum):
    DEPENDENCY = 1
    DEV_DEPENDENCY = 2
    MAINTAINER = 3
    USE = 4
    HAS = 5


TTYPES_MAP = {
    'dependencies': (NodeType.PACKAGE, EdgeType.DEPENDENCY),
    'devDependencies': (NodeType.PACKAGE, EdgeType.DEV_DEPENDENCY),
    'maintainers': (NodeType.DEVELOPER, EdgeType.MAINTAINER),
    'keywords': (NodeType.KEYWORD, EdgeType.USE),
    'license': (NodeType.LICENSE, EdgeType.HAS),
}
# TODO: use the log Library


class PackagesGraph:

    def __init__(self, events_source, G=None, directed=True, node_representation='string', target_type={NodeType.PACKAGE}, error_log_path="./errors_graph.csv"):

        if isinstance(G, (nx.Graph, nx.DiGraph)):
            self.G = G
        elif G is None:
            self.G = nx.DiGraph() if directed else nx.Graph()
        else:
            raise Exception('Invalid G parameter')

        self.error_writer = open(error_log_path, "w")

        if node_representation == 'tuple':
            self.create_node = self.create_tuple_node
        else:
            self.create_node = self.create_string_node

        self.events_source = events_source

    @staticmethod
    def create_string_node(type, name):
        return name

    @staticmethod
    def create_tuple_node(type, name):
        return (type, name)

    def get_isolated_nodes(self):
        return nx.isolates(self.G)

    def get_connected_graph_view(self):
        print('get_connected_graph_view')  # -> bigger view

        graph_size = len(self.G.nodes)
        min_size = graph_size * .9

        views = nx.connected_component_subgraphs(self.G, False)

        largest_size = 0
        largest_view = None

        for view in views:
            view_size = len(view.nodes)

            if view_size > min_size:
                return view

            print('View of', view_size,
                  'nodes is not bigger then the minimum size:', min_size)

            if view_size > largest_size:
                largest_size = view_size
                largest_view = view

        return largest_view

    def log_error(self, location, error, line):
        self.error_writer.write(
            location + ',' + str(error).replace(',', ' ') + ',' + line)
        print(location, line, error)
        pass

    def add_event(self, event, target_type: set(NodeType)):

        pkg_name, _, _, event, event_type, element = event

        node_type, edge_type = TTYPES_MAP[event_type]

        if node_type not in target_type or len(pkg_name) < 1 or len(element) < 1:
            return

        # the data should be clean, but if not, we handle it here
        if node_type != NodeType.PACKAGE:
            element = re.sub(r"[\x00-\x08\x0b\x0e-\x1f\x7f]", "", element)

        elif not self.is_valid_pkg_name(element):
            return

        u = self.create_node(NodeType.PACKAGE, pkg_name)
        v = self.create_node(node_type, element)

        if event == 'add':
            if edge_type == EdgeType.DEPENDENCY:
                self.G.add_edge(u, v, prod=True)
            if edge_type == EdgeType.DEV_DEPENDENCY:
                self.G.add_edge(u, v, dev=True)
            else:
                self.G.add_edge(u, v)

        elif event == 'delete':
            edge_data = self.G.get_edge_data(u, v)
            if edge_data is None:
                self.log_error('network_delete', "edge not exist", event)
            elif edge_type == EdgeType.DEPENDENCY and edge_data.get("dev"):
                edge_data["prod"] = False
            elif edge_type == EdgeType.DEV_DEPENDENCY and edge_data.get("prod"):
                edge_data["dev"] = False
            else:
                self.G.remove_edge(u, v)

    def build_graph_until(self, stop_time):
        stop_time = parser.parse(stop_time).isoformat()[:10]

        print('building until', stop_time)

        if hasattr(self, 'last_event'):
            if self.last_event.date[:10] >= stop_time:
                print("warn: no new events until the targeted time")
                return True

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

    def is_valid_pkg_name(self, name):
        if name.startswith("@"):
            name = name[1:]

        return name == quote(name)
