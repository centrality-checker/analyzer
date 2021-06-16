import logging
import os
from graph import PackagesGraph
from events import EventsReader
from dateutil import parser
from dateutil.relativedelta import relativedelta
from os import path
from scipy import stats
from datetime import datetime

logging.basicConfig(level=logging.INFO)

DATA_DIR = path.abspath(path.join(
    path.dirname(path.abspath(__file__)),
    "../../../storage/registry/npm"
))

PACKAGES_DIR = path.abspath(path.join(DATA_DIR, "../../npm"))


def is_centrality_decline(x, y,  slope_threshold=0, pvalue_threshold=0.001):
    res = stats.linregress(x, y)

    return res.slope > slope_threshold and res.pvalue < pvalue_threshold


def writ_package_result(pkg_name, timestamp, rank):
    # create the scope directory if not exist
    pkg_path = "{}/{}".format(PACKAGES_DIR, pkg_name)
    if pkg_name[0] == "@":
        scope_dir = path.dirname(pkg_path)
        if not path.exists(scope_dir):
            os.makedirs(scope_dir)

    decline = 0
    with open(pkg_path, 'ab+') as f:
        try:
            # get last decline value (number of months)
            while f.read(1) != b',':
                f.seek(-2, os.SEEK_CUR)
            decline = int(f.readline().decode()[:-1] or 0)

            # get the result of last 5 month to me used in the decline test
            lines_num = 0
            while lines_num < 5:
                f.seek(-2, os.SEEK_CUR)

                if f.read(1) == b'\n':
                    lines_num += 1

            lines = f.readlines()

        except OSError:
            lines = []

        if len(lines) == 5:
            x = []
            y = []
            for line in lines:
                l = line.decode().split(",")
                x.append(int(l[0]))
                y.append(int(l[1]))
            x.append(timestamp)
            y.append(rank)

            decline = decline+1 if is_centrality_decline(x, y) else 0

        f.write("{},{},{}\n".format(
            timestamp, rank, decline or ""
        ).encode())


def calculate_centrality(events_dir, min_time_scope, max_time_scope):
    time_scope = min_time_scope

    reader = EventsReader(events_dir=events_dir)
    graph = PackagesGraph(events_source=reader)
    has_more = graph.build_graph_until(time_scope)

    while has_more:
        logging.info("Calculating centrality for time scope: %s", time_scope)
        current_time = parser.parse(time_scope)

        pagerank = graph.get_pagerank(sort=False).items()
        pagerank = sorted(pagerank, key=lambda kv: kv[1], reverse=True)

        rank = 0
        for pkg_name, _ in pagerank:
            rank += 1
            writ_package_result(pkg_name, int(current_time.timestamp()), rank)

        stop_time = current_time + relativedelta(months=+1)
        time_scope = stop_time.strftime("%Y-%m-01")

        if time_scope <= max_time_scope:
            has_more = False
            continue

        has_more = graph.build_graph_until(time_scope)


def read_last_time_scope():
    with open(path.join(DATA_DIR, "last_time_scope"), "r") as f:
        return f.read()


def write_last_time_scope(time_scope):
    with open(path.join(DATA_DIR, "last_time_scope"), "w") as f:
        f.write(time_scope)


def main():

    min_time_scope = read_last_time_scope()
    max_time_scope = datetime.today().strftime("%Y-%m-01")

    events_dir = path.join(DATA_DIR, "./events")
    calculate_centrality(events_dir, min_time_scope, max_time_scope)

    write_last_time_scope(max_time_scope)


if __name__ == "__main__":
    main()
