import logging
import os
from graph import PackagesGraph
from events import EventsReader
from dateutil import parser
from dateutil.relativedelta import relativedelta
from os import path
from scipy import stats
from datetime import datetime

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

DATA_DIR = path.abspath(path.join(
    path.dirname(path.abspath(__file__)),
    "../../../storage/registry/npm"
))
PACKAGES_DIR = path.abspath(path.join(DATA_DIR, "../../npm"))
LAST_TIME_SCOPE_PATH = path.join(DATA_DIR, "last_time_scope")


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

        # whatever the pointer is, it always write to the end because
        # it is opened with 'ab+' mode.
        f.write("{},{},{}\n".format(
            timestamp, rank, decline or ""
        ).encode())


def calculate_centrality(events_dir):
    reader = EventsReader(events_dir=events_dir)
    graph = PackagesGraph(events_source=reader)

    time_scope = read_last_time_scope() + relativedelta(months=+1)

    while graph.build_graph_until(time_scope):
        logging.info("Calculating centrality")

        pagerank = graph.get_pagerank(sort=False).items()
        pagerank = sorted(pagerank, key=lambda kv: kv[1], reverse=True)

        logging.info("Saving the results for %s packages", len(pagerank))
        timestamp = int(time_scope.timestamp())
        rank = 0
        for pkg_name, _ in pagerank:
            rank += 1
            if not pkg_name.islower():
                # we are supporting packages with lowercase names only
                continue

            writ_package_result(pkg_name, timestamp, rank)

        time_scope += relativedelta(months=+1)

    write_last_time_scope(time_scope + relativedelta(months=-1))
    logging.info("Done!")


def read_last_time_scope():
    with open(LAST_TIME_SCOPE_PATH, "r") as f:
        return parser.parse(f.read())


def write_last_time_scope(time_scope: datetime):
    new_time_str = time_scope.strftime("%Y-%m-01")
    with open(LAST_TIME_SCOPE_PATH, "r+") as f:
        # Fail the process if the time scope did not change
        if f.read() == new_time_str:
            raise Exception("No enough events to cover next time scope")

        f.truncate(0)
        f.write(new_time_str)


def main():
    events_dir = path.join(DATA_DIR, "./events")

    calculate_centrality(events_dir)


if __name__ == "__main__":
    main()
