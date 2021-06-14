
from graph import PackagesGraph
from events import EventsReader
from dateutil import parser
from dateutil.relativedelta import relativedelta
import csv


DATA_DIR = "."


def main():
    output_file = open("./centrality_evolution_monthly_test.csv", "w")
    writer = csv.writer(output_file)

    min_time_scope = '2016-01-01'
    max_time_scope = '2021-04-01'
    time_scope = min_time_scope

    reader = EventsReader()
    graph = PackagesGraph(events_source=reader)
    has_more = graph.build_graph_until(time_scope)

    while has_more:
        pagerank = graph.get_pagerank(sort=False).items()
        pagerank = sorted(pagerank, key=lambda kv: kv[1], reverse=True)
        i = 0
        short_time_scope = time_scope[:7]
        for pkg_name, _ in pagerank:
            i += 1

            writer.writerow([
                pkg_name,
                short_time_scope,
                i,
            ])

        output_file.flush()
        print('Results saved to a file!!\n')

        stop_time = parser.parse(time_scope) + relativedelta(months=+1)
        time_scope = stop_time.isoformat()[:10]

        if time_scope < max_time_scope:
            has_more = False
            continue

        has_more = graph.build_graph_until(time_scope)

    output_file.close()


if __name__ == "__main__":
    main()
