from os import listdir, path
from collections import namedtuple

FILE_PREFIX = "sorted_dependency_events_"
FILE_SUFFIX = ".csv"

Event = namedtuple("Event", ["pkg_name", "version",
                             "date", "event_type", "edge_type", "target"])


class EventsReader:
    def __init__(self, events_dir):
        self.events_dir = events_dir

    def __iter__(self):
        files = listdir(self.events_dir)
        files = filter(is_events_file, files)
        files = sorted(files, key=file_sequence)

        for file in files:
            file_path = path.join(self.events_dir, file)
            for line in open(file_path, "r"):
                yield Event(*line[:-1].split(',', 5))


def is_events_file(name):
    return name.startswith(FILE_PREFIX)


def file_sequence(name):
    return int(name[len(FILE_PREFIX):-len(FILE_SUFFIX)])
