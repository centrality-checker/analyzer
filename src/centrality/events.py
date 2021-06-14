from os import listdir, path
from collections import namedtuple

FILE_PREFIX = "sorted_dependency_events_"
FILE_SUFFIX = ".csv"

Event = namedtuple("Event", ["pkg_name", "version",
                             "date", "event", "event_type", "element"])


class EventsReader:
    def __init__(self, events_dir="./events") -> None:
        self.events_dir = events_dir

    def __iter__(self):
        files = listdir(self.events_dir)
        filles = filter(fillter_sorted_files, files)
        filles = sorted(filles, key=file_number)

        for file in files:
            file_path = path.join(self.events_dir, file)
            for line in open(file_path, "r"):
                yield Event(*line[:-1].split(',', 5))


def fillter_sorted_files(name):
    return name.startswith(FILE_PREFIX)


def file_number(name):
    return int(name[len(FILE_PREFIX):-len(FILE_SUFFIX)])
