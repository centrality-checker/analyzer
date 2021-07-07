import {
  createWriteStream,
  readFileSync,
  readdirSync,
  existsSync,
  statSync,
} from "fs";
import changes from "concurrent-couch-follower";
import semver from "semver";
import fetch from "node-fetch";
import { read } from "read-last-lines";
import validateName from "validate-npm-package-name";
import path from "path";
import lineByLine from "n-readlines";
import { SingleBar, Presets } from "cli-progress";
import { promisify } from "util";
import { exec as _exec } from "child_process";
import { fileURLToPath } from "url";
import log from "npmlog";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const exec = promisify(_exec);

const EVENT_FILE_PREFIX = "sorted_dependency_events_";
const EVENT_FILE_SUFFIX = ".csv";

const DATA_DIR = path.join(__dirname, "../../../storage/registry/npm");
const SEQUENCE_PATH = path.join(DATA_DIR, "sequence");

class RegistryReader {
  constructor(dataDir, sequencePath) {
    this.dataDir = dataDir;
    this.eventsDir = path.join(this.dataDir, "events");
    this.lastVersionsPath = path.join(this.dataDir, "last_versions.csv");
    this.lastSequence = Number(readFileValue(sequencePath) || 0);
    this.lastFileSeq = this.getLastEventsFileSequence();
    this.eventsPath = `${this.eventsDir}/dependency_events_${
      this.lastFileSeq + 1 || 0
    }.csv`;

    this.writable = createWriteStream(this.eventsPath, {
      flags: "w",
    });

    this.loadLastVersions();
  }

  loadLastVersions() {
    const lastVersions = new Map();
    this.lastVersions = lastVersions;

    if (!existsSync(this.lastVersionsPath)) {
      return;
    }

    const liner = new lineByLine(this.lastVersionsPath);
    let line, name, version;

    while ((line = liner.next())) {
      [name, version] = line.toString("utf8").split(",");
      lastVersions.set(name, version);
    }
  }

  saveLastVersions() {
    const ws = createWriteStream(this.lastVersionsPath, {
      flags: "w",
    });

    for (let [pkgName, lastVersion] of this.lastVersions) {
      ws.write(`${pkgName},${lastVersion}\n`);
    }

    ws.end();
  }

  getLastEventsFileSequence() {
    return readdirSync(this.eventsDir)
      .filter((s) => s.startsWith(EVENT_FILE_PREFIX))
      .map((s) => getFileSequence(s))
      .sort((a, b) => b - a)[0];
  }

  async getLastEventDate(sequence) {
    if (sequence == undefined) return;

    const filePath = `${this.eventsDir}/${EVENT_FILE_PREFIX}${sequence}.csv`;
    const lastLine = await read(filePath, 1);
    if (!lastLine) return;

    return lastLine.split(",")[2];
  }

  async runCollector(endSequence, configOptions) {
    this.lastDate = await this.getLastEventDate(this.lastFileSeq);
    this.endSequence = endSequence;

    log.info(
      "read from registry",
      "start after sequence: %j - last event was on: %j",
      this.lastSequence,
      this.lastDate
    );
    this.progressBar = new SingleBar(
      {
        etaBuffer: 10000,
        fps: 5,
        format:
          "Progress {bar} {percentage}% | ETA: {eta_formatted} | {value}/{total}",
      },
      Presets.shades_classic
    );
    this.progressBar.start(endSequence - this.lastSequence, 0);

    return new Promise((resolve, reject) => {
      this.resolve = resolve;

      this.stream = changes(this.dataHandler.bind(this), configOptions);
      this.stream.on("error", (err) => reject(err));
    }).then(() => this.orderEvents());
  }

  dataHandler(data, done) {
    this.progressBar.update(data.seq - this.lastSequence);

    if (data.seq >= this.endSequence) {
      this.stream.end();
      this.progressBar.stop();
      log.info(
        "read from registry",
        "ends before sequence: %j",
        this.endSequence
      );
      this.saveLastVersions();
      this.resolve();
      return;
    }

    const pkg = data.doc;
    if (!isPackage(pkg)) return done();

    const versionsList = this.getFilteredVersionsList(pkg);

    versionsList.forEach((versionObj, index) => {
      const lastVersion = versionsList[index - 1];

      const differences = getVersionDifferences(versionObj, lastVersion);

      this.writeOutputString(
        pkg.name,
        versionObj.version,
        versionObj.time,
        differences
      );
    });

    done();
  }

  writeOutputString(pkgName, version, date, differences) {
    for (const dataType in differences) {
      const events = differences[dataType];

      for (const event in events) {
        events[event].forEach((element) => {
          this.writable.write(
            `${pkgName},${version},${date},${event},${dataType},${cleanText(
              element
            )}\n`
          );
        });
      }
    }
  }

  getFilteredVersionsList(pkg) {
    let lastVersion = semver.parse(this.lastVersions.get(pkg.name) || "0.0.0");

    const result = [];
    Object.values(pkg.versions)
      .filter((versionObj) => {
        const versionDate = pkg.time[versionObj.version];
        if (!versionDate) return false;
        versionObj.time = versionDate;

        versionObj.version = semver.clean(versionObj.version);
        const version = semver.parse(versionObj.version);
        if (!version) return false;

        const verComp = version.compare(lastVersion);
        if (verComp == -1 || (verComp != 0 && versionDate < this.lastDate)) {
          return false;
        }

        result.push(versionObj);
        lastVersion = version;
        return true;
      })
      .sort((a, b) => a.time.localeCompare(b.time));

    this.lastVersions.set(pkg.name, lastVersion);

    return result;
  }

  async orderEvents() {
    const f = path.parse(this.eventsPath);
    f.base = "sorted_" + f.base;
    const sortedFile = path.format(f);

    log.info("events file", "sorting events by date");
    await exec(`sort -k3 -t, ${this.eventsPath} > ${sortedFile}`);

    log.info("events file", "deleting the unsorted file");
    await exec(`rm ${this.eventsPath}`);

    var fileSize = statSync(sortedFile).size / (1024 * 1024);
    if (fileSize < 100) {
      return;
    }

    log.info("events file", "split a large file");
    await exec(`split -l 10000000 ${sortedFile} splitted_`);

    log.verbose("events file", "rename the splitted files");
    const splittedFiles = readdirSync(this.eventsDir)
      .filter((s) => s.startsWith("splitted_"))
      .sort();

    const sequence = getFileSequence(f.base);
    for (const i in splittedFiles) {
      const from = `${this.eventsDir}/${splittedFiles[i]}`;
      const to = `${this.eventsDir}/${EVENT_FILE_PREFIX}${sequence + i}.csv`;
      await exec(`mv ${from} ${to}`);
    }
  }
}

function readFileValue(path) {
  try {
    return readFileSync(path, "utf8");
  } catch (err) {
    if (err.code === "ENOENT") {
      return undefined;
    }

    throw err;
  }
}

function cleanText(string) {
  if (typeof string === "string") {
    return string.replace(/[\n,]/g, "");
  } else {
    return "";
  }
}

function getVersionDifferences(newVersion, oldVersion) {
  const newVersionData = getVersionData(newVersion);
  const oldVersionData = oldVersion ? getVersionData(oldVersion) : {};

  const result = {};

  for (const dataType in newVersionData) {
    const newData = newVersionData[dataType] || [];
    const oldData = oldVersionData[dataType] || [];

    result[dataType] = getArrayDifferences(newData, oldData);
  }

  return result;
}

function getFileSequence(name) {
  return parseInt(
    name.slice(EVENT_FILE_PREFIX.length, -EVENT_FILE_SUFFIX.length),
    10
  );
}

function getVersionData(version) {
  return {
    p: getValidatedDependencies(version.dependencies),
    d: getValidatedDependencies(version.devDependencies),
  };
}

function getValidatedDependencies(dependencies) {
  if (typeof dependencies !== "object" || dependencies === null) return;

  return Object.keys(dependencies).filter(function (d) {
    return (
      semver.validRange(dependencies[d]) !== null &&
      validateName(d).validForOldPackages &&
      !d.includes("/.")
    );
  });
}

function getArrayDifferences(newArray, oldArray) {
  if (newArray && !oldArray) return { a: newArray };
  if (!newArray && oldArray) return { d: oldArray };

  const addedElements = [];
  const deletedElements = oldArray.slice(0);

  newArray.forEach(function (element) {
    let index = deletedElements.indexOf(element);
    if (index === -1) {
      addedElements.push(element);
    } else {
      deletedElements.splice(index, 1);
    }
  });

  return { a: addedElements, d: deletedElements };
}

function isPackage(doc) {
  if (
    !doc.name ||
    !doc.time ||
    !doc.versions ||
    !doc._id ||
    doc._id.startsWith("_design/") ||
    doc._deleted === true ||
    (doc.error === "not_found" && doc.reason === "deleted")
  ) {
    return false;
  }

  return true;
}

const configOptions = {
  db: "https://replicate.npmjs.com",
  include_docs: true,
  sequence: SEQUENCE_PATH,
  concurrency: 1,
};

process.on("uncaughtException", (err) => {
  console.error(err);
});

log.info("process", "started!");
const registry = new RegistryReader(DATA_DIR, SEQUENCE_PATH);
fetch(configOptions.db)
  .then((res) => res.json())
  .then((data) => registry.runCollector(data.update_seq, configOptions))
  .catch((err) => log.error(err))
  .finally(() => log.info("process", "all done!"));
