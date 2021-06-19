import { createWriteStream, readFileSync, readdirSync, existsSync } from "fs";
import changes from "concurrent-couch-follower";
import { validRange, SemVer, clean } from "semver";
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

class RegistryReader {
  constructor(dataDir) {
    this.dataDir = dataDir;
    this.eventsDir = path.join(this.dataDir, "events");
    this.lastVersionsPath = path.join(this.dataDir, "last_versions.csv");
    this.lastSequence = Number(readFileValue("npm_registry_sequence") || 0);
    this.eventsPath = `${this.eventsDir}/dependency_events_${this.lastSequence}.csv`;

    this.writable = createWriteStream(this.eventsPath, {
      flags: "a",
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

  async getLastDate() {
    const lastFile = readdirSync(this.eventsDir)
      .filter((s) => s.startsWith(EVENT_FILE_PREFIX))
      .sort((a, b) => (getFileSequence(a) < getFileSequence(b) ? 1 : -1))[0];

    if (!lastFile) return;

    const lastFilePath = path.join(this.eventsDir, lastFile);
    const lines = await read(lastFilePath, 1);

    if (!lines) return;

    return lines[0].split(",")[2];
  }

  async runCollector(endSequence, configOptions) {
    this.lastDate = await this.getLastDate();
    this.endSequence = endSequence;

    log.info(
      "read from registry",
      "start after sequence: %j",
      this.lastSequence
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
    });
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

    const pkg = clean_pkg(data.doc);
    if (!pkg) return done();



    const versionsList = this.getFilteredVersionsList(pkg);

    versionsList.forEach((version, index) => {
      const lastVersion = versionsList[index - 1];

      const differences = getVersionDifferences(
        pkg.versions[version],
        pkg.versions[lastVersion]
      );

      this.writeOutputString(
        pkg.name,
        version,
        pkg.time[version],
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
    let versionsList = Object.keys(pkg.versions);

    // sort versions by the release time
    versionsList.sort((a, b) => pkg.time[a].localeCompare(pkg.time[b]));

    const result = [];
    let lastVersion = new SemVer(this.lastVersions.get(pkg.name) || "0.0.0");
    versionsList.forEach((strVersion) => {
      let version;
      try {
        version = new SemVer(strVersion);
      } catch (e) {
        // ignore invalid versions
        return;
      }

      const verComp = version.compare(lastVersion);
      if (verComp == -1 || (verComp != 0 && pkg.time[strVersion] < this.lastDate))
        return;

      result.push(strVersion);
      lastVersion = version;
    });

    this.lastVersions.set(pkg.name, lastVersion);

    return result;
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
      validRange(dependencies[d]) !== null &&
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

function clean_pkg(doc) {
  if (
    !doc.name ||
    !doc.time ||
    !doc.versions ||
    !doc._id ||
    doc._id.indexOf("_design/") === 0
  )
    return;
  if (
    doc._deleted === true ||
    (doc.error === "not_found" && doc.reason === "deleted")
  )
    return;

  var origVersions = Object.keys(doc.versions);
  origVersions.forEach(function (version) {
    var cleaned = clean(version, true);
    if (cleaned && cleaned !== version) {
      // clean the version
      doc.versions[cleaned] = doc.versions[version];
      delete doc.versions[version];

      doc.versions[cleaned].version = cleaned;
      doc.versions[cleaned]._id = doc._id + "@" + cleaned;

      if (doc.time[version]) {
        doc.time[cleaned] = doc.time[version];
        delete doc.time[version];
      }
    }
  });

  return doc;
}

const DATA_DIR = path.join(__dirname, "../../../storage/registry/npm");

const configOptions = {
  db: "https://replicate.npmjs.com",
  include_docs: true,
  sequence: path.join(DATA_DIR, "sequence"),
  concurrency: 30,
};

async function orderEvents(unsortedFile) {
  const f = path.parse(unsortedFile);
  f.base = "sorted_" + f.base;
  const sortedFile = path.format(f);

  log.info("events file", "sorting events by date");
  await exec(`sort -k3 -t, ${unsortedFile} > ${sortedFile}`);

  log.info("events file", "deleting the unsorted file");
  return await exec(`rm ${unsortedFile}`);
}

log.info("process", "started!");
const registry = new RegistryReader(DATA_DIR);
fetch(configOptions.db)
  .then((res) => res.json())
  .then((data) => registry.runCollector(data.update_seq, configOptions))
  .then(() => orderEvents(registry.eventsPath))
  .catch((err) => log.error(err))
  .finally(() => log.info("process", "all done!"));
