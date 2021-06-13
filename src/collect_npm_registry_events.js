import {
  createWriteStream,
  readFileSync,
  readdirSync,
  existsSync,
  mkdirSync,
} from "fs";
import changes from "concurrent-couch-follower";
import { validRange, SemVer, clean } from "semver";
import fetch from "node-fetch";
import { read } from "read-last-lines";
import validateName from "validate-npm-package-name";
import path from "path";
import lineByLine from "n-readlines";
import { SingleBar, Presets } from "cli-progress";

class RegistryReader {
  constructor(dataDir = ".") {
    this.dataDir = dataDir;
    if (!existsSync(this.dataDir)) {
      mkdirSync(this.dataDir);
    }

    this.eventsDir = path.join(this.dataDir, "events");
    if (!existsSync(this.eventsDir)) {
      mkdirSync(this.eventsDir);
    }

    this.lastVersionsPath = path.join(this.dataDir, "last_versions.csv");

    this.lastSequence = Number(readFileValue("npm_registry_sequence") || 0);
    const eventsPath = `${this.eventsDir}/dependency_events_${this.lastSequence}.csv`;
    this.writable = createWriteStream(eventsPath, {
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
      [name, version] = line.split(",");
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
  }

  async getLastDate() {
    const files = readdirSync(this.eventsDir);
    const event_files = files.filter((s) => s.startsWith("dependency_events"));
    const last_file = event_files.sort()[event_files.length - 1];
    const lines = await read(`${this.eventsDir}/${last_file}`, 1);

    if (!lines) return;

    return lines[0].split(",")[2];
  }

  // @deprecated now we use cli-progress
  printProgress(currentSequence) {
    const progress = (
      ((currentSequence - this.lastSequence) /
        (this.endSequence - this.lastSequence)) *
      100
    ).toFixed(1);

    if (progress == this.lastProgress) return;

    this.lastProgress = progress;
    console.log(progress + "%");
  }

  async runCollector(endSequence, configOptions) {
    this.lastDate = await this.getLastDate();
    this.endSequence = endSequence;

    console.log("Start after sequence:", this.lastSequence);
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
      console.log("End before sequence:", this.endSequence);
      this.saveLastVersions();
      this.resolve();
      return;
    }

    const pkg = clean_pkg(data.doc);
    if (!pkg) return done();

    const versions_list = this.getFilteredVersionsList(pkg);

    versions_list.forEach((this_version, index) => {
      const last_version = versions_list[index - 1];

      const differences = getVersionDifferences(
        pkg.versions[this_version],
        pkg.versions[last_version]
      );

      this.writeOutputString(
        pkg.name,
        this_version,
        pkg.time[this_version],
        differences
      );
    });

    done();
  }

  writeOutputString(pkg_name, version, date, differences) {
    for (const data_type in differences) {
      const events = differences[data_type];

      for (const event in events) {
        events[event].forEach((element) => {
          this.writable.write(
            `${pkg_name},${version},${date},${event},${data_type},${cleanText(
              element
            )}\n`
          );
        });
      }
    }
  }

  getFilteredVersionsList(pkg) {
    let versions_list = Object.keys(pkg.versions);

    // sort versions by the release time
    versions_list.sort((a, b) => pkg.time[a].localeCompare(pkg.time[b]));

    const result = [];
    let last_version = new SemVer(this.lastVersions.get(pkg.name) || "0.0.0");
    versions_list.forEach((version) => {
      let this_version;
      try {
        this_version = new SemVer(version);
      } catch (e) {
        // ignore invalid versions
        return;
      }

      const verComp = this_version.compare(last_version);
      if (verComp == -1 || (verComp != 0 && pkg.time[version] < this.lastDate))
        return;

      result.push(version);
      last_version = this_version;
    });

    this.lastVersions.set(pkg.name, last_version);

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

function getVersionDifferences(new_version, old_version) {
  const new_version_data = getVersionData(new_version);
  const old_version_data = old_version ? getVersionData(old_version) : {};

  const result = {};

  for (const data_type in new_version_data) {
    const new_data = new_version_data[data_type] || [];
    const old_data = old_version_data[data_type] || [];

    result[data_type] = getArrayDifferences(new_data, old_data);
  }

  return result;
}

function getVersionData(version) {
  return {
    dependencies: getValidatedDependencies(version.dependencies),
    devDependencies: getValidatedDependencies(version.devDependencies),
  };
}

function getValidatedDependencies(dependencies) {
  if (typeof dependencies !== "object" || dependencies === null) return;

  return Object.keys(dependencies).filter(function (d) {
    return (
      validRange(dependencies[d]) !== null &&
      validateName(d).validForOldPackages
    );
  });
}

function getArrayDifferences(new_array, old_array) {
  if (new_array && !old_array) return { add: new_array };
  if (!new_array && old_array) return { delete: old_array };

  const added_elements = [];
  const deleted_elements = old_array.slice(0);

  new_array.forEach(function (element) {
    let index = deleted_elements.indexOf(element);
    if (index === -1) {
      added_elements.push(element);
    } else {
      deleted_elements.splice(index, 1);
    }
  });

  return { add: added_elements, delete: deleted_elements };
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

const configOptions = {
  db: "https://replicate.npmjs.com",
  include_docs: true,
  sequence: "npm_registry_sequence",
  concurrency: 30,
};

const registry = new RegistryReader();
fetch(configOptions.db)
  .then((res) => res.json())
  .then((data) => registry.runCollector(data.update_seq, configOptions))
  .catch((err) => console.log("Error:", err))
  .finally(() => console.log("All Done!"));
