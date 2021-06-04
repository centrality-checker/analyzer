import { createWriteStream, readFileSync, readdirSync } from "fs";
import changes from "concurrent-couch-follower";
import { diff, validRange, SemVer, clean } from "semver";
import fetch from "node-fetch";
import { read } from "read-last-lines";
import validateName from "validate-npm-package-name";

class RegistryReader {
  eventsDir = ".";

  constructor() {
    this.lastSequence = Number(readFileValue("npm_registry_sequence") || 0);
    const events_path = `${this.eventsDir}/dependency_events_${this.lastSequence}.csv`;
    this.writable = createWriteStream(events_path, {
      flags: "a",
    });
  }

  async getLastDate() {
    const files = readdirSync(this.eventsDir);
    const event_files = files.filter((s) => s.startsWith("dependency_events"));
    const last_file = event_files.sort()[event_files.length - 1];
    const lines = await read(`${this.eventsDir}/${last_file}`, 1);

    if (!lines) return;

    return lines[0].split(",")[2];
  }

  printProgress(currentSequence) {
    const progress = (
      ((currentSequence - this.lastSequence) /
        (this.endSequence - this.lastSequence)) *
      100
    ).toFixed(2);

    if (progress == this.lastProgress) return;

    this.lastProgress = progress;
    console.log(progress + "%");
  }

  async runCollector(endSequence, configOptions) {
    this.endSequence = endSequence;

    const date = await this.getLastDate();
    changes(this.dataHandler.bind(this), configOptions);
  }

  dataHandler(data, done) {
    this.printProgress(data.seq);

    if (data.seq >= this.endSequence) return console.log("The end for now!");

    const pkg = clean_pkg(data.doc);
    if (!pkg) return done();

    const versions_list = getFilteredVersionsList(pkg);

    versions_list.forEach((this_version, index) => {
      const last_version = versions_list[index - 1];

      const differences = getVersionDifferences(
        pkg.versions[this_version],
        pkg.versions[last_version]
      );

      this.createPackageOutputString(
        pkg.name,
        this_version,
        pkg.time[this_version],
        differences
      );

      const release_type = last_version
        ? diff(last_version, this_version)
        : "first";
    });

    done();
  }

  createPackageOutputString(pkg_name, version, date, differences) {
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

function getFilteredVersionsList(pkg, not_before, not_after) {
  let versions_list = Object.keys(pkg.versions);

  // sort versions by the release time
  versions_list.sort(function (a, b) {
    return pkg.time[a].localeCompare(pkg.time[b]);
  });

  const result = [];
  let last_version;
  versions_list.forEach(function (version) {
    // !! NOTE: not necessary for now, we will need it to build the dynamic graph
    // if (before_date && pkg.time[version] < not_before) return;
    // if (not_after && pkg.time[version] > not_after) return;

    let this_version;
    try {
      this_version = new SemVer(version);
    } catch (e) {
      // ignore invalid versions
      return;
    }

    if (last_version && this_version.compare(last_version) < 0) return;

    result.push(version);
    last_version = this_version;
  });

  return result;
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
  now: false,
  concurrency: 5,
};

const registry = new RegistryReader(configOptions);

fetch(configOptions.db)
  .then((res) => res.json())
  .then((res) => registry.runCollector(res.update_seq, configOptions))
  .catch((err) => console.log(err));
