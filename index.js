var fs = require('fs');
var request = require('request');
var Promise = require('promise');

var _hdfsWebAddress = "http://localhost:9870";
var _hdfsWebNamespace = "webhdfs/v1/hwx-assemblies";
var _localFileSystemBaseDir = "/home/user/hwx-assemblies";

var connecter = {
  setConfigs: function(configs) {
    if (configs) {
      configs.hdfsWebAddress? (_hdfsWebAddress = configs.hdfsWebAddress) : "";
      configs.hdfsWebNamespace? (_hdfsWebNamespace = configs.hdfsWebNamespace) : "";
      configs.localFileSystemBaseDir? (_localFileSystemBaseDir = configs.localFileSystemBaseDir) : "";
    }
  },
  hdfs: {
    getListOfApps: function() {
      var url = _hdfsWebAddress + "/" + _hdfsWebNamespace + "?op=LISTSTATUS_BATCH";
      return new Promise(function(resolve, reject) {
        request(url, function(errr, resp, body) {
          if (errr || (resp && resp.statusCode !== 200)) {
            reject(body);
            return;
          }
          var apps = [];
          var list = JSON.parse(body);
          if (list && list.DirectoryListing && list.DirectoryListing.partialListing
            && list.DirectoryListing.partialListing.FileStatuses
            && list.DirectoryListing.partialListing.FileStatuses.FileStatus) {
              var dirs = list.DirectoryListing.partialListing.FileStatuses.FileStatus;
              [].forEach.call(dirs, function(stat) {
                if (stat.type === "DIRECTORY" && stat.pathSuffix !== ".git" && stat.pathSuffix !== "apps-base") {
                  apps.push(stat);
                }
              });
          }
          resolve(JSON.stringify(apps));
        });
      });
    },
    getListOfVersions: function(app) {
      if (!app) {
        throw new Error("App name is not specified and is mandatory");
      }
      var url = _hdfsWebAddress + "/" + _hdfsWebNamespace + "/" + app + "?op=LISTSTATUS_BATCH";
      return new Promise(function(resolve, reject) {
        request(url, function(errr, resp, body) {
          if (errr || (resp && resp.statusCode !== 200)) {
            reject(body);
            return;
          }
          var vers = [];
          var list = JSON.parse(body);
          if (list && list.DirectoryListing && list.DirectoryListing.partialListing
            && list.DirectoryListing.partialListing.FileStatuses
            && list.DirectoryListing.partialListing.FileStatuses.FileStatus) {
              var dirs = list.DirectoryListing.partialListing.FileStatuses.FileStatus;
              [].forEach.call(dirs, function(stat) {
                if (stat.type === "DIRECTORY") {
                  vers.push(stat);
                }
              });
          }
          resolve(JSON.stringify(vers));
        });
      });
    },
    getAppSpec: function(app, version) {
      if (!app || !version) {
        throw new Error("App name and version are not specified and both are mandatory");
      }
      var url = _hdfsWebAddress + "/" + _hdfsWebNamespace + "/" + app + "/" + version + "/Yarnfile?op=OPEN";
      return new Promise(function(resolve, reject) {
        request(url, function(errr, resp, body) {
          if (errr || (resp && resp.statusCode !== 200)) {
            reject(body);
            return;
          }
          resolve(body);
        });
      });
    }
  },
  local: {
    getListOfApps: function() {
      return new Promise(function(resolve, reject) {
        resolve("TODO");
      });
    },
    getListOfVersions: function(app) {
      if (!app) {
        throw new Error("App name is not specified and is mandatory");
      }
      return new Promise(function(resolve, reject) {
        resolve("TODO");
      });
    },
    getAppSpec: function(app, version) {
      if (!app || !version) {
        throw new Error("App name and version are not specified and both are mandatory");
      }
      return new Promise(function(resolve, reject) {
        resolve("TODO");
      });
    }
  }
};

module.exports = connecter;
