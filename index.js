var fs = require('fs');
var request = require('request');
var Promise = require('promise');

var _hdfsWebAddr = "http://localhost:9870";
var _hdfsWebNamespace = "webhdfs/v1";
var _hdfsBaseDir = "hwx-assemblies";

var _localBaseDir = "hwx-assemblies";
var _localUserName = "user";

var connecter = {
  setConfigs: function(configs) {
    if (configs) {
      configs.hdfsWebAddr? (_hdfsWebAddr = configs.hdfsWebAddr) : "";
      configs.hdfsWebNamespace? (_hdfsWebNamespace = configs.hdfsWebNamespace) : "";
      configs.hdfsBaseDir? (_hdfsBaseDir = configs.hdfsBaseDir) : "";

      configs.localBaseDir? (_localBaseDir = configs.localBaseDir) : "";
      configs.localUserName? (_localUserName = configs.localUserName) : "";
    }
  },
  hdfs: {
    getListOfApps: function() {
      var url = _hdfsWebAddr + "/" + _hdfsWebNamespace + "/" + _hdfsBaseDir + "?op=LISTSTATUS_BATCH";
      return new Promise(function(resolve, reject) {
        request(url, function(errr, resp, body) {
          if (errr) {
            reject(resp);
            throw resp;
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
      var url = _hdfsWebAddr + "/" + _hdfsWebNamespace + "/" + _hdfsBaseDir + "/" + app + "?op=LISTSTATUS_BATCH";
      return new Promise(function(resolve, reject) {
        request(url, function(errr, resp, body) {
          if (errr) {
            reject(resp);
            throw resp;
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
      var url = _hdfsWebAddr + "/" + _hdfsWebNamespace + "/" +_hdfsBaseDir + "/" + app + "/" + version + "/Yarnfile?op=OPEN";
      return new Promise(function(resolve, reject) {
        request(url, function(errr, resp, body) {
          if (errr) {
            reject(resp);
            throw resp;
          }
          resolve(body);
        });
      });
    }
  },
  local: {
    getListOfApps: function() {
      return new Promise(function(resolve, reject) {
        reject("TODO");
      });
    },
    getListOfVersions: function(app) {
      if (!app) {
        throw new Error("App name is not specified and is mandatory");
      }
      return new Promise(function(resolve, reject) {
        reject("TODO");
      });
    },
    getAppSpec: function(app, version) {
      if (!app || !version) {
        throw new Error("App name and version are not specified and both are mandatory");
      }
      return new Promise(function(resolve, reject) {
        reject("TODO");
      });
    }
  }
};

module.exports = connecter;
