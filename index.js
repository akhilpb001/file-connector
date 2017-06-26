"use strict";

var request = require('request');
var Promise = require('promise');

var _hdfsWebAddress = "http://localhost:9870";
var _hdfsWebNamespace = "webhdfs/v1";

var connecter = {
  setConfigs: function(configs) {
    if (configs) {
      _hdfsWebAddress = configs.hdfsWebAddress? configs.hdfsWebAddress : _hdfsWebAddress;
      _hdfsWebNamespace = configs.hdfsWebNamespace? configs.hdfsWebNamespace : _hdfsWebNamespace;
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
        throw new Error("app is not specified and is mandatory");
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
        throw new Error("app and version are not specified and both are mandatory");
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
    },
    getFile: function(filepath) {
      if (!filepath) {
        throw new Error("filepath is not specified and is mandatory");
      }
      var url = _hdfsWebAddress + "/" + _hdfsWebNamespace + "/" + filepath + "?op=OPEN";
      return new Promise(function(resolve, reject) {
        request(url, function(errr, resp, body) {
          if (errr || (resp && resp.statusCode !== 200)) {
            reject(body);
            return;
          }
          resolve(body);
        });
      });
    },
    getListOfDirectories: function(dirPath) {
       if (!dirPath) {
         dirPath = "/";
      }
      if (!"".startsWith.call(dirPath, "/")) {
        dirPath = "/" + dirPath;
      }
      var url = _hdfsWebAddress + "/" + _hdfsWebNamespace + dirPath + "?op=LISTSTATUS_BATCH";
      return new Promise(function(resolve, reject) {
        request(url, function(errr, resp, body) {
          if (errr || (resp && resp.statusCode !== 200)) {
            reject(body);
            return;
          }
          var out = [];
          var list = JSON.parse(body);
          if (list && list.DirectoryListing && list.DirectoryListing.partialListing
            && list.DirectoryListing.partialListing.FileStatuses
            && list.DirectoryListing.partialListing.FileStatuses.FileStatus) {
              var dirs = list.DirectoryListing.partialListing.FileStatuses.FileStatus;
              [].forEach.call(dirs, function(stat) {
                if (stat.type === "DIRECTORY" && stat.pathSuffix !== ".git") {
                  out.push(stat);
                }
              });
          }
          resolve(JSON.stringify(out));
        });
      });
    }
  }
};

module.exports = connecter;
