var fs = require('fs');
var request = require('ajax-request');
var Promise = require('promise');

var _hdfsWebAddr = "http://localhost:9870";
var _hdfsWebNamespace = "webhdfs/v1";
var _hdfsBaseDir = "/hwx-assemblies";
var _localBaseDir = "~/hwx-assemblies";

var connecter = {
  setConfigs: function(configs) {
    if (configs) {
      _hdfsWebAddr = configs.hdfsWebAddr? configs.hdfsWebAddr : _hdfsWebAddr;
      _hdfsWebNamespace = configs.hdfsWebNamespace? configs.hdfsWebNamespace : _hdfsWebNamespace;
      _hdfsBaseDir = configs.hdfsBaseDir? configs.hdfsBaseDir : _hdfsBaseDir;
      _localBaseDir = configs.localBaseDir? configs.localBaseDir : _localBaseDir;
    }
  },
  hdfs: {
    getListOfApps: function() {
      var url = _hdfsWebAddr + "/" + _hdfsWebNamespace + _hdfsBaseDir + "?op=LISTSTATUS_BATCH";
      return new Promise(function(resolve, reject) {
        request(url, function(errr, resp, body) {
          if (errr) {
            reject(resp);
            throw resp;
          }
          resolve(body);
        });
      });
    },
    getListOfVersions: function(app) {
      if (!app) {
        throw new Error("App name is not specified and is mandatory");
      }
      var url = _hdfsWebAddr + "/" + _hdfsWebNamespace + _hdfsBaseDir + "/" + app + "?op=LISTSTATUS_BATCH";
      return new Promise(function(resolve, reject) {
        request(url, function(errr, resp, body) {
          if (errr) {
            reject(resp);
            throw resp;
          }
          resolve(body);
        });
      });
    },
    getAppSpec: function(app, version) {
      if (!app || !version) {
        throw new Error("App name and version are not specified and both are mandatory");
      }
      var url = _hdfsWebAddr + "/" + _hdfsWebNamespace + _hdfsBaseDir + "/" + app + "/" + version + "/Yarnfile?op=OPEN";
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
