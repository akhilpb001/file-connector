var fileConnecter = require('./index');
var hdfs = fileConnecter.hdfs;

/*Testing succecss scenarios for hdfs*/
// hdfs.getListOfApps().then(function(apps) {
//   console.log("[List of apps from HDFS]------------------------------------------------------------");
//   console.log(apps);
//   console.log("------------------------------------------------------------------------------------");
// });

// hdfs.getListOfVersions("hbase").then(function(versions) {
//   console.log("[List of versions for hbase app from HDFS]------------------------------------------");
//   console.log(versions);
//   console.log("------------------------------------------------------------------------------------");
// });

// hdfs.getAppSpec("hbase", "1.1.2").then(function(spec) {
//   console.log("[App spec for hbase app and version 1.1.2 from HDFS]--------------------------------");
//   console.log(spec);
//   console.log("------------------------------------------------------------------------------------");
// });

// hdfs.getFile("metadata.json").then(function(file) {
//   console.log("[metadata.json file from HDFS]------------------------------------------------------");
//   console.log(file);
//   console.log("------------------------------------------------------------------------------------");
// });

/*Testing error scenario for hdfs*/
// hdfs.getAppSpec("hbase", "1.1.2.2").catch(function(errr) {
//   console.log("Catching error.........")
//   console.log(errr);
// });

/*Testing getListOfDirectories with empty input*/
// hdfs.getListOfDirectories().then(function(dirs) {
//   console.log("[List of directories HDFS]------------------------------------------------------------");
//   console.log(dirs);
//   console.log("------------------------------------------------------------------------------------");
// });

/*Testing getListOfDirectories with non-empty input*/
// hdfs.getListOfDirectories("kafka").then(function(dirs) {
//   console.log("[List of directories HDFS]------------------------------------------------------------");
//   console.log(dirs);
//   console.log("------------------------------------------------------------------------------------");
// });