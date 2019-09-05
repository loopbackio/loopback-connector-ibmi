// Copyright IBM Corp. 2016. All Rights Reserved.
// Node module: loopback-connector-db2i
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

'use strict';

const g = require('../lib/globalize');
const DataSource = require('loopback-datasource-juggler').DataSource;
const DB2 = require('../'); // loopback-connector-db2i

const config = {
  username: process.env.DB2I_USERNAME,
  password: process.env.DB2I_PASSWORD,
  hostname: process.env.DB2I_HOSTNAME,
  port: 50000,
  database: 'SQLDB',
};

const db = new DataSource(DB2, config);

const User = db.define('User', {name: {type: String}, email: {type: String},
});

db.autoupdate('User', function(err) {
  if (err) {
    console.log(err);
    return;
  }

  User.create({
    name: 'Tony',
    email: 'tony@t.com',
  }, function(err, user) {
    console.log(err, user);
  });

  User.find({where: {name: 'Tony'}}, function(err, users) {
    console.log(err, users);
  });

  User.destroyAll(function() {
    g.log('example complete');
  });
});
