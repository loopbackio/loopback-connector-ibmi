// Copyright IBM Corp. 2016. All Rights Reserved.
// Node module: loopback-connector-ibmi
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

'use strict';

module.exports = require('should');

const DataSource = require('loopback-datasource-juggler').DataSource;

let db = undefined;

const config = {
  connectionString: 'DSN=LBTEST',
};

global.config = config;

global.getDataSource = global.getSchema = function(options) {
  if (db === undefined) {
    db = new DataSource(require('../'), config);
  }
  return db;
};

global.connectorCapabilities = {
  ilike: false,
  nilike: false,
};

global.sinon = require('sinon');
