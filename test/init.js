// Copyright IBM Corp. 2016,2019. All Rights Reserved.
// Node module: loopback-connector-ibmi
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

'use strict';

module.exports = require('should');

const juggler = require('loopback-datasource-juggler');
let DataSource = juggler.DataSource;

let db = undefined;

const config = {
  connectionString: 'DSN=LBTEST',
};

global.config = config;

global.getDataSource = global.getSchema = function(options) {
  if (db === undefined) {
    console.log('datasource is undefined!');
    db = new DataSource(require('../'), config);
  }
  return db;
};

global.connectorCapabilities = {
  ilike: false,
  nilike: false,
};

global.resetDataSourceClass = function(ctor) {
  DataSource = ctor || juggler.DataSource;
  const promise = db ? db.disconnect() : Promise.resolve();
  db = undefined;
  return promise;
};

global.sinon = require('sinon');
