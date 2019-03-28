// Copyright IBM Corp. 2016. All Rights Reserved.
// Node module: loopback-connector-ibmi
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0


/**
 * IBM i Db2 connector for LoopBack using ODBC driver
 */
const async = require('async');
const util = require('util');
const debug = require('debug')('loopback:connector:ibmiconnector');
const { SqlConnector } = require('loopback-connector');
const odbc = require('../../node-odbc');

const g = require('./globalize');

const { Transaction, ParameterizedSQL } = SqlConnector;

class IBMiConnector {
  constructor(settings) {
    SqlConnector.call(this, 'ibmi', settings);

    // instantiate the pool that is used for all queries
    this.pool = odbc.Pool(settings); // TODO: this isnt right
    // pool is initialized in initialize function below
  }

  query(sql, parameters, callback) {
    this.pool.query(sql, parameters, callback);
  }

  ping(callback) {
    const sql = 'SELECT 1 AS PING FROM SYSIBM.SYSDUMMY1';
    this.query(sql, null, callback);
  }

  // create the pool if it hasn't been initialized, then return a connection
  // TODO: What does connect expect?
  connect(callback) {
    if (this.pool.isInitialized) {
      callback(null, this.pool);
    } else {
      this.pool.init((err) => {
        callback(err, this.pool);
      });
    }
  }

  // disconnect the pool from the database, closing all connections
  disconnect(callback) {
    this.pool.close((err) => {
      callback(err);
    });
  }

  /**
   * Create the data model on the IBM i
   *
   * @param {string} model The model name
   * @param {Object} data The model instance data
   * @param {Object} options Options object
   * @param {Function} [callback] The callback function
   */
  create(model, data, options, callback) {
    const stmt = this.buildInsert(model, data, options);
    const id = this.idColumn(model);
    const sql = `SELECT "${id}" FROM FINAL TABLE (${stmt.sql})`;

    this.query(sql, stmt.params, (err, result) => {
      callback(err, result[0][id]);
    });
  }

  /**
   * Update all instances that match the where clause with the given data
   *
   * @param {string} model The model name
   * @param {Object} where The where object
   * @param {Object} data The property/value object representing changes
   * to be made
   * @param {Object} options The options object
   * @param {Function} callback The callback function
   */
  update(model, where, data, options, callback) {
    const tableName = this.tableEscaped(model);
    const id = this.idName(model);
    const updateStmt = this.buildUpdate(model, where, data, options);
    const selectStmt = new ParameterizedSQL(`SELECT ${this.escapeName(id)} FROM ${tableName}`);

    selectStmt.merge(this.buildWhere(model, where));
    selectStmt.merge(' FOR UPDATE');
    this.parameterize(selectStmt);

    const executeTransaction = (connection, cb) => {
      connection.query(selectStmt.sql, selectStmt.params, (err, data) => {
        debug('IBMiConnector.prototype.update stmt: %j data: %j', selectStmt, data);
        if (err) {
          return cb(err);
        }

        connection.query(updateStmt.sql, updateStmt.params, (err, updateData) => {
          debug('IBMiConnector.prototype.update stmt: %j data: %j', updateStmt, updateData);
          return cb(null, data.length);
        });
      });
    };

    // If a transaction hasn't already been started, then start a local one now.
    // We will have to deal with cleaning this up in the event some error
    // occurs in the code below.
    if (options.transaction) {
      executeTransaction(options.transaction.connection, (err, retVal) => {
        return callback(null, { count: retVal });
      });
    } else {
      this.beginTransaction(Transaction.REPEATABLE_READ, (err, conn) => {
        if (err) { return callback(err); }
        executeTransaction(conn, (err, retVal) => {
          if (err) {
            this.rollback(conn, () => {
              process.nextTick(conn.close);
            });
            return callback(err);
          }
          this.commit(conn, () => {});
          return callback(null, { count: retVal });
        });
      });
    }
  }

  /**
   * Build the `DELETE FROM` SQL statement
   *
   * @param {string} model The model name
   * @param {Object} where The where object
   * @param {Object} options Options object
   * @param {Function} callback function
   */
  destroyAll(model, where, options, callback) {
    debug('IBMiConnector.destroyAll %j %j %j', model, where, options);

    const tableName = this.tableEscaped(model);
    const id = this.idName(model);
    const deleteStmt = this.buildDelete(model, where, options);
    const selectStmt = new ParameterizedSQL(`SELECT ${this.escapeName(id)} FROM ${tableName}`);

    selectStmt.merge(this.buildWhere(model, where));
    this.parameterize(selectStmt);

    const executeTransaction = (connection, cb) => {
      connection.query(selectStmt, (err, selectData) => {
        debug('IBMiConnector.destroyAll stmt: %j data: %j', selectStmt, selectData);
        if (err) {
          return cb(err);
        }

        connection.query(deleteStmt, null, (deleteErr, deleteData) => {
          debug('IBMiConnector.destroyAll stmt: %j data: %j', deleteStmt, deleteData);
          if (deleteErr) { return cb(deleteErr); }
          return cb(deleteErr, { count: selectData.length });
        });
      });
    };

    // If a transaction hasn't already been started, then start a local one now.
    // We will have to deal with cleaning this up in the event some error
    // occurs in the code below.
    if (options.transaction) {
      executeTransaction(options.transaction.connection, (err, retVal) => {
        if (err) { return callback(err); }
        return callback(err, retVal);
      });
    } else {
      this.beginTransaction(Transaction.REPEATABLE_READ, (err, conn) => {
        if (err) {
          return callback(err);
        }
        executeTransaction(conn, (err, data) => {
          if (err) {
            this.rollback(conn, () => {
              process.nextTick(conn.close);
            });
            return callback(err);
          }

          this.commit(conn, () => {});
          return callback(null, data);
        });
      });
    }
  }


  // ///////////////////////////////////////////////////////////////////////////////////////////////
  // /////////////////////////////////// TRANSACTIONS //////////////////////////////////////////////
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  // This functionality is standard to all DBMS using ODBC drivers. In particular, ODBC connections
  // should not use BEGIN_TRANSACTION, ROLLBACK, or COMMIT statements, and instead should use ODBC
  // functions like SQLSetConnectAttr to start and SQLEndTran to end. Therefore, this functionality
  // is handled by the loopback-odbc layer.

  // TODO: Does this need to still wrap aroudn?
  /**
   * Begin a new transaction

   * @param {Integer} isolationLevel
   * @param {Function} cb
   */
  beginTransaction(isolationLevel, callback) {
    debug('beginTransaction: isolationLevel: %s', isolationLevel);

    this.client.open(this.connStr, (err, connection) => {
      if (err) return callback(err);
      connection.beginTransaction((err) => {
        if (isolationLevel) {
          connection.setIsolationLevel(mapIsolationLevel(isolationLevel));
        }
        callback(err, connection);
      });
    });
  };

  /**
   * Commit a transaction
   *
   * @param {Object} connection
   * @param {Function} cb
   */
  commit(connection, callback) {
    debug('Commit a transaction');
    connection.commitTransaction(function(err) {
      if (err) return callback(err);
      connection.close(callback);
    });
  };

  /**
   * Roll back a transaction
   *
   * @param {Object} connection
   * @param {Function} cb
   */
  rollback(connection, cb) {
    debug('Rollback a transaction');
    connection.rollbackTransaction(function(err) {
      if (err) return cb(err);
      // connection.setAutoCommit(true);
      connection.close(cb);
    });
  };

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  // ///////////////////////////////////// MIGRATION ///////////////////////////////////////////////
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  // Adapted DB2iSeries code

  searchForPropertyInActual(model, propName, actualFields) {
    process.nextTick(function() {
      throw new Error(g.f('{{searchForPropertyInActual()}} is ' +
      'not currently supported.'));
    });
  };

  addPropertyToActual(model, propName) {
    process.nextTick(function() {
      throw new Error(g.f('{{addPropertyToActual()}} is ' +
      'not currently supported.'));
    });
  };

  propertyHasNotBeenDeleted(model, propName) {
    process.nextTick(function() {
      throw new Error(g.f('{{propertyHasNotBeenDeleted()}} is ' +
      'not currently supported.'));
    });
  };

  applySqlChanges(model, pendingChanges, cb) {
    process.nextTick(function() {
      return cb(Error(g.f('{{applySqlChanges()}} is not ' +
      'currently supported.')));
    });
  };

  showFields(model, cb) {
    var self = this;
    var sql = 'SELECT COLUMN_NAME AS NAME, DATA_TYPE AS DATATYPE, ' +
              'ORDINAL_POSITION AS COLNO, ' +
              'IS_NULLABLE AS NULLS ' +
              'FROM QSYS2.COLUMNS ' +
              'WHERE TRIM(TABLE_NAME) LIKE \'' +
              self.table(model) + '\' ' +
              'AND TRIM(TABLE_SCHEMA) LIKE \'' +
              self.schema.toUpperCase() + '\'' +
              ' ORDER BY COLNO';

    this.execute(sql, function(err, fields) {
      if (err) {
        return cb(err);
      } else {
        return cb(err, fields);
      }
    });
  };

  showIndexes(model, cb) {
    var self = this;
    var sql = 'SELECT INDEX_NAME as INDNAME ' +
              'FROM QSYS2.SYSINDEXES ' +
              'WHERE TRIM(TABLE_NAME) = \'' + self.table(model) + '\' ' +
              'AND TRIM(TABLE_SCHEMA) = \'' + self.schema.toUpperCase() + '\'';

    this.execute(sql, function(err, indexes) {
      if (err) {
        return cb(err);
      } else {
        return cb(err, indexes);
      }
    });
  };

  isActual(models, cb) {
    process.nextTick(function() {
      return cb(Error(g.f('{{isActual()}} is not currently supported.')));
    });
  };
  
  // TODO: Below is adapted from other packages (mysql, ibmi, db2i, db2ibmi, ibmi)
  /*
   * Perform autoupdate for the given models
   * @param {String[]} [models] A model name or an array of model names.
   * If not present, apply to all models
   * @param {Function} [cb] The callback function
   */
  // autoupdate(models, cb) {
  //   debug('IBMiConnector.autoupdate %j', models);

  //   if ((!cb) && (typeof models === 'function')) {
  //     cb = models;
  //     models = undefined;
  //   }
  //   // First argument is a model name
  //   if (typeof models === 'string') {
  //     models = [models];
  //   }

  //   models = models || Object.keys(this._models);

  //   async.each(models, (model, done) => {
  //     if (!(model in this._models)) {
  //       return process.nextTick(() => {
  //         done(new Error(`Model not found: ${model}`));
  //       });
  //     }
  //     this.getTableStatus(model, (err, fields, indexes) => {
  //       if (err) {
  //         return done(err);
  //       }
  //       if (fields.length) {
  //         this.alterTable(model, fields, indexes, done);
  //       } else {
  //         this.createTable(model, done);
  //       }
  //     });
  //   }, cb);
  // }

  // getTableStatus(model, cb) {
  //   const columnSQL = 'SELECT column_name AS NAME, data_type AS DATATYPE, ' +
  //     'ordinal_position AS COLNO, length AS DATALENGTH, is_nullable AS NULLS ' +
  //     'FROM qsys2.syscolumns ' +
  //     'WHERE table_name = \'' +
  //     self.table(model) + '\' ' +
  //     'AND table_schema = \'' +
  //     self.schema + '\'' +
  //     ' ORDER BY COLNO';

  //   this.query(columnSQL, (err, tableInfo) => {
  //     if (err) {
  //       cb(err);
  //     } else {
  //       const indexSQL = 'SELECT table_name, table_schema, index_name, ' +
  //         'is_unique FROM qsys2.sysindexes ' +
  //         'WHERE table_name = \'' +
  //         self.table(model) + '\' ' +
  //         'AND table_schema = \'' +
  //         self.schema + '\'';

  //       this.query(indexSQL, (err, indexInfo) => {
  //         cb(err, tableInfo, indexInfo);
  //       });
  //     }
  //   });
  // }

  // alterTable(model, actualFields, actualIndexes, callback, checkOnly) {
  //   debug('DB2.prototype.alterTable %j %j %j %j', model, actualFields, actualIndexes, checkOnly);

  //   var m = this.getModelDefinition(model);
  //   var propNames = Object.keys(m.properties).filter(function(name) {
  //     return !!m.properties[name];
  //   });
  //   var indexes = m.settings.indexes || {};
  //   var indexNames = Object.keys(indexes).filter(function(name) {
  //     return !!m.settings.indexes[name];
  //   });
  //   var sql = [];
  //   var tasks = [];
  //   var operations = [];
  //   var ai = {};
  //   var type = '';

  //   if (actualIndexes) {
  //     actualIndexes.forEach(function(i) {
  //       var name = i.INDNAME;
  //       if (!ai[name]) {
  //         ai[name] = {
  //           info: i,
  //           columns: [],
  //         };
  //       }

  //       i.COLNAMES.split(/\+\s*/).forEach(function(columnName, j) {
  //         // This is a bit of a dirty way to get around this but DB2 returns
  //         // column names as a string started with and separated by a '+'.
  //         // The code below will strip out the initial '+' then store the
  //         // actual column names.
  //         if (j > 0)
  //           ai[name].columns[j - 1] = columnName;
  //       });
  //     });
  //   }
  //   var aiNames = Object.keys(ai);

  //   // change/add new fields
  //   propNames.forEach((propName) => {
  //     if (m.properties[propName] && this.id(model, propName)) return;
  //     var found;
  //     if (actualFields) {
  //       actualFields.forEach((f) => {
  //         if (f.NAME === propName) {
  //           found = f;
  //         }
  //       });
  //     }

  //     if (found) {
  //       actualize(propName, found);
  //     } else {
  //       operations.push(`ADD COLUMN ${propName} ${this.buildColumnDefinition(model, propName)}`);
  //     }
  //   });

  //   // drop columns
  //   if (actualFields) {
  //     actualFields.forEach(function(f) {
  //       var notFound = !~propNames.indexOf(f.NAME);
  //       if (m.properties[f.NAME] && this.id(model, f.NAME)) return;
  //       if (notFound || !m.properties[f.NAME]) {
  //         operations.push('DROP COLUMN ' + f.NAME);
  //       }
  //     });
  //   }

  //   if (operations.length) {
  //     // Add the ALTER TABLE statement to the list of tasks to perform later.
  //     sql.push('ALTER TABLE ' + this.schema + '.' +
  //              this.tableEscaped(model) + ' ' + operations.join(' ') + ';');
  //   }

  //   operations = [];

  //   // remove indexes
  //   aiNames.forEach((indexName) => {
  //     if (ai[indexName].info.UNIQUERULE === 'P' || // indexName === 'PRIMARY' ||
  //       (m.properties[indexName] && this.id(model, indexName))) return;

  //     if (indexNames.indexOf(indexName) === -1 && !m.properties[indexName] ||
  //       m.properties[indexName] && !m.properties[indexName].index) {

  //       if (ai[indexName].info.UNIQUERULE === 'P') {
  //         operations.push('DROP PRIMARY KEY');
  //       } else if (ai[indexName].info.UNIQUERULE === 'U') {
  //         operations.push('DROP UNIQUE ' + indexName);
  //       }
  //     } else {
  //       // first: check single (only type and kind)
  //       if (m.properties[indexName] && !m.properties[indexName].index) {
  //         // TODO
  //         return;
  //       }
  //       // second: check multiple indexes
  //       var orderMatched = true;
  //       if (indexNames.indexOf(indexName) !== -1) {
  //         m.settings.indexes[indexName].columns.split(/,\s*/).forEach((columnName, i) => {
  //           if (ai[indexName].columns[i] !== columnName) orderMatched = false;
  //         });
  //       }

  //       if (!orderMatched) {
  //         if (ai[indexName].info.UNIQUERULE === 'P') {
  //           operations.push('DROP PRIMARY KEY');
  //         } else if (ai[indexName].info.UNIQUERULE === 'U') {
  //           operations.push('DROP UNIQUE ' + indexName);
  //         }

  //         delete ai[indexName];
  //       }
  //     }
  //   });

  //   if (operations.length) {
  //     // Add the ALTER TABLE statement to the list of tasks to perform later.
  //     sql.push('ALTER TABLE ' + self.schema + '.' +
  //              self.tableEscaped(model) + ' ' + operations.join(' ') + ';');
  //   }

  //   // add single-column indexes
  //   propNames.forEach(function(propName) {
  //     var i = m.properties[propName].index;
  //     if (!i) {
  //       return;
  //     }
  //     var found = ai[propName] && ai[propName].info;
  //     if (!found) {
  //       var pName = propName;
  //       type = '';
  //       if (i.type) {
  //         type = i.type;
  //       }
  //       sql.push('CREATE ' + type + ' INDEX ' + pName + ' ON ' +
  //                self.schema + '.' + self.tableEscaped(model) +
  //                '(\"' + pName + '\") ');
  //     }
  //   });

  //   // add multi-column indexes
  //   indexNames.forEach(function(indexName) {
  //     var i = m.settings.indexes[indexName];
  //     var found = ai[indexName] && ai[indexName].info;
  //     if (!found) {
  //       var iName = indexName;
  //       var type = '';
  //       if (i.type) {
  //         type = i.type;
  //       }
  //       var stmt = 'CREATE ' + type + 'INDEX ' + iName + ' ON ' +
  //                  self.schema + '.' + self.tableEscaped(model) + '(';

  //       var splitNames = i.columns.split(/,\s*/);
  //       var colNames = splitNames.join('\",\"');

  //       stmt += '\"' + colNames + '\")';

  //       sql.push(stmt);
  //     }
  //   });

  //   sql.forEach(function(i) {
  //     tasks.push(function(cb) {
  //       self.execute(i, function(err, results) {
  //         cb(err);
  //       });
  //     });
  //   });

  //   if (tasks.length) {
  //     if (checkOnly) {
  //       return (callback(null, true, {statements: sql}));
  //     } else {
  //       async.series(tasks, function() {
  //         return (callback());
  //       });
  //     }
  //   } else {
  //     return (callback());
  //   }

  //   function actualize(propName, oldSettings) {
  //     var newSettings = m.properties[propName];
  //     if (newSettings && changed(newSettings, oldSettings)) {
  //       // TODO: NO TESTS EXECUTE THIS CODE PATH
  //       var pName = '\'' + propName + '\'';
  //       operations.push('CHANGE COLUMN ' + pName + ' ' + pName + ' ' +
  //         self.buildColumnDefinition(model, propName));
  //     }
  //   }

  //   function changed(newSettings, oldSettings) {
  //     if (oldSettings.Null === 'YES') {
  //       // Used to allow null and does not now.
  //       if (!self.isNullable(newSettings)) {
  //         return true;
  //       }
  //     }
  //     if (oldSettings.Null === 'NO') {
  //       // Did not allow null and now does.
  //       if (self.isNullable(newSettings)) {
  //         return true;
  //       }
  //     }

  //     return false;
  //   }
  // };


  // searchForPropertyInActual(model, propName, actualFields) {
  // }

  // addPropertyToActual (model, propName) {
  // }

  // propertyHasNotBeenDeleted(model, propName) {
  // }

  // applySqlChanges(model, pendingChanges, cb) {
  // }

  // showFields(model, callback) {
  //   const sql = 'SELECT COLUMN_NAME AS NAME, DATA_TYPE AS DATATYPE, '
  //            + 'ORDINAL_POSITION AS COLNO, '
  //            + 'IS_NULLABLE AS NULLS '
  //            + 'FROM QSYS2.COLUMNS '
  //            + `WHERE TRIM(TABLE_NAME) LIKE '${this.table(model)}' `
  //            + `AND TRIM(TABLE_SCHEMA) LIKE '${this.schema.toUpperCase()}'`
  //            + ' ORDER BY COLNO';

  //   return this.query(sql, callback);
  // }

  // showIndexes(model, callback) {
  //   const sql = 'SELECT INDEX_NAME as INDNAME '
  //             + 'FROM QSYS2.SYSINDEXES '
  //             + `WHERE TRIM(TABLE_NAME) = '${this.table(model)}' `
  //             + `AND TRIM(TABLE_SCHEMA) = '${this.schema.toUpperCase()}'`;

  //   return this.query(sql, callback);
  // }

  // isActual(models, cb) {
  //   // TODO: This is COPIED FROM MYSQL
  //   let ok = false;

  //   if ((!cb) && (typeof models === 'function')) {
  //     cb = models;
  //     models = undefined;
  //   }
  //   // First argument is a model name
  //   if (typeof models === 'string') {
  //     models = [models];
  //   }

  //   models = models || Object.keys(this._models);

  //   async.eachSeries(models, (model, done) => {
  //     self.getTableStatus(model, function(err, fields, indexes) {
  //       self.discoverForeignKeys(self.table(model), {}, function(err, foreignKeys) {
  //         if (err) console.log('Failed to discover "' + self.table(model) +
  //           '" foreign keys', err);

  //         self.alterTable(model, fields, indexes, foreignKeys, function(err, needAlter) {
  //           if (err) {
  //             return done(err);
  //           } else {
  //             ok = ok || needAlter;
  //             done(err);
  //           }
  //         }, true);
  //       });
  //     });
  //   }, function(err) {
  //     cb(err, !ok);
  //   });
  // }

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  // ///////////////////////////////////// DISCOVERY ///////////////////////////////////////////////
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  static paginateSQL(sql, orderBy, options) {
    const opts = options || {};
    let limitClause = '';
    if (opts.offset || opts.skip || opts.limit) {
      // Offset starts from 0
      let offset = Number(opts.offset || opts.skip || 0);
      if (Number.isNaN(offset)) {
        offset = 0;
      }
      if (opts.limit) {
        let limit = Number(opts.limit);
        if (Number.isNaN(limit)) {
          limit = 0;
        }
        limitClause = ` FETCH FIRST ${limit} ROWS ONLY`;
      }
    }
    if (!orderBy) {
      sql = `${sql} ORDER BY ${orderBy}`;
    }

    // return sql + limitClause;
    return sql + limitClause;
  }

  static buildQuerySchemas(options) {
    const sql = 'SELECT table_cat AS "catalog", table_schem AS "schema" FROM sysibm.sqlschemas';
    return IBMiConnector.paginateSQL(sql, 'table_schem', options);
  }

  discoverDatabasesSchemas(options, cb) {
    this.execute(IBMiConnector.querySchemas(options), cb);
  }

  static buildQueryTables(options) {
    let sql = '';
    const schema = options.owner || options.schema;

    if (schema) {
      sql = `SELECT table_type AS "type", table_name AS "name", table_schem AS "owner" FROM sysibm.sqltables WHERE table_schem = '${schema}' AND table_type = 'TABLE'`;
    } else if (options.all) {
      sql = 'SELECT table_type AS "type", table_name AS "name", table_schem AS "owner" FROM sysibm.sqltables WHERE table_type = \'TABLE\'';
    } else {
      sql = 'SELECT table_type AS "type", table_name AS "name", table_schem AS "owner" FROM sysibm.sqltables WHERE table_schem = USER AND table_type = \'TABLE\'';
    }

    return IBMiConnector.paginateSQL(sql, 'table_schem, table_name', options);
  }

  static buildQueryViews(options) {
    let sql = '';
    const schema = options.owner || options.schema;

    if (options.all && !schema) {
      sql = 'SELECT table_type AS "type", table_name AS "name", table_schem AS "owner" FROM sysibm.sqltables WHERE table_type = \'VIEW\'';
    } else if (schema) {
      sql = `SELECT table_type AS "type", table_name AS "name", table_schem AS "owner" FROM sysibm.sqltables WHERE table_schem = '${schema}' AND table_type = 'VIEW'`;
    } else {
      sql = 'SELECT table_type AS "type", table_name AS "name", table_schem AS "owner" FROM sysibm.sqltables WHERE table_schem = USER AND table_type = \'VIEW\'';
    }

    return IBMiConnector.paginateSQL(sql, 'table_schem, table_name', options);
  }

  discoverModelDefinitions(options, callback) {
    let opts = options;
    let cb = callback;

    if (!cb && typeof options === 'function') {
      cb = opts;
      opts = {};
    }
    opts = opts || {};
    const calls = [];

    calls.push((callback) => {
      this.execute(IBMiConnector.buildQueryTables(options), callback);
    });

    if (options.views) {
      calls.push((callback) => {
        this.execute(IBMiConnector.buildQueryViews(options), callback);
      });
    }
    async.parallel(calls, (err, data) => {
      if (err) {
        cb(err, data);
      } else {
        var merged = [];
        merged = merged.concat(data.shift());
        if (data.length) {
          merged = merged.concat(data.shift());
        }
        cb(err, merged);
      }
    });
  }

  static buildQueryColumns(schema, table) {
      return IBMiConnector.paginateSQL('SELECT table_schem AS "owner",' +
      ' table_name AS "tableName",' +
      ' column_name AS "columnName",' +
      ' type_name AS "dataType",' +
      ' column_size AS "dataLength",' +
      ' num_prec_radix AS "dataPrecision",' +
      ' decimal_digits AS "dataScale",' +
      ' nullable AS "nullable"' +
      ' FROM sysibm.sqlcolumns' +
      (schema || table ? ' WHERE' : '') +
      (schema ? ` table_schem = '${schema}'` : '') +
      (schma && table ?  ` AND` : '') +
      (table ? ` table_name = '${table}'` : ''),
      'table_name, ordinal_position', {});
  }

  static buildPropertyType(columnDefinition) {
    const { dataLength } = columnDefinition;
    const type = columnDefinition.dataType.toUpperCase();

    switch (type) {
      case 'CHAR':
        if (dataLength === 1) {
          // Treat char(1) as boolean
          return 'Boolean';
        } // else
        return 'String';
      case 'VARCHAR':
      case 'TINYTEXT':
      case 'MEDIUMTEXT':
      case 'LONGTEXT':
      case 'TEXT':
      case 'ENUM':
      case 'SET':
        return 'String';
      case 'TINYBLOB':
      case 'MEDIUMBLOB':
      case 'LONGBLOB':
      case 'BLOB':
      case 'BINARY':
      case 'VARBINARY':
      case 'BIT':
        return 'Binary';
      case 'TINYINT':
      case 'SMALLINT':
      case 'INT':
      case 'INTEGER':
      case 'MEDIUMINT':
      case 'YEAR':
      case 'FLOAT':
      case 'DOUBLE':
      case 'BIGINT':
        return 'Number';
      case 'DATE':
      case 'TIMESTAMP':
      case 'DATETIME':
        return 'Date';
      case 'POINT':
        return 'GeoPoint';
      default:
        return 'String';
    }
  }

  static getArgs(table, options, cb) {
    // if ('string' !== (typeof table || !table)) {
    //   throw new Error('table is a required string argument: ' + table);
    // }
    if (typeof options !== 'object') {
      throw new TypeError(`options must be an object. Instead, found: ${typeof options}`);
    }
    const opts = options || {};
    // if (!cb && 'function' === (typeof options)) {
    //   cb = options;
    //   options = {};
    // }

    return {
      schema: opts.owner || opts.schema,
      table,
      options: opts,
      cb,
    };
  }

  discoverModelProperties(table, options, cb) {
    const args = IBMiConnector.getArgs(table, options, cb);
    let { schema } = args;
    if (!schema) {
      schema = this.getDefaultSchema();
    }
    table = args.table;
    options = args.options;
    cb = args.cb;

    const sql = IBMiConnector.buildQueryColumns(schema, table);

    return this.execute(sql, (err, results) => {
      if (err) {
        cb(err, results);
      } else {
        results.map(function(r) {
          r.type = this.buildPropertyType(r);
          r.nullable = r.nullable === '1' ? 'Y' : 'N';
        });
        cb(err, results);
      }
    });
  }

  static buildQueryPrimaryKeys(schema, table) {
    return 'SELECT table_schem AS "owner",' +
      ' table_name AS "tableName",' +
      ' column_name AS "columnName",' +
      ' key_seq AS "keySeq",' +
      ' pk_name AS "pkName"' +
      ' FROM sysibm.sqlprimarykeys' +
      (schema || table ? ' WHERE' : '') +
      (schema ? ` table_schem = '${schema}'` : '') +
      (schma && table ?  ` AND` : '') +
      (table ? ` table_name = '${table}'` : '') +
      ' ORDER BY table_schem, table_name, key_seq';
  }

  static buildQueryForeignKeys(schema, table) {
    return 'SELECT pktable_schem AS "fkOwner",' +
    ' fk_name AS "fkName",' +
    ' fktable_name AS "fkTableName",' +
    ' pktable_schem AS "pkOwner", pk_name AS "pkName",' +
    ' pktable_name AS "pkTableName",' +
    ' fkcolumn_name AS "pkColumnName"' +
    ' FROM sysibm.sqlforeignkeys' +
      (schema || table ? ' WHERE' : '') +
      (schema ? ` pktable_schem = '${schema}'` : '') +
      (schma && table ?  ` AND` : '') +
      (table ? ` pktable_name = '${table}'` : '');
  }

  discoverPrimaryKeys(table, options, cb) {
    const args = getArgs(table, options, cb);
    const schema = args.schema;
    if (!schema) {
      schema = this.getDefaultSchema();
    }
    table = args.table;
    options = args.options;
    cb = args.cb;

    const sql = IBMiConnector.queryPrimaryKeys(schema, table);

    this.execute(sql, cb);
  }

  discoverForeignKeys(table, options, cb) {
    const args = getArgs(table, options, cb);
    let { schema, table2, options2, cb2 } = args;
    if (!schema) {
      schema = this.getDefaultSchema();
    }

    const sql = IBMiConnector.queryForeignKeys(schema, table);
    this.execute(sql, cb);
  }

  buildQueryExportedForeignKeys(schema, table) {
    var sql = 'SELECT a.constraint_name AS "fkName",' +
      ' a.tabschema AS "fkOwner",' +
      ' a.tabname AS "fkTableName",' +
      ' a.colname AS "fkColumnName",' +
      ' NULL AS "pkName",' +
      ' a.referenced_table_schema AS "pkOwner",' +
      ' a.referenced_table_name AS "pkTableName",' +
      ' a.referenced_column_name AS "pkColumnName"' +
      ' FROM information_schema.key_column_usage a' +
      ' WHERE a.position_in_unique_constraint IS NOT NULL';
    if (schema) {
      sql += ' AND a.referenced_table_schema="' + schema + '"';
    }
    if (table) {
      sql += ' AND a.referenced_table_name="' + table + '"';
    }
    sql += ' ORDER BY a.table_schema, a.table_name, a.ordinal_position';

    return sql;
  }

  discoverExportedForeignKeys(table, options, cb) {
    var args = getArgs(table, options, cb);
    var schema = args.schema;
    if (!schema) {
      schema = this.getDefaultSchema();
    }
    table = args.table;
    options = args.options;
    cb = args.cb;

    const sql = IBMiConnector.queryExportedForeignKeys(schema, table);
    this.execute(sql, cb);
  }

  getDefaultSchema(options) {
    // TODO: Fix
    return this.schema || process.env.LOGNAME;
  }

  setDefaultOptions(options) {
    // TODO: ?
  }

  setNullableProperty(property) {
    // TODO: ?
  }
}

util.inherits(IBMiConnector, SqlConnector);

/**
 * Initialize the ODBCConnector connector for the given data source
 *
 * @param {DataSource} ds The data source instance
 * @param {Function} [cb] The cb function
 */
exports.initialize = (ds, cb) => {
  const dataSource = ds;
  dataSource.driver = odbc; // Provide access to the native driver
  dataSource.connector = new IBMiConnector(dataSource.settings);
  dataSource.connector.dataSource = ds;

  return cb();

  // return this.pool.init(cb);
};
