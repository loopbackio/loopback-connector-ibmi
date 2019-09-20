// Copyright IBM Corp. 2019. All Rights Reserved.
// Node module: loopback-connector-ibmi
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

/**
 * IBM i Db2 connector for LoopBack using ODBC driver
 */
'use strict';
const async = require('async');
const debug = require('debug')('loopback:connector:ibmiconnector');
const {SQLConnector} = require('loopback-connector');
const odbc = require('odbc');
const moment = require('moment');
const uuid = require('uuid');

const g = require('./globalize');

const {Transaction, ParameterizedSQL} = SQLConnector;

class IBMiConnector extends SQLConnector {
  constructor(settings) {
    super('ibmi', settings);
    this.connectionString = settings.connectionString;
    this.PARAMETER_MARKER = '?';
    this.transactionMap = {};
    this.transactionMap[Transaction.SERIALIZABLE] =
      odbc.SQL_TXN_SERIALIZABLE;
    this.transactionMap[Transaction.REPEATABLE_READ] =
      odbc.SQL_TXN_REPEATABLE_READ;
    this.transactionMap[Transaction.READ_COMMITTED] =
      odbc.SQL_TXN_READ_COMMITTED;
    this.transactionMap[Transaction.READ_UNCOMMITTED] =
      odbc.SQL_TXN_READ_UNCOMMITTED;
  }

  // Override SQLConnector functions

  /**
   * Get the escaped table name
   * @param {String} model The model name
   * @returns {String} the escaped table name
   */
  // Overrides SQLConnector implementation:
  // Includes the schema name if specified in the model
  tableEscaped(model) {
    const tableName = this.escapeName(this.table(model));
    const schemaName = this.escapeName(this.schema(model));

    return `${schemaName ? `${schemaName}.${tableName}` : `${tableName}`}`;
  }

  // Functions that must be implemented

  // Functions must be implemented as-is, many don't need 'this', so disable eslint rule
  /* eslint-disable class-methods-use-this, no-unused-vars */

  getColumnsToAdd(model, actualFields) {
    const m = this._models[model];
    const propNames = Object.keys(m.properties).filter((name) => {
      return !!m.properties[name];
    });

    const operations = [];

    const changed = (newSettings, oldSettings) => {
      if (oldSettings.Null === 'YES') {
        // Used to allow null and does not now.
        if (!this.isNullable(newSettings)) {
          return true;
        }
      }
      if (oldSettings.Null === 'NO') {
        // Did not allow null and now does.
        if (this.isNullable(newSettings)) {
          return true;
        }
      }
      return false;
    };

    const actualize = (propName, oldSettings) => {
      const newSettings = m.properties[propName];
      if (newSettings && changed(newSettings, oldSettings)) {
        // TODO: NO TESTS EXECUTE THIS CODE PATH
        operations.push(`CHANGE COLUMN ${this.columnEscaped(model, propName)}
        ${this.columnEscaped(model, propName)}
        ${this.buildColumnDefinition(model, propName)}`);
      }
    };

    // change/add new fields
    propNames.forEach((propName) => {
      if (m.properties[propName] && this.id(model, propName)) return;
      let found;
      if (actualFields) {
        actualFields.forEach((f) => {
          if (f.NAME === propName) {
            found = f;
          }
        });
      }

      if (found) {
        actualize(propName, found);
      } else {
        operations.push(`ADD COLUMN ${this.columnEscaped(model, propName)}
        ${this.buildColumnDefinition(model, propName)}`);
      }
    });

    return operations;
  }

  getColumnsToDrop(model, actualFields) {
    const m = this._models[model];
    const propNames = Object.keys(m.properties).filter((name) => {
      return !!m.properties[name];
    });

    const operations = [];

    // drop columns
    if (actualFields) {
      actualFields.forEach((f) => {
        const notFound = !~propNames.indexOf(f.NAME);
        if (m.properties[f.NAME] && this.id(model, f.NAME)) return;
        if (notFound || !m.properties[f.NAME]) {
          operations.push(`DROP COLUMN ${this.columnEscaped(model, f.NAME)}`);
        }
      });
    }
    return operations;
  }

  convertTextType(p, defaultType) {
    let dt = defaultType;
    let len = p.length || ((p.type !== String) ? 4096 : p.id ? 255 : 512);

    if (p[this.name]) {
      if (p[this.name].dataLength) {
        len = p[this.name].dataLength;
      }
    }

    if (p[this.name] && p[this.name].dataType) {
      dt = String(p[this.name].dataType);
    } else if (p.dataType) {
      dt = String(p.dataType);
    }

    dt += `(${len})`;

    if (p.charset) {
      dt += ` CHARACTER SET ${p.charset}`;
    }
    if (p.collation) {
      dt += ` COLLATE ${p.collation}`;
    }

    return dt;
  }

  convertNumberType(p, defaultType) {
    let dt = defaultType;
    let precision = p.precision;
    let scale = p.scale;

    if (p[this.name] && p[this.name].dataType) {
      dt = String(p[this.name].dataType);
      precision = p[this.name].dataPrecision;
      scale = p[this.name].dataScale;
    } else if (p.dataType) {
      dt = String(p.dataType);
    } else {
      return dt;
    }

    switch (dt) {
      case 'DECIMAL':
        dt = 'DECIMAL';
        if (precision && scale) {
          dt += '(' + precision + ',' + scale + ')';
        } else if (scale > 0) {
          throw new Error('Scale without Precision does not make sense');
        }
        break;
      default:
        break;
    }

    return dt;
  }

  buildColumnType(property) {
    let dt = '';
    const p = property;
    const type = p.type.name;

    switch (type) {
      case 'Any':
      case 'Text':
      case 'String':
        dt = this.convertTextType(p, 'VARCHAR');
        break;
      case 'Number':
        dt = 'INTEGER';
        dt = this.convertNumberType(p, 'INTEGER');
        break;
      case 'Date':
        dt = 'TIMESTAMP';
        break;
      case 'Boolean':
        dt = 'SMALLINT';
        break;
      case 'Decimal':
        dt = this.convertNumberType(p, 'INTEGER');
        dt = 'DECIMAL';
        break;
      default:
        dt = 'VARCHAR(255)';
    }
    debug('IBMiConnector.prototype.buildColumnType %j %j', p.type.name, dt);
    return dt;
  }

  /**
   * Alters a table
   * @param {String} model The model name
   * @param {Object} fields Fields of the table
   * @param {Object} indexes Indexes of the table
   * @param {Function} cb The callback function
   */
  alterTable(model, actualFields, actualIndexes, callback) {
    debug('IBMiConnector.alterTable %j %j %j %j',
      model, actualFields, actualIndexes);

    const m = this.getModelDefinition(model);
    // Todo: I think this is what we want... not !!
    const propNames = Object.keys(m.properties).filter((name) => {
      return Object.prototype.hasOwnProperty.call(m.properties, name);
    });
    const indexes = m.settings.indexes || {};
    const indexNames = Object.keys(indexes).filter((name) => {
      return Object.prototype.hasOwnProperty.call(indexes, name);
    });
    const sql = [];
    const tasks = [];
    let operations = [];
    const ai = {};
    let type = '';

    if (actualIndexes) {
      actualIndexes.forEach((i) => {
        const name = i.INDNAME;
        if (!ai[name]) {
          ai[name] = {
            info: i,
            columns: [],
          };
        }

        i.COLNAMES.split(/\+\s*/).forEach((columnName, j) => {
          // This is a bit of a dirty way to get around this but DB2 returns
          // column names as a string started with and separated by a '+'.
          // The code below will strip out the initial '+' then store the
          // actual column names.
          if (j > 0) ai[name].columns[j - 1] = columnName;
        });
      });
    }
    const aiNames = Object.keys(ai);

    const changed = (newSettings, oldSettings) => {
      if (oldSettings.Null === 'YES') {
        // Used to allow null and does not now.
        if (!this.isNullable(newSettings)) {
          return true;
        }
      }
      if (oldSettings.Null === 'NO') {
        // Did not allow null and now does.
        if (this.isNullable(newSettings)) {
          return true;
        }
      }

      return false;
    };

    const actualize = (propName, oldSettings) => {
      const newSettings = m.properties[propName];
      if (newSettings && changed(newSettings, oldSettings)) {
        const pName = `'${propName}'`;
        operations.push(`CHANGE COLUMN ${pName} ${pName}
        ${this.buildColumnDefinition(model, propName)}`);
      }
    };

    // change/add new fields
    propNames.forEach((propName) => {
      if (m.properties[propName] && this.id(model, propName)) return;
      let found;
      if (actualFields) {
        actualFields.forEach((f) => {
          if (f.NAME === propName) {
            found = f;
          }
        });
      }

      if (found) {
        actualize(propName, found);
      } else {
        operations.push(`ADD COLUMN "${propName}"
        ${this.buildColumnDefinition(model, propName)}`);
      }
    });

    // drop columns
    if (actualFields) {
      actualFields.forEach((f) => {
        const notFound = (propNames.indexOf(f.NAME) === -1);
        if (m.properties[f.NAME] && this.id(model, f.NAME)) return;
        if (notFound || !m.properties[f.NAME]) {
          operations.push(`DROP COLUMN ${f.NAME}`);
        }
      });
    }

    if (operations.length) {
      // Add the ALTER TABLE statement to the list of tasks to perform later.
      sql.push(`ALTER TABLE ${this.schema(model) ?
        `${this.schema(model)}.` : ''}${this.tableEscaped(model)}
      ${operations.join(' ')}`);
    }

    operations = [];

    // remove indexes
    aiNames.forEach((indexName) => {
      if (ai[indexName].info.UNIQUERULE === 'P' ||
      (m.properties[indexName] && this.id(model, indexName))) {
        return;
      }

      if ((indexNames.indexOf(indexName) === -1 && !m.properties[indexName]) ||
        (m.properties[indexName] && !m.properties[indexName].index)) {
        if (ai[indexName].info.UNIQUERULE === 'P') {
          operations.push('DROP PRIMARY KEY');
        } else if (ai[indexName].info.UNIQUERULE === 'U') {
          operations.push(`DROP UNIQUE ${indexName}`);
        }
      } else {
        // first: check single (only type and kind)
        if (m.properties[indexName] && !m.properties[indexName].index) {
          // TODO
          return;
        }
        // second: check multiple indexes
        let orderMatched = true;
        if (indexNames.indexOf(indexName) !== -1) {
          m.settings.indexes[indexName].columns.split(/,\s*/)
            .forEach((columnName, i) => {
              if (ai[indexName].columns[i] !== columnName) orderMatched = false;
            });
        }

        if (!orderMatched) {
          if (ai[indexName].info.UNIQUERULE === 'P') {
            operations.push('DROP PRIMARY KEY');
          } else if (ai[indexName].info.UNIQUERULE === 'U') {
            operations.push(`DROP UNIQUE ${indexName}`);
          }

          delete ai[indexName];
        }
      }
    });

    if (operations.length) {
      // Add the ALTER TABLE statement to the list of tasks to perform later.
      sql.push(`ALTER TABLE ${this.schema(model) ?
        `${this.schema(model)}.` : ''}${this.tableEscaped(model)}
      ${operations.join(' ')}`);
    }

    // add single-column indexes
    propNames.forEach((propName) => {
      const i = m.properties[propName].index;
      if (!i) {
        return;
      }
      const found = ai[propName] && ai[propName].info;
      if (!found) {
        const pName = propName;
        type = '';
        if (i.type) {
          type = i.type;
        }
        sql.push(`CREATE ${type} INDEX ${pName} ON
        ${this.schema}.${this.tableEscaped(model)} ("${pName}") `);
      }
    });

    // add multi-column indexes
    indexNames.forEach((indexName) => {
      const i = m.settings.indexes[indexName];
      const found = ai[indexName] && ai[indexName].info;
      if (!found) {
        const iName = indexName;
        let type = '';
        if (i.type) {
          type = i.type;
        }
        let stmt = `CREATE ${type} INDEX ${iName} ON
        ${this.tableEscaped(model)} (`;

        const splitNames = i.columns.split(/,\s*/);
        const colNames = splitNames.join('","');

        stmt += `"${colNames}")`;

        sql.push(stmt);
      }
    });

    sql.forEach((i) => {
      tasks.push((cb) => {
        this.execute(i, (err, results) => {
          cb(err, results);
        });
      });
    });

    if (tasks.length) {
      // if (checkOnly) {
      //   return callback(null, true, {statements: sql});
      // }

      async.series(tasks, () => callback());
    } else {
      return callback();
    }
  }

  /**
   * Get fields from a table
   * @param {String} model The model name
   * @param {Function} cb The callback function
   */
  showFields(model, cb) {
    const table = this.table(model);
    const schema = this.schema(model);
    const sql = `SELECT
                  COLUMN_NAME AS NAME,
                  DATA_TYPE AS DATATYPE,
                  ORDINAL_POSITION AS COLNO,
                  IS_NULLABLE AS NULLS
                FROM
                  QSYS2.SYSCOLUMNS
                WHERE 
                  TRIM(TABLE_NAME) = '${table}'
                  AND TRIM(TABLE_SCHEMA) = ${schema ? `'${schema}'` : 'USER'}
                ORDER BY
                  COLNO`;
    this.execute(sql, cb);
  }

  /**
   * Get indexes from a table
   * @param {String} model The model name
   * @param {Function} cb The callback function
   */
  showIndexes(model, cb) {
    const table = this.table(model);
    const schema = this.schema(model);
    const sql = `SELECT
                  INDEX_NAME as INDNAME
                FROM
                  QSYS2.SYSINDEXES
                WHERE 
                  TRIM(TABLE_NAME) = '${table}'
                  AND TRIM(TABLE_SCHEMA) = ${schema ? `'${schema}'` : 'USER'}`;

    this.execute(sql, cb);
  }

  buildColumnDefinition(model, prop) {
    const p = this.getModelDefinition(model).properties[prop];
    return `${this.columnDataType(model, prop)}
    ${(!this.isNullable(p) ? 'NOT NULL' : '')}`;
  }

  buildIndex(model, property) {
    const prop = this.getModelDefinition(model).properties[property];
    const i = prop && prop.index;
    if (!i) {
      return '';
    }
    let type = '';
    let kind = '';
    if (i.type) {
      type = `USING ${i.type}`;
    }
    if (i.kind) {
      kind = i.kind;
    }
    const columnName = this.columnEscaped(model, property);

    if (kind && type) {
      return `${kind} INDEX ${columnName} (${columnName}) ${type}`;
    }
    if (typeof i === 'object' && i.unique && i.unique === true) {
      kind = 'UNIQUE';
    }
    return `${kind} INDEX ${columnName} ${type} (${columnName})`;
  }

  buildIndexes(model) {
    const indexClauses = [];
    const definition = this.getModelDefinition(model);
    const indexes = definition.settings.indexes || {};

    // Build model level indexes
    Object.keys(indexes).forEach((index) => {
      const i = indexes[index];
      let type = '';
      let kind = '';
      if (i.type) {
        type = 'USING ' + i.type;
      }
      if (i.kind) {
        // if index uniqueness is configured as "kind"
        kind = i.kind;
      } else if (i.options && i.options.unique && i.options.unique == true) {
        // if index unique indicator is configured
        kind = 'UNIQUE';
      }
      const indexedColumns = [];
      const indexName = this.escapeName(index);
      let columns = '';
      // if indexes are configured as "keys"
      if (i.keys) {
        // for each field in "keys" object
        for (const key in i.keys) {
          if (i.keys[key] !== -1) {
            indexedColumns.push(this.escapeName(key));
          } else {
            // mysql does not support index sorting Currently
            // but mysql has added DESC keyword for future support
            indexedColumns.push(this.escapeName(key) + ' DESC ');
          }
        }
      }
      if (indexedColumns.length) {
        columns = indexedColumns.join(',');
      } else if (i.columns) {
        columns = i.columns;
      }
      if (columns.length) {
        if (kind && type) {
          indexClauses.push(kind + ' INDEX ' +
          indexName + ' (' + columns + ') ' + type);
        } else {
          indexClauses.push(kind + ' INDEX ' + type +
          ' ' + indexName + ' (' + columns + ')');
        }
      }
    });
    // Define index for each of the properties
    for (const p in definition.properties) {
      const propIndex = this.buildIndex(model, p);
      if (propIndex) {
        indexClauses.push(propIndex);
      }
    }
    return indexClauses;
  }

  buildColumnDefinitions(model) {
    const pks = this.idNames(model).map(i => this.columnEscaped(model, i));

    const definition = this.getModelDefinition(model);
    const sql = [];
    if (pks.length === 1) {
      const idName = this.idName(model);
      const idProp = this.getModelDefinition(model).properties[idName];
      if (idProp.generated) {
        sql.push(`${this.columnEscaped(model, idName)}
        ${this.buildColumnDefinition(model, idName)}
                  GENERATED BY DEFAULT AS IDENTITY
                  (START WITH 1, INCREMENT BY 1) PRIMARY KEY`);
      } else {
        idProp.nullable = false;
        sql.push(`${this.columnEscaped(model, idName)}
        ${this.buildColumnDefinition(model, idName)} PRIMARY KEY`);
      }
    }
    Object.keys(definition.properties).forEach((prop) => {
      if (this.id(model, prop) && pks.length === 1) {
        return;
      }
      const colName = this.columnEscaped(model, prop);
      sql.push(`${colName} ${this.buildColumnDefinition(model, prop)}`);
    });
    if (pks.length > 1) {
      sql.push(`PRIMARY KEY(${pks.join(',')})`);
    }

    // const indexes = this.buildIndexes(model);
    // indexes.forEach((i) => {
    //   sql.push(i);
    // });

    return sql.join(', ');
  }

  /**
   * Create the table for the given model
   *
   * @param {string} model The model name
   * @param {Function} [cb] The callback function
   */
  createTable(model, cb) {
    debug('IBMiConnector.createTable ', model);
    const tableName = this.tableEscaped(model);
    const columnDefinitions = this.buildColumnDefinitions(model);
    const tasks = [];

    tasks.push((callback) => {
      const sql = `CREATE TABLE ${tableName} (${columnDefinitions})`;
      this.execute(sql, callback);
    });

    // const indexes = this.buildIndexes(model);
    // indexes.forEach((i) => {
    //   tasks.push((callback) => {
    //     this.execute(i, callback);
    //   });
    // });

    async.series(tasks, cb);
  }

  /**
   * Get the default database schema name
   * @returns {string} The default schema name, such as 'public' or 'dbo'
   */
  getDefaultSchemaName() {
    return this.defaultSchema || undefined;
  }

  /**
   * Converts a model property value into the form required by the
   * database column. The result should be one of following forms:
   *
   * - {sql: "point(?,?)", params:[10,20]}
   * - {sql: "'John'", params: []}
   * - "John"
   *
   * @param {Object} propertyDef Model property definition
   * @param {*} value Model property value
   * @returns {ParameterizedSQL|*} Database column value.
   *
   */
  toColumnValue(prop, val) {
    if (val === undefined && this.isNullable(prop)) {
      return null;
    }
    if (val === null) {
      if (this.isNullable(prop)) {
        return val;
      } else {
        try {
          const castNull = prop.type(val);
          if (prop.type === Object) {
            return JSON.stringify(castNull);
          }
          return castNull;
        } catch (err) {
          // if we can't coerce null to a certain type,
          // we just return it
          return 'null';
        }
      }
    }
    if (!prop) {
      return val;
    }
    if (prop.type === String) {
      return String(val);
    }
    if (prop.type === Number) {
      if (isNaN(val)) {
        return val;
      }
      return val;
    }
    if (prop.type === Date) {
      const mome = moment(val);
      return mome.format('YYYY-MM-DD HH:mm:ss.SSS');
    }
    if (prop.type.name === 'DateString') {
      return val.when;
    }
    if (prop.type === Boolean) {
      return !!val;
    }
    if (prop.type.name === 'GeoPoint') {
      return new ParameterizedSQL({
        sql: '?',
        params: [String(val)],
      });
    }
    if (prop.type === Buffer) {
      return val;
    }
    if (prop.type === Object) {
      return this._serializeObject(val);
    }
    if (typeof prop.type === 'function') {
      return this._serializeObject(val);
    }
    return this._serializeObject(val);
  }

  _serializeObject(obj) {
    let val;
    if (obj && typeof obj.toJSON === 'function') {
      obj = obj.toJSON();
    }
    if (typeof obj !== 'string') {
      val = JSON.stringify(obj);
    } else {
      val = obj;
    }
    return val;
  }

  /**
   * Convert the data from database column to model property
   * @param {object} propertyDef Model property definition
   * @param {*} value Column value
   * @returns {*} Model property value
   */
  fromColumnValue(prop, val) {
    if (val == null) {
      return val;
    }
    if (prop) {
      switch (prop.type.name) {
        case 'Number':
          return Number(val);
        case 'String':
          return String(val);
        case 'Date':
        case 'DateString':
          return new Date(val);
        case 'Boolean':
          return Boolean(val);
        case 'GeoPoint':
        case 'Point':
          const points = val.split(',');
          return {
            lng: points[0],
            lat: points[1],
          };
        case 'ObjectID':
          return new prop.type(val);
        case 'Buffer':
          return prop.type(val);
        case 'List':
        case 'Array':
        case 'Object':
        case 'JSON':
          if (typeof val === 'string') {
            return JSON.parse(val);
          }
        default:
          return val;
      }
    }
    return val;
  }

  /**
   * Escape the name for the underlying database
   * @param {String} value The value to be escaped
   * @returns {*} An escaped value for SQL
   */
  escapeName(name) {
    debug(`IBMiConnector.escapeName name=${name}`, name);
    if (!name) return name;
    name.replace(/["]/g, '""');
    return `"${name}"`;
  }

  /**
   * Escape the name for the underlying database
   * @param {String} value The value to be escaped
   * @returns {*} An escaped value for SQL
   */
  escapeValue(value) {
    // TODO: No connectors seem to implement this. Taking a stab in the dark
    return `'${value}'`;
  }

  buildReplaceFields(model, data, excludeIds) {
    const props = this.getModelDefinition(model).properties;
    const keys = Object.keys(props);
    return this._buildFieldsForReplaceKeys(model, data, keys, excludeIds);
  }

  _buildFieldsForReplaceKeys(model, data, keys, excludeIds) {
    const props = this.getModelDefinition(model).properties;
    const fields = {
      names: [], // field names
      columnValues: [], // an array of ParameterizedSQL
      properties: [], // model properties
    };
    for (let i = 0, n = keys.length; i < n; i++) {
      const key = keys[i];
      const p = props[key];
      if (p == null) {
        // Unknown property, ignore it
        debug('Unknown property %s is skipped for model %s', key, model);
        continue;
      }

      if (excludeIds && p.id) {
        continue;
      }
      const k = this.columnEscaped(model, key);
      const v = this.toColumnValue(p, data[key]);
      fields.names.push(k);
      if (v instanceof ParameterizedSQL) {
        fields.columnValues.push(v);
      } else {
        if (v !== undefined) {
          fields.columnValues.push(new ParameterizedSQL('?', [v]));
        } else {
          fields.columnValues.push(new ParameterizedSQL('?', [null]));
        }
      }
      fields.properties.push(p);
    }
    return fields;
  }

  /**
   * Get the place holder in SQL for identifiers, such as ??
   * @param {String} key Optional key, such as 1 or id
   * @returns {String} The place holder
   */
  getPlaceholderForIdentifier(key) {
    throw new Error(g.f('Placeholder for identifiers is not supported: %s',
      key));
  }

  /**
   * Get the place holder in SQL for values, such as :1 or ?
   *
   * @param {string} key Optional key, such as 1 or id
   * @returns {string} The place holder
   */
  getPlaceholderForValue(key) {
    debug(`IBMiConnector.prototype.getPlaceholderForValue key=${key}`);
    return this.PARAMETER_MARKER;
  }

  buildLimit(limit, offset) {
    let lim = limit;
    let off = offset;

    if (Number.isNaN(limit)) { lim = undefined; }
    if (Number.isNaN(offset)) { off = undefined; }

    return `${off ? `OFFSET ${off} ROWS` : ''} ${lim ?
      `FETCH FIRST ${lim} ROWS ONLY ` : ''}`;
  }

  /**
   * Build a new SQL statement with pagination support by wrapping the given sql
   * @param {String} model The model name
   * @param {ParameterizedSQL} stmt The sql statement
   * @param {Object} filter The filter object from the query
   */
  applyPagination(model, stmt, filter) {
    debug('IBMiConnector.prototype.applyPagination');
    const limitClause = this.buildLimit(filter.limit, filter.offset ||
      filter.skip);
    return stmt.merge(limitClause);
  }

  /**
   * Parse the result for SQL UPDATE/DELETE/INSERT for the number of rows
   * affected
   * @param {String} model Model name
   * @param {Object} info Status object
   * @returns {Number} Number of rows affected
   */
  getCountForAffectedRows(model, info) {
    // odbc results object countains 'count' property which holds the result of
    // SQLRowCount function, giving "the number of rows affected by an UPDATE,
    // INSERT, or DELETE statement
    if (info === undefined || info.count < 0) {
      return 0;
    }
    return info.count;
  }

  /**
   * Override and call SQLConnector buildInsert function to wrap the entire
   * statement in a `SELECT * FROM FINAL TABLE` to get the inserted id.
   * @param {String} model The model name
   * @param {Object} data The model data object
   * @param {Object} options The options object
   * @returns {string} The INSERT SQL statement
   */
  buildInsert(model, data, options) {
    const idCol = this.idColumn(model);
    if (typeof data[idCol] !== undefined && data[idCol] === null) {
      data[idCol] = undefined;
    }
    const fields = this.buildFields(model, data);
    const insertStmt = this.buildInsertInto(model, fields, options);
    const columnValues = fields.columnValues;
    const fieldNames = fields.names;
    if (fieldNames.length) {
      const values = ParameterizedSQL.join(columnValues, ',');
      values.sql = 'VALUES(' + values.sql + ')';
      insertStmt.merge(values);
    } else {
      insertStmt.merge(this.buildInsertDefaultValues(model, data, options));
    }
    const returning = this.buildInsertReturning(model, data, options);
    if (returning) {
      insertStmt.merge(returning);
    }
    const parameterizedStatement = this.parameterize(insertStmt);
    parameterizedStatement.sql =
      `SELECT * FROM FINAL TABLE(${parameterizedStatement.sql})`;
    return parameterizedStatement;
  }

  buildInsertDefaultValues(model, data, options) {
    const idColumnEscaped = this.idColumnEscaped(model);
    return `(${idColumnEscaped}) VALUES (DEFAULT)`;
  }

  /**
   * Parse the result for SQL INSERT for newly inserted id
   * @param {String} model Model name
   * @param {Object} info The status object from driver
   * @returns {*} The inserted id value
   */
  getInsertedId(model, info) {
    const idColumn = this.idColumn(model);
    return info && info.length === 1 ? info[0][idColumn] : undefined;
  }

  /**
   * Execute a SQL statement with given parameters
   * @param {string} sql The SQL statement
   * @param {*[]} [params] An array of parameter values
   * @param {Object} [options] Options object
   * @param {Function} [callback] The callback function
   */
  async executeSQL(sql, params, options, callback) {
    const transaction = options.transaction;
    // running in a transaction
    if (transaction && transaction.connector === this) {
      if (!transaction.connection) {
        return process.nextTick(() => {
          callback(new Error(g.f('Transaction connection does not exist')));
        });
      }
      if (transaction.connection.connected === false) {
        return process.nextTick(() => {
          callback(new Error(g.f('Transaction connection is not active')));
        });
      }

      return transaction.connection.query(sql, params, callback);
    }

    if (!this.pool) {
      return callback(Error('The connector has not been intialized!'));
    }
    this.pool.query(sql, params, callback);
  }

  /**
   * Build sql for listing schemas
   * @param {Object} options Options for discoverDatabaseSchemas
   */
  buildQuerySchemas(options) {
    const sql = `SELECT table_cat AS "catalog",
      table_schem AS "schema" FROM SYSIBM.SQLSCHEMAS`;
    return IBMiConnector.paginateSQL(sql, 'table_schem', options);
  }

  /**
   * Paginate the results returned from database
   * @param {String} sql The sql to execute
   * @param {Object} orderBy The property name by which results are ordered
   * @param {Object} options Options for discoverDatabaseSchemas
   */
  paginateSQL(sql, orderBy, options) {
    const opts = options || {};

    const limit = opts.limit;
    const offset = opts.offset || opts.skip;

    const limitAndOffset = this.buildLimit(limit, offset);

    return `${orderBy ? `ORDER BY ${orderBy}` : ''}
      ${limitAndOffset}`;
  }

  /**
  * Build sql for listing tables
  * @param options {all: for all owners, owner: for a given owner}
  * @returns {string} The sql statement
  */
  buildQueryTables(options) {
    let whereClause = '';
    if (!options.all) {
      if (options.owner) {
        whereClause = `TABLE_OWNER = '${options.owner}' AND `;
      }
      if (options.schema) {
        whereClause = `TABLE_SCHEMA = '${options.schema}' AND `;
      }
    }
    const sql = `SELECT TABLE_TYPE AS "type", TABLE_NAME AS "name",
    TABLE_SCHEMA AS "owner" FROM QSYS2.SYSTABLES
    WHERE ${whereClause} table_type = 'T'`;

    return IBMiConnector.paginateSQL(sql, 'table_schem, table_name', options);
  }

  /**
   * Build sql for listing views
   * @param options {all: for all owners, owner: for a given owner}
   * @returns {string} The sql statement
   */
  buildQueryViews(options) {
    let whereClause = '';
    if (!options.all) {
      if (options.owner) {
        whereClause = `TABLE_OWNER = '${options.owner}' AND `;
      }
      if (options.schema) {
        whereClause = `TABLE_SCHEMA = '${options.schema}' AND `;
      }
    }
    const sql = `SELECT TABLE_TYPE AS "type", TABLE_NAME AS "name",
    TABLE_SCHEMA AS "owner" FROM QSYS2.SYSTABLES
    WHERE ${whereClause} table_type = 'V'`;

    return IBMiConnector.paginateSQL(sql, 'table_schem, table_name', options);
  }

  /**
   * Build sql for listing columns
   * @param {String} schema The schema name
   * @param {String} table The table name
   */
  // Due to the different implementation structure of information_schema across
  // connectors, each connector will have to generate its own query
  buildQueryColumns(schema, table) {
    return `SELECT 
              table_schem AS "owner",
              table_name AS "tableName",
              column_name AS "columnName",
              type_name AS "dataType",
              column_size AS "dataLength",
              num_prec_radix AS "dataPrecision",
              decimal_digits AS "dataScale",
              nullable AS "nullable"
            FROM
              SYSIBM.SQLCOLUMNS
            WHERE
              TABLE_SCHEM = '${schema}'
              AND TABLE_NAME = '${table}'
            ORDER BY
              ORDINAL_POSITION`;
  }

  /**
   * Map the property type from database to loopback
   * @param {Object} columnDefinition The columnDefinition of the table/schema
   * @param {Object} options The options for the connector
   */
  buildPropertyType(columnDefinition, options) {
    const type = columnDefinition.dataType.toUpperCase();
    switch (type) {
      case 'CHAR':
      case 'VARCHAR':
      case 'XML':
        return 'String';
      case 'TINYBLOB':
      case 'MEDIUMBLOB':
      case 'LONGBLOB':
      case 'BLOB':
      case 'BINARY':
      case 'VARBINARY':
      case 'BIT':
        return 'String';
      case 'SMALLINT':
      case 'INTEGER':
      case 'BIGINT':
      case 'DECIMAL':
      case 'NUMERIC':
      case 'DECFLOAT':
      case 'REAL':
      case 'DOUBLE':
        return 'Number';
      case 'TIME':
      case 'TIMESTAMP':
      case 'DATETIME':
        return 'Date';
      default:
        return 'String';
    }
  }

  /*!
  * Normalize the arguments
  * @param table string, required
  * @param options object, optional
  * @param cb function, optional
  */
  getArgs(table, opts, cb) {
    let options = opts;
    let callback = cb;
    if (!table || typeof table !== 'string') {
      throw new TypeError(
        `table must be a string. Instead, found: ${typeof string}`
      );
    }
    if (!callback && typeof options === 'function') {
      callback = options;
      options = {};
    }
    if (typeof options !== 'object') {
      throw new TypeError(
        `options must be an object. Instead, found: ${typeof options}`
      );
    }

    return {
      schema: opts.owner || opts.schema,
      table,
      options,
      cb: callback,
    };
  }

  /**
   * Build the sql statement for querying primary keys of a given table
   * @param schema
   * @param table
   * @returns {string}
   */
  // http://docs.oracle.com/javase/6/docs/api/java/sql/DatabaseMetaData.html
  // #getPrimaryKeys(java.lang.String, java.lang.String, java.lang.String)
  // Due to the different implementation structure of information_schema across
  // connectors, each connector will have to generate its own query
  buildQueryPrimaryKeys(schema, table) {
    return `SELECT 
              TABLE_SCHEM AS "owner",
              TABLE_NAME AS "tableName",
              COLUMN_NAME AS "columnName",
              KEY_SEQ AS "keySeq",
              PK_NAME AS "pkName"
            FROM
              SYSIBM.SQLPRIMARYKEYS
            WHERE
              TABLE_SCHEM = '${schema}'
              AND TABLE_NAME = '${table}'
            ORDER BY
                TABLE_SCHEM, TABLE_NAME, PK_NAME, KEY_SEQ`;
  }

  /**
   * Build the sql statement for querying foreign keys of a given table
   * @param schema
   * @param table
   * @returns {string}
   */
  // Due to the different implementation structure of information_schema across
  // connectors, each connector will have to generate its own query
  buildQueryForeignKeys(schema, table) {
    return `SELECT 
              FKTABLE_SCHEM AS "fkOwner",
              FKTABLE_NAME AS "fkTableName",
              FK_NAME AS "fkName",
              FKCOLUMN_NAME AS "fkColumnName",
              KEY_SEQ AS "keySeq",
              PKTABLE_SCHEM AS "pkOwner",
              PKTABLE_NAME AS "pkTableName",
              PK_NAME AS "pkName",
              PKCOLUMN_NAME AS "pkColumnName"
            FROM
              SYSIBM.SQLFOREIGNKEYS
            WHERE
              FKTABLE_SCHEM = '${schema}'
              AND FKTABLE_NAME = '${table}'`;
  }

  /**
   * Retrieves a description of the foreign key columns that reference the
   * given table's primary key columns (the foreign keys exported by a table).
   * They are ordered by fkTableOwner, fkTableName, and keySeq.
   * @param schema
   * @param table
   * @returns {string}
   */
  // Due to the different implementation structure of information_schema across
  // connectors, each connector will have to generate its own query
  buildQueryExportedForeignKeys(schema, table) {
    return `SELECT 
              FKTABLE_SCHEM AS "fkOwner",
              FKTABLE_NAME AS "fkTableName",
              FK_NAME AS "fkName",
              FKCOLUMN_NAME AS "fkColumnName",
              KEY_SEQ AS "keySeq",
              PKTABLE_SCHEM AS "pkOwner",
              PKTABLE_NAME AS "pkTableName",
              PK_NAME AS "pkName",
              PKCOLUMN_NAME AS "pkColumnName"
            FROM
              SYSIBM.SQLFOREIGNKEYS
            WHERE
              PKTABLE_SCHEM = '${schema}'
              AND PKTABLE_NAME = '${table}'`;
  }

  /**
   * Discover default schema of a database
   * @param {Object} options The options for discovery
   */
  getDefaultSchema(options) {
    this.getDefaultSchemaName();
  }

  /**
   * Set default options for the connector
   * @param {Object} options The options for discovery
   */
  setDefaultOptions(options) {
    // no op
  }

  /**
   * Set the nullable value for the property
   * @param {Object} property The property to set nullable
   */
  setNullableProperty(property) {
    property.nullable = property.nullable ? true : false;
  }

  /**
   * Drop the table for the given model from the database
   * @param {String} model The model name
   * @param {Function} [cb] The callback function
   */
  // Overwrites the dropTable function in SQLConnector
  dropTable(model, cb) {
    return this.execute(`BEGIN IF EXISTS (SELECT NAME FROM SYSIBM.TABLES
      WHERE${this.schema(model) ? ` TABLE_SCHEMA = '${this.schema(model)}'
      AND` : ''} TABLE_NAME = '${this.table(model)}')
      THEN DROP TABLE ${this.tableEscaped(model)}; COMMIT; END IF; END`, cb);
  }

  ping(callback) {
    const sql = 'SELECT 1 AS PING FROM SYSIBM.SYSDUMMY1';
    this.execute(sql, callback);
  }

  searchForPropertyInActual(model, propName, actualFields) {
    let found = false;
    actualFields.forEach((f) => {
      if (f.name === this.column(model, propName)) {
        found = f;
      }
    });
    return found;
  }

  // ///////////////////////////////////////////////////////////////////////////
  // //////////////////////////// TRANSACTIONS /////////////////////////////////
  // ///////////////////////////////////////////////////////////////////////////

  // This functionality is standard to all DBMS using ODBC drivers. In
  // particular, ODBC connections should not use BEGIN_TRANSACTION, ROLLBACK,
  // or COMMIT statements, and instead should use ODBC functions like
  // SQLSetConnectAttr to start and SQLEndTran to end.

  /**
   * Begin a new transaction

   * @param {Integer} level
   * @param {Function} cb
   */
  beginTransaction(level, callback) {
    debug('beginTransaction: isolationLevel: %s', level);
    this.pool.connect((error1, connection) => {
      if (error1) {
        return callback(error1);
      }
      connection.setIsolationLevel(this.transactionMap[level], (error2) => {
        if (error2) {
          return callback(error2);
        }
        return connection.beginTransaction((error3) => {
          if (error3) {
            return callback(error3);
          }
          const tx = new Transaction(this, connection);
          tx.txId = uuid.v1();
          connection.txId = tx.txId;
          return callback(null, tx);
        });
      });
    });
  }

  /**
   * Commit a transaction
   *
   * @param {Object} connection
   * @param {Function} cb
   */
  commit(connection, callback) {
    debug('Commit a transaction');
    connection.commit((err) => {
      if (err) return callback(err);
      return connection.close(callback);
    });
  }

  /**
   * Roll back a transaction
   *
   * @param {Object} connection
   * @param {Function} cb
   */
  rollback(connection, cb) {
    debug('Rollback a transaction');
    connection.rollback((err) => {
      if (err) return cb(err);
      return connection.close(cb);
    });
  }
  /* eslint-enable class-methods-use-this, no-unused-vars */

  disconnect(callback) {
    this.pool.close(callback);
  }
}

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
  odbc.pool(dataSource.connector.connectionString, (error, pool) => {
    dataSource.connector.pool = pool;
    dataSource.connector.odbc = odbc;
    cb(error);
  });
};
