class ParameterizedSQL {

}

abstract class LoopbackConnector {
  /**
   * What if I add some sort of comment here?
   * @param options: This is where I something!
   */
  constructor() {

  }

  abstract executeSQL(sql: string, parameters: any[], options: object, callback: Function): void;
  abstract executeSQL(sql: string, options: object, callback: Function): void;
  abstract toColumnValue(propertyDef: object, value: any): ParameterizedSQL;
  abstract fromColumnValue(propertyDef: object, value: any): any;

  // TODO: These need return values
  abstract applyPagination(model: string, statement: ParameterizedSQL, filter: object);
  abstract getCountForAffectedRows(model: string, info: object);

  abstract getInsertedId(model: string, info: object);

  abstract escapeName(name: string);
  abstract escapeValue(value: any);

  abstract getPlaceholderForIdentifier(key?: any);
  abstract getPlaceholderForValue(key?: any);

  // Can be overwritten by the connector to definte behavior specific to the connector
  beginTransaction() {

  }
  commit(connection: any, callback: Function) {

  } // Connection should have a Connection subtype? But that would force people to use TypeScript in their connector
  rollback(connection: any, callback: Function) {

  }
}

interface LoopbackConnectorMigration {
  /**
   * What if I add some sort of comment here?
   * @param options: This is where I something!
   */
  autoupdate(models: string|string[], callback: Function);
  autoupdate(callback: Function);
  automigrate(models: string|string[], callback: Function);
  automigrate(callback: Function);

  createTable(model: string, callback: Function);
  isActual(model: string|string[], callback: Function);
  isActual(model: Function);
  alterTable(model, actualFields, actualIndexes, done, checkOnly);
  buildColumnDefinitions(model: string);
  propertiesSQL(model: string);
  buildIndex(model: string, property: string);
  buildIndexes(model: string);
  buildColumnDefinition(model: string, property: string);
  columnDataType(model: string, ptoperty: string);
}

interface LoopbackConnectorDiscovery {
  /**
   * What if I add some sort of comment here?
   * @param options: This is where I something!
   */
  buildQuerySchemas(options: object): string;
  buildQueryTables(options: object): string;
  buildQueryViews(options: object): string;
  buildQueryColumns(schema: string, table: string): string;
  buildQueryPrimaryKeys(schema: string, table: string): string;
  buildQueryForeignKeys(schema: string, table: string): string;
  buildQueryExportedForeignKeys(schema: string, table: string): string;
}


class IBMiConnector extends LoopbackConnector implements LoopbackConnectorDiscovery, LoopbackConnectorMigration {

  executeSQL(sql: string, parameters: any[], options: object, callback: Function): void {

  }

  executeSQL(sql: string, options: object, callback: Function): void {

  }

  // LoopbackDiscovery implementations
  
  buildQuerySchemas(options: object): string {
    return 'SELECT table_cat AS "catalog", table_schem AS "schema" FROM sysibm.sqlschemas';
  }

  buildQueryTables(options: object): string {
    return 'TODO';
  }
  buildQueryViews(options: object): string {
    return 'TODO';
  }
  buildQueryColumns(schema: string, table: string): string {
    return 'TODO';
  }
  buildQueryPrimaryKeys(schema: string, table: string): string {
    return 'TODO';
  }
  buildQueryForeignKeys(schema: string, table: string): string {
    return 'TODO';
  }
  buildQueryExportedForeignKeys(schema: string, table: string): string {
    return 'TODO';
  }
}