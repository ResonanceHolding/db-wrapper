'use strict';

const pg = require('pg');
const pgFormat = require('pg-format');

const { Info, Err } = require('logger');

class DB {
  constructor(pgConfig) {
    if (!pgConfig) throw new Error('Empty parameter in constructor');
    this.name = pgConfig?.name;

    this.pool = new pg.Pool(pgConfig);
    this.pool.on('error', (err) => Err(err));
  }
  async connect() {
    try {
      await this.pool.connect();
      Info(`PG: ${this.name} connected`);
    } catch (err) {
      Err(err);
      Info('Try to reconnect');
      await this.connect();
    }
  }
  getDBName() {
    return this.pool.options.database;
  }
  query(sql) {
    if (!sql) throw new Error('sql can not be empty');
    return this.pool.query(sql);
  }
  formatQuery(query, param) {
    if (!query || !param) throw new Error('query and param can not be empty');
    const sql = pgFormat.format(query, ...param);
    return this.pool.query(sql);
  }
}

module.exports = DB;
