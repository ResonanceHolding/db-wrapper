'use strict';

const pg = require('pg');
const pgFormat = require('pg-format');
const moment = require('moment');

const { Info, Err } = require('logger');

async function throttle(boundary) {
  return new Promise((resolve) => {
    if (!boundary) resolve();
    if (boundary >= 30 || boundary < 1)
      throw new Error(
        'Boundaries must give access time between 1 minute candle: 0 < boundary <= 30'
      );
    const leftBorder = boundary;
    const rightBorder = 60 - boundary;
    const now = moment().utc();
    const seconds = now.seconds();
    let timeout;
    if (seconds >= leftBorder && seconds < rightBorder) {
      timeout = 0;
    } else if (seconds < leftBorder) {
      timeout = (leftBorder - seconds) * 1000;
    } else {
      timeout = (boundary - seconds + rightBorder + leftBorder) * 1000;
    }
    setTimeout(resolve, timeout);
  });
}

class DB {
  #pool = null;
  #status = false;
  constructor(pgConfig) {
    if (!pgConfig) throw new Error('Empty parameter in constructor');
    this.name = pgConfig?.name;

    this.client = null;
    this.#pool = new pg.Pool(pgConfig);
    this.#pool.on('error', (err) => Err(err));
  }

  async connect() {
    try {
      if (!this.#status) this.client = await this.#pool.connect();
      this.#status = true;
      Info(`PG: ${this.name} connected`);
    } catch (err) {
      this.#status = false;
      Err(err);
      Info('Try to reconnect');
      await this.connect();
    }
  }

  getDBName() {
    return this.#pool.options.database;
  }

  async query(sql, boundary) {
    if (!sql) throw new Error('sql can not be empty');
    await throttle(boundary);
    if (!this.#status) await this.connect();
    return this.client.query(sql);
  }

  async formatQuery(query, param, boundary) {
    if (!query || !param) throw new Error('query and param can not be empty');
    const sql = pgFormat.format(query, ...param);
    await throttle(boundary);
    if (!this.#status) await this.connect();
    return this.client.query(sql);
  }
}

module.exports = DB;
