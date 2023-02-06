'use strict';

const pg = require('pg');
const pgFormat = require('pg-format');
const moment = require('moment');

const { Info, Err, Debug } = require('logger');

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
  #connection = null;
  #status = false;
  #isPool = true;

  constructor(pgConfig, client) {
    if (!pgConfig) throw new Error('Empty parameter in constructor');
    this.name = pgConfig?.name;

    if (client) {
      this.#connection = client;
      this.#status = true;
      this.#isPool = false;
    } else {
      this.#connection = new pg.Pool(pgConfig);
      this.#connection.on('error', (err) => Err(`PG: ${this?.name} ` + err));
    }
  }

  async connect() {
    if (!this.#isPool) return;
    try {
      if (this.#status) {
        Info(`PG: ${this.name} attempted second connection`);
        return;
      }
      const { rows: [{ now }] } = await this.#connection.query(`SELECT now()`);
      this.#status = true;
      Info(`PG: ${this.name} connected at ${now}`);
    } catch (err) {
      this.#status = false;
      Err(`PG: ${this?.name} ` + err);
      Info(`PG: ${this?.name} Try to reconnect`);
      await this.connect();
    }
  }

  async withClient(fn) {
    const client = this.#isPool ? await this.#connection.connect() : this.#connection;
    const wrapped = new DB({ name: this.name}, client);
    try {
      const res = await fn(wrapped);
      return res;
    } finally {
      client.release();
    }
  }

  getDBName() {
    return this.#connection.options.database;
  }

  async query(sql, boundary) {
    if (!sql) throw new Error(`PG: ${this?.name} sql can not be empty`);
    const { stack } = new Error();
    await throttle(boundary);
    if (!this.#status) await this.connect();
    try {
      Debug(sql);
      return this.client.query(sql);
    } catch (error) {
      const message = `Failed to execute query:\n${sql}\nMessage:\n${error.message}`;
      Err(`${message}\nErrorStack:\n${stack}`,)
      throw new Error(message);
    }
  }

  async formatQuery(query, param, boundary) {
    if (!query || !param)
      throw new Error(`PG: ${this?.name} query and param can not be empty`);
    const sql = pgFormat.format(query, ...param);
    await throttle(boundary);
    if (!this.#status) await this.connect();
    return this.client.query(sql);
  }
}

module.exports = DB;
