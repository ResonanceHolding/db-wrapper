# db-wrapper

## Requirements

No additional requirents, except packages in `package.json`

### Constructor arguments

- default postgres config + name field, which is used in logging purposes

### Example

```javascript
const DB = require('./index');

const db = new DB({
  host: 'localhost',
  port: 5432,
  user: 'postgres',
  password: '1234'
});

(async () => {
  await db.connect();
  const { rows } = await db.query(`SELECT * FROM tickers."CurrenciesV3");
})();
```
