## Synopsis

MySQL DB Module created by Festival For You to simplify query creation

## Dependencies

This module runs on top of the [mysql](https://www.npmjs.com/package/mysql) npm package created by Douglas Wilson.

## Code Example

```
var db = require('ffy-db');
var options = {
  host: '127.0.0.1',
  user: 'root',
  password: '',
  database: 'database'
}

db.setup(options);
```

## Disclaimer

This module is provided as is, and will be maintained/modified at our discretion.  Anyone is welcome to download and amend this modile to suit their requirements, the module is provided under [GPL](https://www.gnu.org/licenses/gpl-3.0.en.html).

## Installation

```
npm install ffy-db
```

## API Reference

*Setup*
*Select*
*Insert*
*Update*
*Query*