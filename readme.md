# DB

DB is a PHP database library written by FindTheBest engineers. The DB library is designed with major emphasis on security, performance, and simplicity. It contains two main modules - DBQuery and SchemaController. DBQuery is a query builder with retrieval functions that mirror the PDO library, while SchemaController is responsible for modifying databases, tables, columns, and indexes.

DB currently has support for MySQL and PostgreSQL, based on PDO. The grand vision for DB is to potentially implement more drivers than just SQL, such as MongoDB, SphinxQL, or ElasticSearch. This is still speculative though, as the library is in an early state.

## Installation
### Using Composer
```json
{
	"repositories": [
		{
			"type": "vcs",
			"url": "git@github.com:FindTheBest/DB.git"
		}
	],
	"require": {
		"FindTheBest/DB": "dev-master"
	}
}
```
and run
```bash
composer install FindTheBest/DB
```

### Manually
```bash
cd /to/your/project
git clone https://github.com/FindTheBest/DB
```

### Updating
When changes are made to the DB library, it will be important to update your code to the latest stable release. In order to do this
you will do the following:

To update using `composer`:
```bash
cd /path/to/your/project/composer.json
composer update FindTheBest/DB
```

To update manually:
```bash
cd /to/your/project/DB
git pull
```

## Query Builder Tutorial

### Selecting Data

```PHP
// Select all rows from a table as associative arrays
$rows = DB::with('users')->select('*')->fetchAll();

// Select one row from a table as an associative array, or object, or zero-indexed array
// (zero-indexed arrays can be handy for optimizing PHP memory/performance on massive SELECT queries)
$row = DB::with('users')->select('*')->where(['name' => 'john'])->fetch();
$row = DB::with('users')->select('*')->where(['name' => 'john'])->fetch(DB::FETCH_OBJ);
$row = DB::with('users')->select('*')->where(['name' => 'john'])->fetch(DB::FETCH_NUM);

// Simple SQL functions will be extracted and escaped automatically
$aggregates = DB::with('users')->select(['MIN(points)', 'MAX(points) max', 'COUNT(*) AS count'])->fetch();

// If you have to select crazy shit, you can pass the 2nd parameter $no_escape as true.
// Be careful, this will allow SQL injection. You have been warned.
$value = DB::with('cool_geo_data')->select('POWER(SIN((57.7 - latitude) * PI() / 180 / 2), 2)', true)->value();

// There are several ways of applying a WHERE condition, and you can chain them additively.
$rows = DB::with('users')
	->select(['id', 'name', 'points'])
	->where('name', '!=', 'bob')
	->where([['name', 'LIKE', 'd%'], ['id', '>', 1000]])
	->whereNot(['name' => 'david', 'name' => 'devin'])
	->fetchAll();

// Get an array of just the names of three users (e.g. ['Herp', 'Derp', 'Derpina'])
$names = DB::with('users')->select('name')->where(['id' => [44,55,66]])->values();

// Or, get that as an associative array keyed on ID
$names = DB::with('users')->select(['id', 'name'])->where(['id' => [44,55,66]])->assocValues('id', 'name');

// Use a database cursor to iterate through rows when you're selecting more than fit in memory
$query = DB::with('users')->select('*')->order(['points' => 'DESC', 'name' => 'ASC']);
while ($row = $query->fetch()) {
	do_something($row);
}

// Group by some fields, and fetch the rows as an associative array keyed on name
$rows = DB::with('users')
	->select(['name', 'company', 'COUNT(*) count'])
	->group(['name', 'company'])
	->order(['count' => 'DESC'])
	->fetchAllAssoc('name');

// Offset and Limit
$paginated_rows = DB::with('users')->select('*')->order(['id' => 'ASC'])->offset(40)->limit(20);

// Raw Queries with manual escaping
$db = DB::with('');
$column = $db->quoteKeyword($column);
$value = $db->quote($value);
$db->query("UPDATE table SET $column = RAND() * $value");
$rows = $db->fetchAll();

// Raw queries using "?" placeholders (coming soon, as seen in PDO library)
```

### Inserting & Updating Data

```PHP
// Insert a single user
$success = DB::with('users')->insert(['name' => 'david', 'company' => 'FindTheBest']);

// Insert a single user and return the auto-increment ID that was inserted
// In MySQL this takes 2 queries, but PostgreSQL just takes 1
$id = DB::with('users')->insertGetID(['name' => 'david', 'company' => 'FindTheBest']);

// Insert multiple users using associative arrays. This is a batched query for performance.
$success = DB::with('users')->insertMultiAssoc([
	['name' => 'david', 'company' => 'FindTheBest'],
	['name' => 'dylan', 'company' => 'FindTheBest'],
]);

// Insert multiple users using zero-indexed arrays. This is a batched query,
// and will be faster and more memory efficient than insertMultiAssoc when inserting many rows.
$success = DB::with('users')->insertMulti(['name', 'company'], [
	['david', 'FindTheBest'],
	['dylan', 'FindTheBest'],
]);

// Upsert a row, but don't modify the id and name fields on duplicate key
$success = DB::with('users')->upsert(['id' => 1, 'name' => 'bob', 'points' => 10], ['id', 'name']);

// upsertMulti and upsertMultiAssoc also exist, mirroring insertMulti and insertMultiAssoc

// Update some rows
$success = DB::with('users')->update(['points' => 0])->where(['name' => 'bob'])->execute();

// Update a single column for many rows, using a batched query for performance.
// This can be used to greatly improve multi-update performance.
$success = DB::with('users')->updateColumn('id', 'name', [
	22 => 'Gerald', // changes the name of user with ID 22 to Gerald
	69 => 'Murph',
	...
]);
```

### Deleting Data

```PHP
// Delete some rows
$success = DB::with('users')->delete()->where(['name' => 'bob'])->execute();

// Delete the same rows less verbosely
$success = DB::with('users')->delete(['name' => 'bob'])->execute();
```

## Schema Controller Tutorial

to be continued...
