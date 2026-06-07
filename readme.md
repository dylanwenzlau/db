# DB

DB is a modern PHP database library designed for security, performance, and simplicity.

SQLQuery is the query builder, somewhat mirroring the PDO library, but with many convenience functions.

SQLSchema is responsible for modifying databases, tables, columns, and indexes.

DB is somewhat engine-agnostic and could easily be extended to work with standards-forward SQL databases, but for now
supports primarily MySQL.

## Installation
### Using Composer
```json
{
	"repositories": [
		{
			"type": "vcs",
			"url": "git@github.com:dylanwenzlau/db.git"
		}
	],
	"require": {
		"dylanwenzlau/db": "dev-master"
	}
}
```
and run
```bash
composer install dylanwenzlau/db
```

### Manually
```bash
cd /to/your/project
git clone https://github.com/dylanwenzlau/db
```

### Updating
When changes are made to the DB library, it will be important to update your code to the latest stable release.
In order to do this you will do the following:

To update using `composer`:
```bash
cd /path/to/your/project/composer.json
composer update dylanwenzlau/db
```

To update manually:
```bash
cd /to/your/project/db
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
$value = DB::with('geo')->select('POWER(SIN((57.7 - latitude) * PI() / 180 / 2), 2)', true)->value();

// There are several ways of applying a WHERE condition, and you can chain them additively.
$rows = DB::with('users')
	->select(['id', 'name', 'points'])
	->where('name', '!=', 'bob')
	->where('id', 'BETWEEN', [1, 1000])
	->where([['name', 'LIKE', 'd%'], ['date', '<', DB::raw('NOW() - INTERVAL 1 HOUR')]])
	->whereNot(['name' => 'david', 'name' => 'devin'])
	->fetchAll();

// Apply a multi-column WHERE IN of the form WHERE (a, b) IN ((1, 2), (3, 4))
// NOTE: this type of query cannot utilize indexes until MySQL 5.7, so avoid it if you're not on 5.7+
$rows = DB::with('users')
    ->select('*')
    ->whereIn(['first', 'last'], [['jane', 'doe'], ['john', 'smith']])
    ->fetchAll();

// Apply where conditions joined by OR. Avoid using this when possible to ensure optimal performance.
// It can be used as a more performant alternative (as of MySQL 5.6) to WHERE (a, b) IN ((1, 2), (3, 4)) syntax
$rows = DB::with('users')
	->select('*')
	->whereOr([['first' => 'jane', 'last' => 'doe'], ['first' => 'john', 'last' => 'smith']])
	->fetchAll();

// Get an array of just the names of three users (e.g. ['Herp', 'Derp', 'Derpina'])
$names = DB::with('users')->select('name')->where(['id' => [44,55,66]])->values();

// Or, get that as an associative array keyed on ID
$names = DB::with('users')->select(['id', 'name'])->where(['id' => [44,55,66]])->assocValues();

// Use a database cursor to iterate through rows when you're selecting more than fit in memory
$query = DB::with('users')->select('*')->order(['points' => 'DESC', 'name' => 'ASC']);
while ($row = $query->fetch()) {
	do_something($row);
}

// Group by company, and fetch the rows as an associative array keyed on company
$rows = DB::with('users')
	->select(['company', 'COUNT(*) count'])
	->group(['company'])
	->order(['count' => 'DESC'])
	->fetchAllAssoc('company');
	
// Fetch rows into nested associative arrays
// example output: ['Smith' => ['John' => [...], 'Jane' => [...]]]
$rows = DB::with('users')->select('*')->fetchAllAssoc(['last_name', 'first_name']);

// Offset and Limit
$paginated_rows = DB::with('users')->select('*')->order(['id' => 'ASC'])->offset(40)->limit(20)->fetchAll();

// Raw Queries with manual escaping
$db = DB::with('');
$column = $db->quoteKeyword($column);
$value = $db->quote($value);
$db->query("UPDATE table SET $column = RAND() * $value");

// Raw queries using "?" placeholders (PDO syntax)
DB::query("UPDATE table SET column = RAND() * ?", [$value]);
$rows = DB::query("SELECT * FROM table WHERE column = ?", [$value])->fetchAll();
```

### Inserting & Updating Data

```PHP
// Insert a single user
$success = DB::with('users')->insert(['first_name' => 'John', 'last_name' => 'Smith']);

// Insert a single user and return the auto-increment ID that was inserted
$id = DB::with('users')->insertGetID(['first_name' => 'John', 'last_name' => 'Smith']);

// Insert multiple users using associative arrays. This is a batched query for performance.
$success = DB::with('users')->insertMultiAssoc([
	['first_name' => 'John', 'last_name' => 'Smith'],
	['first_name' => 'Jane', 'last_name' => 'Smith'],
]);

// Insert multiple users using zero-indexed arrays. This is a batched query,
// and will be faster and more memory efficient than insertMultiAssoc when inserting many rows.
$success = DB::with('users')->insertMulti(['first_name', 'last_name'], [
	['John', 'Smith'],
	['Jane', 'Smith'],
]);

// Upsert a row, but don't modify the id and name fields on duplicate key
$success = DB::with('users')->upsert(['id' => 1, 'name' => 'bob', 'points' => 10], ['id', 'name']);

// upsertMulti and upsertMultiAssoc also exist, mirroring insertMulti and insertMultiAssoc
// NOTE: upsert should generally not be used with databases like MySQL that burn an auto-increment ID on every upsert,
// particularly with bulk upsert, since it wastes ID space and can create large gaps and exhaust your integer size

// Update some rows
$success = DB::with('users')->where(['name' => 'bob'])->update(['points' => 0]);

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
$success = DB::with('users')->where(['name' => 'bob'])->delete();

// Delete the same rows less verbosely
$success = DB::with('users')->delete(['name' => 'bob']);
```

## Schema Tutorial

to be continued...
