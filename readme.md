# DB

DB is a database library written by FindTheBest engineers. The DB library is designed with major emphasis on security, performance, and simplicity. It contains two main modules - DBQuery and SchemaController. DBQuery is a query builder with retrieval functions that mirror the PDO library, while SchemaController is responsible for modifying databases, tables, columns, and indexes.

The grand vision for DB is to potentially implement more drivers than just SQL, such as MongoDB, SphinxQL, or ElasticSearch. This is still speculative though, as the library is in an early state.

MySQL is currently implemented using Drupal's db_query library, but we plan to implement all SQL drivers using PDO at some point, including MySQL. PDO is quickly becoming the far-and-away best way to access SQL from PHP.

## Query Builder Tutorial

### Selecting Data
```PHP
	// Select all rows from a table as associative arrays
	$rows = DB::with('users')->select('*')->fetchAll();
	
	// Select one row from a table as an associative array, or object, or zero-indexed array
	// (zero-indexed arrays can be handy for optimizing performance on massive SELECT queries)
	$row = DB::with('users')->select('*')->where(['name' => 'john'])->fetch();
	$row = DB::with('users')->select('*')->where(['name' => 'john'])->fetch(DB::FETCH_OBJ);
	$row = DB::with('users')->select('*')->where(['name' => 'john'])->fetch(DB::FETCH_NUM);
	
	// Columns within simple functions will be extracted and escaped automatically
	$aggregates = DB::with('users')->select(['MIN(points) min', 'MAX(points) max', 'COUNT(*) count'])->fetch();
	
	// If you have to select crazy shit, you can pass the 2nd parameter $no_escape as true.
	// Be careful, this will allow SQL injection. You have been warned.
	$value = DB::with('users')->select('POWER(SIN((57.7 - latitude) * PI() / 180 / 2), 2)', true)->value();
	
	// There are several ways of applying a WHERE condition, and you can chain them additively.
	$rows = DB::with('users')
		->select(['id', 'name', 'points'])
		->where([['name', 'LIKE', 'd%'], ['id', '>', 1000]])
		->whereNot(['name' => 'david', 'name' => 'devin'])
		->fetchAll();
	
	// Get an array of just the names of three users
	$names = DB::with('users')->select('name')->where(['id' => [44,55,66]])->values();
	
	// Or, get that as an associative array keyed on ID
	$names = DB::with('users')->select(['id', 'name'])->where(['id' => [44,55,66]])->assocValues('id', 'name');
	
	// Use a database cursor to iterate through rows when you're selecting more than fit in memory
	$query = DB::with('users')->select('*')->order(['points' => 'DESC', 'name' => 'ASC']);
	while ($row = $query->fetch()) {
		do_something($row);
	}
	
	// Group by some fields, and fetch the rows as an associative array keys on name
	$rows = DB::with('users')->select(['name', 'company'])->group(['name', 'company'])->fetchAllAssoc('name');
	
	// Now everything together!
	$rows = DB::with('users')
		->select('*')
		->where('name', '!=', 'jenny')
		->group('name')
		->order(['name' => 'ASC'])
		->offset(20) // If we're paginating
		->limit(20)
		->fetchAll();
```

### Inserting Data

```PHP
	// Insert a single user
	$success = DB::with('users')->insert(['name' => 'david', 'company' => 'FindTheBest'])->execute();
	
	// Insert a single user and return the auto-increment ID that was inserted
	// On some databases, this can be performed in a single query, hence the separate function
	$id = DB::with('users')->insertGetID(['name' => 'david', 'company' => 'FindTheBest'])->execute();
	
	// Insert multiple users using associative arrays. This is a batched query for performance.
	$success = DB::with('users')->insertMultiAssoc([
		['name' => 'david', 'company' => 'FindTheBest'],
		['name' => 'dylan', 'company' => 'FindTheBest'],
	]);
	
	// Insert multiple users using zero-indexed arrays. This is a batched query,
	// and can be much faster than insertMultiAssoc when inserting many rows.
	$success = DB::with('users')->insertMulti(['name', 'company'], [
		['david', 'FindTheBest'],
		['dylan', 'FindTheBest'],
	]);
```

### Updating Data

```PHP
	// Update some rows
	$success = DB::with('users')->update(['points' => 0])->where(['name' => 'bob'])->execute();
```

to be continued...
