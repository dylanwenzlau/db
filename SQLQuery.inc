<?php
/**
 * @author Dylan Wenzlau <dylan@findthebest.com>
 * @author Skyler Lipthay <slipthay@findthebest.com>
 */

require_once MODULE_PATH . '/ormish/db_value.inc';
require_once MODULE_PATH . '/main/main.module';
require_once MODULE_PATH . '/common/common.module';

/**
 * Builds a valid SQL query with value escapes.
 *
 * Example #1
 *
 *   SQLQuery::with('table_name')->
 *     select(['one', 'two'])->
 *     where(['three' => 123, 'four' => ['a', 'b', 'c']])->
 *     offset('5')->
 *     limit('10')->
 *     order(['five' => 'DESC', 'six' => 'ASC'])->
 *     execute();
 *
 *   // Executes the following query:
 *   // SELECT `one`, `two` FROM `{table_name}`
 *   // WHERE `three`=123 AND `four` IN('a', 'b', 'c')
 *   // ORDER `five` DESC, `six` ASC LIMIT 10 OFFSET 5
 *
 * Example #2
 *
 *   $sql = SQLQuery::with('table_name')->insert(['one' => 1, 'two' => 't']);
 *   $sql->to_string();
 *   // => "INSERT INTO `table_name`(`one`, `two`) VALUES(%d, '%s')"
 *   $sql->bind_values();
 *   // => [1, 't']
 */
class SQLQuery {

	const ENGINE_MYSQL = 'mysql';
	const ENGINE_POSTGRES = 'postgresql';

	private static $VALID_OPERATORS = ['=', '!=', '>', '>=', '<', '<=', 'LIKE'];
	private static $last_insert_id = null;

	private static $debug = false;
	private static $queries_executed = [];

	private $db;
	private $engine;
	private $data;
	private $group;
	private $having;
	private $limit;
	private $offset;
	// By design, a SQLQuery instance will default to SELECT *.
	private $operation = 'SELECT';
	// All operations are allowed by default
	private $allowed_operations = [];
	private $order;
	private $query_args;
	private $select = '*';
	private $table;
	private $table_escaped;
	private $update;
	private $where;
	private $where_values = [];
	private $delayed = false;
	private $tick;
	private $result;
	private $pdo;
	private $return_id = false;

	/**
	 * Creates a new SQLQuery instance set with a specified table name.
	 *
	 * @param string $table The table name.
	 * @param string $db
	 * @param array $allowed_ops
	 * @return SQLQuery A new instance.
	 */
	public function __construct($table, $db = '', array $allowed_ops = []) {
		$this->db = $db;
		$this->engine = substr($this->db, 0, 3) === 'pg_' ? self::ENGINE_POSTGRES : self::ENGINE_MYSQL;
		$this->tick = $this->engine === 'mysql' ? '`' : '"';
		$this->table = $table;
		$this->table_escaped = $this->tick . str_replace('.', "$this->tick.$this->tick", $table) . $this->tick;
		$this->allowed_operations = $allowed_ops;
		if ($this->engine === self::ENGINE_POSTGRES) {
			$this->pdo = get_pdo($this->db);
		}
	}

	public function __toString() {
		return $this->to_string();
	}

	public function as_array() {
		return $this->fetchArray();
	}

	public function as_arrays() {
		if (!isset($this->result)) {
			$this->execute();
		}
		switch ($this->engine) {
			case self::ENGINE_MYSQL:
				return db_fetch_arrays($this->result);

			case self::ENGINE_POSTGRES:
				return $this->result ? $this->result->fetchAll(PDO::FETCH_ASSOC) : [];
		}
	}

	public function as_assoc_arrays($key) {
		$rows = [];
		while ($row = $this->fetchArray()) {
			$rows[$row[$key]] = $row;
		}
		return $rows;
	}

	/**
	 * @deprecated
	 */
	public function as_object() {
		return $this->fetchObject();
	}

	/**
	 * @deprecated
	 */
	public function as_objects() {
		$rows = [];
		while ($row = $this->fetchObject()) {
			$rows[] = $row;
		}
		return $rows;
	}

	public function values() {
		if (!isset($this->result)) {
			$this->execute();
		}
		switch ($this->engine) {
			case self::ENGINE_MYSQL:
				return db_fetch_results($this->result);

			case self::ENGINE_POSTGRES:
				return $this->result->fetchAll(PDO::FETCH_COLUMN);
		}
	}

	public function assocValues($key_column, $value_column) {
		if (!isset($this->result)) {
			$this->execute();
		}
		$values = [];
		while ($row = $this->fetchArray()) {
			$values[$row[$key_column]] = $row[$value_column];
		}
		return $values;
	}

	public function value() {
		$row = $this->fetchArray();
		return $row ? reset($row) : false;
	}

	public function set_operation($operation) {
		if ($this->allowed_operations && !in_array($operation, $this->allowed_operations)) {
			throw new Exception("$operation is not allowed");
		}
		$this->operation = $operation;
	}

	/**
	 * @see SQLQuery::to_string()
	 *
	 * @return array The values to be passed to a database querying function,
	 *   indexed in exact order to the corresponding placeholders in the string
	 *   representation of this query.
	 */
	public function bind_values() {
		$values = [];

		if (!empty($this->query_args)) {
			$values = $this->query_args;
		}

		if (!empty($this->where_values)) {
			$values = array_merge($values, $this->where_values);
		}

		return array_flatten($values);
	}

	/**
	 * @see SQLQuery::where()
	 *
	 * @param mixed ... Parameters that are passed directly to
	 *   apply_where_conditions().
	 * @return SQLQuery $this for chaining.
	 */
	public function delete(/* ... */) {
		$this->set_operation('DELETE');
		$this->apply_where_conditions(func_get_args());
		return $this;
	}

	/**
	 * Executes the built query
	 *
	 * @return bool True on success, false on failure
	 */
	public function execute() {
		$string = $this->to_string();
		$values = $this->bind_values();
		return $this->db_query($string, $values);
	}

	/**
	 * Executes a batch update based on the values of the key column.
	 *
	 *   // Sets `b` to 2 where `a` is 1, and `b` to 3 where `a` is 10:
	 *   SQLQuery::with('table')->execute_column_update('a', 'b', [
	 *     1 => 2,
	 *     10 => 3
	 *   ]);
	 *
	 * @see SQLQuery::execute()
	 *
	 * @param string $key_column The name of the column to base the CASE clause
	 *   off of.
	 * @param string|array $value_column The name of the column of which to set
	 *   the value. If an array is passed, multiple value columns will be set
	 *   based on the key column instead of just one.
	 * @param array $data Associative array mapping values of $key_column to
	 *   values of $value_column. If $value_column is an array, this argument is
	 *   actually an associative array mapping value column names to associative
	 *   arrays that map values of $key_column to the particular value column.
	 * @return bool True on success, false on failure
	 */
	public function execute_column_update($key_column, $value_column, $data) {
		if (is_array($value_column)) {
			foreach ($value_column as $column) {
				$this->execute_column_update($key_column, $column, $data[$column]);
			}

			return;
		}

		if (count($data) > 1000) {
			$callback = function($chunk) use ($key_column, $value_column) {
				$this->execute_column_update($key_column, $value_column, $chunk);
			};

			return self::chunk_query($data, $callback, 1000);
		} else if (empty($data)) {
			return true;
		}

		$key_column_escaped = $this->quoteKeyword($key_column);
		$value_column_escaped = $this->quoteKeyword($value_column);
		$sql = "UPDATE {$this->table_escaped} SET {$value_column_escaped} = CASE {$key_column_escaped}";

		$keys = [];
		$arguments = [];
		foreach ($data as $key => $value) {
			$keys[] = $key;
			$key_string = $this->sql_value_and_add_arguments($key, $arguments);
			$value_string = $this->sql_value_and_add_arguments($value, $arguments);
			$sql .= " WHEN {$key_string} THEN {$value_string}";
		}

		$in = $this->sql_condition_in($key_column, '=', $keys, $arguments);
		$sql .= " END WHERE {$in}";

		return $this->db_query($sql, $arguments);
	}

	/**
	 * Copy all the data in one column to another column, overwriting the destination column
	 * @param string $from_column
	 * @param string $to_column
	 * @return bool
	 */
	public function copyColumnData($from_column, $to_column) {
		$from_column = $this->quoteKeyword($from_column);
		$to_column = $this->quoteKeyword($to_column);
		return $this->db_query("UPDATE $this->table_escaped SET $to_column = $from_column");
	}

	/**
	 * Executes a batch insert to the selected table based on data provided in an
	 * array of associative arrays.
	 *
	 *   // Inserts rows into a table specifying `name` and `value`.
	 *   SQLQuery::with('table')->insert_multi_assoc([
	 *     ['name' => '_111111', 'value' => 'abc'],
	 *     ['name' => '_222222', 'value' => 'def'],
	 *     ['name' => '_333333', 'value' => 'ghi']
	 *   ]);
	 *
	 * @see SQLQuery::execute()
	 *
	 * @param array $data A list of associative arrays mapping column names to
	 *   corresponding values.
	 * @return A database query result resource, false if the query was not
	 *   executed correctly, or true if $data was empty and there was nothing to
	 *   be done.
	 */
	public function insert_multi_assoc($data) {
		$count = count($data);
		if ($count > 10000) {
			return self::chunk_query($data, [$this, 'insert_multi_assoc'], 10000);
		} else if ($count === 0) {
			return true;
		}

		$keys = array_keys(current($data));
		$keys_string = implode(',', $keys);

		$arguments = [];
		$sql = "INSERT INTO {$this->table_escaped} ({$keys_string}) VALUES";

		foreach ($data as $object) {
			$count--;

			$values = [];
			foreach ($keys as $key) {
				$values[] = $object[$key];
			}

			$list = $this->sql_condition_list($values, $arguments);

			$sql .= "({$list})";

			if ($count === 0) {
				break;
			}

			$sql .= ',';
		}

		return $this->db_query($sql, $arguments);
	}

	public function insert_multi($column_names, $rows) {
		$num_columns = count($column_names);
		$num_rows = count($rows);
		if (!$num_columns || !$num_rows || count($rows[0]) !== $num_columns) {
			throw new Exception("Invalid parameters");
		}
		foreach ($column_names as $key => $column) {
			$column_names[$key] = $this->quoteKeyword($column);
		}
		$column_str = "(" . implode(",", $column_names) . ")";
		$query = "INSERT INTO {$this->table_escaped} $column_str VALUES ";
		for ($i = 0; $i < $num_rows; $i++) {
			$value_str = "";
			for ($j = 0; $j < $num_columns; $j++) {
				$value_str .= ($j ? "," : "");
				if ($rows[$i][$j] === null) {
					$value_str .= 'NULL';
				} else {
					$value_str .= $this->quote($rows[$i][$j]);
				}
			}
			$query .= ($i ? "," : "") . "($value_str)";
		}

		return $this->db_query($query);
	}

	/**
	 * Specifies a GROUP BY clause for the query.
	 *
	 *   // Adds GROUP BY `column_one`, `column_two` to the query.
	 *   $sql_query->group('`column_one`, `column_two`');
	 *
	 * @param string $group The GROUP BY clause.
	 * @return SQLQuery $this for chaining.
	 */
	public function group($group) {
		if (is_array($group)) {
			$this->group = '';
			foreach ($group as $db_name) {
				$this->group .= ($this->group ? ',' : '') . $this->quoteKeyword($db_name);
			}
		} else {
			$this->group = $group;
		}
		return $this;
	}

	/**
	 * Specifies a HAVING clause for the query.
	 *
	 *   // Adds HAVING COUNT(*) > 12 to the query.
	 *   $sql_query->having('COUNT(*) > 12');
	 *
	 * @param string $having The HAVING clause.
	 * @return SQLQuery $this for chaining.
	 */
	public function having($having) {
		$this->having = $having;
		return $this;
	}

	/**
	 * Sets the query mode to INSERT INTO and attaches values to insert.
	 *
	 *   // Starts the query with INSERT INTO table(`one`, `two`) VALUES(1, 't').
	 *   $sql_query->insert(['one' => 1, 'two' => 't']);
	 *
	 * @param array $data An associative array with keys as column names and
	 *   values as column values.
	 * @return SQLQuery $this for chaining.
	 * @throws Exception
	 */
	public function insert(array $data = []) {
		if (!is_hash($data) && $data !== []) {
			throw new Exception('Inserting requires a hash');
		}

		$this->set_operation('INSERT');
		$this->data = $data;
		return $this;
	}

	/**
	 * Like insert(), except it will return the "id" of the row inserted.
	 * This will require one extra query for MySQL, but no extra overhead for Postgres.
	 *
	 * @see SQLQuery::insert
	 * @param array $data
	 * @return SQLQuery $this for chaining.
	 */
	public function insertGetID(array $data = []) {
		$this->insert($data);
		$this->return_id = true;
		return $this;
	}

	public function delayed() {
		$this->delayed = true;
		return $this;
	}

	/**
	 * Specifies a LIMIT clause for the query.
	 *
	 *   // Adds LIMIT 5 to the query.
	 *   $sql_query->limit(5);
	 *
	 * @param int $limit The LIMIT row count.
	 * @return SQLQuery $this for chaining.
	 */
	public function limit($limit) {
		$this->limit = intval($limit);
		return $this;
	}

	/**
	 * Specifies an offset for the LIMIT clause for the query.
	 *
	 *   // Adds LIMIT 5, 10 to the query.
	 *   $sql_query->limit(10)->offset(5);
	 *
	 * @param int $offset The LIMIT offset.
	 * @return SQLQuery $this for chaining.
	 */
	public function offset($offset) {
		$this->offset = intval($offset);
		return $this;
	}

	/**
	 * Specifies an ORDER BY clause for the query.
	 *
	 *   // Adds ORDER BY `column_one` ASC, `column_two` DESC to the query.
	 *   $sql_query->order(['column_one' => 'ASC', 'column_two' => 'DESC']);
	 *
	 * @param string|array $order The ORDER BY clause.
	 * @param bool $half_escape When true, will not use backticks or escaping on the field name
	 * @return SQLQuery $this for chaining.
	 * @throws Exception
	 */
	public function order(array $order, $half_escape = false) {
		$this->order = '';

		foreach ($order as $column => $direction) {
			if ($direction !== 'ASC' && $direction !== 'DESC') {
				throw new Exception('Invalid sort direction');
			}
			if (!$half_escape) {
				$column = $this->quoteKeyword($column);
			}
			$this->order .= ($this->order ? ', ' : '') . "{$column} {$direction}";
		}

		return $this;
	}

	/**
	 * Order by particular values in a given field.
	 *
	 *   // Adds ORDER BY FIELD(`column_one`, 3, 5, 7) to the query.
	 *   $sql_query->order_by_field('column_one', [3, 5, 7]);
	 *
	 * @param string $field A field on the table whose values will be used to sort
	 * @param array $values An array of values to sort on
	 * @return $this
	 */
	public function order_by_values($field, array $values) {
		if (count($values) === 1) {
			return $this;
		}
		$field = $this->quoteKeyword($field);
		foreach ($values as $key => $value) {
			$values[$key] = $this->quote($value);
		}
		switch ($this->engine) {
			case self::ENGINE_POSTGRES:
				foreach ($values as $value) {
					$this->order .= ($this->order ? ',' : '') . "$field=$value DESC";
				}
				break;
			case self::ENGINE_MYSQL:
				$values = implode(',', $values);
				$this->order .= "FIELD({$field}, {$values})";
				break;
		}
		return $this;
	}

	/**
	 * Sets the query mode to SELECT FROM and specifies the columns to fetch.
	 *
	 *   // Starts the query with SELECT `column_one`, `column_two` FROM...
	 *   $sql_query->select(['column_one', 'column_two']);
	 *
	 *   // Starts the query with SELECT COUNT(*) AS `count`, `column_one` FROM...
	 *   $sql_query->select('COUNT(*) AS `count`, `column_one`');
	 *
	 * @param array|string $select An array of columns to select, or a raw string
	 *   of SQL to be used as the SELECT clause.
	 * @return SQLQuery $this for chaining.
	 *
	 * TODO: add SQL injection protection here
	 */
	public function select($select = '*') {
		$this->set_operation('SELECT');
		$this->select = is_array($select) ? implode(',', $select) : $select;
		return $this;
	}

	/**
	 * Converts the current SQL query into a string to be passed into a database
	 * querying function. Note that this function returns a string with %-escaped
	 * value placeholders.
	 *
	 * @see SQLQuery::bind_values()
	 *
	 * @return string SQL string with value placeholders.
	 */
	public function to_string() {
		$func = 'build_' . strtolower($this->operation);
		return $this->$func();
	}

	/**
	 * Sets the query mode to UPDATE and attaches values to insert.
	 *
	 *   // Starts the query with UPDATE table SET `one`=1, `two`='t'.
	 *   $sql_query->update(['one' => 1, 'two' => 't']);
	 *
	 *   // Starts the query with UPDATE table SET `one`=`one` + 1.
	 *   $sql_query->update('`one`=`one` + 1');
	 *
	 * @param array|string $data An associative array with keys as column names
	 *   and values as column values, or a raw SQL string to be used as the SET
	 *   clause.
	 * @return SQLQuery $this for chaining.
	 */
	public function update($mixed) {
		$this->set_operation('UPDATE');

		if (is_hash($mixed)) {
			$this->data = $mixed;
		} else if (is_string($mixed)) {
			$this->update = $mixed;
		} else {
			throw new Exception('Updating requires an array or string');
		}

		return $this;
	}

	/**
	 * Specifies WHERE conditions for the query.
	 *
	 *   // Adds WHERE `three`=NOW() AND `four` IN('a', 'b') to the query.
	 *   $sql_query->where(['three' => new DBValueNow(), 'four' => ['a', 'b']]);
	 *
	 *   // Adds WHERE `field2` < 20 to the query
	 *   $sql_query->where('field2', '<', 20);
	 *
	 *   // Adds WHERE `field3` > 10 AND `field4` != 'bar'
	 *   $sql_query->where([['field3', '>', 10], ['field4', '!=', 'bar']]);
	 *
	 *   // Adds WHERE `three` < `four` OR `five`=5 to the query.
	 *   $sql_query->where('`three` < `four` OR `five`=%d', 5);
	 *
	 * @see class DBValue if interested in using SQL constructs as hash values
	 *
	 * @param mixed ... see examples above
	 * @return SQLQuery $this for chaining.
	 */
	public function where(/* (conditions, values) || (hash[, extra]) */) {
		$this->apply_where_conditions(func_get_args());
		return $this;
	}

	public function whereNot(/* (conditions, values) || (hash[, extra]) */) {
		$this->apply_where_conditions(func_get_args(), $negate = true);
		return $this;
	}

	/**
	 * Converts a string like "id_and_name_or_z" into a conditions value like
	 * ["id=? AND name=? OR z=?", values, ...].
	 *
	 * @param string $name Underscored string.
	 * @param array $values Array of values for the field names. This is used
	 *   to determine what kind of bind marker to use: =%d, IN(%s, %d), IS NULL.
	 * @return array Conditions of the form [sql_string, value_1, value_2, ...].
	 */
	public function conditions_from_string($name, $values = []) {
		if (!is_string($name)) {
			return null;
		}

		$split_flags = PREG_SPLIT_DELIM_CAPTURE;
		$parts = preg_split('/(_and_|_or_)/i', $name, -1, $split_flags);
		$parts = self::group_condition_parts($parts);
		if (empty($parts)) {
			return null;
		}

		$fields = $parts['fields'];
		$operators = $parts['operators'];

		if (count($operators) !== count($fields) - 1) {
			return null;
		}

		if (count($fields) !== count($values)) {
			return null;
		}

		$arguments = [];
		$conditions = '';

		while (!empty($fields)) {
			$field_pair = array_shift($fields);
			$field = $field_pair[0];
			$value = array_shift($values);

			$condition = $this->sql_condition($field, '=', $value, $arguments);
			if ($condition === null) {
				return null;
			}

			$conditions .= $condition;

			$operator_pair = array_shift($operators);
			$operator = $operator_pair[0];
			if ($operator !== null) {
				$conditions .= $operator;
			}
		}

		return array_merge([$conditions], $arguments);
	}

	/**
	 * Creates a new SQLQuery instance from a table name and an array of options.
	 * Really just a shortcut function for building a query instead of chaining
	 * method calls.
	 *
	 * @see SQLQuery::parse_option() to reference what options are valid and how
	 *   each one is used to build the query.
	 * @see SQLQuery::build_delete(), SQLQuery::build_insert(),
	 *   SQLQuery::build_select(), and SQLQuery::build_update() to see how each
	 *   option is used for a particular query type.
	 *
	 * @param array $options An associative array of SQL query options.
	 * @param string $table The table name.
	 * @return SQLQuery A new instance.
	 */
	public static function from_options($options, $table) {
		$sql = new self($table);

		foreach ($options as $key => $value) {
			self::parse_option($sql, $key, $value);
		}

		return $sql;
	}

	/**
	 * Creates a new SQLQuery instance set with a specified table name.
	 *
	 * @param string $table The table name.
	 * @param string $db
	 * @param array $allowed_operations
	 * @return SQLQuery A new instance.
	 */
	public static function with($table, $db = '', array $allowed_operations = []) {
		return new self($table, $db, $allowed_operations);
	}

	public static function getRegexpOperator($engine) {
		switch ($engine) {
			case self::ENGINE_POSTGRES:
				return '~';
			case self::ENGINE_MYSQL:
				return 'REGEXP';
		}
	}

	private function apply_where_conditions($args, $negate = false) {
		// Ignore empty WHEREs
		if (empty($args) || (count($args) === 1 && !$args[0])) {
			return;
		}

		$is_oper_syntax = count($args) === 3 && in_array($args[1], static::$VALID_OPERATORS);

		// e.g. where("`%s` >= '%d' OR `field` IS NULL", 'field1', 125)
		// We have to override any existing WHERE in this case, because the string could be anything
		if (is_string($args[0]) && !$is_oper_syntax) {
			$this->where = $args[0];
			$this->where_values = array_slice($args, 1);
			return;
		}

		// e.g. where('field', '>=', 99)
		// If it's this syntax, just put it in an array to be dealt with by multiple syntax
		if ($is_oper_syntax) {
			$args = [[$args[0], $args[1], $args[2]]];
		}

		if (!is_array($args[0])) {
			return;
		}

		// arg 0 is an array, arg 1 is an optional additional raw SQL WHERE string
		$extra = null;
		if (count($args) >= 2 && is_string($args[1])) {
			$extra = $args[1];
		}

		$sql = [];
		$array = $args[0];

		// e.g. where(['id' => 10], '`field` IS NULL')
		if (is_hash($array)) {
			foreach ($array as $field => $value) {
				$sql[] = $this->sql_condition($field, $negate ? '!=' : '=', $value, $this->where_values);
			}

		// e.g. where([['id', '>', 100], ['id', '<', 200]], '`field` IS NULL')
		} else {
			// If it's just a single array, convert it to an array of arrays
			if (!is_array($array[0])) {
				$array = [$array];
			}
			foreach ($array as $condition) {
				$sql[] = $this->sql_condition($condition[0], $condition[1], $condition[2], $this->where_values);
			}
		}

		$this->where .= ($this->where ? ' AND ' : '') . implode(' AND ', $sql);
		if (is_string($extra)) {
			$this->where .= ($this->where ? ' AND ' : '') . $extra;
		}
	}

	private function build_delete() {
		$sql = "DELETE FROM {$this->table_escaped}";

		if ($this->where) {
			$sql .= " WHERE {$this->where}";
		}

		return $sql;
	}

	private function build_insert() {
		$keys = implode(',', $this->quoted_key_names());

		$values = array_values($this->data);
		$this->query_args = [];
		$list = $this->sql_condition_list($values, $this->query_args);
		$sql = "INSERT";
		if ($this->delayed) {
			$sql .= " DELAYED";
		}
		$sql .= " INTO {$this->table_escaped} ({$keys}) VALUES ({$list})";
		if ($this->return_id && $this->engine === self::ENGINE_POSTGRES) {
			$sql .= " RETURNING \"id\"";
		}

		return $sql;
	}

	private function build_select() {
		$sql = "SELECT {$this->select} FROM {$this->table_escaped}";

		if ($this->where) {
			$sql .= " WHERE {$this->where}";
		}

		if ($this->group) {
			$sql .= " GROUP BY {$this->group}";
		}

		if ($this->having) {
			$sql .= " HAVING {$this->having}";
		}

		if ($this->order) {
			$sql .= " ORDER BY {$this->order}";
		}

		if ($this->limit || $this->offset) {
			$offset = is_null($this->offset) ? '' : intval($this->offset) . ',';
			$limit = intval($this->limit);
			$sql .= " LIMIT {$offset}{$limit}";
		}

		return $sql;
	}

	private function build_update() {
		if (strlen($this->update) > 0) {
			$set = $this->update;
		} else {
			$set = [];
			$this->query_args = [];
			foreach ($this->data as $field => $value) {
				$set[] = $this->sql_assignment($field, $value, $this->query_args);
			}

			$set = implode(', ', $set);
		}

		$sql = "UPDATE {$this->table_escaped} SET {$set}";

		if ($this->where) {
			$sql .= " WHERE {$this->where}";
		}

		return $sql;
	}

	private function quoted_key_names() {
		$keys = [];

		foreach (array_keys($this->data) as $key) {
			$keys[] = $this->quoteKeyword($key);
		}

		return $keys;
	}

	private static function chunk_query($data, $callback, $size = 1000) {
		$return = true;
		$offset = 0;

		for (;;) {
			$chunk = array_slice($data, $offset, $size, true);
			if (empty($chunk)) {
				break;
			}

			$return = call_user_func($callback, $chunk);
			if ($return === false) {
				return false;
			}

			$offset += $size;
		}

		return $return;
	}

	private static function group_condition_parts($parts) {
		$fields = [];
		$operators = [];

		foreach ($parts as $index => $part) {
			$is_operator = in_array($part, ['_and_', '_or_']);
			if ($is_operator && $index % 2 === 0) {
				return null;
			}

			if ($is_operator) {
				$operator = strtoupper(str_replace('_', ' ', $part));
				$operators[] = [$operator, $index];
				continue;
			}

			if ($index % 2 === 1) {
				return null;
			}

			$fields[] = [$part, $index];
		}

		return ['fields' => $fields, 'operators' => $operators];
	}

	private static function parse_option(&$sql, $key, $value) {
		switch ($key) {
		case 'delete':
			$sql->delete();
			break;

		case 'insert':
			$sql->insert($value);
			break;

		case 'insertGetID':
			$sql->insertGetID($value);
			break;

		case 'update':
			$sql->update($value);
			break;

		case 'select':
			$sql->select($value);
			break;

		case 'where':
			if (is_hash($value)) {
				$sql->where($value);
				break;
			} else if (is_string($value)) {
				$value = [$value];
			}
			call_user_func_array([$sql, 'where'], $value);
			break;

		case 'order':
			$sql->order($value, true);
			break;

		case 'limit':
			$sql->limit($value);
			break;

		case 'offset':
			$sql->offset($value);
			break;

		case 'group':
			$sql->group($value);
			break;

		case 'having':
			$sql->having($value);
			break;

		default:
			throw new Exception("Unknown option \"{$key}\" used to build query.");
		}
	}

	private function sql_condition($field, $oper, $value, &$arguments) {
		if (is_array($value)) {
			if ($oper !== '=' && $oper !== '!=') {
				throw new Exception('Array values only supported for = and != operators');
			}
			return $this->sql_condition_in($field, $oper, $value, $arguments);
		}
		switch ($oper) {
			case '=':
			case '!=':
				return $this->sql_condition_equal($field, $oper, $value, $arguments);

			case '<':
			case '<=':
			case '>':
			case '>=':
			case 'LIKE':
				$field = $this->quoteKeyword($field);
				$chunk = $this->sql_value_and_add_arguments($value, $arguments);
				return "{$field} {$oper} {$chunk}";

			default:
				throw new Exception("Invalid operator ($oper)");
		}

	}

	private function sql_condition_equal($field, $oper, $value, &$arguments) {
		$chunk = $this->sql_value_and_add_arguments($value, $arguments);
		$field = $this->quoteKeyword($field);
		if ($value === null) {
			$oper = $oper === '!=' ? 'IS NOT' : 'IS';
		}

		return "{$field} {$oper} {$chunk}";
	}

	private function sql_condition_in($field, $oper, $array, &$arguments) {
		$chunks = $this->sql_condition_list($array, $arguments);
		$field = $this->quoteKeyword($field);
		$not = $oper === '!=' ? ' NOT' : '';

		return "{$field}{$not} IN ({$chunks})";
	}

	private function sql_condition_list($array, &$arguments = null) {
		$chunks = [];
		foreach ($array as $value) {
			$chunks[] = $this->sql_value_and_add_arguments($value, $arguments);
		}

		return implode(',', $chunks);
	}

	private function sql_assignment($field, $value, &$arguments) {
		$chunk = $this->sql_value_and_add_arguments($value, $arguments);
		$field = $this->quoteKeyword($field);
		return "{$field}={$chunk}";
	}

	private function sql_value(&$value, &$is_placeholder) {
		$is_placeholder = true;

		switch (gettype($value)) {
			// Numerics do not need placeholders or escaping because they are primitives..
			case 'boolean':
				$value = intval($value);
			case 'integer':
			case 'double':
				$is_placeholder = false;
				return "'{$value}'";

			case 'string':
				switch ($this->engine) {
					case self::ENGINE_MYSQL:
						return "'%s'";
					case self::ENGINE_POSTGRES:
						$is_placeholder = false;
						return $this->quote($value);
				}

			case 'object':
				if (array_key_exists('DBValue', class_implements($value))) {
					$is_placeholder = false;
					return $value->get_value();
				}
				break;

			case 'NULL':
				$is_placeholder = false;
				return 'NULL';

			default:
				throw new Exception('Invalid type ihavenoideahowitgothere');
		}
	}

	private function sql_value_and_add_arguments($value, &$arguments) {
		$is_placeholder = false;
		$chunk = $this->sql_value($value, $is_placeholder);

		if ($is_placeholder) {
			$arguments[] = $value;
		}

		return $chunk;
	}

	private function db_query($query, array $args = []) {
		self::$last_insert_id = null;
		switch ($this->engine) {
			case self::ENGINE_MYSQL:
				if ($this->db) {
					$this->result = self::ftb_db_query($this->db, $query, $args);
				} else {
					$this->result = db_query($query, $args);
				}
				if ($this->result && $this->return_id && $this->operation === 'INSERT') {
					$query = "SELECT LAST_INSERT_ID()";
					return db_result($this->db ? self::ftb_db_query($this->db, $query) : db_query($query));
				}
				return (bool)$this->result;

			case self::ENGINE_POSTGRES:
				if (self::$debug === true) {
					$t = microtime(true);
					$this->result = $this->pdo->query($query);
					self::$queries_executed[] = [
						'query' => $query,
						'time' => microtime(true) - $t,
						'db' => $this->db,
					];
				} else {
					$this->result = $this->pdo->query($query);
				}
				if ($this->result && $this->return_id && $this->operation === 'INSERT') {
					return $this->value();
				}
				return (bool)$this->result;
		}
	}

	/**
	 * RAW QUERY FUNCTION
	 *
	 * DO NOT USE THIS FUNCTION UNLESS YOU HAVE BEEN TO THE OPPOSITE END
	 * OF THE UNIVERSE IN SEARCH OF A BETTER WAY
	 *
	 * @param string $query
	 * @return bool
	 */
	public function executeRawQuery($query) {
		// We cannot currently allow the use of placeholders in this function
		// because placeholders are different across database interfaces
		return $this->db_query($query);
	}

	public function fetchArray() {
		// Automatically execute if not called yet. "syntactic sugar"
		if (!isset($this->result)) {
			$this->execute();
		}
		switch ($this->engine) {
			case self::ENGINE_MYSQL:
				return db_fetch_array($this->result);

			case self::ENGINE_POSTGRES:
				return $this->result ? $this->result->fetch(PDO::FETCH_ASSOC) : false;
		}
	}

	/**
	 * @deprecated
	 */
	public function fetchObject() {
		// Automatically execute if not called yet. "syntactic sugar"
		if (!isset($this->result)) {
			$this->execute();
		}
		switch ($this->engine) {
			case self::ENGINE_MYSQL:
				return db_fetch_object($this->result);

			case self::ENGINE_POSTGRES:
				return $this->result ? $this->result->fetch(PDO::FETCH_OBJ) : false;
		}
	}

	public function rowCount() {
		switch ($this->engine) {
			case self::ENGINE_MYSQL:
				return is_object($this->result) ? $this->result->num_rows : 0;

			case self::ENGINE_POSTGRES:
				return is_object($this->result) ? $this->result->rowCount() : 0;
		}
	}

	/**
	 * Use this to quote strings, ie LIKE 'sometext'
	 * @param $text
	 * @return string
	 */
	public function quote($text) {
		switch ($this->engine) {
			case self::ENGINE_MYSQL:
				return "'" . db_escape_string($text) . "'";

			case self::ENGINE_POSTGRES:
				return $this->pdo->quote($text);
		}
	}

	/**
	 * Use this to quote things like table names
	 * @param $text
	 * @return string
	 */
	public function quoteKeyword($text) {
		return $this->tick . $this->escapeKeyword($text) . $this->tick;
	}

	public function escapeKeyword($text) {
		if (preg_match('/[^a-zA-Z0-9_]/', $text)) {
			throw new Exception("Invalid SQL identifier ($text)");
		}
		return $text;
	}

	public function setPDO(PDO $pdo) {
		$this->pdo = $pdo;
	}

	public static function setDebug($debug) {
		self::$debug = (bool)$debug;
	}

	public static function getExecutedQueries() {
		return self::$queries_executed;
	}

	/*
	 * To be deleted as soon as we can handle this stuff in a better place
	 * @deprecated
	 */
	private static function ftb_db_query($db, $query, array $args = []) {
		// Connect to correct database
		global $current_db;
		$current_db = $db ?: 'default';
		db_set_active($current_db);

		$t0 = microtime(true);

		// The main query call
		$resource = db_query($query, $args);

		$execution_time = microtime(true) - $t0;

		// Save the database name so if we do EXPLAINS on the query later we can know what to switch to.
		global $queries;
		if ($queries) {
			$queries[count($queries) - 1]['QL_DB'] = $current_db;
		}

		// Revert back to default connection
		if ($db) {
			db_set_active('default');
			$current_db = 'default';
		}

		// Try to guess whether this is an app query by using a regex. Do not depend on this.
		preg_match('/from\s*\{?`?((app_[^\s|}`]*)|(view_[^\s|}`]*))/i', $query, $matches);
		$app_table_name = $matches[1];

		if ($app_table_name) {
			global $query_tracking_enabled;
			if ($query_tracking_enabled) {
				// Augment the last element/query in the $queries array with our additional info.
				global $queries, $current_query_info;
				if ($queries) {
					$last_index = count($queries) - 1;
					$queries[$last_index][3] = $current_query_info['q'];
					$queries[$last_index][4] = $current_query_info['server-name'];
					$queries[$last_index][5] = $current_query_info['zone'];
					$queries[$last_index][6] = $current_query_info['cross-zone'];
				}

				global $table_stats;
				if (!isset($table_stats[$app_table_name])) {
					$table_stats[$app_table_name] = array('cnt' => 1, 'time' => $execution_time);
				} else {
					$table_stats[$app_table_name]['cnt']++;
					$table_stats[$app_table_name]['time'] += $execution_time;
				}
			}
		}

		return $resource;
	}
}
