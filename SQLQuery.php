<?php

namespace FTB\core\db;
use DB;
use Exception;

/**
 * @author Dylan Wenzlau <dylan@findthebest.com>
 * @author Skyler Lipthay <slipthay@findthebest.com>
 */

require_once MODULE_PATH . '/ormish/db_value.inc';

/**
 * Builds a valid SQL query with value escapes.
 *
 * Example #1
 *
 *   DB::with('table_name')->
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
 *   $sql = DB::with('table_name')->insert(['one' => 1, 'two' => 't']);
 *   $sql->to_string();
 *   // => "INSERT INTO `table_name`(`one`, `two`) VALUES(%d, '%s')"
 *   $sql->bind_values();
 *   // => [1, 't']
 */
abstract class SQLQuery extends DBQuery {

	const INSERT_CHUNK_SIZE = 10000;

	protected static $VALID_OPERATORS = ['=', '!=', '>', '>=', '<', '<=', 'LIKE', 'NOT LIKE'];
	protected static $last_insert_id = null;

	protected static $queries_executed = [];

	protected $data;
	protected $group;
	protected $having;
	protected $limit;
	protected $offset;
	protected $operation = '';
	protected $order;
	protected $query_args;
	protected $select = '*';
	protected $table_escaped;
	protected $update;
	protected $where;
	protected $where_values = [];
	protected $delayed = false;
	protected $tick;
	protected $result;
	protected $return_id = false;

	/**
	 * Creates a new SQLQuery instance set with a specified table name.
	 *
	 * @param string $table The table name.
	 * @param string $db
	 * @param array $allowed_ops
	 * @return SQLQuery A new instance.
	 */
	public function __construct($table, $db = '', array $allowed_ops = []) {
		parent::__construct($table, $db, $allowed_ops);
		$this->tick = $this->getKeywordEscapeChar();
		$this->table_escaped = $this->tick . str_replace('.', "$this->tick.$this->tick", $table) . $this->tick;
	}

	public static function with($table, $db = '', array $allowed_ops = []) {
		return DB::with($table, $db, $allowed_ops);
	}

	/**
	 * Executes the built query
	 *
	 * @return DBStatement|bool A statement object on success, false on failure
	 */
	public function execute() {
		if (!isset($this->result)) {
			$string = $this->to_string();
			$values = $this->bind_values();
			$this->query($string, $values);
		}
		return $this->result;
	}

	/**
	 * Sets the query mode to SELECT FROM and specifies the columns to fetch.
	 *
	 *   // Starts the query with SELECT `column_one`, `column_two` FROM...
	 *   $sql_query->select(['column_one', 'column_two']);
	 *
	 *   // Starts the query with SELECT MIN(id), MAX(id), COUNT(*) FROM...
	 *   $sql_query->select(['MIN(id)', 'MAX(id)', 'COUNT(*)']);
	 *
	 *   // Starts the query with SELECT COUNT(DISTINCT(column_one)), column_one FROM...
	 *   $sql_query->select('COUNT(DISTINCT(column_one)), column_one', true);
	 *
	 * @param array|string $select An array of columns to select, or a raw string
	 *   of SQL to be used as the SELECT clause.
	 * @param bool $no_escape DANGER SQL INJECTION - If true, no escaping will be done
	 * @return SQLQuery $this for chaining.
	 *
	 * TODO: add SQL injection protection here
	 */
	public function select($select, $no_escape = false) {
		$this->set_operation('SELECT');

		if (is_array($select)) {
			foreach ($select as $key => $column_or_expression) {
				if (!$no_escape) {
					$select[$key] = $this->quoteExpression($column_or_expression);
				}
			}
			$this->select = implode(',', array_filter($select));
		} else {
			if (!$no_escape) {
				$this->select = $this->quoteExpression($select);
			} else {
				$this->select = $select;
			}
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
	public function orderByValues($field, array $values) {
		if (count($values) === 1) {
			return $this;
		}
		$field = $this->quoteKeyword($field);
		foreach ($values as $key => $value) {
			$values[$key] = $this->quote($value);
		}
		foreach ($values as $value) {
			$this->order .= ($this->order ? ',' : '') . "$field=$value DESC";
		}
		return $this;
	}

	/**
	 * Specifies an offset for the LIMIT clause for the query.
	 *
	 *   // Adds LIMIT 5, 10 to the query.
	 *   $sql_query->offset(5)->limit(10);
	 *
	 * @param int $offset The LIMIT offset.
	 * @return SQLQuery $this for chaining.
	 */
	public function offset($offset) {
		$this->offset = (int)$offset;
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
		$this->limit = (int)$limit;
		return $this;
	}

	/**
	 * Sets the query mode to UPDATE and attaches values to insert.
	 *
	 *   // Starts the query with UPDATE table SET `one`=1, `two`='t'.
	 *   $sql_query->update(['one' => 1, 'two' => 't']);
	 *
	 *   // Starts the query with UPDATE table SET `one`=`one` + 1.
	 *   $sql_query->update(['one' => 'one + 1'], true);
	 *
	 * @param array $updates An associative array with keys as column names
	 *   and values as column values
	 * @param bool $no_escape If true, no escaping will be done on $updates array
	 * @return SQLQuery $this for chaining.
	 */
	public function update(array $updates, $no_escape = false) {
		$this->set_operation('UPDATE');
		$set = [];
		$this->query_args = [];
		foreach ($updates as $field => $value) {
			if ($no_escape) {
				$set[] = "$field=$value";
			} else {
				$set[] = $this->sql_assignment($field, $value, $this->query_args);
			}
		}

		$this->update = implode(', ', $set);
		return $this;
	}

	/**
	 * Sets the query mode to UPDATE and attaches columns to increment/decrement.
	 *
	 *   // Starts the query with UPDATE table SET `one`=`one` + 1.
	 *   $sql_query->increment(['one' => 1]);
	 *
	 * @param array $updates An associative array with keys as column names
	 *   and values as amounts by which to increment (positive) or decrement (negative)
	 * @return SQLQuery $this for chaining.
	 */
	public function increment(array $updates) {
		$this->set_operation('UPDATE');
		$set = [];
		$this->query_args = [];
		foreach ($updates as $field => $value) {
			$field = $this->quoteKeyword($field);
			if (is_numeric($value) && $value < 0) {
				$set[] = "$field=$field - " . $this->quote(abs($value));
			} else {
				$set[] = "$field=$field + " . $this->quote($value);
			}
		}

		$this->update = implode(', ', $set);
		return $this;
	}

	/**
	 * Executes a batch update based on the values of the key column.
	 *
	 *   // Sets `b` to 2 where `a` is 1, and `b` to 3 where `a` is 10:
	 *   DB::with('table')->updateColumn('a', 'b', [
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
	public function updateColumn($key_column, $value_column, array $data) {
		if (is_array($value_column)) {
			foreach ($value_column as $column) {
				$this->updateColumn($key_column, $column, $data[$column]);
			}

			return;
		}

		if (count($data) > 1000) {
			$callback = function($chunk) use ($key_column, $value_column) {
				$this->updateColumn($key_column, $value_column, $chunk);
			};

			return static::chunk_query($data, $callback, 1000);
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

		return $this->query($sql, $arguments);
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
		return $this->query("UPDATE $this->table_escaped SET $to_column = $from_column");
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
	 * Executes a batch insert to the selected table based on data provided in an
	 * array of associative arrays.
	 *
	 *   // Inserts rows into a table specifying `name` and `value`.
	 *   DB::with('table')->insertMultiAssoc([
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
	public function insertMultiAssoc(array $data) {
		if (empty($data)) {
			return true;
		}

		$keys = [];
		$keys_string = '';
		foreach (current($data) as $column_name => $value) {
			$keys[] = $column_name;
			$keys_string .= ($keys_string ? ',' : '') . $this->quoteKeyword($column_name);
		}
		$base_sql = "INSERT INTO {$this->table_escaped} ({$keys_string}) VALUES ";
		$sql = '';
		$i = 0;
		$arguments = [];

		foreach ($data as $row) {
			$sql .= ($i !== 0 ? ',' : '') . "(" . $this->sql_condition_list($row, $arguments) . ")";
			$i++;
			if ($i % static::INSERT_CHUNK_SIZE === 0) {
				$success = $this->query($base_sql . $sql, $arguments);
				$sql = '';
				$i = 0;
				$arguments = [];
				if (!$success) {
					return false;
				}
			}
		}
		if ($i !== 0) {
			return $this->query($base_sql . $sql, $arguments);
		}
		return true;
	}

	public function insertMulti(array $column_names, array $rows) {
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

		return $this->query($query);
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
	 * Converts the current SQL query into a string to be passed into a database
	 * querying function. Note that this function returns a string with %-escaped
	 * value placeholders.
	 *
	 * @see SQLQuery::bind_values()
	 *
	 * @return string SQL string with value placeholders.
	 */
	public function to_string() {
		switch ($this->operation) {
			case 'SELECT':
				return $this->build_select();
			case 'UPDATE':
				return $this->build_update();
			case 'INSERT':
				return $this->build_insert();
			case 'DELETE':
				return $this->build_delete();
		}
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
		$parts = static::group_condition_parts($parts);
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
		$sql = static::with($table);

		foreach ($options as $key => $value) {
			static::parse_option($sql, $key, $value);
		}

		return $sql;
	}

	abstract public function getRegexpOperator();

	protected function apply_where_conditions($args, $negate = false) {
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

	protected function build_delete() {
		$sql = "DELETE FROM {$this->table_escaped}";

		if ($this->where) {
			$sql .= " WHERE {$this->where}";
		}

		return $sql;
	}

	protected function build_insert() {
		$keys = implode(',', $this->quoted_key_names());

		$values = array_values($this->data);
		$this->query_args = [];
		$list = $this->sql_condition_list($values, $this->query_args);
		$sql = "INSERT";
		if ($this->delayed) {
			$sql .= " DELAYED";
		}
		$sql .= " INTO {$this->table_escaped} ({$keys}) VALUES ({$list})";

		return $sql;
	}

	protected function build_select() {
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

	protected function build_update() {
		$sql = "UPDATE {$this->table_escaped} SET {$this->update}";

		if ($this->where) {
			$sql .= " WHERE {$this->where}";
		}

		return $sql;
	}

	protected function quoted_key_names() {
		$keys = [];
		foreach (array_keys($this->data) as $key) {
			$keys[] = $this->quoteKeyword($key);
		}
		return $keys;
	}

	protected static function chunk_query($data, $callback, $size = 1000) {
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

	protected static function group_condition_parts($parts) {
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

	protected static function parse_option(&$sql, $key, $value) {
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

	protected function sql_condition($field, $oper, $value, &$arguments) {
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
			case 'NOT LIKE':
				$field = $this->quoteKeyword($field);
				$chunk = $this->sql_value_and_add_arguments($value, $arguments);
				return "{$field} {$oper} {$chunk}";

			default:
				throw new Exception("Invalid operator ($oper)");
		}

	}

	protected function sql_condition_equal($field, $oper, $value, &$arguments) {
		$chunk = $this->sql_value_and_add_arguments($value, $arguments);
		$field = $this->quoteKeyword($field);
		if ($value === null) {
			$oper = $oper === '!=' ? 'IS NOT' : 'IS';
		}

		return "{$field} {$oper} {$chunk}";
	}

	protected function sql_condition_in($field, $oper, array $array, &$arguments) {
		// Ensure values are unique, since query engine might not be smart enough
		// to remove duplicates
		$array = array_unique($array);
		$chunks = $this->sql_condition_list($array, $arguments);
		$field = $this->quoteKeyword($field);
		$not = $oper === '!=' ? ' NOT' : '';

		return "{$field}{$not} IN ({$chunks})";
	}

	protected function sql_condition_list(array $array, &$arguments = null) {
		$chunks = [];
		foreach ($array as $value) {
			$chunks[] = $this->sql_value_and_add_arguments($value, $arguments);
		}

		return implode(',', $chunks);
	}

	protected function sql_assignment($field, $value, &$arguments) {
		$chunk = $this->sql_value_and_add_arguments($value, $arguments);
		$field = $this->quoteKeyword($field);
		return "{$field}={$chunk}";
	}

	protected function sql_value(&$value, &$is_placeholder) {
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
				$is_placeholder = false;
				return $this->quote($value);

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

	protected function sql_value_and_add_arguments($value, &$arguments) {
		$is_placeholder = false;
		$chunk = $this->sql_value($value, $is_placeholder);

		if ($is_placeholder) {
			$arguments[] = $value;
		}

		return $chunk;
	}

	abstract public function query($query, array $args = []);

	/**
	 * Use this to both quote and escape strings, protecting from SQL injection.
	 * @param string $text
	 * @return string
	 */
	abstract public function quote($text);

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

	public function quoteExpression($text) {
		if ($text === '*' || $text === 'COUNT(*)' || is_numeric($text)) {
			return $text;
		}
		// Detect function syntax, e.g. MIN(field_name) as min_field
		$is_function = preg_match('/\A([a-z_]+)\(([a-z_][a-z0-9_]*)\)(( AS)? ([a-z_][a-z0-9_]*))?\z/i', $text, $matches);
		if ($is_function) {
			$text = $matches[1] . '(' . $this->tick . $matches[2] . $this->tick . ')';
			if ($matches[3]) {
				if ($matches[4]) {
					$text .= " AS";
				}
				$text .= ' ' . $this->tick . $matches[5] . $this->tick;
			}
			return $text;
		}
		return $this->quoteKeyword($text);
	}

	abstract protected function getKeywordEscapeChar();

	public static function getExecutedQueries() {
		return static::$queries_executed;
	}

	protected function logQuery($engine, $query, $success, $time) {
		if (!isset(static::$queries_executed[$engine])) {
			static::$queries_executed[$engine] = [];
		}
		static::$queries_executed[$engine][] = [
			'query' => $query,
		    'success' => $success,
			'time' => $time,
			'db' => $this->db,
		];
	}

	public function __toString() {
		return $this->to_string();
	}
}
