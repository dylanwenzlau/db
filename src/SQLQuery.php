<?php

namespace FindTheBest\DB;
use DB;
use Exception;
use DBValueRaw;

// Classes to handle special SQL data types
require_once __DIR__ . '/db_value.php';

/**
 * Class SQLQuery
 *
 * @author Dylan Wenzlau <dylan@findthebest.com>
 * @author Skyler Lipthay <slipthay@findthebest.com>
 */
abstract class SQLQuery extends DBQuery {

	const BATCH_SIZE = 10000;

	protected static $query_modifier;

	protected $_pdo;
	protected $data;
	protected $group;
	protected $having;
	protected $limit;
	protected $offset;
	protected $operation = '';
	protected $order;
	protected $query_args;
	protected $select = '*';
	protected $distinct = false;
	protected $table_escaped;
	protected $update;
	protected $where;
	protected $where_values = [];
	protected $unions = [];
	protected $delayed = false;
	protected $tick;
	protected $result;
	protected $return_id = false;
	protected $no_escape = false;
	protected $sql = '';

	/**
	 * Creates a new SQLQuery instance
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

	/**
	 * Lazily constructed PDO instance
	 * @return \PDO
	 */
	protected function pdo() {
		if (!$this->_pdo) {
			$this->setPDO();
		}
		return $this->_pdo;
	}

	protected function setPDO($query = '') {
		// Easy to determine read vs. write if query builder is being used
		if ($this->operation !== '') {
			$access = $this->operation === 'SELECT' ? 'read' : 'write';

		// Default to read if we don't know anything about the query yet
		} else if (!$query) {
			$access = 'read';

		// Otherwise we have to do a bit of parsing
		} else {
			// Change to write connection for anything other than SELECT queries
			if (self::isSelectQuery($query)) {
				$master_only_tables = DB::getDBConfig($this->db, 'read')['master_only_tables'];
				if ($master_only_tables && preg_match('/from\s+[`"]?(?:' . implode('|', $master_only_tables) . ')[`"]?/i', $query)) {
					$access = 'write';
				} else {
					if (stripos($query, 'LAST_INSERT_ID') !== false) {
						$access = 'write';
					} else {
						$access = 'read';
					}
				}
			} else {
				$access = 'write';
			}
		}
		$this->_pdo = DB::getPDO($this->db, $access);
	}

	public static function isSelectQuery($query) {
		return (bool)preg_match('%\A\s*(?:/\*.*?\*/\s*)*(?:SELECT |SHOW FULL PROCESSLIST|EXPLAIN(?: EXTENDED)? SELECT).*\Z%si', $query);
	}

	/**
	 * Executes the built query
	 *
	 * @return DBStatement|bool A statement object on success, false on failure
	 */
	public function execute() {
		if (!isset($this->result)) {
			$string = $this->to_string();
			if (!$string) {
				return false;
			}
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
	 */
	public function select($select, $no_escape = false) {
		$this->set_operation('SELECT');

		if (is_array($select)) {
			if (!$no_escape) {
				foreach ($select as $key => $column_or_expression) {
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
	 * Set the current SELECT query to use SELECT DISTINCT
	 */
	public function distinct() {
		$this->distinct = true;
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
	 * @see class DBValue if interested in using SQL constructs as hash values
	 *
	 * @param array|string $array_or_field
	 * @param string $oper
	 * @param mixed $value
	 * @return SQLQuery $this for chaining.
	 */
	public function where($array_or_field, $oper = '', $value = null) {
		$this->apply_where_conditions($array_or_field, $oper, $value);
		return $this;
	}

	/**
	 * Negated version of SQLQuery::where
	 *
	 * @param array|string $array_or_field
	 * @param string $oper
	 * @param mixed $value
	 * @return SQLQuery $this for chaining
	 */
	public function whereNot($array_or_field, $oper = '', $value = null) {
		$this->apply_where_conditions($array_or_field, $oper, $value, $negate = true);
		return $this;
	}

	/**
	 * Add a raw where clause to the query. This will not be validated or
	 * escaped, so please use the quote() method on any user input.
	 * @param string $where_clause
	 * @return SQLQuery $this for chaining.
	 */
	public function whereRaw($where_clause) {
		if ($where_clause) {
			$this->where .= ($this->where ? ' AND ' : '') . $where_clause;
		}
		return $this;
	}

	/**
	 * Specifies a GROUP BY clause for the query.
	 *
	 *   // Adds GROUP BY `column_one`, `column_two` to the query.
	 *   $sql_query->group(['column_one', 'column_two']);
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
				throw new Exception("Invalid sort direction: $direction");
			}
			if (!$half_escape) {
				$column = $this->quoteExpression($column);
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
			$values[$key] = $this->sql_value($value);
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

	public function union(SQLQuery $sql_query) {
		$this->unions[] = $sql_query;
		return $this;
	}

	/**
	 * Sets the query mode to UPDATE and attaches values to insert.
	 *
	 *   // Starts the query with UPDATE table SET `one`=1, `two`='t'.
	 *   $sql_query->update(['one' => 1, 'two' => 't']);
	 *
	 *   // Starts the query with UPDATE table SET one=one + 1.
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
	 * @param bool $coalesce_null_to_zero
	 * @return SQLQuery $this for chaining.
	 */
	public function increment(array $updates, $coalesce_null_to_zero = true) {
		$this->set_operation('UPDATE');
		$set = [];
		$this->query_args = [];
		foreach ($updates as $field => $value) {
			$field = $this->quoteKeyword($field);
			if ($coalesce_null_to_zero) {
				$current = "COALESCE($field, 0)";
			} else {
				$current = $field;
			}
			if (is_numeric($value) && $value < 0) {
				$value = abs($value);
				$set[] = "$field=$current - " . $this->sql_value($value);
			} else {
				$set[] = "$field=$current + " . $this->sql_value($value);
			}
		}

		$this->update = implode(', ', $set);
		if (DB::$auto_execute) {
			return $this->execute();
		}
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
			$success = true;
			foreach ($value_column as $column) {
				$success = $this->updateColumn($key_column, $column, $data[$column]) && $success;
			}
			return $success;
		}

		if (count($data) > 1000) {
			$callback = function($chunk) use ($key_column, $value_column) {
				$this->updateColumn($key_column, $value_column, $chunk);
			};

			return static::chunk_query($data, $callback, 1000);
		} else if (empty($data)) {
			return true;
		}

		$key_column_quoted = $this->quoteKeyword($key_column);
		$value_column_quoted = $this->quoteKeyword($value_column);
		$sql = "UPDATE {$this->table_escaped} SET {$value_column_quoted} = CASE {$key_column_quoted}";

		$keys = [];
		$arguments = [];
		foreach ($data as $key => $value) {
			$keys[] = $key;
			$key_string = $this->sql_value_and_add_arguments($key, $arguments);
			$value_string = $this->sql_value_and_add_arguments($value, $arguments, false);
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
	 * Execute an upsert query
	 *
	 *   INSERT INTO table(`uid`, `name`) VALUES ('1', 'john')
	 *   ON DUPLICATE KEY UPDATE `name` = VALUES(`name`);
	 *
	 *   $sql_query->upsert(['uid' => 1, 'name' => 'john'], ['uid']);
	 *
	 * @param array $data An associative array with keys as column names and
	 *   values as column values.
	 * @param array $ignore_columns columns that should NOT be updated on duplicate key
	 * @param bool $no_escape Pass true to enable SQL injection and watch civilization crumble
	 * @return DBStatement on success, false on failure
	 */
	public function upsert(array $data = [], array $ignore_columns = [], $no_escape = false) {
		return $this->upsertMultiAssoc([$data], $ignore_columns, $no_escape);
	}

	/**
	 * Execute a batch upsert query
	 *
	 *   INSERT INTO `table` (`uid`, `name`) VALUES ('1', 'john'), ('2', 'jane')
	 *   ON DUPLICATE KEY UPDATE `name` = VALUES(`name`);
	 *
	 *   $sql_query->upsertMulti(['uid', 'name'], [[1, 'john'], [2, 'jane']], ['uid']);
	 *
	 * @param array $column_names
	 * @param array $data
	 * @param array $ignore_columns columns that should NOT be updated on duplicate key
	 * @param array $column_updates [column => 'sql string'] e.g. ['sum_field' => 'sum_field + VALUES(sum_field)']
	 *      where sql string can include VALUES(column) to perform computations such as incrementing
	 * @param bool $no_escape Pass true to enable SQL injection and watch civilization crumble
	 * @return DBStatement on success, false on failure
	 */
	public function upsertMulti(array $column_names, array $data, array $ignore_columns = [], array $column_updates = [], $no_escape = false) {
		if (empty($column_names) || empty($data)) {
			return false;
		}
		$update_str = '';
		foreach ($column_names as $column_name) {
			$column_quoted = $this->quoteKeyword($column_name);
			if (!in_array($column_name, $ignore_columns)) {
				$update_str .= ($update_str ? ', ' : '') . "$column_quoted=";
				if (isset($column_updates[$column_name])) {
					$update_str .= $column_updates[$column_name];
				} else {
					$update_str .= "VALUES($column_quoted)";
				}
			}
		}
		$old_auto_execute = DB::$auto_execute;
		DB::$auto_execute = false;
		$this->insertMulti($column_names, $data, false, $no_escape);
		DB::$auto_execute = $old_auto_execute;
		$this->sql .= $update_str ? " ON DUPLICATE KEY UPDATE $update_str" : '';
		if (DB::$auto_execute) {
			return $this->query($this->sql);
		}
		return $this;
	}

	/**
	 * Execute a batch upsert query using associative array syntax.
	 * CAUTION: This method is slower than upsertMulti() and should only be used
	 * if your data is already in associative format, otherwise massive batch upserts
	 * will be up to roughly twice as slow.
	 *
	 *   INSERT INTO `table` (`uid`, `name`) VALUES ('1', 'john'), ('2', 'jane')
	 *   ON DUPLICATE KEY UPDATE `name` = VALUES(`name`);
	 *
	 *   $sql_query->upsertMultiAssoc([
	 *     ['uid' => 1, 'name' => 'john'],
	 *     ['uid' => 2, 'name' => 'jane']
	 *   ], ['uid']);
	 *
	 * @param array $data
	 * @param array $ignore_columns columns that should NOT be updated on duplicate key
	 * @param bool $no_escape Pass true to enable SQL injection and watch civilization crumble
	 * @return DBStatement on success, false on failure
	 */
	public function upsertMultiAssoc(array $data, array $ignore_columns = [], $no_escape = false) {
		$column_names = array_keys(reset($data));
		if (empty($column_names) || empty($data)) {
			return false;
		}
		$update_str = '';
		foreach ($column_names as $column_name) {
			$column_quoted = $this->quoteKeyword($column_name);
			if (!in_array($column_name, $ignore_columns)) {
				$update_str .= ($update_str ? ', ' : '') . "$column_quoted=VALUES($column_quoted)";
			}
		}
		$old_auto_execute = DB::$auto_execute;
		DB::$auto_execute = false;
		$this->insertMultiAssoc($data, false, $no_escape);
		DB::$auto_execute = $old_auto_execute;
		$this->sql .= $update_str ? " ON DUPLICATE KEY UPDATE $update_str" : '';
		if (DB::$auto_execute) {
			return $this->query($this->sql);
		}
		return $this;
	}


	/**
	 * Sets the query mode to INSERT INTO and attaches values to insert.
	 *
	 *   // Starts the query with INSERT INTO `table` (`one`, `two`) VALUES ('1', 't').
	 *   $sql_query->insert(['one' => 1, 'two' => 't']);
	 *
	 * @param array $data An associative array with keys as column names and
	 *   values as column values.
	 * @param bool $ignore a boolean value, if true will turn this query into
	 *   an INSERT IGNORE, default is false
	 * @return SQLQuery $this for chaining.
	 * @throws Exception
	 */
	public function insert(array $data = [], $ignore = false) {
		if ($ignore) {
			$this->set_operation('INSERT IGNORE');
		} else {
			$this->set_operation('INSERT');
		}
		$this->data = $data;
		if (DB::$auto_execute) {
			return $this->execute();
		}
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
		$this->return_id = true;
		return $this->insert($data);
	}

	public function delayed() {
		$this->delayed = true;
		return $this;
	}

	/**
	 * Executes a batch insert to the selected table based on data provided in an
	 * array of associative arrays. NOTE: this is less efficient than insertMulti,
	 * given a choice between the two.
	 *
	 *   // Inserts rows into a table specifying `name` and `value`.
	 *   DB::with('table')->insertMultiAssoc([
	 *     ['name' => '_111111', 'value' => 'abc'],
	 *     ['name' => '_222222', 'value' => 'def'],
	 *   ]);
	 *
	 * @param array $data A list of associative arrays mapping column names to
	 *   corresponding values.
	 * @param bool $ignore
	 * @param bool $no_escape Pass true to enable SQL injection and watch civilization crumble
	 * @return mixed A DBStatement objects on success, false if the query was not
	 *   executed correctly, or true if $data was empty and there was nothing to
	 *   be done.
	 */
	public function insertMultiAssoc(array $data, $ignore = false, $no_escape = false) {
		if (empty($data)) {
			return true;
		}

		$keys = [];
		$keys_string = '';
		foreach (reset($data) as $column_name => $value) {
			$keys[] = $column_name;
			$keys_string .= ($keys_string ? ',' : '') . $this->quoteKeyword($column_name);
		}
		$ignore = $ignore ? ' IGNORE' : '';
		$value_str = '';
		foreach ($data as $row) {
			$value_str .= ($value_str ? ',' : '') . '(';
			$j = 0;
			foreach ($keys as $key) {
				$value_str .= ($j ? ',' : '');
				$value_str .= $no_escape ? $row[$key] : $this->sql_value($row[$key]);
				$j++;
			}
			$value_str .= ')';
		}

		$this->sql = "INSERT$ignore INTO $this->table_escaped ($keys_string) VALUES $value_str";
		if (DB::$auto_execute) {
			return $this->query($this->sql);
		}
		return $this;
	}


	/**
	 * Executes a batch insert to the selected table.
	 *
	 *   // Inserts rows into a table specifying `name` and `value`.
	 *   DB::with('table')->insertMulti(['name', 'value'], [
	 *     ['david', '111'],
	 *     ['john', '222'],
	 *   ]);
	 *
	 * @param array $column_names
	 * @param array $rows An array of numeric-keyed arrays
	 * @param bool $ignore
	 * @param bool $no_escape
	 * @return mixed A DBStatement objects on success, false on failure
	 * @throws Exception
	 */
	public function insertMulti(array $column_names, array $rows, $ignore = false, $no_escape = false) {
		if (empty($column_names) || empty($rows)) {
			return false;
		}
		foreach ($column_names as $key => $column) {
			$column_names[$key] = $this->quoteKeyword($column);
		}
		$column_str = "(" . implode(",", $column_names) . ")";
		$ignore = $ignore ? ' IGNORE' : '';
		$value_str = '';
		$column_count = count($column_names);
		foreach ($rows as $row) {
			$value_str .= ($value_str ? ',' : '') . '(';
			$j = 0;
			foreach ($row as $value) {
				$value_str .= ($j ? "," : "");
				$value_str .= $no_escape ? $value : $this->sql_value($value);
				$j++;
				// Allow each data row to contain more data than actually being inserted
				if ($j === $column_count) {
					break;
				}
			}
			$value_str .= ')';
		}

		$this->sql = "INSERT$ignore INTO $this->table_escaped $column_str VALUES $value_str";
		if (DB::$auto_execute) {
			return $this->query($this->sql);
		}
		return $this;
	}

	/**
	 * Set the query operation to DELETE.
	 *
	 * @see SQLQuery::where()
	 *
	 * @param array|string $array_or_field
	 * @param string $oper
	 * @param mixed $value
	 * @return SQLQuery $this for chaining.
	 */
	public function delete($array_or_field = [], $oper = '', $value = null) {
		$this->set_operation('DELETE');
		$this->apply_where_conditions($array_or_field, $oper, $value);
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

		return self::array_flatten($values);
	}

	/**
	 * Flatten down an array of arrays.
	 */
	private static function array_flatten($array) {
		$index = 0;

		while ($index < count($array)) {
			if (is_array($array[$index])) {
				array_splice($array, $index, 1, $array[$index]);
			} else {
				$index++;
			}
		}

		return $array;
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
			case 'INSERT IGNORE':
				return $this->build_insert(true);
			case 'DELETE':
				return $this->build_delete();
			default:
				if ($this->sql) {
					return $this->sql;
				}
				return '';
		}
	}

	abstract public function getRegexpOperator();

	/**
	 * Allowed formats for args:
	 *
	 * ['app_id' => 10, 'listing_id' => 400]
	 *
	 * 'screen_size', '>', 9.9
	 *
	 * ['screen_size', '>', 9.9]
	 *
	 * [['screen_size', '>', 9.9], ['talk_time', '=', 20]]
	 *
	 * @param array|string $array_or_field
	 * @param string $oper
	 * @param mixed $value
	 * @param bool $negate
	 * @throws Exception
	 */
	protected function apply_where_conditions($array_or_field, $oper = '', $value = null, $negate = false) {
		// Ignore empty WHEREs
		if (!$array_or_field) {
			return;
		}

		// e.g. where('field', '>=', 99)
		// If it's this syntax, just put it in an array to be dealt with by multiple syntax
		if ($oper) {
			$array_or_field = [[$array_or_field, $oper, $value]];
		}

		if (!is_array($array_or_field)) {
			throw new Exception("first where argument must be an array, instead found " . gettype($array_or_field));
		}

		$sql = [];
		$array = $array_or_field;

		// e.g. where(['id' => 10])
		if (self::is_hash($array)) {
			foreach ($array as $field => $value) {
				$sql[] = $this->sql_condition($field, $negate ? '!=' : '=', $value, $this->where_values);
			}

		// e.g. where([['id', '>', 100], ['id', '<', 200]])
		} else {
			// If it's just a single array, convert it to an array of arrays
			if (!is_array($array[0])) {
				$array = [$array];
			}
			foreach ($array as $condition) {
				if ($negate) {
					$condition[1] = self::negate_operator($condition[1]);
				}
				$sql[] = $this->sql_condition($condition[0], $condition[1], $condition[2], $this->where_values);
			}
		}

		$this->where .= ($this->where ? ' AND ' : '') . implode(' AND ', $sql);
	}

	protected function build_delete() {
		$sql = "DELETE FROM {$this->table_escaped}";

		if ($this->where) {
			$sql .= " WHERE {$this->where}";
		}

		return $sql;
	}

	protected function build_insert($ignore = false) {
		$keys = implode(',', $this->quoted_key_names());

		$values = array_values($this->data);
		$this->query_args = [];
		$list = $this->sql_condition_list($values, $this->query_args);

		$sql = "INSERT";
		if ($ignore) {
			$sql .= " IGNORE";
		}

		if ($this->delayed) {
			$sql .= " DELAYED";
		}
		$sql .= " INTO {$this->table_escaped} ({$keys}) VALUES ({$list})";

		return $sql;
	}

	protected function build_select() {
		$sql = "SELECT";

		if ($this->distinct) {
			$sql .= " DISTINCT";
		}

		$sql .= " {$this->select} FROM {$this->table_escaped}";

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

		if (isset($this->limit)) {
			$sql .= " LIMIT {$this->limit}";
			if ($this->offset > 0) {
				$sql .= " OFFSET {$this->offset}";
			}
		}

		if ($this->unions) {
			return "($sql) UNION (" . implode(') UNION (', $this->unions) . ')';
		}

		return $sql;
	}

	protected function build_update() {
		if (!$this->update) {
			return '';
		}
		$sql = "UPDATE {$this->table_escaped} SET {$this->update}";
		if ($this->where) {
			$sql .= " WHERE {$this->where}";
		}
		return $sql;
	}

	protected function quoted_key_names() {
		$keys = [];
		foreach ($this->data as $key => $dontcare) {
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

	protected function sql_condition($field, $oper, $value, &$arguments) {
		switch ($oper) {
			case '=':
			case '!=':
				if (is_array($value)) {
					return $this->sql_condition_in($field, $oper, $value, $arguments);
				}
				return $this->sql_condition_equal($field, $oper, $value, $arguments);

			case '<':
			case '<=':
			case '>':
			case '>=':
			case 'LIKE':
			case 'NOT LIKE':
			case 'LIKE BINARY':
			case 'NOT LIKE BINARY':
			case 'REGEXP':
			case 'NOT REGEXP':
			case 'MATCH':
				$field = $this->quoteKeyword($field);
				$chunk = $this->sql_value_and_add_arguments($value, $arguments);
				return "{$field} {$oper} {$chunk}";

			case 'BETWEEN':
			case 'NOT BETWEEN':
				if (!is_array($value) || count($value) !== 2) {
					throw new Exception("$oper operator requires array of length 2, found ($value)");
				}
				$field = $this->quoteKeyword($field);
				$min = $this->sql_value_and_add_arguments($value[0], $arguments);
				$max = $this->sql_value_and_add_arguments($value[1], $arguments);
				return "{$field} {$oper} {$min} AND {$max}";

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

	// always_quote can be useful to ensure BTREE indexes on textual fields
	// are utilized, even if the data in the WHERE clause is numeric
	protected function sql_value(&$value, &$is_placeholder = true, $always_quote = true) {
		$is_placeholder = true;

		switch (gettype($value)) {
			// Numerics do not need placeholders or escaping because they are primitives..
			case 'boolean':
				$value = intval($value);
			case 'integer':
			case 'double':
				$is_placeholder = false;
				return $always_quote ? "'{$value}'" : $value;

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
				throw new Exception('Invalid SQL datatype: "' . gettype($value) .'".');
		}
	}

	protected function sql_value_and_add_arguments($value, &$arguments, $always_quote = true) {
		$is_placeholder = false;
		$chunk = $this->sql_value($value, $is_placeholder, $always_quote);

		if ($is_placeholder) {
			$arguments[] = $value;
		}

		return $chunk;
	}

	protected static function negate_operator($oper) {
		static $map = [
			'=' => '!=',
			'!=' => '=',
			'<' => '>=',
			'<=' => '>',
			'>' => '<=',
			'>=' => '<',
			'LIKE' => 'NOT LIKE',
			'NOT LIKE' => 'LIKE',
			'LIKE BINARY' => 'NOT LIKE BINARY',
			'NOT LIKE BINARY' => 'LIKE BINARY',
			'REGEXP' => 'NOT REGEXP',
			'NOT REGEXP' => 'REGEXP',
			'BETWEEN' => 'NOT BETWEEN',
			'NOT BETWEEN' => 'BETWEEN',
		];
		if (!isset($map[$oper])) {
			throw new Exception("Operator $oper cannot be negated");
		}
		return $map[$oper];
	}

	/**
	 * Checks that the array is associative, having at least 1 non-integer key.
	 * O(n)
	 */
	private static function is_hash(&$array) {
		if (!is_array($array)) {
			return false;
		}

		foreach ($array as $key => $value) {
			if (!is_int($key)) {
				return true;
			}
		}
		return false;
	}

	abstract public function query($query, array $args = []);

	/**
	 * Use this to both quote and escape strings, protecting from SQL injection.
	 * @param string $text
	 * @return string
	 */
	public function quote($text) {
		return $this->pdo()->quote($text);
	}

	/**
	 * Use this to quote things like table names and column names
	 * @param $text
	 * @return string
	 */
	public function quoteKeyword($text) {
		return $this->tick . $this->escapeKeyword($text) . $this->tick;
	}

	public function escapeKeyword($text) {
		// SQL identifiers can technically contain any utf-8 character
		if (preg_match('/[`"]/', $text)) {
			throw new Exception("Invalid SQL identifier: $text. Value may not contain ` or \".");
		}
		return $text;
	}

	public function quoteExpression($text) {
		if ($text instanceof DBValueRaw) {
			return $text->get_value();
		}
		if ($text === '*' || is_numeric($text)) {
			return $text;
		}
		// Detect function syntax, e.g. MIN(field_name) as min_field
		$is_function = preg_match('/\A([a-z_]+)\(([a-z0-9_\*]*)\)(( AS)? ([a-z0-9_]*))?\z/i', $text, $matches);
		if ($is_function) {
			if ($matches[2] === '*') {
				$text = $matches[1] . '(*)';
			} else {
				$text = $matches[1] . '(' . $this->tick . $matches[2] . $this->tick . ')';
			}
			if ($matches[3]) {
				if ($matches[4]) {
					$text .= ' AS';
				}
				$text .= ' ' . $this->tick . $matches[5] . $this->tick;
			}
			return $text;
		}
		return $this->quoteKeyword($text);
	}

	abstract protected function getKeywordEscapeChar();

	public function rowsAffected() {
		return is_object($this->result) ? $this->result->rowsAffected() : false;
	}

	/**
	 * http://php.net/manual/en/pdo.errorinfo.php
	 * [SQLSTATE error code, Driver-specific error code, Driver-specific error message]
	 * @return array
	 */
	public function errorInfo() {
		// If the error occurred after a successful PDOStatement was created,
		// the error will be on the statement but NOT on the main database handle
		if (is_object($this->result)) {
			return $this->result->errorInfo() ?: $this->pdo()->errorInfo();
		}
		return $this->pdo()->errorInfo();
	}

	public static function setQueryModifier(callable $function) {
		self::$query_modifier = $function;
	}

	protected static function modifyQuery($query) {
		if (isset(self::$query_modifier)) {
			return call_user_func(self::$query_modifier, $query);
		}
		return $query;
	}

	public function __toString() {
		return $this->to_string();
	}
}
