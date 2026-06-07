<?php

namespace DB;
use Closure;
use DB;
use Exception;
use DBValueRaw;
use PDO;
use PDOException;

abstract class SQLQuery {

	protected static Closure $query_modifier;

	protected ?PDO $_pdo = null;
	protected array $data = [];
	protected string $group = '';
	protected string $having = '';
	protected ?int $limit = null;
	protected int $offset = 0;
	protected string $operation = '';
	protected string $order = '';
	protected string $select = '*';
	protected string $table_escaped = '';
	protected string $update = '';
	protected string $where = '';
	protected array $unions = [];
	protected array $joins = [];
	protected array $indexes = [];
	protected string $tick = '';
	protected SQLStatement|int|false|null $result = null;
	protected bool $return_id = false;
	protected string $sql = '';
	protected array $allowed_operations = ['SELECT', 'UPDATE', 'UPDATE IGNORE', 'INSERT', 'INSERT IGNORE', 'UPSERT', 'DELETE'];

	/**
	 * Creates a new SQLQuery instance
	 *
	 * @param string $table The table name.
	 */
	public function __construct(
		protected readonly string $table,
		protected readonly string $db = '',
		array $allowed_ops = []
	) {
		$this->allowed_operations = $allowed_ops ?: $this->allowed_operations;
		$this->tick = $this->getKeywordEscapeChar();
		$this->table_escaped = $table !== '' ? $this->quoteAliasExpression($table) : '';
	}

	protected function setOperation(string $operation) : void {
		if ($this->allowed_operations && !in_array($operation, $this->allowed_operations, true)) {
			throw new Exception("$operation is not allowed");
		}
		$this->operation = $operation;
	}

	/**
	 * Lazily constructed PDO instance
	 */
	protected function pdo() : PDO {
		if (!$this->_pdo) {
			$this->setPDO();
		}
		return $this->_pdo;
	}

	protected function setPDO(string $query = '') : void {
		$master_only_tables = DB::getDBConfig($this->db, DB::ACCESS_READ)['master_only_tables'] ?? [];
		// Easy to determine read vs. write if query builder is being used
		if ($this->operation) {
			$access = $this->operation === 'SELECT' && !in_array($this->table, $master_only_tables, true) ? DB::ACCESS_READ : DB::ACCESS_WRITE;

		// Default to read if we don't know anything about the query yet
		} else if (!$query) {
			$access = DB::ACCESS_READ;

		// Change to write connection for anything other than SELECT queries
		} else if (!self::isSelectQuery($query)) {
			$access = DB::ACCESS_WRITE;
		} else if ($master_only_tables && preg_match('/from\s+[`"]?(?:' . implode('|', $master_only_tables) . ')[`"]?/i', $query)) {
			$access = DB::ACCESS_WRITE;
		} else if (stripos($query, 'LAST_INSERT_ID') !== false) {
			$access = DB::ACCESS_WRITE;
		} else {
			$access = DB::ACCESS_READ;
		}
		$this->_pdo = DB::getPDO($this->db, $access);
		DB::$_last_access_level_used = $access;
	}

	public static function isSelectQuery(string $query) : bool {
		return (bool)preg_match('%\A\s*(?:/\*.*?\*/\s*)*(?:SELECT |SHOW FULL PROCESSLIST|EXPLAIN(?: EXTENDED)? SELECT).*\Z%si', $query);
	}

	/**
	 * Prepares and executes the given query against PDO.
	 *
	 * @return SQLStatement|int|false A statement object on success, false on failure
	 */
	public function query(string $query, array $args = []) : SQLStatement|int|false {
		$this->setPDO($query); // ensure write is enabled if necessary
		$query = self::modifyQuery($query);
		$t = DB::queryLogEnabled() ? microtime(true) : 0;
		$pdo_statement = $this->pdo()->prepare($query);
		// PDO throws on error by default in PHP 8, convert that to the library's false-return convention
		try {
			$pdo_success = $pdo_statement->execute($args);
		} catch (PDOException $e) {
			$pdo_success = false;
		}
		if ($pdo_success === false) {
			DB::handleError($pdo_statement->errorInfo(), $query, $this->db);
		}
		DB::setLastErrorInfo($pdo_success === false ? $pdo_statement->errorInfo() : [], $this->db);

		if (DB::queryLogEnabled()) {
			DB::logQuery($this->db, $query, $pdo_success, microtime(true) - $t, $pdo_statement->rowCount());
		}

		if ($pdo_success && $this->return_id && $this->operation === 'INSERT') {
			$this->result = (int)$this->pdo()->lastInsertId();
		} else if ($pdo_success) {
			$this->result = new SQLStatement($pdo_statement);
		} else {
			$this->result = false;
		}
		return $this->result;
	}

	/**
	 * Executes the built query.
	 * This should generally not be used publicly except when DB::$auto_execute_updates or DB::$auto_execute is false for
	 * unit testing or other purposes. Shortcut execution functions such as value(), fetch(), fetchAll(), update(),
	 * insert(), delete(), etc. should be used instead for more concise code.
	 *
	 * @return SQLStatement|int|false A statement object on success, false on failure
	 */
	public function execute() : SQLStatement|int|false {
		if (!isset($this->result)) {
			$string = $this->toSQL();
			if (!$string) {
				return false;
			}
			$this->query($string);
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
	 * @return static $this for chaining.
	 */
	public function select(array|string $select, bool $no_escape = false) : static {
		$this->setOperation('SELECT');

		if (is_array($select)) {
			if (!$no_escape) {
				foreach ($select as $key => $column_or_expression) {
					$select[$key] = $this->quoteExpression($column_or_expression);
				}
			}
			$this->select = implode(',', array_filter($select));
		} else if (!$no_escape) {
			$this->select = $this->quoteExpression($select);
		} else {
			$this->select = $select;
		}
		return $this;
	}

	/**
	 * Specifies WHERE conditions for the query.
	 *
	 *   // Adds WHERE `three`=NOW() AND `four` IN('a', 'b') to the query.
	 *   $sql_query->where(['three' => DB::raw('NOW()'), 'four' => ['a', 'b']]);
	 *
	 *   // Adds WHERE `field2` < 20 to the query
	 *   $sql_query->where('field2', '<', 20);
	 *
	 *   // Adds WHERE `field3` > 10 AND `field4` != 'bar'
	 *   $sql_query->where([['field3', '>', 10], ['field4', '!=', 'bar']]);
	 *
	 * @return static $this for chaining.
	 */
	public function where(array|string $array_or_field, string $oper = '', string|int|float|bool|array|null|DBValueRaw $value = null) : static {
		$this->applyWhereConditions($array_or_field, $oper, $value);
		return $this;
	}

	/**
	 * Negated version of SQLQuery::where
	 *
	 * @return static $this for chaining
	 */
	public function whereNot(array|string $array_or_field, string $oper = '', string|int|float|bool|array|null|DBValueRaw $value = null) : static {
		$this->applyWhereConditions($array_or_field, $oper, $value, true);
		return $this;
	}

	/**
	 * Add a raw where clause to the query. This will not be validated or
	 * escaped, so please use the quote() method on any user input.
	 * @return static $this for chaining.
	 */
	public function whereRaw(string $where_clause) : static {
		if ($where_clause !== '') {
			$this->where .= ($this->where ? ' AND ' : '') . $where_clause;
		}
		return $this;
	}

	/**
	 * Add multiple where clauses to the query, joined together with OR.
	 * This can be useful for WHERE IN-like queries, but with multiple columns instead of just 1.
	 *
	 * // Adds WHERE ((`a` = '1' AND `b` = '2') OR (`a` = '3' AND `b` = '4')) to the query
	 * $sql_query->whereOr([['a' => 1, 'b' => 2], ['a' => 3, 'b' => 4]]);
	 *
	 * @param array $wheres a zero-indexed array where each item is a WHERE array supported by the where() method
	 * @return static $this for chaining.
	 */
	public function whereOr(array $wheres) : static {
		$sql = [];
		foreach ($wheres as $where) {
			$sql[] = implode(' AND ', $this->getWhereConditions($where));
		}
		if ($sql) {
			$this->where .= ($this->where ? ' AND ' : '') . '((' . implode(') OR (', $sql) . '))';
		}
		return $this;
	}

	/**
	 * Add a multi-column WHERE IN to the query. This results in a more compact query
	 * than if whereOr were used instead.
	 *
	 * // Adds WHERE ((`a`,`b`) IN (('1','2'),('3','4')) to the query
	 * $sql_query->whereIn(['a', 'b'], [[1, 2], [3, 4]]);
	 *
	 * @return static $this for chaining.
	 */
	public function whereIn(array $columns, array $rows) : static {
		if (!$columns || !$rows) {
			return $this;
		}
		$column_sql = '';
		foreach ($columns as $column) {
			$column_sql .= ($column_sql ? ',' : '') . $this->quoteKeyword($column);
		}
		$column_sql = "($column_sql)";
		$row_sql = '';
		foreach ($rows as $row) {
			$row_sql .= ($row_sql ? ',' : '') . '(' . $this->sqlConditionList($row) . ')';
		}
		$row_sql = "($row_sql)";

		$this->where .= ($this->where ? ' AND ' : '') . "($column_sql IN $row_sql)";
		return $this;
	}

	/**
	 * Specifies a GROUP BY clause for the query.
	 *
	 *   // Adds GROUP BY `column_one`, `column_two` to the query.
	 *   $sql_query->group(['column_one', 'column_two']);
	 *
	 * @param array|string $group The GROUP BY clause.
	 * @return static $this for chaining.
	 */
	public function group(array|string $group) : static {
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
	 * Specifies a HAVING clause for the query. NO ESCAPING IS APPLIED.
	 *
	 *   // Adds HAVING COUNT(*) > 12 to the query.
	 *   $sql_query->having('COUNT(*) > 12');
	 */
	public function having(string $raw_sql) : static {
		$this->having = $raw_sql;
		return $this;
	}

	/**
	 * Specifies an ORDER BY clause for the query.
	 *
	 *   // Adds ORDER BY `column_one` ASC, `column_two` DESC to the query.
	 *   $sql_query->order(['column_one' => 'ASC', 'column_two' => 'DESC']);
	 *
	 * @param array $order The ORDER BY clause.
	 * @param bool $half_escape When true, will not use backticks or escaping on the field name
	 * @return static $this for chaining.
	 * @throws Exception
	 */
	public function order(array $order, bool $half_escape = false) : static {
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
	 *   $sql_query->orderByValues('column_one', [3, 5, 7]);
	 *
	 * @param string $field A field on the table whose values will be used to sort
	 * @param array $values An array of values to sort on
	 */
	public function orderByValues(string $field, array $values) : static {
		if (count($values) === 1) {
			return $this;
		}
		$field = $this->quoteKeyword($field);
		foreach ($values as $key => $value) {
			$values[$key] = $this->sqlValue($value);
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
	 * @return static $this for chaining.
	 */
	public function offset(int $offset) : static {
		$this->offset = $offset;
		return $this;
	}

	/**
	 * Specifies a LIMIT clause for the query.
	 *
	 *   // Adds LIMIT 5 to the query.
	 *   $sql_query->limit(5);
	 *
	 * @param int $limit The LIMIT row count.
	 * @return static $this for chaining.
	 */
	public function limit(int $limit) : static {
		$this->limit = $limit;
		return $this;
	}

	public function useIndex(array|string $index) : static {
		$indexes = is_array($index) ? $index : [$index];
		$this->indexes = array_map(fn($i) => $this->quoteKeyword($i), $indexes);
		return $this;
	}

	public function union(SQLQuery $sql_query) : static {
		$this->unions[] = $sql_query;
		return $this;
	}

	public function innerJoin(string $table, array|string $array_or_field, string $oper = '', string|int|float|bool|array|null|DBValueRaw $value = null) : static {
		$sql = 'INNER JOIN ' . $this->quoteAliasExpression($table) . ' ON ';
		$sql .= implode(' AND ', $this->getWhereConditions($array_or_field, $oper, $value, false, true));
		$this->joins[] = $sql;
		return $this;
	}

	public function leftJoin(string $table, array|string $array_or_field, string $oper = '', string|int|float|bool|array|null|DBValueRaw $value = null) : static {
		$sql = 'LEFT JOIN ' . $this->quoteAliasExpression($table) . ' ON ';
		$sql .= implode(' AND ', $this->getWhereConditions($array_or_field, $oper, $value, false, true));
		$this->joins[] = $sql;
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
	 * @return static|SQLStatement|false $this for chaining, or the execution result when auto-executing.
	 */
	public function update(array $updates, bool $no_escape = false, bool $ignore = false, bool $allow_empty_where = false) : static|SQLStatement|false {
		$this->setOperation($ignore ? 'UPDATE IGNORE' : 'UPDATE');
		$set = [];
		foreach ($updates as $field => $value) {
			if ($no_escape) {
				$set[] = "$field=$value";
			} else {
				$set[] = $this->quoteKeyword($field) . '=' . $this->sqlValue($value);
			}
		}

		$this->update = implode(', ', $set);
		if (DB::$auto_execute && DB::$auto_execute_updates) {
			if (!$this->where && !$allow_empty_where) {
				throw new Exception('Cannot update() without a where clause unless allow_empty_where=true for safety reasons.');
			}
			return $this->execute();
		}
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
	 * @return static|SQLStatement|false $this for chaining, or the execution result when auto-executing.
	 */
	public function increment(array $updates, bool $coalesce_null_to_zero = true) : static|SQLStatement|false {
		$this->setOperation('UPDATE');
		$set = [];
		foreach ($updates as $field => $value) {
			$field = $this->quoteKeyword($field);
			$current = $coalesce_null_to_zero ? "COALESCE($field, 0)" : $field;
			if (is_numeric($value) && $value < 0) {
				$value = abs($value);
				$set[] = "$field=$current - " . $this->sqlValue($value);
			} else {
				$set[] = "$field=$current + " . $this->sqlValue($value);
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
	 * @return SQLStatement|bool A statement object or true on success, false on failure
	 */
	public function updateColumn(string $key_column, string|array $value_column, array $data, bool $ignore = false) : SQLStatement|bool {
		if (is_array($value_column)) {
			$success = true;
			foreach ($value_column as $column) {
				$success = $this->updateColumn($key_column, $column, $data[$column], $ignore) && $success;
			}
			return $success;
		}

		if (count($data) > 1000) {
			$callback = fn($chunk) => $this->updateColumn($key_column, $value_column, $chunk, $ignore);
			return static::chunkQuery($data, $callback, 1000);
		} else if (!$data) {
			return true;
		}

		$key_column_quoted = $this->quoteKeyword($key_column);
		$value_column_quoted = $this->quoteKeyword($value_column);
		$ignore_flag = $ignore ? ' IGNORE' : '';

		$sql = "UPDATE{$ignore_flag} {$this->table_escaped} SET {$value_column_quoted} = CASE {$key_column_quoted}";

		$keys = [];
		foreach ($data as $key => $value) {
			$keys[] = $key;
			$key_string = $this->sqlValue($key);
			$value_string = $this->sqlValue($value, false);
			$sql .= " WHEN {$key_string} THEN {$value_string}";
		}

		$in = $this->sqlConditionIn($key_column, '=', $keys);
		$sql .= " END WHERE {$in}";

		return $this->query($sql);
	}

	/**
	 * Executes a batch update of multiple rows based on the values of the key column.
	 * The first row of data determines which columns will be updated, and they must be present for all rows.
	 *
	 *   // Sets `b` to 2 where `a` is 1, and `b` to 3 where `a` is 10:
	 *   DB::with('table')->updateMultiAssoc('a', [
	 *     1 => ['b' => 2],
	 *     10 => ['b' => 3]
	 *   ]);
	 *
	 * @param string $key_column The name of the column which identifies each given row to be updated, e.g. PRIMARY id
	 * @return SQLStatement|bool a statement object on success, true if $rows was empty and there was nothing to
	 *   be done, or false on query failure
	 */
	public function updateMultiAssoc(string $key_column, array $rows) : SQLStatement|bool {
		if (!$rows) {
			return true;
		}
		$key_column_quoted = $this->quoteKeyword($key_column);
		$columns = array_keys(current($rows));

		$sql = "UPDATE {$this->table_escaped} JOIN (VALUES ";
		$first = true;
		foreach ($rows as $key => $row) {
			$sql .= ($first ? '' : ',') . "ROW(";
			$sql .= $this->sqlValue($key);
			foreach ($columns as $column) {
				$sql .= ',' . $this->sqlValue($row[$column]);
			}
			$sql .= ")";
			$first = false;
		}
		$sql .= ") AS v($key_column_quoted," . implode(',', array_map($this->quoteKeyword(...), $columns)) . ")";
		$sql .= " ON v.$key_column_quoted = {$this->table_escaped}.$key_column_quoted";
		$sql .= " SET ";
		for ($i = 0; $i < count($columns); $i++) {
			$sql .= ($i ? ',' : '') . "{$this->table_escaped}." . $this->quoteKeyword($columns[$i]) . " = v." . $this->quoteKeyword($columns[$i]);
		}

		return $this->query($sql);
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
	 * @return static|SQLStatement|bool $this for chaining, a statement object on success, true if there was no
	 *   data to upsert, or false on query failure
	 */
	public function upsert(array $data = [], array $ignore_columns = [], bool $no_escape = false) : static|SQLStatement|bool {
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
	 * @param array $ignore_columns columns that should NOT be updated on duplicate key
	 * @param array $column_updates [column => 'sql string'] e.g. ['sum_field' => 'sum_field + VALUES(sum_field)']
	 *      where sql string can include VALUES(column) to perform computations such as incrementing
	 * @param bool $no_escape Pass true to enable SQL injection and watch civilization crumble
	 * @return static|SQLStatement|bool $this for chaining, a statement object on success, true if there was no
	 *   data to upsert, or false on query failure
	 */
	public function upsertMulti(array $column_names, array $data, array $ignore_columns = [], array $column_updates = [], bool $no_escape = false) : static|SQLStatement|bool {
		if (!$column_names || !$data) {
			return true;
		}
		$update_str = '';
		foreach ($column_names as $column_name) {
			$column_quoted = $this->quoteKeyword($column_name);
			if (!in_array($column_name, $ignore_columns, true)) {
				$update_str .= ($update_str ? ', ' : '') . "$column_quoted=";
				$update_str .= $column_updates[$column_name] ?? "VALUES($column_quoted)";
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
	 * @param array $ignore_columns columns that should NOT be updated on duplicate key
	 * @param bool $no_escape Pass true to enable SQL injection and watch civilization crumble
	 * @return static|SQLStatement|bool $this for chaining, a statement object on success, true if there was no
	 *   data to upsert, or false on query failure
	 */
	public function upsertMultiAssoc(array $data, array $ignore_columns = [], bool $no_escape = false) : static|SQLStatement|bool {
		if (!$data) {
			return true;
		}
		$column_names = array_keys(reset($data));
		if (!$column_names) {
			return true;
		}
		$update_str = '';
		foreach ($column_names as $column_name) {
			$column_quoted = $this->quoteKeyword($column_name);
			if (!in_array($column_name, $ignore_columns, true)) {
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
	 * @return static|SQLStatement|int|false $this for chaining, or the execution result when auto-executing.
	 * @throws Exception
	 */
	public function insert(array $data = [], bool $ignore = false) : static|SQLStatement|int|false {
		$this->setOperation($ignore ? 'INSERT IGNORE' : 'INSERT');
		$this->data = $data;
		if (DB::$auto_execute) {
			return $this->execute();
		}
		return $this;
	}

	/**
	 * Like insert(), except it will return the "id" of the row inserted.
	 *
	 * @see SQLQuery::insert
	 * @return static|SQLStatement|int|false $this for chaining, or the inserted id when auto-executing.
	 */
	public function insertGetID(array $data = []) : static|SQLStatement|int|false {
		$this->return_id = true;
		return $this->insert($data);
	}

	/**
	 * Executes a batch insert to the selected table based on data provided in an
	 * array of associative arrays. The columns to insert are determined by scanning
	 * all specified rows and checking their keys.
	 *
	 * NOTE: this method is less efficient than insertMulti, given a choice between the two.
	 *
	 *   // Inserts rows into a table specifying `name` and `value`.
	 *   DB::with('table')->insertMultiAssoc([
	 *     ['name' => '_111111', 'value' => 'abc'],
	 *     ['name' => '_222222', 'value' => 'def'],
	 *   ]);
	 *
	 * @param array $data A list of associative arrays mapping column names to
	 *   corresponding values.
	 * @param bool $no_escape Pass true to enable SQL injection and watch civilization crumble
	 * @return static|SQLStatement|bool $this for chaining, a SQLStatement object on success,
	 *   false if the query was not executed correctly, or true if $data was empty and there
	 *   was nothing to be done.
	 */
	public function insertMultiAssoc(array $data, bool $ignore = false, bool $no_escape = false) : static|SQLStatement|bool {
		if (!$data) {
			return true;
		}
		$keys = [];
		foreach ($data as $row) {
			foreach ($row as $key => $value) {
				$keys[$key] = true;
			}
		}
		$keys = array_keys($keys);

		$keys_string = '';
		foreach ($keys as $key) {
			$keys_string .= ($keys_string ? ',' : '') . $this->quoteKeyword($key);
		}
		$value_str = '';
		foreach ($data as $row) {
			$value_str .= ($value_str ? ',' : '') . '(';
			$j = 0;
			foreach ($keys as $key) {
				$value_str .= ($j ? ',' : '');
				$value_str .= $no_escape ? $row[$key] : $this->sqlValue($row[$key]);
				$j++;
			}
			$value_str .= ')';
		}

		$cmd = 'INSERT' . ($ignore ? ' IGNORE' : '');
		$this->sql = "$cmd INTO $this->table_escaped ($keys_string) VALUES $value_str";
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
	 * @param array $rows An array of numeric-keyed arrays
	 * @return static|SQLStatement|bool $this for chaining, a SQLStatement object on success, true if there was no
	 *   data to insert, or false on query failure
	 * @throws Exception
	 */
	public function insertMulti(array $column_names, array $rows, bool $ignore = false, bool $no_escape = false) : static|SQLStatement|bool {
		if (!$column_names || !$rows) {
			return true;
		}

		foreach ($column_names as $key => $column) {
			$column_names[$key] = $this->quoteKeyword($column);
		}
		$column_str = "(" . implode(",", $column_names) . ")";
		$value_str = '';
		$column_count = count($column_names);
		foreach ($rows as $row) {
			$value_str .= ($value_str ? ',' : '') . '(';
			$j = 0;
			foreach ($row as $value) {
				$value_str .= ($j ? "," : "");
				$value_str .= $no_escape ? $value : $this->sqlValue($value);
				$j++;
				// Allow each data row to contain more data than actually being inserted
				if ($j === $column_count) {
					break;
				}
			}
			$value_str .= ')';
		}

		$cmd = 'INSERT' . ($ignore ? ' IGNORE' : '');
		$this->sql = "$cmd INTO $this->table_escaped $column_str VALUES $value_str";
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
	 * @return static|SQLStatement|false $this for chaining, or the execution result when auto-executing.
	 */
	public function delete(array|string $array_or_field = [], string $oper = '', string|int|float|bool|array|null|DBValueRaw $value = null) : static|SQLStatement|false {
		$this->setOperation('DELETE');
		$this->applyWhereConditions($array_or_field, $oper, $value);
		if (DB::$auto_execute && DB::$auto_execute_updates) {
			if (!$this->where) {
				throw new Exception('Cannot delete() without a where clause for safety reasons. To delete an entire table,' .
					' using truncate is more performant anyway.');
			}
			return $this->execute();
		}
		return $this;
	}


	/*** Convenience shortcuts to automatically call execute() and return results ***/

	public function fetch(int $fetch_type = DB::FETCH_ASSOC) : array|object|false {
		$result = $this->execute();
		return is_object($result) ? $result->fetch($fetch_type) : false;
	}
	public function fetchAll(int $fetch_type = DB::FETCH_ASSOC) : array {
		$result = $this->execute();
		return is_object($result) ? $result->fetchAll($fetch_type) : [];
	}
	public function fetchAllAssoc(string|array $key_column, int $fetch_type = DB::FETCH_ASSOC) : array {
		$result = $this->execute();
		return is_object($result) ? $result->fetchAllAssoc($key_column, $fetch_type) : [];
	}
	public function value() : string|int|float|null|false {
		$result = $this->execute();
		return is_object($result) ? $result->value() : false;
	}
	public function values() : array {
		$result = $this->execute();
		return is_object($result) ? $result->values() : [];
	}
	public function assocValues() : array {
		$result = $this->execute();
		return is_object($result) ? $result->assocValues() : [];
	}

	/**
	 * Converts the current built query into a complete SQL string, with all
	 * values escaped and inlined.
	 */
	public function toSQL() : string {
		return match ($this->operation) {
			'SELECT' => $this->buildSelect(),
			'UPDATE' => $this->buildUpdate(),
			'UPDATE IGNORE' => $this->buildUpdate(true),
			'INSERT' => $this->buildInsert(),
			'INSERT IGNORE' => $this->buildInsert(true),
			'DELETE' => $this->buildDelete(),
			default => $this->sql,
		};
	}

	abstract public function getRegexpOperator() : string;

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
	 * @throws Exception
	 */
	protected function getWhereConditions(array|string $array_or_field, string $oper = '', string|int|float|bool|array|null|DBValueRaw $value = null, bool $negate = false, bool $strings_are_keywords = false) : array {
		// Ignore empty WHEREs
		if (!$array_or_field) {
			return [];
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
		if (!array_is_list($array)) {
			foreach ($array as $field => $value) {
				$sql[] = $this->sqlCondition($field, $negate ? '!=' : '=', $value, $strings_are_keywords);
			}

		// e.g. where([['id', '>', 100], ['id', '<', 200]])
		} else {
			// If it's just a single array, convert it to an array of arrays
			if (!is_array($array[0])) {
				$array = [$array];
			}
			foreach ($array as $condition) {
				if ($negate) {
					$condition[1] = self::negateOperator($condition[1]);
				}
				$sql[] = $this->sqlCondition($condition[0], $condition[1], $condition[2], $strings_are_keywords);
			}
		}

		return $sql;
	}

	protected function applyWhereConditions(array|string $array_or_field, string $oper = '', string|int|float|bool|array|null|DBValueRaw $value = null, bool $negate = false) : void {
		$sql = $this->getWhereConditions($array_or_field, $oper, $value, $negate);
		if ($sql) {
			$this->where .= ($this->where ? ' AND ' : '') . implode(' AND ', $sql);
		}
	}

	protected function buildDelete() : string {
		$sql = "DELETE FROM {$this->table_escaped}";

		if ($this->where) {
			$sql .= " WHERE {$this->where}";
		}

		return $sql;
	}

	protected function buildInsert(bool $ignore = false) : string {
		$keys = $this->sqlConditionList(array_keys($this->data), true);
		$values = $this->sqlConditionList(array_values($this->data));

		$sql = "INSERT";
		if ($ignore) {
			$sql .= " IGNORE";
		}
		$sql .= " INTO {$this->table_escaped} ({$keys}) VALUES ({$values})";

		return $sql;
	}

	protected function buildSelect() : string {
		$sql = "SELECT {$this->select} FROM {$this->table_escaped}";

		if ($this->indexes) {
			$sql .= ' USE INDEX (' . implode(',', $this->indexes) . ')';
		}

		if ($this->joins) {
			$sql .= ' ' . implode(' ', $this->joins);
		}

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

	protected function buildUpdate(bool $ignore = false) : string {
		if (!$this->update) {
			return '';
		}
		$ignore_flag = $ignore ? ' IGNORE' : '';
		$sql = "UPDATE{$ignore_flag} {$this->table_escaped} SET {$this->update}";
		if ($this->where) {
			$sql .= " WHERE {$this->where}";
		}
		return $sql;
	}

	protected static function chunkQuery(array $data, callable $callback, int $size = 1000) : SQLStatement|bool {
		$return = true;
		$offset = 0;

		for (;;) {
			$chunk = array_slice($data, $offset, $size, true);
			if (!$chunk) {
				break;
			}

			$return = $callback($chunk);
			if ($return === false) {
				return false;
			}

			$offset += $size;
		}

		return $return;
	}

	protected function sqlCondition(string $field, string $oper, string|int|float|bool|array|null|DBValueRaw $value, bool $strings_are_keywords = false) : string {
		return match ($oper) {
			'=',
			'!=' =>
				is_array($value)
				? $this->sqlConditionIn($field, $oper, $value, $strings_are_keywords)
				: $this->sqlConditionEqual($field, $oper, $value, $strings_are_keywords),

			'<',
			'<=',
			'>',
			'>=',
			'LIKE',
			'NOT LIKE',
			'LIKE BINARY',
			'NOT LIKE BINARY',
			'REGEXP',
			'NOT REGEXP',
			'MATCH' =>
				$this->quoteKeyword($field) . " {$oper} " . $this->sqlValue($value, true, $strings_are_keywords),

			'BETWEEN',
			'NOT BETWEEN' =>
				$this->sqlConditionBetween($field, $oper, $value, $strings_are_keywords),

			default => throw new Exception("Invalid operator ($oper)"),
		};
	}

	protected function sqlConditionBetween(string $field, string $oper, string|int|float|bool|array|null|DBValueRaw $value, bool $strings_are_keywords = false) : string {
		if (!is_array($value) || count($value) !== 2) {
			throw new Exception("$oper operator requires array of length 2");
		}
		$field = $this->quoteKeyword($field);
		$min = $this->sqlValue($value[0], true, $strings_are_keywords);
		$max = $this->sqlValue($value[1], true, $strings_are_keywords);
		return "{$field} {$oper} {$min} AND {$max}";
	}

	protected function sqlConditionEqual(string $field, string $oper, string|int|float|bool|null|DBValueRaw $value, bool $strings_are_keywords = false) : string {
		$chunk = $this->sqlValue($value, true, $strings_are_keywords);
		$field = $this->quoteKeyword($field);
		if ($value === null) {
			$oper = $oper === '!=' ? 'IS NOT' : 'IS';
		}
		return "{$field} {$oper} {$chunk}";
	}

	protected function sqlConditionIn(string $field, string $oper, array $array, bool $strings_are_keywords = false) : string {
		// Ensure values are unique, since query engine might not be smart enough
		// to remove duplicates
		$array = array_unique($array);
		$chunks = $this->sqlConditionList($array, $strings_are_keywords);
		$field = $this->quoteKeyword($field);
		$not = $oper === '!=' ? ' NOT' : '';

		return "{$field}{$not} IN ({$chunks})";
	}

	protected function sqlConditionList(array $array, bool $strings_are_keywords = false) : string {
		$chunks = [];
		foreach ($array as $value) {
			$chunks[] = $this->sqlValue($value, true, $strings_are_keywords);
		}
		return implode(',', $chunks);
	}

	// always_quote can be useful to ensure BTREE indexes on textual fields
	// are utilized, even if the data in the WHERE clause is numeric
	protected function sqlValue(string|int|float|bool|null|DBValueRaw $value, bool $always_quote = true, bool $strings_are_keywords = false) : string {
		// Numerics are primitives, safe to inline without escaping
		if (is_bool($value)) {
			$value = (int)$value;
		}

		if ($value instanceof DBValueRaw) {
			return $value->getValue();
		}

		// Any other object type falls through to the match and throws below
		return match (gettype($value)) {
			'integer',
			'double' =>
				$always_quote ? "'{$value}'" : $value,
			'string' => $strings_are_keywords ? $this->quoteKeyword($value) : $this->quote($value),
			'NULL' => 'NULL',
		};
	}

	protected static function negateOperator(string $oper) : string {
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

	public function rowsAffected() : int|false {
		return is_object($this->result) ? $this->result->rowsAffected() : false;
	}

	/**
	 * Use this to both quote and escape strings, protecting from SQL injection.
	 */
	public function quote(string $text) : string {
		return $this->pdo()->quote($text);
	}

	/**
	 * Use this to quote things like table names and column names
	 */
	public function quoteKeyword(string $text) : string {
		$text = explode('.', $this->escapeKeyword($text));
		// don't backtick '*'
		$end = end($text);
		if ($end === '*') {
			return $this->tick . implode("$this->tick.$this->tick", array_slice($text, 0, count($text) - 1)) . $this->tick . ".$end";
		}
		return $this->tick . implode("$this->tick.$this->tick", $text) . $this->tick;
	}

	public function escapeKeyword(string $text) : string {
		// SQL identifiers can technically contain any utf-8 character
		if (preg_match('/[`"]/', $text)) {
			throw new Exception("Invalid SQL identifier: $text. Value may not contain ` or \".");
		}
		return $text;
	}

	public function quoteExpression(string|DBValueRaw $text) : string {
		if ($text instanceof DBValueRaw) {
			return $text->getValue();
		}
		if ($text === '*' || is_numeric($text)) {
			return $text;
		}
		// Detect function syntax, e.g. MIN(field_name) as min_field
		$is_function = preg_match('/^([a-z_]+)\(([a-z0-9_\*]*)\)(( AS)? ([a-z0-9_]*))?$/i', $text, $matches);
		if ($is_function) {
			if ($matches[2] === '*') {
				$text = $matches[1] . '(*)';
			} else {
				$text = $matches[1] . '(' . $this->tick . $matches[2] . $this->tick . ')';
			}
			if (isset($matches[3])) {
				if (isset($matches[4])) {
					$text .= ' AS';
				}
				$text .= ' ' . $this->tick . $matches[5] . $this->tick;
			}
			return $text;
		}
		return $this->quoteAliasExpression($text);
	}

	public function quoteAliasExpression(string $text) : string {
		// Detect alias syntax
		$is_aliased = preg_match('/^([^\s]+)( AS)? ([^\s]+)$/i', $text, $matches);
		if ($is_aliased) {
			return $this->quoteKeyword($matches[1]) .
			($matches[2] ? ' AS' : '') . ' ' .
			$this->quoteKeyword($matches[3]);
		}
		return $this->quoteKeyword($text);
	}

	abstract protected function getKeywordEscapeChar() : string;

	/**
	 * http://php.net/manual/en/pdo.errorinfo.php
	 * [SQLSTATE error code, Driver-specific error code, Driver-specific error message]
	 */
	public function errorInfo() : array {
		// If the error occurred after a successful PDOStatement was created,
		// the error will be on the statement but NOT on the main database handle
		if (is_object($this->result)) {
			return $this->result->errorInfo() ?: $this->pdo()->errorInfo();
		}
		return $this->pdo()->errorInfo();
	}

	public static function setQueryModifier(callable $function) : void {
		self::$query_modifier = Closure::fromCallable($function);
	}

	protected static function modifyQuery(string $query) : string {
		if (isset(self::$query_modifier)) {
			return (self::$query_modifier)($query);
		}
		return $query;
	}

	public function __toString() : string {
		return $this->toSQL();
	}
}
