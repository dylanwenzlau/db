<?php

namespace FTB\core\db;

class MySQLQuery extends SQLQuery {

	// Override so we can use MySQL's special FIELD() function
	public function orderByValues($field, array $values) {
		if (count($values) === 1) {
			return $this;
		}
		$field = $this->quoteKeyword($field);
		foreach ($values as $key => $value) {
			$values[$key] = $this->quote($value);
		}
		$values = implode(',', $values);
		$this->order .= "FIELD({$field}, {$values})";
		return $this;
	}

	protected function sql_value(&$value, &$is_placeholder) {
		if (is_string($value)) {
			$is_placeholder = true;
			return "'%s'";
		}
		return parent::sql_value($value, $is_placeholder);
	}

	public function query($query, array $args = []) {
		static::$last_insert_id = null;
		$t = static::$debug === true ? microtime(true) : 0;
		if ($this->db) {
			$result = static::ftb_db_query($this->db, $query, $args);
		} else {
			$result = db_query($query, $args);
		}
		if (static::$debug === true) {
			$this->logQuery('mysql', $query, (bool)$result, microtime(true) - $t);
		}

		if ($result && $this->return_id && $this->operation === 'INSERT') {
			// If the ID was manually inserted (as opposed to auto-increment), just return it
			if ($this->data['id']) {
				return $this->result = $this->data['id'];
			}
			$query = "SELECT LAST_INSERT_ID()";
			return $this->result = db_result($this->db ? static::ftb_db_query($this->db, $query) : db_query($query));
		}
		if ($result) {
			return $this->result = new MySQLStatement($result);
		}
		return $this->result = false; //intended to assign and return
	}

	public function quote($text) {
		return "'" . db_escape_string($text) . "'";
	}

	public function getRegexpOperator() {
		return 'REGEXP';
	}

	protected function getKeywordEscapeChar() {
		return '`';
	}

	public function showProcesslist() {
		// TODO: remove this FTB-specific connection manager hack
		$this->query("/* cm:skip */ SHOW FULL PROCESSLIST");
		$rows = [];
		while ($row = $this->fetch()) {
			$rows[] = [
				'process_id' => $row['id'],
				'database' => $row['db'],
				'username' => $row['User'],
				'client_hostname' => explode(':', $row['Host'])[0],
				'client_port' => explode(':', $row['Host'])[1],
				'time' => $row['Time'],
				'state' => $row['State'],
				'query' => $row['Info'],
				'command' => $row['Command'],
			];
		}
		return $rows;
	}

	/*
	 * To be deleted as soon as we can handle this stuff in a better place
	 * @deprecated
	 */
	protected static function ftb_db_query($db, $query, array $args = []) {
		// Connect to correct database
		global $current_db;
		$current_db = $db ?: 'default';
		db_set_active($current_db);
		$matches = [];

		// The main query call
		if (IS_LOCAL && stripos($query, '`field_stats_') !== false && preg_match_all('/(FROM|JOIN) `(.*)` WHERE/i', $query, $matches)) {
			$table = $matches[2][0];
			$sc = new MySQLSchemaController();
			if (!$sc->tableExists($table)) {
				// We only want to display this message once...
				static $local_missing_table = [];
				if (!$local_missing_table[$table]) {
					drupal_set_message("Attempting to dump missing db1 table $table");
					$local_missing_table[$table] = true;
				}
				`fdump -l $table`;
			}
		}

		$resource = db_query($query, $args);

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

		return $resource;
	}
}