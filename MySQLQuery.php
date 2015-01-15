<?php

namespace FTB\core\db;
use PDO;

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

	public function query($query, array $args = []) {
		static::$last_insert_id = null;
		$t = static::$debug === true ? microtime(true) : 0;
		$pdo_statement = $this->pdo->prepare($query);
		$pdo_success = $pdo_statement->execute($args);

		if (static::$debug === true) {
			$this->logQuery('mysql', $query, $pdo_success, microtime(true) - $t);
		}

		if ($pdo_success && $this->return_id && $this->operation === 'INSERT') {
			// If the ID was manually inserted (as opposed to auto-increment), just return it
			if ($this->data['id']) {
				$this->result = $this->data['id'];
			} else {
				$pdo_statement = $this->pdo->prepare("SELECT LAST_INSERT_ID()");
				$pdo_statement->execute();
				$this->result = $pdo_statement->fetch(PDO::FETCH_NUM)[0];
			}
		} else if ($pdo_success) {
			$this->result = new MySQLStatement($pdo_statement);
		} else {
			$this->result = false;
		}
		return $this->result;
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
}