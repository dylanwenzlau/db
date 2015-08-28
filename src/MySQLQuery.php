<?php

namespace FindTheBest\DB;
use DB;
use PDO;
use PDOException;

class MySQLQuery extends SQLQuery {

	// Override so we can use MySQL's special FIELD() function
	public function orderByValues($field, array $values) {
		if (count($values) === 1) {
			return $this;
		}
		$field = $this->quoteKeyword($field);
		foreach ($values as $key => $value) {
			$values[$key] = $this->sql_value($value);
		}
		$values = implode(',', $values);
		$this->order .= "FIELD({$field}, {$values})";
		return $this;
	}

	public function query($query, array $args = []) {
		$this->setPDO($query); // ensure write is enabled if necessary
		$query = self::modifyQuery($query);
		$t = DB::queryLogEnabled() ? microtime(true) : 0;
		$pdo_statement = $this->pdo()->prepare($query);
		// Apparently hhvm forces PDO to throw exceptions on failure
		try {
			$pdo_success = $pdo_statement->execute($args);
		} catch (PDOException $e) {
			$pdo_success = false;
		}
		if ($pdo_success === false) {
			DB::handleError($pdo_statement->errorInfo(), $query);
		}

		if (DB::queryLogEnabled()) {
			DB::logQuery($this->db, $query, $pdo_success, microtime(true) - $t, $pdo_statement->rowCount());
		}

		if ($pdo_success && $this->return_id && $this->operation === 'INSERT') {
			$this->result = $this->retrieveNewID();
		} else if ($pdo_success) {
			$this->result = new MySQLStatement($pdo_statement);
		} else {
			$this->result = false;
		}
		return $this->result;
	}

	public function estimatedCount($exact_count_threshold = 10000) {
		$this->query("EXPLAIN SELECT COUNT(*) FROM $this->table_escaped");
		$explain_rows = $this->fetchAll();
		if (!$explain_rows) {
			return false;
		}

		// MyISAM has O(1) exact counts
		if ($explain_rows[0] && $explain_rows[0]['Extra'] === 'Select tables optimized away') {
			$this->query("SELECT COUNT(*) FROM $this->table_escaped");
			return $this->value();
		}

		$row_count = 0;
		foreach ($explain_rows as $row) {
			// If "rows" is '' or null or something, it might be some weird case with a VIEW
			if (!is_numeric($row['rows'])) {
				return false;
			}
			$row_count += $row['rows'];
		}

		if ($row_count < $exact_count_threshold) {
			$this->query("SELECT COUNT(*) FROM $this->table_escaped");
			return $this->value();
		}

		return $row_count;
	}

	public function getRegexpOperator() {
		return 'REGEXP';
	}

	protected function getKeywordEscapeChar() {
		return '`';
	}

	public function showProcesslist() {
		// We only care about the processlist on master?
		$this->_pdo = DB::getPDO($this->db, 'write');
		$this->query("SHOW FULL PROCESSLIST");
		$rows = [];
		while ($row = $this->fetch()) {
			$rows[] = [
				'process_id' => $row['Id'],
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

	private function retrieveNewID() {
		// If the ID was manually inserted (as opposed to auto-increment), just return it
		if ($this->data['id'] && !$this->insert_multi_count) {
			return $this->data['id'];
		}
		$pdo_statement = $this->pdo()->prepare("SELECT LAST_INSERT_ID()");
		$pdo_statement->execute();
		$inserted_id = $pdo_statement->fetch(PDO::FETCH_NUM);
		return $this->insert_multi_count ? range($inserted_id, $inserted_id + $this->insert_multi_count) : $$inserted_id;
	}
}