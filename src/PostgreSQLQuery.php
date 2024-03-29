<?php

namespace DB;
use DB;
use Exception;
use PDOException;

class PostgreSQLQuery extends SQLQuery {

	protected function build_insert($ignore = false) {
		$sql = parent::build_insert();
		if ($this->return_id) {
			$sql .= " RETURNING \"id\"";
		}
		return $sql;
	}

	public function upsert(array $data = [], array $unique_key_fields = [], $no_escape = false) {
		throw new Exception("UPSERT (INSERT INTO ON DUPLICATE KEY) method is NOT implemented for PostgreSQL");
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
			DB::handleError($pdo_statement->errorInfo(), $query, $this->db);
		}
		DB::setLastErrorInfo($pdo_success === false ? $pdo_statement->errorInfo() : [], $this->db);

		if (DB::queryLogEnabled()) {
			DB::logQuery($this->db, $query, $pdo_success, microtime(true) - $t, $pdo_statement->rowCount());
		}

		if ($pdo_success && $this->return_id && $this->operation === 'INSERT') {
			// If the ID was manually inserted (as opposed to auto-increment), just return it
			if ($this->data['id']) {
				$this->result = $this->data['id'];
			} else {
				$this->result = (new PostgreSQLStatement($pdo_statement));
				$this->result = $this->insert_multi_count ? $this->result->values() : $this->result->value();
			}
		} else if ($pdo_success) {
			$this->result = new PostgreSQLStatement($pdo_statement);
		} else {
			$this->result = false;
		}
		return $this->result;
	}

	public function estimatedCount($exact_count_threshold = 10000) {
		$this->query("SELECT reltuples FROM pg_class WHERE relname = " . $this->quote($this->table));
		$row_count = $this->value();
		if ($row_count < $exact_count_threshold) {
			$this->query("SELECT COUNT(*) FROM $this->table_escaped");
			return $this->value();
		}
		return $row_count;
	}

	public function getRegexpOperator() {
		return '~';
	}

	public function getKeywordEscapeChar() {
		return '"';
	}

	public function showProcesslist() {
		$this->query("SELECT * FROM pg_stat_activity");
		$rows = [];
		while ($row = $this->fetch()) {
			$rows[] = [
				'process_id' => $row['pid'],
				'database' => $row['datname'],
				'username' => $row['usename'],
				'client_hostname' => $row['client_hostname'] ?: $row['client_addr'],
				'client_port' => $row['client_port'],
				'time' => time() - strtotime($row['query_start']),
				'state' => $row['state'],
				'query' => $row['query'],
				'command' => '',
			];
		}
		return $rows;
	}

}
