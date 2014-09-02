<?php

namespace FTB\core\db;
use PDO;

class PostgreSQLQuery extends SQLQuery {

	protected $pdo;

	public function __construct($table, $db = '', array $allowed_ops = []) {
		parent::__construct($table, $db, $allowed_ops);
		$this->pdo = get_pdo($this->db);
	}

	protected function build_insert() {
		$sql = parent::build_insert();
		if ($this->return_id) {
			$sql .= " RETURNING \"id\"";
		}
		return $sql;
	}

	public function query($query, array $args = []) {
		static::$last_insert_id = null;
		if (static::$debug === true) {
			$t = microtime(true);
			$pdo_result = $this->pdo->query($query);
			$this->logQuery('postgresql', $query, (bool)$pdo_result, microtime(true) - $t);
		} else {
			$pdo_result = $this->pdo->query($query);
		}

		if ($pdo_result && $this->return_id && $this->operation === 'INSERT') {
			return $this->result = (new PostgreSQLStatement($pdo_result))->value();
		}
		if ($pdo_result) {
			return $this->result = new PostgreSQLStatement($pdo_result);
		}
		return $this->result = false;
	}

	public function quote($text) {
		return $this->pdo->quote($text);
	}

	public function getRegexpOperator() {
		return '~';
	}

	public function getKeywordEscapeChar() {
		return '"';
	}

	public function setPDO(PDO $pdo) {
		$this->pdo = $pdo;
	}

	public function showProcesslist() {
		$this->query("SELECT * FROM pg_stat_activity");
		$rows = [];
		while ($row = $this->fetch()) {
			$rows[] = [
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
