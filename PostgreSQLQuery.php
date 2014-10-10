<?php

namespace FTB\core\db;

class PostgreSQLQuery extends SQLQuery {

	protected $pdo;
	protected static $pg_connections = [];

	public function __construct($table, $db = '', array $allowed_ops = []) {
		parent::__construct($table, $db, $allowed_ops);
		if (FTB_USE_PDO_PGSQL) {
			$this->pdo = get_pdo($this->db);
		} else {
			if (!isset(static::$pg_connections[$this->db])) {
				global $db_url;
				$info = parse_url($db_url[$this->db]);
				$pdo_url = "host={$info['host']} dbname={$this->db} user={$info['user']} password={$info['pass']}";
				static::$pg_connections[$this->db] = pg_connect($pdo_url);
			}
		}
	}

	/**
	 * http://php.net/manual/en/pdo.errorinfo.php
	 * @return array
	 * [SQLSTATE error code, Driver-specific error code, Driver-specific error message]
	 */
	public function errorInfo() {
		if (FTB_USE_PDO_PGSQL) {
			return $this->pdo->errorInfo();
		} else {
			return [
				-1, //TODO
				-1, //TODO
				pg_last_error(static::$pg_connections[$this->db])
			];
		}
	}

	protected function build_insert() {
		$sql = parent::build_insert();
		if ($this->return_id) {
			$sql .= " RETURNING \"id\"";
		}
		return $sql;
	}

	public function upsert(array $data = [], array $skip_on_update = [], $no_escape = false) {
		throw new Exception("UPSERT (INSERT INTO ON DUPLICATE KEY) method is NOT implemented for PostgreSQL");
	}

	public function query($query, array $args = []) {
		static::$last_insert_id = null;
		if (static::$debug === true) {
			$t = microtime(true);
			if (FTB_USE_PDO_PGSQL) {
				$pdo_result = $this->pdo->query($query);
			} else {
				$pdo_result = pg_query(static::$pg_connections[$this->db], $query);
			}
			$this->logQuery('postgresql', $query, (bool)$pdo_result, microtime(true) - $t);
		} else {
			if (FTB_USE_PDO_PGSQL) {
				$pdo_result = $this->pdo->query($query);
			} else {
				$pdo_result = pg_query(static::$pg_connections[$this->db], $query);
			}
		}

		if ($pdo_result && $this->return_id && $this->operation === 'INSERT') {
			// If the ID was manually inserted (as opposed to auto-increment), just return it
			if ($this->data['id']) {
				return $this->result = $this->data['id'];
			}
			return $this->result = (new PostgreSQLStatement($pdo_result))->value();
		}
		if ($pdo_result) {
			return $this->result = new PostgreSQLStatement($pdo_result);
		}
		return $this->result = false;
	}

	public function quote($text) {
		if (FTB_USE_PDO_PGSQL) {
			return $this->pdo->quote($text);
		} else {
			return pg_escape_literal(static::$pg_connections[$this->db], $text);
		}
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
