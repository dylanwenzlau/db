<?php

namespace FTB\core\db;
use DB;
use PDO;
use Exception;

class PostgreSQLQuery extends SQLQuery {

	protected $pdo;
	protected static $pg_connections = [];
	protected static $pdo_connections = [];
	private $legacy;

	public function __construct($table, $db = '', array $allowed_ops = []) {
		parent::__construct($table, $db, $allowed_ops);
		$db_config = DB::getDBConfig($db);
		$this->legacy = (bool)$db_config['use_legacy_driver'];
		if (!$this->legacy) {
			if (!isset(static::$pdo_connections[$db])) {
				$pdo_url = "pgsql:host={$db_config['host']};dbname={$db_config['database']}";
				static::$pdo_connections[$db] = new PDO($pdo_url, $db_config['username'], $db_config['password']);
			}
			$this->pdo = static::$pdo_connections[$db];
		} else {
			if (!isset(static::$pg_connections[$this->db])) {
				$pg_url =
					"host={$db_config['host']}" .
					" dbname={$db_config['database']}" .
					" user={$db_config['username']}" .
					" password={$db_config['password']}";
				static::$pg_connections[$this->db] = pg_connect($pg_url);
			}
		}
	}

	/**
	 * http://php.net/manual/en/pdo.errorinfo.php
	 * @return array
	 * [SQLSTATE error code, Driver-specific error code, Driver-specific error message]
	 */
	public function errorInfo() {
		if (!$this->legacy) {
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

	public function upsert(array $data = [], array $unique_key_fields = [], $no_escape = false) {
		throw new Exception("UPSERT (INSERT INTO ON DUPLICATE KEY) method is NOT implemented for PostgreSQL");
	}

	public function query($query, array $args = []) {
		static::$last_insert_id = null;
		if (static::$debug === true) {
			$t = microtime(true);
			if (!$this->legacy) {
				$pdo_result = $this->pdo->query($query);
			} else {
				$pdo_result = pg_query(static::$pg_connections[$this->db], $query);
			}
			$this->logQuery('postgresql', $query, (bool)$pdo_result, microtime(true) - $t);
		} else {
			if (!$this->legacy) {
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
			return $this->result = (new PostgreSQLStatement($pdo_result, $this->legacy))->value();
		}
		if ($pdo_result) {
			return $this->result = new PostgreSQLStatement($pdo_result, $this->legacy);
		}
		return $this->result = false;
	}

	public function quote($text) {
		if (!$this->legacy) {
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

	public function rowsAffected() {
		if (!$this->legacy) {
			return is_object($this->result) ? $this->result->rowsAffected() : false;
		}
		return pg_affected_rows(static::$pg_connections[$this->db]);
	}

}
