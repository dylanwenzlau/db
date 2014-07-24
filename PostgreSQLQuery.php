<?php

namespace FTB\core\db;
use SQLQuery;
use PDO;

class PostgreSQLQuery extends SQLQuery {

	protected $pdo;

	public function __construct($table, $db = '', array $allowed_ops = []) {
		parent::__construct($table, $db, $allowed_ops);
		$this->pdo = get_pdo($this->db);
	}

	public function fetchArray() {
		if (!isset($this->result)) {
			$this->execute();
		}
		return $this->result ? $this->result->fetch(PDO::FETCH_ASSOC) : false;
	}

	/**
	 * @deprecated
	 */
	public function fetchObject() {
		if (!isset($this->result)) {
			$this->execute();
		}
		return $this->result ? $this->result->fetch(PDO::FETCH_OBJ) : false;
	}

	// Override to take advantage of PDO's fetchAll()
	public function as_arrays() {
		if (!isset($this->result)) {
			$this->execute();
		}
		return $this->result ? $this->result->fetchAll(PDO::FETCH_ASSOC) : [];
	}

	// Override to take advantage of PDO's fetchAll()
	public function values() {
		if (!isset($this->result)) {
			$this->execute();
		}
		return $this->result ? $this->result->fetchAll(PDO::FETCH_COLUMN) : [];
	}

	public function rowCount() {
		return is_object($this->result) ? $this->result->rowCount() : 0;
	}

	protected function build_insert() {
		$sql = parent::build_insert();
		if ($this->return_id) {
			$sql .= " RETURNING \"id\"";
		}
		return $sql;
	}

	protected function db_query($query, array $args = []) {
		static::$last_insert_id = null;
		if (static::$debug === true) {
			$t = microtime(true);
			$this->result = $this->pdo->query($query);
			$this->logQuery('postgresql', $query, microtime(true) - $t);
		} else {
			$this->result = $this->pdo->query($query);
		}
		if ($this->result && $this->return_id && $this->operation === 'INSERT') {
			return $this->value();
		}
		return (bool)$this->result;
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

}
