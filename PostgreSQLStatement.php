<?php

namespace FTB\core\db;
use DB;
use PDO;
use Exception;

class PostgreSQLStatement extends DBStatement {

	private $legacy;

	public function __construct($result, $legacy) {
		parent::__construct($result);
		$this->legacy = (bool)$legacy;
	}

	public function fetch($fetch_type = DB::FETCH_ASSOC) {
		if (!$this->legacy) {
			return is_object($this->result) ? $this->result->fetch($fetch_type) : false;
		}
		switch ($fetch_type) {
			case DB::FETCH_ASSOC:
				return pg_fetch_assoc($this->result);
			case DB::FETCH_OBJ:
				return pg_fetch_object($this->result);
			case DB::FETCH_NUM:
				return pg_fetch_array($this->result);
			default:
				throw new Exception("Fetch type ($fetch_type) not supported");
		}
	}

	/**
	 * Note: For Postgres, PDOStatement::fetchAll() returns an array of
	 * empty arrays on UPDATE queries, even though nothing has been selected.
	 * MySQL PDO simply returns an empty array. True for both PHP and HHVM.
	 */
	public function fetchAll($fetch_type = DB::FETCH_ASSOC) {
		if (!$this->legacy) {
			return is_object($this->result) ? $this->result->fetchAll($fetch_type) : false;
		}
		return parent::fetchAll($fetch_type);
	}

	public function values() {
		if (!$this->legacy) {
			return is_object($this->result) ? $this->result->fetchAll(PDO::FETCH_COLUMN) : false;
		}
		return parent::values();
	}

	public function resultCount() {
		if (!$this->legacy) {
			// PDO::Statement's rowCount method is not guaranteed to return the
			// result count for SELECT statements, but IS guaranteed to return
			// rows affected for INSERT/UPDATE/DELETE queries
			return is_object($this->result) ? $this->result->rowCount() : false;
		}
		return pg_num_rows($this->result);
	}

	public function rowsAffected() {
		if (!$this->legacy) {
			// Yes, PDOStatement::rowCount actually means rows affected
			return is_object($this->result) ? $this->result->rowCount() : false;
		}
		return 0;
	}

}