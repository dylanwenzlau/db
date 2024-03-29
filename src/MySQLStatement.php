<?php

namespace DB;
use DB;
use PDO;

class MySQLStatement extends DBStatement {

	public function fetch($fetch_type = DB::FETCH_ASSOC) {
		return is_object($this->result) ? $this->result->fetch($fetch_type) : false;
	}

	/**
	 * @param int $fetch_type
	 * @return array|bool
	 */
	public function fetchAll($fetch_type = DB::FETCH_ASSOC) {
		return is_object($this->result) ? $this->result->fetchAll($fetch_type) : false;
	}

	/**
	 * @return array|bool
	 */
	public function values() {
		return is_object($this->result) ? $this->result->fetchAll(PDO::FETCH_COLUMN) : false;
	}

	// 4X faster than manually building the array in PHP, 1.3X faster in HHVM
	public function assocValues() {
		if (!is_object($this->result)) {
			return false;
		}
		return $this->result->fetchAll(PDO::FETCH_COLUMN | PDO::FETCH_ASSOC | PDO::FETCH_UNIQUE);
	}

	public function resultCount() {
		// PDO::Statement's rowCount method is not guaranteed to return the
		// result count for SELECT statements, but IS guaranteed to return
		// rows affected for INSERT/UPDATE/DELETE queries
		return is_object($this->result) ? $this->result->rowCount() : false;
	}

	public function rowsAffected() {
		// Yes, PDOStatement::rowCount actually means rows affected
		return is_object($this->result) ? $this->result->rowCount() : false;
	}

	public function errorInfo() {
		return is_object($this->result) ? $this->result->errorInfo() : false;
	}

}