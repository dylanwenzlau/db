<?php

namespace FindTheBest\DB;
use DB;
use PDO;

class PostgreSQLStatement extends DBStatement {

	public function fetch($fetch_type = DB::FETCH_ASSOC) {
		return is_object($this->result) ? $this->result->fetch($fetch_type) : false;
	}

	/**
	 * Note: For Postgres, PDOStatement::fetchAll() returns an array of
	 * empty arrays on UPDATE queries, even though nothing has been selected.
	 * MySQL PDO simply returns an empty array. True for both PHP and HHVM.
	 */
	public function fetchAll($fetch_type = DB::FETCH_ASSOC) {
		return is_object($this->result) ? $this->result->fetchAll($fetch_type) : false;
	}

	public function values() {
		return is_object($this->result) ? $this->result->fetchAll(PDO::FETCH_COLUMN) : false;
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