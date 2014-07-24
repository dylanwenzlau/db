<?php

namespace FTB\core\db;
use DB;
use PDO;

class PostgreSQLStatement extends DBStatement {

	public function fetch($fetch_type = DB::FETCH_ASSOC) {
		return is_object($this->result) ? $this->result->fetch($fetch_type) : false;
	}

	public function fetchAll($fetch_type = DB::FETCH_ASSOC) {
		return is_object($this->result) ? $this->result->fetchAll($fetch_type) : [];
	}

	public function values() {
		return is_object($this->result) ? $this->result->fetchAll(PDO::FETCH_COLUMN) : [];
	}

	public function rowCount() {
		return is_object($this->result) ? $this->result->rowCount() : 0;
	}

}