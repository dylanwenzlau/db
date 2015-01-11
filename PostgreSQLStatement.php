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

	public function rowCount() {
		if (!$this->legacy) {
			// Ironically PDO::Statement does have a rowCount method, but it really means rowsAffected, not result count
			throw new Exception("PDO does not support rowCount");
		}
		return pg_num_rows($this->result);
	}

}