<?php

namespace FTB\core\db;
use DB;
use Exception;

class MySQLStatement extends DBStatement {

	public function fetch($fetch_type = DB::FETCH_ASSOC) {
		switch ($fetch_type) {
			case DB::FETCH_ASSOC:
				return mysql_fetch_assoc($this->result);
			case DB::FETCH_OBJ:
				return mysql_fetch_object($this->result);
			case DB::FETCH_NUM:
				return mysql_fetch_row($this->result);
			default:
				throw new Exception("Fetch type ($fetch_type) not supported");
		}
	}

	public function rowCount() {
		return mysql_num_rows($this->result);
	}

}