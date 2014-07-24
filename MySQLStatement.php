<?php

namespace FTB\core\db;
use DB;

class MySQLStatement extends DBStatement {

	public function fetch($fetch_type = DB::FETCH_ASSOC) {
		switch ($fetch_type) {
			case DB::FETCH_ASSOC:
				return db_fetch_array($this->result);
			case DB::FETCH_OBJ:
				return db_fetch_object($this->result);
		}
	}

	public function rowCount() {
		return mysql_num_rows($this->result);
	}

}