<?php

namespace FTB\core\db;
use DB;

abstract class DBStatement {

	protected $result;
	public $success;

	public function __construct($result) {
		$this->result = $result;
		$this->success = (bool)$result;
	}

	abstract public function fetch($fetch_type = DB::FETCH_ASSOC);

	public function fetchAll($fetch_type = DB::FETCH_ASSOC) {
		$rows = [];
		while ($row = $this->fetch($fetch_type)) {
			$rows[] = $row;
		}
		return $rows;
	}

	public function fetchAllAssoc($key_column, $fetch_type = DB::FETCH_ASSOC) {
		$rows = [];
		switch ($fetch_type) {
			case DB::FETCH_ASSOC:
				while ($row = $this->fetch($fetch_type)) {
					$rows[$row[$key_column]] = $row;
				}
				return $rows;
			case DB::FETCH_OBJ:
				while ($row = $this->fetch($fetch_type)) {
					$rows[$row->$key_column] = $row;
				}
				return $rows;
		}
	}

	public function value() {
		$row = $this->fetch(DB::FETCH_NUM);
		return $row ? $row[0] : false;
	}

	public function values() {
		$values = [];
		while ($row = $this->fetch(DB::FETCH_NUM)) {
			$values[] = $row[0];
		}
		return $values;
	}

	public function assocValues($key_column, $value_column) {
		$values = [];
		while ($row = $this->fetch()) {
			$values[$row[$key_column]] = $row[$value_column];
		}
		return $values;
	}

	abstract public function rowCount();

}