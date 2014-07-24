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

	public function fetchAllAssoc($key_column) {
		$rows = [];
		while ($row = $this->fetch()) {
			$rows[$row[$key_column]] = $row;
		}
		return $rows;
	}

	public function value() {
		$row = $this->fetch();
		return $row ? current($row) : false;
	}

	public function values() {
		$values = [];
		while ($row = $this->fetch()) {
			$values[] = current($row);
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