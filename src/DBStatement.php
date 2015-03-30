<?php

namespace FindTheBest\DB;
use DB;
use Exception;

abstract class DBStatement {

	protected $result;
	public $success;

	public function __construct($result) {
		$this->result = $result;
		$this->success = (bool)$result;
	}

	/**
	 * @param int $fetch_type
	 * @return array|object|bool
	 */
	abstract public function fetch($fetch_type = DB::FETCH_ASSOC);

	/**
	 * @param int $fetch_type
	 * @return array|bool
	 */
	public function fetchAll($fetch_type = DB::FETCH_ASSOC) {
		// $this->result might be a boolean if the query was an INSERT/UPDATE/DELETE
		if (is_bool($this->result)) {
			return false;
		}
		$rows = [];
		while ($row = $this->fetch($fetch_type)) {
			$rows[] = $row;
		}
		return $rows;
	}

	/**
	 * @param string $key_column
	 * @param int $fetch_type
	 * @return array|bool
	 * @throws Exception
	 */
	public function fetchAllAssoc($key_column, $fetch_type = DB::FETCH_ASSOC) {
		// $this->result might be a boolean if the query was an INSERT/UPDATE/DELETE
		if (is_bool($this->result)) {
			return false;
		}
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
			default:
				throw new Exception("Invalid fetch type: $fetch_type");
		}
	}

	/**
	 * @return string|bool
	 */
	public function value() {
		$row = $this->fetch(DB::FETCH_NUM);
		return $row ? $row[0] : false;
	}

	/**
	 * @return array|bool
	 */
	public function values() {
		// $this->result might be a boolean if the query was an INSERT/UPDATE/DELETE
		if (is_bool($this->result)) {
			return false;
		}
		$values = [];
		while ($row = $this->fetch(DB::FETCH_NUM)) {
			$values[] = $row[0];
		}
		return $values;
	}

	/**
	 * Fetch all values in selected column #2 as values in an associative
	 * array, keyed on values from selected column #1. This ordered syntax is
	 * used instead of having $key_column and $value_column parameters because
	 * it is required for PDO::fetchAll, and because it enforces high performance.
	 *
	 * e.g. SELECT key_column, value_column FROM table ...
	 *
	 * @return array|bool
	 */
	public function assocValues() {
		// $this->result might be a boolean if the query was an INSERT/UPDATE/DELETE
		if (is_bool($this->result)) {
			return false;
		}
		$values = [];
		while ($row = $this->fetch(DB::FETCH_NUM)) {
			$values[$row[0]] = $row[1];
		}
		return $values;
	}

	/**
	 * @return int|bool
	 */
	abstract public function resultCount();

	/**
	 * @return int|bool
	 */
	abstract public function rowsAffected();

}