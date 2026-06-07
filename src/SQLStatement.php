<?php

namespace DB;
use DB;
use Exception;
use PDO;
use PDOStatement;

class SQLStatement {

	public bool $success;

	public function __construct(protected PDOStatement $result) {
		$this->success = true;
	}

	public function fetch(int $fetch_type = DB::FETCH_ASSOC) : array|object|false {
		return $this->result->fetch($fetch_type);
	}

	public function fetchAll(int $fetch_type = DB::FETCH_ASSOC) : array {
		return $this->result->fetchAll($fetch_type);
	}

	/**
	 * @param string|array $key_column If array, N-depth nested associative arrays will be created based
	 *  on the values in the $key_column array, where N is the length of the $key_column array.
	 * @throws Exception
	 */
	public function fetchAllAssoc(string|array $key_column, int $fetch_type = DB::FETCH_ASSOC) : array {
		$rows = [];
		// code is basically duplicated between these two cases because object syntax is different :/
		switch ($fetch_type) {
			case DB::FETCH_ASSOC:
				if (is_array($key_column)) {
					$num_keys = count($key_column);
					while ($row = $this->fetch($fetch_type)) {
						$pointer = &$rows;
						for ($i = 0; $i < $num_keys - 1; $i++) {
							if (!isset($pointer[$row[$key_column[$i]]])) {
								$pointer[$row[$key_column[$i]]] = [];
							}
							$pointer = &$pointer[$row[$key_column[$i]]];
						}
						$pointer[$row[$key_column[$num_keys - 1]]] = $row;
						unset($pointer);
					}
				} else {
					while ($row = $this->fetch($fetch_type)) {
						$rows[$row[$key_column]] = $row;
					}
				}
				return $rows;
			case DB::FETCH_OBJ:
				if (is_array($key_column)) {
					$num_keys = count($key_column);
					while ($row = $this->fetch($fetch_type)) {
						$pointer = &$rows;
						for ($i = 0; $i < $num_keys - 1; $i++) {
							if (!isset($pointer[$row->{$key_column[$i]}])) {
								$pointer[$row->{$key_column[$i]}] = [];
							}
							$pointer = &$pointer[$row->{$key_column[$i]}];
						}
						$pointer[$row->{$key_column[$num_keys - 1]}] = $row;
						unset($pointer);
					}
				} else {
					while ($row = $this->fetch($fetch_type)) {
						$rows[$row->$key_column] = $row;
					}
				}
				return $rows;
			default:
				throw new Exception("Invalid fetch type: $fetch_type");
		}
	}

	public function value() : string|int|float|null|false {
		$row = $this->fetch(DB::FETCH_NUM);
		return $row ? $row[0] : false;
	}

	public function values() : array {
		return $this->result->fetchAll(PDO::FETCH_COLUMN);
	}

	/**
	 * Fetch all values in selected column #2 as values in an associative
	 * array, keyed on values from selected column #1. This ordered syntax is
	 * used instead of having $key_column and $value_column parameters because
	 * it is required for PDO::fetchAll, and because it enforces high performance.
	 *
	 * e.g. SELECT key_column, value_column FROM table ...
	 *
	 * 4X faster than manually building the array in PHP
	 */
	public function assocValues() : array {
		return $this->result->fetchAll(PDO::FETCH_COLUMN | PDO::FETCH_ASSOC | PDO::FETCH_UNIQUE);
	}

	/**
	 * PDOStatement's rowCount method is not guaranteed to return the
	 * result count for SELECT statements, but IS guaranteed to return
	 * rows affected for INSERT/UPDATE/DELETE queries
	 */
	public function resultCount() : int {
		return $this->result->rowCount();
	}

	/**
	 * PDOStatement::rowCount actually means rows affected
	 */
	public function rowsAffected() : int {
		return $this->result->rowCount();
	}

	public function errorInfo() : array {
		return $this->result->errorInfo();
	}

}
