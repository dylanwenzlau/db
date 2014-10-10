<?php

namespace FTB\core\db;
use DB;
use Exception;

abstract class DBQuery {

	protected static $debug = false;

	protected $table;
	protected $db;
	protected $allowed_operations = ['SELECT', 'UPDATE', 'INSERT', 'INSERT IGNORE', 'UPSERT', 'DELETE'];

	public function __construct($table, $db = '', array $allowed_ops = []) {
		$this->table = $table;
		$this->db = $db;
		$this->allowed_operations = $allowed_ops ?: $this->allowed_operations;
	}

	protected function set_operation($operation) {
		if ($this->allowed_operations && !in_array($operation, $this->allowed_operations)) {
			throw new Exception("$operation is not allowed");
		}
		$this->operation = $operation;
	}

	/**
	 * Set whether DBQuery should collect debug info for all queries.
	 * @param bool $debug
	 */
	public static function setDebug($debug) {
		static::$debug = (bool)$debug;
	}

	abstract public function execute();


	/*** SELECT ***/

	abstract public function select($select);
	abstract public function where(/* conditions */);
	abstract public function whereNot(/* conditions */);
	abstract public function group($group);
	abstract public function having($having);
	abstract public function order(array $order, $half_escape = false);
	abstract public function orderByValues($column, array $values);
	abstract public function offset($offset);
	abstract public function limit($limit);

	abstract public function showProcesslist();


	/*** UPDATE/INSERT/DELETE ***/

	abstract public function update(array $updates, $no_escape = false);
	abstract public function increment(array $updates, $coalesce_null_to_zero = true);
	abstract public function insert(array $row = []);
	abstract public function upsert(array $row = []);
	abstract public function insertGetID(array $row = []);
	abstract public function delayed();
	abstract public function delete(/* where condition */);


	/*** Convenience shortcuts to automatically call execute() and return results ***/

	public function fetch($fetch_type = DB::FETCH_ASSOC) {
		$result = $this->execute();
		return is_object($result) ? $result->fetch($fetch_type) : false;
	}
	public function fetchAll($fetch_type = DB::FETCH_ASSOC) {
		$result = $this->execute();
		return is_object($result) ? $result->fetchAll($fetch_type) : [];
	}
	public function fetchAllAssoc($key_column, $fetch_type = DB::FETCH_ASSOC) {
		$result = $this->execute();
		return is_object($result) ? $result->fetchAllAssoc($key_column, $fetch_type) : [];
	}
	public function value() {
		$result = $this->execute();
		return is_object($result) ? $result->value() : false;
	}
	public function values() {
		$result = $this->execute();
		return is_object($result) ? $result->values() : [];
	}
	public function assocValues($key_column, $value_column) {
		$result = $this->execute();
		return is_object($result) ? $result->assocValues($key_column, $value_column) : [];
	}


	/*** Batch UPDATE/INSERT/DELETE ***/

	abstract public function updateColumn($key_column, $value_column, array $data);
	abstract public function insertMulti(array $column_names, array $rows);
	abstract public function insertMultiAssoc(array $data);
}