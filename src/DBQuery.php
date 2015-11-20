<?php

namespace Graphiq\DB;
use DB;
use Exception;

abstract class DBQuery {

	protected $table;
	protected $db;
	protected $allowed_operations = ['SELECT', 'UPDATE', 'UPDATE IGNORE', 'INSERT', 'INSERT IGNORE', 'UPSERT', 'DELETE'];

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

	abstract public function execute();


	/*** SELECT ***/

	abstract public function select($select);
	abstract public function where($array_or_field, $oper = '', $value = null);
	abstract public function whereNot($array_or_field, $oper = '', $value = null);
	abstract public function group($group);
	abstract public function having($having);
	abstract public function order(array $order, $half_escape = false);
	abstract public function orderByValues($column, array $values);
	abstract public function offset($offset);
	abstract public function limit($limit);

	abstract public function estimatedCount($exact_count_threshold);
	abstract public function showProcesslist();


	/*** UPDATE/INSERT/DELETE ***/

	abstract public function update(array $updates, $no_escape = false);
	abstract public function increment(array $updates, $coalesce_null_to_zero = true);
	abstract public function insert(array $row = []);
	abstract public function upsert(array $data = [], array $unique_key_fields = [], $no_escape = false);
	abstract public function insertGetID(array $row = []);
	abstract public function delayed();
	abstract public function delete($array_or_field, $oper = '', $value = null);


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
	public function assocValues() {
		$result = $this->execute();
		return is_object($result) ? $result->assocValues() : [];
	}
	abstract public function rowsAffected();


	/*** Batch UPDATE/INSERT/DELETE ***/

	abstract public function updateColumn($key_column, $value_column, array $data);
	abstract public function insertMulti(array $column_names, array $rows);
	abstract public function insertMultiAssoc(array $data);
}