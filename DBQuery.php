<?php

namespace FTB\core\db;
use Exception;

abstract class DBQuery {

	protected $table;
	protected $db;
	protected $allowed_operations = ['SELECT', 'UPDATE', 'INSERT', 'DELETE'];

	/**
	 * Creates a new SQLQuery instance set with a specified table name.
	 *
	 * @param string $table The table name.
	 * @param string $db
	 * @param array $allowed_ops
	 * @return DBQuery A new instance.
	 */
	public function __construct($table, $db = '', array $allowed_ops = []) {
		$this->table = $table;
		$this->db = $db;
		$this->allowed_operations = $allowed_ops ?: $this->allowed_operations;
	}

	public function set_operation($operation) {
		if ($this->allowed_operations && !in_array($operation, $this->allowed_operations)) {
			throw new Exception("$operation is not allowed");
		}
		$this->operation = $operation;
	}

	abstract public function execute();


	/*** SELECT ***/

	abstract public function select($select = '*');
	abstract public function where(/* conditions */);
	abstract public function whereNot(/* conditions */);
	abstract public function group($group);
	abstract public function having($having);
	abstract public function order(array $order, $half_escape = false);
	abstract public function order_by_values($column, array $values);
	abstract public function offset($offset);
	abstract public function limit($limit);

	abstract public function fetchArray();
	abstract public function fetchObject();
	abstract public function rowCount();


	/*** UPDATE/INSERT/DELETE ***/

	abstract public function update($updates);
	abstract public function execute_column_update($key_column, $value_column, array $data);
	abstract public function insert(array $row);
	abstract public function insertGetID(array $row);
	abstract public function delayed();
	abstract public function insert_multi(array $column_names, array $rows);
	abstract public function insert_multi_assoc(array $data);
	abstract public function delete(/* where condition */);
}