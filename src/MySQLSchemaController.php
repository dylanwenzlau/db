<?php

namespace Graphiq\DB;
use DB;
use Exception;

class MySQLSchemaController extends SQLSchemaController {

	private static $allowed_lock_modes = ['NONE', 'SHARED', 'EXCLUSIVE', 'DEFAULT'];

	public function addColumn($table, $name, $type, $options = []) {
		unset($options['name']);
		return $this->_alterColumn($table, $name, $type, $options, 'ADD');
	}

	public function alterColumn($table, $name, $type, $options = []) {
		return $this->_alterColumn($table, $name, $type, $options, 'CHANGE');
	}

	public function _alterColumn($table, $name, $type, $options = [], $action = 'CHANGE') {
		$type = strtoupper($type);
		if ($type !== 'TEXT' && strpos($type, 'VARCHAR') !== 0) {
			$options['charset'] = '';
			$options['collate'] = '';
		}

		$query = DB::with($table, $this->db);
		$table = $query->quoteKeyword($table);
		$name = $query->quoteKeyword($name);
		$new_name = $options['name'] ? ' ' . $query->quoteKeyword($options['name']) : '';
		$sql = "ALTER TABLE $table $action COLUMN {$name}{$new_name} $type";
		if ($options['charset'] && $options['collate'] && $this->engine === static::ENGINE_MYSQL) {
			$sql .= " CHARACTER SET {$options['charset']} COLLATE {$options['collate']}";
		}
		if ($options['not_null']) {
			$sql .= " NOT NULL";
		}
		if (array_key_exists('default', $options)) {
			if ($options['default'] === null) {
				$sql .= " DEFAULT NULL";
			} else {
				$sql .= " DEFAULT " . $query->quote($options['default']);
			}
		}

		return $query->query($sql);
	}

	public function renameColumn($table, $old_name, $new_name) {
		$column_config = reset($this->showColumns($table, [$old_name]));
		$column_type = $column_config['column_type'];

		$query = DB::with($table, $this->db);
		$table = $query->quoteKeyword($table);
		$old_name = $query->quoteKeyword($old_name);
		$new_name = $query->quoteKeyword($new_name);

		$query_str = "ALTER TABLE $table CHANGE COLUMN $old_name $new_name $column_type";
		return $query->query($query_str);
	}

	/**
	 * Add some indexes to a table.
	 * @param string $table
	 * @param array $indexes Each item in this array may be either a string column name,
	 *      or an array in the format [name => 'Name', length => 128], to specify column length
	 * @param array $options
	 * @return bool|MySQLStatement
	 * @throws Exception
	 */
	public function addIndexes($table, array $indexes, array $options = []) {
		if (empty($indexes)) {
			throw new Exception('No indexes provided');
		}
		$query = DB::with($table, $this->db);
		$table = $query->quoteKeyword($table);
		$adds = [];
		foreach ($indexes as $index) {
			if ($index['type'] !== 'btree') {
				continue;
			}
			foreach ($index['columns'] as $key => $column) {
				if (is_array($column)) {
					$index['columns'][$key] = $query->quoteKeyword($column['name']);
					if (isset($column['length'])) {
						$index['columns'][$key] .= '(' . (int)$column['length'] . ')';
					}
				} else {
					$index['columns'][$key] = $query->quoteKeyword($column);
				}
			}
			$name = $query->quoteKeyword($index['name']);
			$unique = $index['unique'] ? ' UNIQUE' : '';
			$adds[] = "ADD$unique INDEX $name (" . implode(',', $index['columns']) . ")";
		}
		$sql = "ALTER TABLE $table " . implode(', ', $adds);

		// Add lock mode if specified. These were introduced in the MySQL 5.6 "Online DDL" feature
		if ($options['lock'] && in_array($options['lock'], self::$allowed_lock_modes)) {
			$sql .= ", LOCK = {$options['lock']}";
		}

		return $query->query($sql);
	}

	public function addIndex($table, $name, $type, array $columns, $unique = false, array $options = []) {
		return $this->addIndexes($table, [[
			'name' => $name,
			'type' => $type,
			'columns' => $columns,
			'unique' => $unique,
		]], $options);
	}

	public function tableSizeInfo($table, $schema) {
		return DB::with('information_schema.tables', $this->db)
			->select(['data_length', 'index_length'])
			->where(['table_schema' => $schema, 'table_name' => $table])
			->fetch();
	}

}