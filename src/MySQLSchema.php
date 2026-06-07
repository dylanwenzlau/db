<?php

namespace DB;
use DB;
use Exception;

class MySQLSchema extends SQLSchema {

	private static array $allowed_lock_modes = ['NONE', 'SHARED', 'EXCLUSIVE', 'DEFAULT'];

	public function addColumn(string $table, string $name, string $type, array $options = []) : SQLStatement|false {
		unset($options['name']);
		return $this->applyColumn($table, $name, $type, $options, 'ADD');
	}

	public function alterColumn(string $table, string $name, string $type, array $options = []) : SQLStatement|false {
		return $this->applyColumn($table, $name, $type, $options, 'CHANGE');
	}

	private function applyColumn(string $table, string $name, string $type, array $options, string $action) : SQLStatement|false {
		$type = strtoupper($type);
		if ($type !== 'TEXT' && !str_starts_with($type, 'VARCHAR')) {
			$options['charset'] = '';
			$options['collate'] = '';
		}

		$query = DB::with($table, $this->db);
		$table = $query->quoteKeyword($table);
		$name = $query->quoteKeyword($name);
		$new_name = !empty($options['name']) ? ' ' . $query->quoteKeyword($options['name']) : '';
		$sql = "ALTER TABLE $table $action COLUMN {$name}{$new_name} $type";
		if (!empty($options['charset']) && !empty($options['collate'])) {
			$sql .= " CHARACTER SET {$options['charset']} COLLATE {$options['collate']}";
		}
		$sql .= ($options['not_null'] ?? false) ? ' NOT NULL' : ' NULL';
		if (array_key_exists('default', $options)) {
			$sql .= $options['default'] === null ? " DEFAULT NULL" : " DEFAULT " . $query->quote($options['default']);
		}

		return $query->query($sql);
	}

	public function renameColumn(string $table, string $old_name, string $new_name) : SQLStatement|false {
		$columns = $this->showColumns($table, [$old_name]);
		$column_type = reset($columns)['column_type'];

		$query = DB::with($table, $this->db);
		$table = $query->quoteKeyword($table);
		$old_name = $query->quoteKeyword($old_name);
		$new_name = $query->quoteKeyword($new_name);

		$query_str = "ALTER TABLE $table CHANGE COLUMN $old_name $new_name $column_type";
		return $query->query($query_str);
	}

	/**
	 * Add some indexes to a table.
	 * @param array $indexes Each item in this array may be either a string column name,
	 *      or an array in the format [name => 'Name', length => 128], to specify column length
	 * @throws Exception
	 */
	public function addIndexes(string $table, array $indexes, array $options = []) : SQLStatement|false {
		if (!$indexes) {
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
			$unique = ($index['unique'] ?? false) ? ' UNIQUE' : '';
			$adds[] = "ADD$unique INDEX $name (" . implode(',', $index['columns']) . ")";
		}
		$sql = "ALTER TABLE $table " . implode(', ', $adds);

		// Add lock mode if specified. These were introduced in the MySQL 5.6 "Online DDL" feature
		$lock = $options['lock'] ?? '';
		if ($lock && in_array($lock, self::$allowed_lock_modes, true)) {
			$sql .= ", LOCK = {$lock}";
		}

		return $query->query($sql);
	}

	public function addIndex(string $table, string $name, string $type, array $columns, bool $unique = false, array $options = []) : SQLStatement|false {
		return $this->addIndexes($table, [[
			'name' => $name,
			'type' => $type,
			'columns' => $columns,
			'unique' => $unique,
		]], $options);
	}

	public function tableSizeInfo(string $table, string $schema) : array|false {
		return DB::with('information_schema.tables', $this->db)
			->select(['data_length', 'index_length'])
			->where(['table_schema' => $schema, 'table_name' => $table])
			->fetch();
	}

}
