<?php

namespace FTB\core\db;
use DB;

class MySQLSchemaController extends SQLSchemaController {

	public function addColumn($table, $name, $type, $options = []) {
		return $this->_alterColumn($table, $name, $type, $options, 'ADD');
	}

	public function alterColumn($table, $name, $type, $options = []) {
		return $this->_alterColumn($table, $name, $type, $options, 'MODIFY');
	}

	public function _alterColumn($table, $name, $type, $options = [], $action = 'MODIFY') {
		$type = strtoupper($type);
		if ($type !== 'TEXT' && strpos($type, 'VARCHAR') !== 0) {
			$options['charset'] = '';
			$options['collate'] = '';
		}

		$query = DB::with($table, $this->db);
		$table = $query->quoteKeyword($table);
		$name = $query->quoteKeyword($name);
		$sql = "ALTER TABLE $table $action COLUMN $name $type";
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

	public function tableSizeInfo($table, $schema) {
		return DB::with('information_schema.tables', $this->db)
			->select(['data_length', 'index_length'])
			->where(['table_schema' => $schema, 'table_name' => $table])
			->fetch();
	}

}