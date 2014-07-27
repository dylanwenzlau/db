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

}