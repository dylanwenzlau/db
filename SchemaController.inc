<?php

class SchemaController {

	const ENGINE_MYSQL = 'mysql';
	const ENGINE_POSTGRES = 'postgresql';

	public function __construct($db = '') {
		$this->db = $db;
		$this->engine = substr($db, 0, 3) === 'pg_' ? self::ENGINE_POSTGRES : self::ENGINE_MYSQL;
	}

	public static function forge($db) {
		return new static($db);
	}

	public function addColumn($table, $name, $type, $options = []) {
		return static::modifyColumn($table, $name, $type, $options, 'ADD');
	}

	public function modifyColumn($table, $name, $type, $options = [], $action = 'MODIFY') {
		if ($type !== 'TEXT' && strpos($type, 'VARCHAR') !== 0) {
			$options['charset'] = '';
			$options['collate'] = '';
		}
		switch ($this->engine) {
			case static::ENGINE_MYSQL:
			case static::ENGINE_POSTGRES:
				$query = SQLQuery::with($table, $this->db);
				$table = $query->quoteKeyword($table);
				$name = $query->quoteKeyword($name);
				$action = $action === 'ADD' ? 'ADD' : 'MODIFY';
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

				return $query->executeRawQuery($sql);

		}
	}

	public function dropColumn($table, $name) {
		switch ($this->engine) {
			case static::ENGINE_MYSQL:
			case static::ENGINE_POSTGRES:
				$query = SQLQuery::with($table, $this->db);
				$table = $query->quoteKeyword($table);
				$name = $query->quoteKeyword($name);
				$sql = "ALTER TABLE $table DROP COLUMN $name";
				return $query->executeRawQuery($sql);
		}
	}

	public function showColumns($table) {
		$query = SQLQuery::with($table, $this->db);
		$table = $query->quote($table);
		$query->executeRawQuery("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = $table");
		$rows = [];
		while ($mysql_sucks = $query->fetchArray()) {
			$row = [];
			foreach ($mysql_sucks as $key => $value) {
				$row[strtolower($key)] = $value;
			}
			$rows[$row['column_name']] = $row;
		}
		return $rows;
	}

	public function showIndexes($table) {
		$schema = $this->db ? : 'ourfa5_drupal';
		$rows = SQLQuery::with('information_schema.statistics', $this->db)
			->select(['index_name', 'index_type', 'column_name', 'non_unique'])
			->where([
				'table_schema' => $schema,
				'table_name' => $table,
				'index_type' => 'BTREE'
			])
			->order(['index_name' => 'ASC', 'seq_in_index' => 'ASC'])
			->as_arrays();

		$indexes = [];
		foreach ($rows as $row) {
			if (!$indexes[$row['index_name']]) {
				$indexes[$row['index_name']] = [
					'index_name' => $row['index_name'],
					'index_type' => strtolower($row['index_type']),
					'columns' => [$row['column_name']],
					'unique' => !$row['non_unique'],
				];
			} else {
				$indexes[$row['index_name']]['columns'][] = $row['column_name'];
			}
		}

		return $indexes;
	}
}
