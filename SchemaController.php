<?php

namespace FTB\core\db;
use SQLQuery;

class SchemaController {

	const ENGINE_MYSQL = 'mysql';
	const ENGINE_POSTGRES = 'postgresql';

	protected $db;
	protected $engine;

	public function __construct($db = '', $engine = self::ENGINE_MYSQL) {
		$this->db = $db;
		$this->engine = $engine;
	}

	public static function forge($db) {
		$engine = substr($db, 0, 3) === 'pg_' ? self::ENGINE_POSTGRES : self::ENGINE_MYSQL;
		switch ($engine) {
			case self::ENGINE_MYSQL:
				return new static($db, $engine);
			case self::ENGINE_POSTGRES:
				return new PostgresqlSchemaController($db);
		}
	}

	/******************************************************************/
	/************************** COLUMNS! ******************************/
	/******************************************************************/

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
		$query = SQLQuery::with($table, $this->db);
		$table = $query->quoteKeyword($table);
		$name = $query->quoteKeyword($name);
		$sql = "ALTER TABLE $table DROP COLUMN $name";
		return $query->executeRawQuery($sql);
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

	/******************************************************************/
	/************************** INDEXES! ******************************/
	/******************************************************************/

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
					'name' => $row['index_name'],
					'type' => strtolower($row['index_type']),
					'columns' => [$row['column_name']],
					'unique' => !$row['non_unique'],
				];
			} else {
				$indexes[$row['index_name']]['columns'][] = $row['column_name'];
			}
		}

		return $indexes;
	}

	public function addIndexes($table, array $indexes) {
		if (empty($indexes)) {
			throw new Exception('No indexes provided');
		}
		$query = SQLQuery::with($table, $this->db);
		$table = $query->quoteKeyword($table);
		$adds = [];
		foreach ($indexes as $index) {
			if ($index['type'] !== 'btree') {
				continue;
			}
			foreach ($index['columns'] as $key => $column) {
				$index['columns'][$key] = $query->quoteKeyword($column);
			}
			$name = $query->quoteKeyword($index['name']);
			$unique = $index['unique'] ? ' UNIQUE' : '';
			$adds[] = "ADD$unique INDEX $name (" . implode(',', $index['columns']) . ")";
		}
		$sql = "ALTER TABLE $table " . implode(', ', $adds);

		return $query->executeRawQuery($sql);
	}

	public function addIndex($table, $name, $type, array $columns, $unique = false) {
		return $this->addIndexes($table, [[
			'name' => $name,
		    'type' => $type,
		    'columns' => $columns,
		    'unique' => $unique,
		]]);
	}

	public function dropIndex($table, $name) {
		$query = SQLQuery::with($table, $this->db);
		$table = $query->quoteKeyword($table);
		$name = $query->quoteKeyword($name);
		return $query->executeRawQuery("ALTER TABLE $table DROP INDEX $name");
	}

	/******************************************************************/
	/************************** TABLES! *******************************/
	/******************************************************************/

	public function tableExists($table) {
		$query = SQLQuery::with($table, $this->db);
		$table = $query->quote($table);
		$query->executeRawQuery("SHOW TABLES LIKE $table");
		return $query->value();
	}

	public function dropTable($table) {
		$query = SQLQuery::with($table, $this->db);
		return $query->executeRawQuery("DROP TABLE " . $query->quoteKeyword($table));
	}

	public function dropTableIfExists($table) {
		$query = SQLQuery::with($table, $this->db);
		return $query->executeRawQuery("DROP TABLE IF EXISTS " . $query->quoteKeyword($table));
	}

	public function dropView($view) {
		$query = SQLQuery::with($view, $this->db);
		return $query->executeRawQuery("DROP VIEW " . $query->quoteKeyword($view));
	}

	public function truncateTable($table) {
		$query = SQLQuery::with($table, $this->db);
		return $query->executeRawQuery("TRUNCATE TABLE " . $query->quoteKeyword($table));
	}

	public function copyTable($from_table, $to_table, $include_data = false) {
		$query = SQLQuery::with($from_table, $this->db);
		$from_table = $query->quoteKeyword($from_table);
		$to_table = $query->quoteKeyword($to_table);
		$success = $query->executeRawQuery("CREATE TABLE $to_table (LIKE $from_table)");
		if (!$success || !$include_data) {
			return $success;
		}
		return $query->executeRawQuery("INSERT INTO $to_table SELECT * FROM $from_table");
	}
}
