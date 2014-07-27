<?php

namespace FTB\core\db;
use DB;

abstract class SQLSchemaController {

	const ENGINE_MYSQL = 'mysql';
	const ENGINE_POSTGRES = 'postgresql';

	protected $db;
	protected $engine;

	public function __construct($db = '', $engine = self::ENGINE_MYSQL) {
		$this->db = $db;
		$this->engine = $engine;
	}

	/******************************************************************/
	/************************** COLUMNS! ******************************/
	/******************************************************************/

	abstract public function addColumn($table, $name, $type, $options = []);

	abstract public function alterColumn($table, $name, $type, $options = []);

	public function dropColumn($table, $name) {
		$query = DB::with($table, $this->db);
		$table = $query->quoteKeyword($table);
		$name = $query->quoteKeyword($name);
		$sql = "ALTER TABLE $table DROP COLUMN $name";
		return $query->query($sql);
	}

	public function showColumns($table, Array $column_names = []) {
		$query = DB::with($table, $this->db);
		$table = $query->quote($table);
		$query_str = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = $table";

		if ($column_names) {
			$query_str .= " AND column_name in (";
			foreach ($column_names as $column_name) {
				$query_str .= $query->quote($column_name) . ',';
			}
			$query_str = rtrim($query_str, ',');
			$query_str .= ")";
		}
		$query->query($query_str);
		$rows = [];
		while ($mysql_sucks = $query->fetch()) {
			$row = [];
			foreach ($mysql_sucks as $key => $value) {
				$row[strtolower($key)] = $value;
			}
			$rows[$row['column_name']] = $row;
		}
		return $rows;
	}

	public function hasColumn($table, $column) {
		return count(self::showColumns($table, [$column])) === 1;
	}

	/******************************************************************/
	/************************** INDEXES! ******************************/
	/******************************************************************/

	public function showIndexes($table) {
		$schema = $this->db ? : 'ourfa5_drupal';
		$rows = DB::with('information_schema.statistics', $this->db)
			->select(['index_name', 'index_type', 'column_name', 'non_unique'])
			->where([
				'table_schema' => $schema,
				'table_name' => $table,
				'index_type' => 'BTREE'
			])
			->order(['index_name' => 'ASC', 'seq_in_index' => 'ASC'])
			->fetchAll();

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
			throw new \Exception('No indexes provided');
		}
		$query = DB::with($table, $this->db);
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

		return $query->query($sql);
	}

	public function addIndex($table, $name, $type, array $columns, $unique = false) {
		return $this->addIndexes($table, [[
			'name' => $name,
		    'type' => $type,
		    'columns' => $columns,
		    'unique' => $unique,
		]]);
	}

	public function addPrimaryIndex($table, array $columns) {
		$query = DB::with('', $this->db);
		$table = $query->quoteKeyword($table);
		foreach ($columns as $key => $column) {
			$columns[$key] = $query->quoteKeyword($column);
		}
		return $query->query("ALTER TABLE $table ADD PRIMARY KEY (" . implode(',', $columns) . ")");
	}

	public function dropIndex($table, $name) {
		$query = DB::with($table, $this->db);
		$table = $query->quoteKeyword($table);
		$name = $query->quoteKeyword($name);
		return $query->query("ALTER TABLE $table DROP INDEX $name");
	}

	/******************************************************************/
	/************************** TABLES! *******************************/
	/******************************************************************/

	public function tableExists($table) {
		$query = DB::with($table, $this->db);
		$table = $query->quote($table);
		$query->query("SHOW TABLES LIKE $table");
		return $query->value();
	}

	public function dropTable($table) {
		$query = DB::with($table, $this->db);
		return $query->query("DROP TABLE " . $query->quoteKeyword($table));
	}

	public function dropTableIfExists($table) {
		$query = DB::with($table, $this->db);
		return $query->query("DROP TABLE IF EXISTS " . $query->quoteKeyword($table));
	}

	public function dropView($view) {
		$query = DB::with($view, $this->db);
		return $query->query("DROP VIEW " . $query->quoteKeyword($view));
	}

	public function truncateTable($table) {
		$query = DB::with($table, $this->db);
		return $query->query("TRUNCATE TABLE " . $query->quoteKeyword($table));
	}

	public function copyTable($from_table, $to_table, $include_data = false) {
		$query = DB::with($from_table, $this->db);
		$from_table = $query->quoteKeyword($from_table);
		$to_table = $query->quoteKeyword($to_table);
		$success = $query->query("CREATE TABLE $to_table (LIKE $from_table)");
		if (!$success || !$include_data) {
			return $success;
		}
		return $query->query("INSERT INTO $to_table SELECT * FROM $from_table");
	}

	public function renameTable($old_table_name, $new_table_name) {
		$query = DB::with('', $this->db);
		$old_table_name = $query->quoteKeyword($old_table_name);
		$new_table_name = $query->quoteKeyword($new_table_name);
		return $query->query("ALTER TABLE $old_table_name RENAME TO $new_table_name");
	}
}
