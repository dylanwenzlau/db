<?php

namespace Graphiq\DB;
use DB;

abstract class SQLSchemaController {

	const ENGINE_MYSQL = 'mysql';
	const ENGINE_POSTGRES = 'postgresql';

	protected $db;
	protected $engine;
	protected $db_config;

	public function __construct($db = '', $engine = self::ENGINE_MYSQL) {
		$this->db = $db;
		$this->engine = $engine;
		$this->db_config = DB::getDBConfig($db, 'write');
	}

	/******************************************************************/
	/************************** COLUMNS! ******************************/
	/******************************************************************/

	abstract public function addColumn($table, $name, $type, $options = []);

	abstract public function alterColumn($table, $name, $type, $options = []);

	abstract public function renameColumn($table, $old_name, $new_name);

	public function dropColumn($table, $name) {
		$query = DB::with($table, $this->db);
		$table = $query->quoteKeyword($table);
		$name = $query->quoteKeyword($name);
		$sql = "ALTER TABLE $table DROP COLUMN $name";
		return $query->query($sql);
	}

	public function showColumns($table, array $column_names = []) {
		$query = DB::with('information_schema.columns', $this->db)
			->select('*')
			->where(['table_name' => $table])
			->where(['table_schema' => $this->db_config['database']])
			->where($column_names ? ['column_name' => $column_names] : [])
			->execute();
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

	/**
	 * @param string $table
	 * @return array
	 */
	public function showIndexes($table) {
		$rows = DB::with('information_schema.statistics', $this->db)
			->select(['index_name', 'index_type', 'column_name', 'sub_part', 'non_unique'])
			->where([
				'table_schema' => $this->db_config['database'],
				'table_name' => $table,
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
					'column_sub_parts' => [$row['sub_part']],
					'unique' => !$row['non_unique'],
				];
			} else {
				$indexes[$row['index_name']]['columns'][] = $row['column_name'];
				$indexes[$row['index_name']]['column_sub_parts'][] = $row['sub_part'];
			}
		}

		return $indexes;
	}

	abstract public function addIndexes($table, array $indexes, array $options = []);

	abstract public function addIndex($table, $name, $type, array $columns, $unique = false, array $options = []);

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

	public function dropAllIndexes($table, $include_primary = false) {
		foreach ($this->showIndexes($table) as $index) {
			if ($include_primary || $index['name'] !== 'PRIMARY') {
				$this->dropIndex($table, $index['name']);
			}
		}
	}

	/******************************************************************/
	/************************** TABLES! *******************************/
	/******************************************************************/

	/**
	 * Returns all tables with a given naming pattern (e.g. dir_stats_%)
	 *
	 * @param $table: table name pattern (using '%' where necessary)
	 * @return array: array of tables that match the pattern
	 */
	public function findTablesLike($table) {
		$query = DB::with($table, $this->db);
		$table = $query->quote($table);
		$query->query("SHOW TABLES LIKE $table");
		return $query->values();
	}

	public function tableExists($table) {
		return (bool)DB::with('information_schema.tables', $this->db)
			->select(1)
			->where(['table_schema' => $this->db_config['database'], 'table_name' => $table])
			->value();
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

	public function swapTable($table_one, $table_two) {
		$query = DB::with('', $this->db);
		$table_one = $query->quoteKeyword($table_one);
		$table_two = $query->quoteKeyword($table_two);
		// Create a random table name 62 characters long
		// Maximum table name length in mysql is 64 (63 for NDB storage engine)
		$table_temp = $query->quoteKeyword(self::randomTableName(62));
		return $query->query("RENAME TABLE $table_one TO $table_temp, $table_two TO $table_one, $table_temp TO $table_two");
	}

	abstract public function tableSizeInfo($table, $schema);

	private static function randomTableName($length) {
		$chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
		$str = '';
		for ($i = 0; $i < $length; $i++) {
			$str .= $chars[mt_rand(0, 51)];
		}
		return $str;
	}
}
