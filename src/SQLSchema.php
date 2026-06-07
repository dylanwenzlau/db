<?php

namespace DB;
use DB;

abstract class SQLSchema {

	protected readonly array $db_config;

	public function __construct(protected readonly string $db = '') {
		$this->db_config = DB::getDBConfig($db, DB::ACCESS_WRITE);
	}

	/******************************************************************/
	/************************** COLUMNS! ******************************/
	/******************************************************************/

	abstract public function addColumn(string $table, string $name, string $type, array $options = []) : SQLStatement|false;

	abstract public function alterColumn(string $table, string $name, string $type, array $options = []) : SQLStatement|false;

	abstract public function renameColumn(string $table, string $old_name, string $new_name) : SQLStatement|false;

	public function dropColumn(string $table, string $name) : SQLStatement|false {
		$query = DB::with($table, $this->db);
		$table = $query->quoteKeyword($table);
		$name = $query->quoteKeyword($name);
		$sql = "ALTER TABLE $table DROP COLUMN $name";
		return $query->query($sql);
	}

	public function showColumns(string $table, array $column_names = []) : array {
		$query = DB::with('information_schema.columns', $this->db)
			->select('*')
			->where(['table_name' => $table])
			->where(['table_schema' => $this->db_config['database']])
			->where($column_names ? ['column_name' => $column_names] : [])
			->execute();
		$rows = [];
		while ($row = $query->fetch()) {
			$lower = [];
			foreach ($row as $key => $value) {
				$lower[strtolower($key)] = $value;
			}
			$rows[$lower['column_name']] = $lower;
		}
		return $rows;
	}

	public function hasColumn(string $table, string $column) : bool {
		return count($this->showColumns($table, [$column])) === 1;
	}

	/******************************************************************/
	/************************** INDEXES! ******************************/
	/******************************************************************/

	public function showIndexes(string $table) : array {
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
			if (!isset($indexes[$row['index_name']])) {
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

	abstract public function addIndexes(string $table, array $indexes, array $options = []) : SQLStatement|false;

	abstract public function addIndex(string $table, string $name, string $type, array $columns, bool $unique = false, array $options = []) : SQLStatement|false;

	public function addPrimaryIndex(string $table, array $columns) : SQLStatement|false {
		$query = DB::with('', $this->db);
		$table = $query->quoteKeyword($table);
		$columns = array_map($query->quoteKeyword(...), $columns);
		return $query->query("ALTER TABLE $table ADD PRIMARY KEY (" . implode(',', $columns) . ")");
	}

	public function dropIndex(string $table, string $name) : SQLStatement|false {
		$query = DB::with($table, $this->db);
		$table = $query->quoteKeyword($table);
		$name = $query->quoteKeyword($name);
		return $query->query("ALTER TABLE $table DROP INDEX $name");
	}

	public function dropAllIndexes(string $table, bool $include_primary = false) : void {
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
	 * @param string $table table name pattern (using '%' where necessary)
	 * @return array array of tables that match the pattern
	 */
	public function findTablesLike(string $table) : array {
		$query = DB::with($table, $this->db);
		$table = $query->quote($table);
		$query->query("SHOW TABLES LIKE $table");
		return $query->values();
	}

	public function tableExists(string $table) : bool {
		return (bool)DB::with('information_schema.tables', $this->db)
			->select(1)
			->where(['table_schema' => $this->db_config['database'], 'table_name' => $table])
			->value();
	}

	public function dropTable(string $table) : SQLStatement|false {
		$query = DB::with($table, $this->db);
		return $query->query("DROP TABLE " . $query->quoteKeyword($table));
	}

	public function dropTableIfExists(string $table) : SQLStatement|false {
		$query = DB::with($table, $this->db);
		return $query->query("DROP TABLE IF EXISTS " . $query->quoteKeyword($table));
	}

	public function dropView(string $view) : SQLStatement|false {
		$query = DB::with($view, $this->db);
		return $query->query("DROP VIEW " . $query->quoteKeyword($view));
	}

	public function truncateTable(string $table) : SQLStatement|false {
		$query = DB::with($table, $this->db);
		return $query->query("TRUNCATE TABLE " . $query->quoteKeyword($table));
	}

	public function copyTable(string $from_table, string $to_table, bool $include_data = false) : SQLStatement|false {
		$query = DB::with($from_table, $this->db);
		$from_table = $query->quoteKeyword($from_table);
		$to_table = $query->quoteKeyword($to_table);
		$success = $query->query("CREATE TABLE $to_table (LIKE $from_table)");
		if (!$success || !$include_data) {
			return $success;
		}
		return $query->query("INSERT INTO $to_table SELECT * FROM $from_table");
	}

	public function renameTable(string $old_table_name, string $new_table_name) : SQLStatement|false {
		$query = DB::with('', $this->db);
		$old_table_name = $query->quoteKeyword($old_table_name);
		$new_table_name = $query->quoteKeyword($new_table_name);
		return $query->query("ALTER TABLE $old_table_name RENAME TO $new_table_name");
	}

	public function swapTable(string $table_one, string $table_two) : SQLStatement|false {
		$query = DB::with('', $this->db);
		$table_one = $query->quoteKeyword($table_one);
		$table_two = $query->quoteKeyword($table_two);
		// Create a random table name 62 characters long
		// Maximum table name length in mysql is 64 (63 for NDB storage engine)
		$table_temp = $query->quoteKeyword(self::randomTableName(62));
		return $query->query("RENAME TABLE $table_one TO $table_temp, $table_two TO $table_one, $table_temp TO $table_two");
	}

	abstract public function tableSizeInfo(string $table, string $schema) : array|false;

	private static function randomTableName(int $length) : string {
		$chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
		$str = '';
		for ($i = 0; $i < $length; $i++) {
			$str .= $chars[mt_rand(0, 51)];
		}
		return $str;
	}
}
