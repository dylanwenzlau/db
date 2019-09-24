<?php

namespace DB;
use DB;
use Exception;

class PostgreSQLSchemaController extends SQLSchemaController {

	public function addColumn($table, $name, $type, $options = []) {
		$query = DB::with($table, $this->db);
		$table = $query->quoteKeyword($table);
		$name = $query->quoteKeyword($name);
		$sql = "ALTER TABLE $table ADD COLUMN $name $type";
		if ($options['not_null']) {
			$sql .= " NOT NULL";
		}
		if (array_key_exists('default', $options)) {
			if ($options['default'] !== null) {
				$sql .= " DEFAULT " . $query->quote($options['default']);
			}
		}

		return $query->query($sql);
	}

	public function alterColumn($table, $name, $type, $options = []) {
		$query = DB::with($table, $this->db);
		$table = $query->quoteKeyword($table);
		$name = $query->quoteKeyword($name);
		if ($options['not_null']) {
			$alters = ["ALTER COLUMN $name TYPE $type USING CAST(($name AS $type)"];
			$alters[] = "ALTER COLUMN $name SET NOT NULL";
		} else {
			$alters = ["ALTER COLUMN $name TYPE $type USING (NULLIF($name, '')::$type)"];
			$alters[] = "ALTER COLUMN $name DROP NOT NULL";
		}
		if (array_key_exists('default', $options)) {
			if ($options['default'] === null) {
				$alters[] = "ALTER COLUMN $name SET DEFAULT NULL";
			} else {
				$alters[] = "ALTER COLUMN $name SET DEFAULT " . $query->quote($options['default']);
			}
		} else {
			$alters[] = "ALTER COLUMN $name DROP DEFAULT";
		}

		$sql = "ALTER TABLE $table " . implode(', ', $alters);

		return $query->query($sql);
	}

	public function renameColumn($table, $old_name, $new_name) {
		$query = DB::with($table, $this->db);
		$table = $query->quoteKeyword($table);
		$old_name = $query->quoteKeyword($old_name);
		$new_name = $query->quoteKeyword($new_name);

		$query_str = "ALTER TABLE $table RENAME COLUMN $old_name TO $new_name";
		return $query->query($query_str);
	}

	public function showColumns($table, array $column_names = []) {
		return DB::with('information_schema.columns', $this->db)
			->select('*')
			->where(['table_name' => $table])
			->where(['table_catalog' => $this->db_config['database']])
			->where($column_names ? ['column_name' => $column_names] : [])
			->fetchAllAssoc('column_name');
	}

	public function showIndexes($table) {
		$rows = DB::with('pg_indexes', $this->db)
			->select(['tablename', 'indexname', 'indexdef'])
			->where(['tablename' => $table])
			->fetchAll();
		$indexes = [];

		foreach ($rows as $row) {
			// Postgres has no convenient way to get index info. Parsing the
			// CREATE string is literally the easiest way.
			$regex = "/CREATE( UNIQUE)? INDEX \"?{$row['indexname']}\"? ON \"?$table\"? USING ([a-z]+) \(([a-zA-Z0-9_,\" ]+)\)/";
			preg_match($regex, $row['indexdef'], $matches);

			// Skip unknown indexes
			if (!$matches) {
				continue;
			}

			$columns = explode(',', $matches[3]);
			foreach ($columns as $key => $column) {
				// Strip any extra info, such as ASC NULLS FIRST
				$columns[$key] = trim(explode(' ', trim($column))[0], ' "');
			}

			$indexes[$row['indexname']] = [
				'name' => $row['indexname'],
				'type' => $matches[2],
				'columns' => $columns,
				'unique' => (bool)$matches[1],
			];
		}

		return $indexes;
	}

	public function addIndexes($table, array $indexes, array $options = []) {
		if (empty($indexes)) {
			throw new Exception('No indexes provided');
		}
		foreach ($indexes as $index) {
			if ($index['type'] !== 'btree') {
				continue;
			}
			$success = $this->addIndex($table, $index['name'], $index['type'], $index['columns'], $index['unique'], $options);
			if (!$success) {
				return false;
			}
		}
		return true;
	}

	public function addIndex($table, $name, $type, array $columns, $unique = false, array $options = []) {
		if ($type !== 'btree') {
			throw new Exception("Index type ($type} not currently supported");
		}
		$query = DB::with($table, $this->db);
		$table = $query->quoteKeyword($table);
		$name = $query->quoteKeyword($name);
		$unique = $unique ? ' UNIQUE' : '';
		foreach ($columns as $key => $column) {
			// Always use NULLS FIRST for now, to be consistent with MySQL
			$columns[$key] = $query->quoteKeyword($column) . " NULLS FIRST";
		}
		$sql = "CREATE$unique INDEX $name ON $table USING $type (" . implode(',', $columns) . ")";
		return $query->query($sql);
	}

	public function dropIndex($table, $name) {
		$query = DB::with($table, $this->db);
		$name = $query->quoteKeyword($name);
		// Postgres enforces unique index names across the entire database...
		return $query->query("DROP INDEX $name");
	}

	public function findTablesLike($table) {
		$query = DB::with($table, $this->db);
		$table = $query->quote($table);

		$sql = "
			    SELECT relname
			    FROM pg_catalog.pg_class c
			    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			    WHERE c.relname LIKE {$table}
			    AND c.relkind = 'r' -- Tables only
			";
		$query->query($sql);
		return $query->fetchAll();
	}

	public function tableExists($table) {
		$query = DB::with($table, $this->db);
		$table = $query->quote($table);
		// Some dude on the internet said this query was faster than information_schema
		$sql = "
			SELECT EXISTS(
			    SELECT 1
			    FROM pg_catalog.pg_class c
			    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			    WHERE c.relname = {$table}
			    AND c.relkind = 'r' -- Tables only
			)";
		$query->query($sql);
		return $query->value();
	}

	public function tableDiskSize($table, $schema) {
		$query = DB::with($table, $this->db);
		$table = $query->quote($table);
		$schema = $query->quote($schema);
		$sql = "
			SELECT pg_total_relation_size(C.oid)
			FROM pg_class C
			LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
			WHERE nspname = $schema AND relname = $table
			AND C.relkind <> 'i'";
		$query->query($sql);
		return $query->value();
	}

	public function tableSizeInfo($table, $schema) {
		$db = DB::with('', $this->db);
		$table = $db->quote($table);
		$db->query("SELECT pg_relation_size($table), pg_total_relation_size($table)");
		$row = $db->fetch();
		return [
			'data_length' => $row['pg_relation_size'],
		    'index_length' => $row['pg_total_relation_size'] - $row['pg_relation_size'],
		];
	}

	public function copyTable($from_table, $to_table, $include_data = false) {
		$query = DB::with($from_table, $this->db);
		$from_table = $query->quoteKeyword($from_table);
		$to_table = $query->quoteKeyword($to_table);
		$success = $query->query("CREATE TABLE $to_table (LIKE $from_table INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)");
		if (!$success || !$include_data) {
			return $success;
		}
		return $query->query("INSERT INTO $to_table SELECT * FROM $from_table");
	}


}