<?php

namespace FTB\core\db;
use SQLQuery;
use Exception;

class PostgresqlSchemaController extends SchemaController {

	public function showIndexes($table) {
		$rows = SQLQuery::with('pg_indexes', $this->db)
			->select(['tablename', 'indexname', 'indexdef'])
			->where(['tablename' => $table])
			->as_arrays();
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

	public function addIndexes($table, array $indexes) {
		if (empty($indexes)) {
			throw new Exception('No indexes provided');
		}
		foreach ($indexes as $index) {
			if ($index['type'] !== 'btree') {
				continue;
			}
			$success = $this->addIndex($table, $index['name'], $index['type'], $index['columns'], $index['unique']);
			if (!$success) {
				return false;
			}
		}
		return true;
	}

	public function addIndex($table, $name, $type, array $columns, $unique = false) {
		if ($type !== 'btree') {
			throw new Exception("Index type ($type} not currently supported");
		}
		$query = SQLQuery::with($table, $this->db);
		$table = $query->quoteKeyword($table);
		$name = $query->quoteKeyword($name);
		$unique = $unique ? ' UNIQUE' : '';
		foreach ($columns as $key => $column) {
			// Always use NULLS FIRST for now, to be consistent with MySQL
			$columns[$key] = $query->quoteKeyword($column) . " NULLS FIRST";
		}
		$sql = "CREATE$unique INDEX $name ON $table USING $type (" . implode(',', $columns) . ")";
		return $query->executeRawQuery($sql);
	}

	public function dropIndex($table, $name) {
		$query = SQLQuery::with($table, $this->db);
		$name = $query->quoteKeyword($name);
		// Postgres enforces unique index names across the entire database...
		return $query->executeRawQuery("DROP INDEX $name");
	}

	public function tableExists($table) {
		$query = SQLQuery::with($table, $this->db);
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
		$query->executeRawQuery($sql);
		return $query->value();
	}

}
