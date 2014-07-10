<?php

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
			$regex = "/CREATE( UNIQUE)? INDEX {$row['indexname']} ON $table USING ([a-z]+) \(([a-zA-Z0-9_, ]+)\)/";
			preg_match($regex, $row['indexdef'], $matches);

			// Skip unknown indexes
			if (!$matches) {
				continue;
			}

			$indexes[$row['indexname']] = [
				'index_name' => $row['indexname'],
				'index_type' => $matches[2],
				'columns' => explode(', ', $matches[3]),
				'unique' => (bool)$matches[1],
			];
		}

		return $indexes;
	}

}
