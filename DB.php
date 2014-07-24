<?php

use FTB\core\db\DBQuery;
use FTB\core\db\MySQLQuery;
use FTB\core\db\PostgreSQLQuery;
use FTB\core\db\MySQLSchemaController;
use FTB\core\db\PostgreSQLSchemaController;

class DB {

	const ENGINE_MYSQL = 'mysql';
	const ENGINE_POSTGRES = 'postgresql';
	const FETCH_ASSOC = PDO::FETCH_ASSOC;
	const FETCH_OBJ = PDO::FETCH_OBJ;

	/**
	 * Creates a new SQLQuery instance set with a specified table name.
	 *
	 * @param string $table The table name.
	 * @param string $db
	 * @param array $allowed_operations
	 * @return DBQuery A new instance.
	 */
	public static function with($table, $db = '', array $allowed_operations = []) {
		if (substr($db, 0, 3) === 'pg_') {
			$engine = static::ENGINE_POSTGRES;
		} else {
			$engine = static::ENGINE_MYSQL;
		}
		switch ($engine) {
			case static::ENGINE_MYSQL:
				return new MySQLQuery($table, $db, $allowed_operations);
			case static::ENGINE_POSTGRES:
				return new PostgreSQLQuery($table, $db, $allowed_operations);
		}
	}

	public static function schema($db = '') {
		$engine = substr($db, 0, 3) === 'pg_' ? self::ENGINE_POSTGRES : self::ENGINE_MYSQL;
		switch ($engine) {
			case self::ENGINE_MYSQL:
				return new MySQLSchemaController($db, $engine);
			case self::ENGINE_POSTGRES:
				return new PostgreSQLSchemaController($db, $engine);
		}
	}
}
