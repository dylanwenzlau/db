<?php

use FTB\core\db\MySQLQuery;
use FTB\core\db\PostgreSQLQuery;
use FTB\core\db\MySQLSchemaController;
use FTB\core\db\PostgreSQLSchemaController;

/**
 * Class DB
 *
 * A general database controller for querying data and
 * modifying schemas. This class utilizes the classes
 * DBQuery and SchemaController which may be extended by
 * different engine drivers.
 *
 * @author Dylan Wenzlau <dylan@findthebest.com>
 *
 */
class DB {

	const ENGINE_MYSQL = 'mysql';
	const ENGINE_POSTGRES = 'postgresql';

	const FETCH_ASSOC = PDO::FETCH_ASSOC;
	const FETCH_OBJ = PDO::FETCH_OBJ;
	const FETCH_NUM = PDO::FETCH_NUM;

	/**
	 * Create a new instance of a driver class that extends DBQuery.
	 *
	 * @param string $table The table name, if using the query builder functions
	 * @param string $db
	 * @param array $allowed_operations Operations the new query class will be
	 *          allowed to execute. Options: SELECT, INSERT, UPDATE, DELETE
	 * @return MySQLQuery|PostgreSQLQuery A new instance.
	 */
	public static function with($table, $db = '', array $allowed_operations = []) {
		if (substr($db, 0, 3) === 'pg_' || $db === 'postgres') {
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

	/**
	 * Create a new instance of a driver class that extends SchemaController.
	 *
	 * @param string $db
	 * @return MySQLSchemaController|PostgreSQLSchemaController
	 */
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
