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
 * This library gets inspiration from PDO and Laravel, and attempts
 * to follow PDO where possible.
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

	public static $auto_execute = true;

	private static $config = ['connections' => []];
	private static $pdo_connections = [];

	public static function query($query, array $args = []) {
		return DB::with('')->query($query, $args);
	}

	public static function connQuery($db, $query, array $args = []) {
		return DB::with('', $db)->query($query, $args);
	}

	/**
	 * Create a new instance of a driver class that extends DBQuery.
	 *
	 * @param string $table The table name, if using the query builder functions
	 * @param string $db
	 * @param array $allowed_operations Operations the new query class will be
	 *          allowed to execute. Options: SELECT, INSERT, UPDATE, DELETE
	 * @return MySQLQuery|PostgreSQLQuery A new instance.
	 * @throws Exception
	 */
	public static function with($table, $db = '', array $allowed_operations = []) {
		$db = $db ?: self::$config['default'];
		if (!isset(self::$config['connections'][$db])) {
			throw new Exception("Database ($db) has not been configured");
		}
		$db_config = self::$config['connections'][$db];
		switch ($db_config['engine']) {
			case static::ENGINE_MYSQL:
				return new MySQLQuery($table, $db, $allowed_operations);
			case 'pgsql':
			case 'postgresql':
				return new PostgreSQLQuery($table, $db, $allowed_operations);
			default:
				throw new Exception("Invalid engine ({$db_config['engine']})");
		}
	}

	/**
	 * Create a new instance of a driver class that extends SchemaController.
	 *
	 * @param string $db
	 * @return MySQLSchemaController|PostgreSQLSchemaController
	 * @throws Exception
	 */
	public static function schema($db = '') {
		$db = $db ?: self::$config['default'];
		if (!isset(self::$config['connections'][$db])) {
			throw new Exception("Database ($db) has not been configured");
		}
		$db_config = self::$config['connections'][$db];
		switch ($db_config['engine']) {
			case self::ENGINE_MYSQL:
				return new MySQLSchemaController($db, $db_config['engine']);
			case 'pgsql':
			case 'postgresql':
				return new PostgreSQLSchemaController($db, $db_config['engine']);
			default:
				throw new Exception("Invalid engine({$db_config['engine']})");
		}
	}


	/**
	 * Set the database connection configuration. Example configuration:
	 *
	[
		'default' => 'my_db',
		'connections' => [
			'my_db' => [
				'engine' => 'mysql',
				'host' => '111.111.111.111',
				'username' => 'mysql_user',
				'password' => 'mysql_pass',
				'database' => 'my_database',
			],
			'my_other_db' => [
				'engine' => 'pgsql',
				'host' => '222.222.222.222',
				'username' => 'pgsql_user',
				'password' => 'pgsql_pass',
				'database' => 'my_pg_database',
			]
		],
	]
	 *
	 * @param array $config
	 * @throws Exception
	 */
	public static function setConfig(array $config) {
		if (!isset($config['default']) || !isset($config['connections'][$config['default']])) {
			throw new Exception("A default database must be specified");
		}
		self::$config = $config;
	}

	public static function getConfig() {
		return self::$config;
	}

	public static function getDBConfig($db = '') {
		return self::$config['connections'][$db ?: self::$config['default']];
	}

	public static function getPDO($db = '') {
		if (!isset(self::$pdo_connections[$db])) {
			$db_config = DB::getDBConfig($db);
			$pdo_url = "{$db_config['engine']}:host={$db_config['host']};dbname={$db_config['database']}";
			self::$pdo_connections[$db] = new PDO($pdo_url, $db_config['username'], $db_config['password']);

			// Always use emulated prepares, since true prepares are much slower on a per-query
			// basis, since they require a round trip to the server
			self::$pdo_connections[$db]->setAttribute(PDO::ATTR_EMULATE_PREPARES, true);

			// Force MySQL to use the UTF-8 character set. Also set the collation, if a
			// certain one has been set; otherwise, MySQL defaults to 'utf8_general_ci'
			// for UTF-8.
			// TODO: do this at the database level to remove performance overhead
			if ($db_config['engine'] === 'mysql') {
				self::$pdo_connections[$db]->query("SET NAMES utf8");
			}
		}
		return self::$pdo_connections[$db];
	}

	public static function rawValue($value) {
		return new DBValueRaw($value);
	}
}
