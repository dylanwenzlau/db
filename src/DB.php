<?php

use FindTheBest\DB\MySQLQuery;
use FindTheBest\DB\PostgreSQLQuery;
use FindTheBest\DB\MySQLSchemaController;
use FindTheBest\DB\PostgreSQLSchemaController;

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

	const READ_PREFERENCE_DEFAULT = 'default';
	const READ_PREFERENCE_MASTER = 'master';

	public static $auto_execute = true;

	private static $read_preference = self::READ_PREFERENCE_DEFAULT;
	private static $config = ['connections' => []];
	private static $pdo_connections = [];
	private static $error_handler;
	private static $connect_error_handler;
	private static $query_log_enabled = false;
	private static $query_log = [];

	public static function query($query, array $args = []) {
		return DB::with('')->query($query, $args);
	}

	public static function connQuery($db, $query, array $args = []) {
		return DB::with('', $db)->query($query, $args);
	}

	/**
	 * Create a new instance of a driver class that extends DBQuery.
	 *
	 * If an instance must be constructed to access certain methods
	 * without actually building a query, an empty string can be used
	 * as the table name.
	 *
	 * @param string $table The table name, if using the query builder functions
	 * @param string $db
	 * @param array $allowed_operations Operations the new query class will be
	 *          allowed to execute. Options: SELECT, INSERT, UPDATE, DELETE
	 * @return MySQLQuery|PostgreSQLQuery A new instance.
	 * @throws Exception
	 */
	public static function with($table, $db = '', array $allowed_operations = []) {
		$db_config = self::getDBConfig($db);
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
		$db_config = self::getDBConfig($db);
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
				'read' => [
					'host' => '33.33.33.33'
				],
				'write' => [
					'host' => '44.44.44.44'
				],
				'master_only_tables' => ['hyper_critical_system_table_zomg'],
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

	public static function setDBConfig($db, array $config) {
		self::$config['connections'][$db] = $config;
	}

	public static function getDBConfig($db = '', $access = '') {
		if (!$db) {
			$db = self::$config['default'];
		}
		// If the DB config contains a callback function, call the function
		// and replace the entire config array with the function result.
		if (isset(self::$config['connections'][$db]['callback'])) {
			self::$config['connections'][$db] = call_user_func(self::$config['connections'][$db]['callback']);
		}
		// If no specific read/write access was requested and configured
		if (!$access || !isset(self::$config['connections'][$db][$access])) {
			return self::$config['connections'][$db];
		}
		// Allow the "read" or "write" config to override the default config
		return array_merge(self::$config['connections'][$db], self::$config['connections'][$db][$access]);
	}

	public static function getDBConfigDSN($db, $access = '') {
		$config = self::getDBConfig($db, $access);
		$dsn = $config['engine'] . '://';
		if ($config['username']) {
			$dsn .= urlencode($config['username']);
			if ($config['password']) {
				$dsn .= ':' . urlencode($config['password']);
			}
			$dsn .= '@';
		}
		$dsn .= $config['host'];
		if ($config['database']) {
			$dsn .= '/' . $config['database'];
		}
		return $dsn;
	}

	public static function setReadPreference($read_preference) {
		switch ($read_preference) {
			case self::READ_PREFERENCE_DEFAULT:
			case self::READ_PREFERENCE_MASTER:
				self::$read_preference = $read_preference;
				break;
			default:
				throw new Exception("Invalid read preference: $read_preference");
		}
	}

	public static function getReadPreference() {
		return self::$read_preference;
	}

	public static function setErrorHandler(callable $handler) {
		self::$error_handler = $handler;
	}

	public static function handleError(array $error_info, $query) {
		if (isset(self::$error_handler)) {
			call_user_func(self::$error_handler, $error_info, $query);
		}
	}

	public static function setConnectErrorHandler(callable $handler) {
		self::$connect_error_handler = $handler;
	}

	public static function getPDO($db = '', $access = 'read') {
		if (self::$read_preference === 'master') {
			$access = 'write';
		}
		$db_config = DB::getDBConfig($db, $access);
		$cache_key = $db_config['host'] . '|' . $db_config['database'];
		if (!isset(self::$pdo_connections[$cache_key])) {
			$pdo_url = "{$db_config['engine']}:host={$db_config['host']};dbname={$db_config['database']}";
			try {
				self::$pdo_connections[$cache_key] = new PDO($pdo_url, $db_config['username'], $db_config['password']);
			} catch (PDOException $e) {
				if (isset(self::$connect_error_handler)) {
					call_user_func(self::$connect_error_handler, $db_config);
				}
				throw $e;
			}

			// Always use emulated prepares, since true prepares are much slower on a per-query
			// basis, since they require a round trip to the server
			self::$pdo_connections[$cache_key]->setAttribute(PDO::ATTR_EMULATE_PREPARES, true);
			foreach ((array) $db_config['pdo_options'] as $opt => $value) {
				self::$pdo_connections[$cache_key]->setAttribute($opt, $value);
			}

			//self::$pdo_connections[$cache_key]->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);

			// Force MySQL to use the UTF-8 character set. Also set the collation, if a
			// certain one has been set; otherwise, MySQL defaults to 'utf8_general_ci'
			// for UTF-8.
			// TODO: do this at the database level to remove performance overhead
			if ($db_config['engine'] === 'mysql') {
				self::$pdo_connections[$cache_key]->query("SET NAMES utf8");
			}
		}
		return self::$pdo_connections[$cache_key];
	}

	/**
	 * http://php.net/manual/en/pdo.errorinfo.php
	 * [SQLSTATE error code, Driver-specific error code, Driver-specific error message]
	 * @param string $db
	 * @return array
	 * TODO: fix this to read from whichever pdo connection (read vs. write) was just used
	 */
	public static function errorInfo($db = '') {
		return self::getPDO($db)->errorInfo();
	}

	public static function rawValue($value) {
		return new DBValueRaw($value);
	}

	/**
	 * Set whether DBQuery should collect debug info for all queries.
	 * @param bool $debug
	 */
	public static function toggleQueryLog($debug) {
		static::$query_log_enabled = (bool)$debug;
	}

	public static function queryLogEnabled() {
		return static::$query_log_enabled;
	}

	public static function getQueryLog() {
		return static::$query_log;
	}

	public static function mergeQueryLog(array $executed_queries) {
		foreach ($executed_queries as $engine => $queries) {
			if (!isset(static::$query_log[$engine])) {
				static::$query_log[$engine] = [];
			}
			static::$query_log[$engine] += $queries;
		}
	}

	public static function clearQueryLog() {
		static::$query_log = [];
	}

	public static function logQuery($db, $query, $success, $time, $row_count) {
		$config = self::getDBConfig($db);
		if (!isset(static::$query_log[$config['engine']])) {
			static::$query_log[$config['engine']] = [];
		}
		static::$query_log[$config['engine']][] = [
			'query' => $query,
			'success' => $success,
			'time' => $time,
			'db' => $db,
		    'row_count' => $row_count,
		];
	}
}
