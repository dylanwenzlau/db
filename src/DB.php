<?php

use Graphiq\DB\MySQLQuery;
use Graphiq\DB\PostgreSQLQuery;
use Graphiq\DB\MySQLSchemaController;
use Graphiq\DB\PostgreSQLSchemaController;

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
 * @author Dylan Wenzlau <dylan@graphiq.com>
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
	const READ_PREFERENCE_PREFER_MASTER = 'prefer_master';

	public static $auto_execute = true;

	// to be maintained by DBQuery objects whenever they execute a query, so that
	// the DB class can use the correct connection for retrieving errorInfo (or other things)
	public static $_last_access_level_used;

	// last PDO errorInfo object returned by a DBQuery query failure
	private static $last_error_info = [];

	private static $read_preference = self::READ_PREFERENCE_DEFAULT;
	private static $read_preferences = []; // db-level read preference overrides
	private static $config = ['connections' => []];

	/**
	 * @var PDO[]
	 */
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
		// Clear connection cache. We could be smarter and only clear things that have changed.
		self::$pdo_connections = [];
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

		// No read/write access was requested, return default one
		if (!$access) {
			return self::$config['connections'][$db];
		}

		// If the DB config contains a callback function, call the function
		// and replace the entire config array with the function result.
		if (isset(self::$config['connections'][$db][$access]['callback'])) {
			self::$config['connections'][$db][$access] = call_user_func(self::$config['connections'][$db][$access]['callback'], $access);
		}

		// If no specific read/write access was configured
		if (!isset(self::$config['connections'][$db][$access])) {
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

	/**
	 * Set the preference for all database reads, optionally constrained to a single db.
	 *
	 * READ_PREFERENCE_DEFAULT - only read from read connection
	 * READ_PREFERENCE_MASTER - only read from write connection
	 * READ_PREFERENCE_PREFER_MASTER - read from write connection if possible, but fallback to read on failure
	 *
	 * @param string $read_preference one of the DB::READ_PREFERENCE_* constants
	 * @param string $db
	 * @throws Exception
	 */
	public static function setReadPreference($read_preference, $db = null) {
		switch ($read_preference) {
			case self::READ_PREFERENCE_DEFAULT:
			case self::READ_PREFERENCE_MASTER:
			case self::READ_PREFERENCE_PREFER_MASTER:
				if ($db === null) {
					self::$read_preference = $read_preference;
				} else {
					self::$read_preferences[$db] = $read_preference;
				}
				break;
			default:
				throw new Exception("Invalid read preference: $read_preference");
		}
	}

	public static function getReadPreference($db = null) {
		if ($db === null) {
			return self::$read_preference;
		}
		return self::$read_preferences[$db] ?: self::$read_preference;
	}

	public static function setErrorHandler(callable $handler) {
		self::$error_handler = $handler;
	}

	public static function handleError(array $error_info, $query, $db = '') {
		if (isset(self::$error_handler)) {
			call_user_func(self::$error_handler, $error_info, $query, $db);
		}
	}

	public static function setLastErrorInfo(array $error_info, $db = '') {
		self::$last_error_info[$db] = $error_info;
	}

	public static function setConnectErrorHandler(callable $handler) {
		self::$connect_error_handler = $handler;
	}

	/**
	 * Get a PDO instance for the given db
	 * @param string $db
	 * @param string $access
	 * @return PDO
	 */
	public static function getPDO($db = '', $access = 'read') {
		$db = $db === '' ? self::$config['default'] : $db;
		// Enforce that write connection be used if read preference is master only
		$read_preference = self::getReadPreference($db);
		if ($read_preference === self::READ_PREFERENCE_MASTER) {
			$access = 'write';
		}
		if ($access === 'read' && $read_preference === self::READ_PREFERENCE_PREFER_MASTER) {
			// Try write access first, then fallback to read if write connection fails
			try {
				return self::_getPDO($db, 'write');
			} catch (PDOException $e) {
				// If the read and write connections are the same, just throw the exception
				// to ensure that we don't try connecting to the same [malfunctioning] database twice in a row.
				if (DB::getDBConfig($db, 'read') === DB::getDBConfig($db, 'write')) {
					throw $e;
				}
			}
		}
		return self::_getPDO($db, $access);
	}

	private static function _getPDO($db, $access) {
		$db_config = DB::getDBConfig($db, $access);
		$cache_key = $db_config['host'] . '|' . $db_config['database'];
		if (!isset(self::$pdo_connections[$cache_key])) {
			$pdo_url = "{$db_config['engine']}:host={$db_config['host']};dbname={$db_config['database']}";
			try {
				$options = $db_config['pdo_options'] ?: [];
				self::$pdo_connections[$cache_key] = new PDO($pdo_url, $db_config['username'], $db_config['password'], $options);
			} catch (PDOException $e) {
				if (isset(self::$connect_error_handler)) {
					call_user_func(self::$connect_error_handler, $db_config);
				}
				throw $e;
			}

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
	 * Get the errorInfo from the last PDO object used, or if that's empty,
	 * the errorInfo from the last PDOStatement that was executed
	 * http://php.net/manual/en/pdo.errorinfo.php
	 * [SQLSTATE error code, Driver-specific error code, Driver-specific error message]
	 * @param string $db
	 * @return array
	 */
	public static function errorInfo($db = '') {
		$error_info = self::getPDO($db, self::$_last_access_level_used ?: 'read')->errorInfo();
		return $error_info[2] ? $error_info : (self::$last_error_info[$db] ?: []);
	}

	public static function quote($value, $db = '') {
		return DB::with('', $db)->quote($value);
	}

	public static function quoteKeyword($keyword, $db = '') {
		return DB::with('', $db)->quoteKeyword($keyword);
	}

	/**
	 * Escape a SQL string to be used as a LIKE string (escapes _ and %)
	 * @param string $str
	 * @return string
	 */
	public static function escapeLike($str) {
		return str_replace(['_', '%'], ['\\_', '\\%'], $str);
	}

	/**
	 * @deprecated use DB::raw instead
	 */
	public static function rawValue($value) {
		return new DBValueRaw($value);
	}

	/**
	 * Return a new DBValueRaw() object to be used for writing raw SQL with the query builder
	 * @param $value
	 * @return DBValueRaw
	 */
	public static function raw($value) {
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
			static::$query_log[$engine] = array_merge(static::$query_log[$engine], $queries);
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
