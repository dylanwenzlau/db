<?php

use DB\MySQLQuery;
use DB\MySQLSchema;
use DB\SQLQuery;
use DB\SQLSchema;
use DB\SQLStatement;

/**
 * Class DB
 *
 * A general database controller for querying data and
 * modifying schemas. This class utilizes the classes
 * SQLQuery and SQLSchema which may be extended by
 * different engine drivers.
 *
 * This library gets inspiration from PDO and Laravel, and attempts
 * to follow PDO where possible.
 *
 */
class DB {

	const ENGINE_MYSQL = 'mysql';

	const ACCESS_READ = 'read';
	const ACCESS_WRITE = 'write';

	const FETCH_ASSOC = PDO::FETCH_ASSOC;
	const FETCH_OBJ = PDO::FETCH_OBJ;
	const FETCH_NUM = PDO::FETCH_NUM;

	const READ_PREFERENCE_DEFAULT = 'default';
	const READ_PREFERENCE_MASTER = 'master';
	const READ_PREFERENCE_PREFER_MASTER = 'prefer_master';
	const READ_PREFERENCES = [self::READ_PREFERENCE_DEFAULT, self::READ_PREFERENCE_MASTER, self::READ_PREFERENCE_PREFER_MASTER];

	public static bool $auto_execute = true;
	public static bool $auto_execute_updates = true;

	// to be maintained by SQLQuery objects whenever they execute a query, so that
	// the DB class can use the correct connection for retrieving errorInfo (or other things)
	public static string $_last_access_level_used = '';

	// last PDO errorInfo object returned by a SQLQuery query failure
	private static array $last_error_info = [];

	private static string $read_preference = self::READ_PREFERENCE_DEFAULT;
	private static array $read_preferences = []; // db-level read preference overrides
	private static array $config = ['connections' => []];

	/**
	 * @var PDO[]
	 */
	private static array $pdo_connections = [];
	private static Closure $error_handler;
	private static Closure $connect_error_handler;
	private static bool $query_log_enabled = false;
	private static array $query_log = [];

	public static function query(string $query, array $args = []) : SQLStatement|int|false {
		return DB::with('')->query($query, $args);
	}

	public static function connQuery(string $db, string $query, array $args = []) : SQLStatement|int|false {
		return DB::with('', $db)->query($query, $args);
	}

	/**
	 * Create a new instance of a driver class that extends SQLQuery.
	 *
	 * If an instance must be constructed to access certain methods
	 * without actually building a query, an empty string can be used
	 * as the table name.
	 *
	 * @param string $table The table name, if using the query builder functions
	 * @param array $allowed_operations Operations the new query class will be
	 *          allowed to execute. Options: SELECT, INSERT, UPDATE, DELETE
	 */
	public static function with(string $table, string $db = '', array $allowed_operations = []) : SQLQuery {
		return match (self::getDBConfig($db)['engine']) {
			self::ENGINE_MYSQL => new MySQLQuery($table, $db, $allowed_operations),
		};
	}

	/**
	 * Create a new instance of a driver class that extends SQLSchema.
	 */
	public static function schema(string $db = '') : SQLSchema {
		return match (self::getDBConfig($db)['engine']) {
			self::ENGINE_MYSQL => new MySQLSchema($db),
		};
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
		],
	]
	 *
	 * @throws Exception
	 */
	public static function setConfig(array $config) : void {
		if (!isset($config['default']) || !isset($config['connections'][$config['default']])) {
			throw new Exception("A default database must be specified");
		}
		self::$config = $config;
		// Clear connection cache. We could be smarter and only clear things that have changed.
		self::$pdo_connections = [];
	}

	public static function getConfig() : array {
		return self::$config;
	}

	public static function setDBConfig(string $db, array $config) : void {
		self::$config['connections'][$db] = $config;
	}

	public static function getDBConfig(string $db = '', string $access = '') : array {
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
			self::$config['connections'][$db][$access] = (self::$config['connections'][$db][$access]['callback'])($access);
		}

		// If no specific read/write access was configured
		if (!isset(self::$config['connections'][$db][$access])) {
			return self::$config['connections'][$db];
		}
		// Allow the "read" or "write" config to override the default config
		return [...self::$config['connections'][$db], ...self::$config['connections'][$db][$access]];
	}

	public static function getDBConfigDSN(string $db, string $access = '') : string {
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
	 * Pass an empty $db to set the preference for all databases.
	 * @throws Exception
	 */
	public static function setReadPreference(string $read_preference, string $db = '') : void {
		if (!in_array($read_preference, self::READ_PREFERENCES, true)) {
			throw new Exception("Invalid read preference: $read_preference");
		}
		if (!$db) {
			self::$read_preference = $read_preference;
		} else {
			self::$read_preferences[$db] = $read_preference;
		}
	}

	public static function getReadPreference(string $db = '') : string {
		if (!$db) {
			return self::$read_preference;
		}
		return self::$read_preferences[$db] ?? self::$read_preference;
	}

	public static function setErrorHandler(callable $handler) : void {
		self::$error_handler = Closure::fromCallable($handler);
	}

	public static function handleError(array $error_info, string $query, string $db = '') : void {
		if (isset(self::$error_handler)) {
			(self::$error_handler)($error_info, $query, $db);
		}
	}

	public static function setLastErrorInfo(array $error_info, string $db = '') : void {
		self::$last_error_info[$db] = $error_info;
	}

	public static function setConnectErrorHandler(callable $handler) : void {
		self::$connect_error_handler = Closure::fromCallable($handler);
	}

	/**
	 * Get a PDO instance for the given db
	 */
	public static function getPDO(string $db = '', string $access = self::ACCESS_READ) : PDO {
		$db = $db === '' ? self::$config['default'] : $db;
		// Enforce that write connection be used if read preference is master only
		$read_preference = self::getReadPreference($db);
		if ($read_preference === self::READ_PREFERENCE_MASTER) {
			$access = self::ACCESS_WRITE;
		}
		if ($access === self::ACCESS_READ && $read_preference === self::READ_PREFERENCE_PREFER_MASTER) {
			// Try write access first, then fallback to read if write connection fails
			try {
				return self::_getPDO($db, self::ACCESS_WRITE);
			} catch (PDOException $e) {
				// If the read and write connections are the same, just throw the exception
				// to ensure that we don't try connecting to the same [malfunctioning] database twice in a row.
				if (DB::getDBConfig($db, self::ACCESS_READ) === DB::getDBConfig($db, self::ACCESS_WRITE)) {
					throw $e;
				}
			}
		}
		return self::_getPDO($db, $access);
	}

	private static function _getPDO(string $db, string $access) : PDO {
		$db_config = DB::getDBConfig($db, $access);
		$cache_key = $db_config['host'] . '|' . $db_config['database'];
		if (!isset(self::$pdo_connections[$cache_key])) {
			// always set charset=utf8mb4 for full support of utf8, does not affect performance unless used in tables
			// charset sets character_set_client, character_set_connection, and character_set_results
			$pdo_url = "{$db_config['engine']}:host={$db_config['host']};dbname={$db_config['database']};charset=utf8mb4";
			try {
				$options = $db_config['pdo_options'] ?? [];
				self::$pdo_connections[$cache_key] = new PDO($pdo_url, $db_config['username'], $db_config['password'], $options);
			} catch (PDOException $e) {
				if (isset(self::$connect_error_handler)) {
					(self::$connect_error_handler)($db_config);
				}
				throw $e;
			}
		}
		return self::$pdo_connections[$cache_key];
	}

	/**
	 * Get the errorInfo from the last PDO object used, or if that's empty,
	 * the errorInfo from the last PDOStatement that was executed
	 * http://php.net/manual/en/pdo.errorinfo.php
	 * [SQLSTATE error code, Driver-specific error code, Driver-specific error message]
	 */
	public static function errorInfo(string $db = '') : array {
		$error_info = self::getPDO($db, self::$_last_access_level_used ?: self::ACCESS_READ)->errorInfo();
		return ($error_info[2] ?? '') ? $error_info : (self::$last_error_info[$db] ?? []);
	}

	public static function quote(string $value, string $db = '') : string {
		return DB::with('', $db)->quote($value);
	}

	public static function quoteKeyword(string $keyword, string $db = '') : string {
		return DB::with('', $db)->quoteKeyword($keyword);
	}

	/**
	 * Escape a SQL string to be used as a LIKE string (escapes _ and %)
	 */
	public static function escapeLike(string $str) : string {
		return str_replace(['_', '%'], ['\\_', '\\%'], $str);
	}

	/**
	 * Return a new DBValueRaw() object to be used for writing raw SQL with the query builder
	 */
	public static function raw(string $value) : DBValueRaw {
		return new DBValueRaw($value);
	}

	/**
	 * Set whether SQLQuery should collect debug info for all queries.
	 */
	public static function toggleQueryLog(bool $debug) : void {
		self::$query_log_enabled = $debug;
	}

	public static function queryLogEnabled() : bool {
		return self::$query_log_enabled;
	}

	public static function getQueryLog() : array {
		return self::$query_log;
	}

	public static function mergeQueryLog(array $executed_queries) : void {
		foreach ($executed_queries as $engine => $queries) {
			self::$query_log[$engine] ??= [];
			self::$query_log[$engine] = [...self::$query_log[$engine], ...$queries];
		}
	}

	public static function clearQueryLog() : void {
		self::$query_log = [];
	}

	public static function logQuery(string $db, string $query, bool $success, float $time, int $row_count) : void {
		$config = self::getDBConfig($db);
		self::$query_log[$config['engine']] ??= [];
		self::$query_log[$config['engine']][] = [
			'query' => $query,
			'success' => $success,
			'time' => $time,
			'db' => $db,
			'row_count' => $row_count,
		];
	}
}
