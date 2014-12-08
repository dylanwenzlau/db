<?php
/**
 * @author Skyler Lipthay <slipthay@findthebest.com>
 */

/**
 * Provides a template for SQL constructs to be represented as class instances
 * rather than raw SQL strings.
 */
interface DBValue {
	/**
	 * @return string that the object represents in DB queries
	 */
	public function get_value();
}

/**
 * SQL interface for COUNT().
 */
class DBValueCount implements DBValue {
	private $options;

	/**
	 * @param array [$options] Optional.
	 * @param string [$options['column']] Optional, defaults to '*'. Specifies
	 *   which table column to COUNT on (most RDBMSs ignore NULL fields).
	 * @param bool [$options['distinct']] Optional, defaults to false. Specifies
	 *   whether or not to count DISTINCT values.
	 */
	public function __construct($options = []) {
		$defaults = [
			'column' => '*',
			'distinct' => false
		];

		$this->options = array_merge($defaults, $options);
	}

	public function get_value() {
		$distinct = $this->options['distinct'] ? 'DISTINCT ' : '';
		$column = $this->options['column'];

		return "COUNT({$distinct}{$column})";
	}
}

/**
 * SQL interface for NOW().
 */
class DBValueNow implements DBValue {
	public function get_value() {
		return 'NOW()';
	}
}

class DBValueRaw implements DBValue {

	private $value;

	public function __construct($raw_sql_value) {
		$this->value = $raw_sql_value;
	}

	public function get_value() {
		return $this->value;
	}
}
