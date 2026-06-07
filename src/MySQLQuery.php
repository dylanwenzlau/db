<?php

namespace DB;

class MySQLQuery extends SQLQuery {

	// Override so we can use MySQL's special FIELD() function
	public function orderByValues(string $field, array $values) : static {
		if (count($values) === 1) {
			return $this;
		}
		$field = $this->quoteKeyword($field);
		foreach ($values as $key => $value) {
			$values[$key] = $this->sqlValue($value);
		}
		$values = implode(',', $values);
		$this->order .= "FIELD({$field}, {$values})";
		return $this;
	}

	public function getRegexpOperator() : string {
		return 'REGEXP';
	}

	protected function getKeywordEscapeChar() : string {
		return '`';
	}
}
