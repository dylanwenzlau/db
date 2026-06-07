<?php

class DBValueRaw {

	public function __construct(private readonly string $value) {}

	public function getValue() : string {
		return $this->value;
	}
}
