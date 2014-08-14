# DB

DB is a database library written by FindTheBest engineers. The DB library is designed with major emphasis on security, performance, and simplicity. It contains two main modules - DBQuery and SchemaController. DBQuery is a query builder with retrieval functions that mirror the PDO library, while SchemaController is responsible for modifying databases, tables, columns, and indexes.

The grand vision for DB is to potentially implement more drivers than just SQL, such as MongoDB, SphinxQL, or ElasticSearch. This is still speculative though, as the library is in an early state.

MySQL is currently implemented using Drupal's db_query library, but we plan to implement all SQL drivers using PDO at some point, including MySQL. PDO is quickly becoming the far-and-away best way to access SQL from PHP.

## Query Builder Tutorial

