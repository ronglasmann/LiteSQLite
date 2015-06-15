# LiteSQLite

A wrapper around sqlite4java (which in turn wraps SQLite).  Provides several benefits:
* Simple, low-friction usage pattern
* Concurrency, multiple threads can safely access the same SQLite database
* Schema versioning and migration

## Installation

TODO - Download, Maven, etc

### SQLite.Def - Database definitions

Start by creating a database definition.  This is a class that extends SQLite.Def.  In the constructor, set the name of the database and use the schema(..) method to create the structure.  Note that schema(..) takes a version number allowing you to version your database and migrate data and structures between versions as needed.  

Here is an example of a database Def that contains two versions.
```
import net.glasmann.base.SQLite;
import net.glasmann.base.SQLite.*;

public static class Test extends Def {
	public Test() {
		setName("test");
		schema(1, new SQL()
			.append("create table if not exists test_table ( ")
			.append("id integer, ")
			.append("value text ")
			.append(") ")
		);
		schema(2, new SQL()
			.append("create table if not exists test_table_2 (id integer, value_2 text)")
		);
	}
}
```

### Usage

Call SQLite.db(..) specifying a class that extends SQLite.Def (and optionally a File that points to where you want the SQLite database file on disk).  Once you have the db instance you can call execute(..) and query(..).  Call close(..) when you are done with the database instance.

Here is a simple example:
```
SQLite db = SQLite.db(Test.class);

db.execute(new SQL("insert into test_table (id, value) values (1, 'test1')"));

List<SQLite.Record> list = db.query(new SQL("select * from test_table"));

db.close();
```

### SQLite.SQL - Parameterized SQL statements

The execute(..) and query(..) methods accept an instance of SQLite.SQL.  This class allows for easy construction of parameterized SQL statements.  

Here is a simple example:
```
SQL sql = new SQL("insert into test_table (id, value) values (4, ?)");

sql.set(1, "test 4");
```

### SQLite.Tx - Transactions

The execute(..) method can also accept an instance of SQLite.Tx allowing for execution (and rollback) of transactions.  SQLite.Tx allows for easy construction of groups of SQL statements that should be executed as a unit.

Here is a simple example:
```
db.execute(new Tx()
	.add(new SQL("insert into test_table (id, value) values (1, 'test1')"))
	.add(new SQL("insert into test_table (id, value) values (2, 'test2')"))
	.add(new SQL("insert into test_table (id, value) values (3, 'test3')"))
	.add(new SQL("insert into test_table (id, value) values (4, ?)").set(1, "test 4"))
);
```

### SQList.Insert - Builder class for INSERT statements

SQLite.Insert is a builder class that simplifies and standardizes the construction of INSERT statements.  It has built-in support for incrementing integer keys and constructs the INSERT as a transaction that returns the inserted row.  The SQLite class has an insert(..) convenience method that makes it easy to access the returned row.

Here is a simple example:
```
List<Record> inserted = db.insert(new Insert("test_table")
	.field("id")
	.field("value", "test value one")
	.build()
);
```

### SQLite.Upsert - Builder class for INSERT | UPDATE operations

SQLite.Upsert is a builder class that simplifies and standardizes the "UPSERT" operation.  An upseet is an "update or insert" operation that will INSERT a row if a record with the specified keys does not already exist in the database.  If it does exist the existing row is updated instead.  Upsert creates a translation that will return the affected row.  The SQLite class has an upsert(..) convenience method that makes it easy to access the returned row.

Here is a simple example:
```
List<Record> upserted = db.upsert(new Upsert("test_table")
	.key("id", 2)
	.field("value", "upserted two")
	.build()
);
```
