package net.glasmann.base;

import net.glasmann.base.SQLite.*;

import java.io.File;
import java.util.List;

import static org.junit.Assert.*;

public class SQLiteTest {

    public static class Test extends Def {
        public Test() {
            setName("test");
            schema(1, new SQL()
                .append("create table if not exists test_table ( ")
                .append("id integer, ")
                .append("value text ")
                .append(") ")
            );
        }
    }

    public static class TestV2 extends Def {
        public TestV2() {
            setName("testv2");
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

    @org.junit.Test
    public void testQuery() throws Exception {

        File dbf = new File(SQLite.SQLITE_HOME, "test");

        try {

            SQLite db = SQLite.db(Test.class, dbf);

            db.execute(new SQL("delete from test_table"));

            db.execute(new Tx()
                .add(new SQL("insert into test_table (id, value) values (1, 'test1')"))
                .add(new SQL("insert into test_table (id, value) values (2, 'test2')"))
                .add(new SQL("insert into test_table (id, value) values (3, 'test3')"))
                .add(new SQL("insert into test_table (id, value) values (4, ?)").set(1, "test'4"))
            );

            List<SQLite.Record> list = db.query(new SQL("select * from test_table"));

            assertEquals(4, list.size());

            assertEquals(list.get(2).getString("value"), "test3");

            db.close();


            SQLite dbV2 = SQLite.db(TestV2.class, dbf);

            dbV2.execute(new SQL("delete from test_table_2"));

            dbV2.execute(new Tx()
                .add(new SQL("insert into test_table_2 (id, value_2) values (1, 'test1')"))
                .add(new SQL("insert into test_table_2 (id, value_2) values (2, 'test2')"))
                .add(new SQL("insert into test_table_2 (id, value_2) values (3, 'test3')"))
            );

            List<SQLite.Record> list2 = dbV2.query(new SQL("select * from test_table_2"));

            assertEquals(3, list2.size());

            assertEquals(list2.get(2).getString("value_2"), "test3");

            dbV2.close();

        }

        finally {
            dbf.delete();
            SQLite.info().execute(new Tx()
                .add(new SQL("delete from versions where db_name = 'test'"))
                .add(new SQL("delete from versions where db_name = 'testv2'"))
            );
        }

    }
}