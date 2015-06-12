package net.glasmann.base;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteConstants;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteJob;
import com.almworks.sqlite4java.SQLiteQueue;
import com.almworks.sqlite4java.SQLiteStatement;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** ~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~
 * A wrapper around sqlite4java (which in turn wraps SQLite).
 */
public class SQLite {

    // location on the file system for use by this wrapper, native libs will be deployed here
    public static final File SQLITE_HOME = new File(System.getProperty("user.home"), ".sqlite");

    // instance of this class for storage database metadata, versions, etc
    private static SQLite s_infoDb;

    // static Map containing instances of this class by Def and DB File
    private static Map<Class<? extends Def>, Map<File, SQLite>> s_dbMap = new HashMap<Class<? extends Def>, Map<File, SQLite>>();

    // a Logger
    private static Logger s_log = Logger.getLogger(SQLite.class.getName());

    // filenames for the native libs packaged with this class, these will be extracted to the filesystem
    // when this class is referenced
    private static final String[] NATIVE_LIB_FILENAMES = {
        "libsqlite4java-linux-amd64.so",
        "libsqlite4java-linux-i386.so",
        "libsqlite4java-osx.dylib",
        "sqlite4java-win32-x64.dll",
        "sqlite4java-win32-x86.dll",
    };

    static {
//        System.setProperty("sqlite4java.debug", "true");

        // extract the native SQLite libs to the file system and tell sqlite4java where they are
        File libs = new File(SQLITE_HOME, ".libs");
        if (!libs.exists()) {
            libs.mkdirs();
        }
        for(String filename: NATIVE_LIB_FILENAMES) {
            final InputStream in = SQLite.class.getResourceAsStream("/" + filename);
            if (in != null) {
                try {
                    s_log.info("Extracting " + filename);
                    Files.copy(in, FileSystems.getDefault().getPath(libs.getAbsolutePath(), filename), StandardCopyOption.REPLACE_EXISTING);
                }
                catch (IOException e) {
                    s_log.severe("Can't extract " + filename);
                    e.printStackTrace();
                }
            }
        }
        System.setProperty(com.almworks.sqlite4java.SQLite.LIBRARY_PATH_PROPERTY, libs.getAbsolutePath());

        // initialize the metadata info db
        Def infoDef = new Info();
        s_infoDb = new SQLite(infoDef, new File(SQLITE_HOME, infoDef.getName()));
        try {
            s_infoDb.execute(s_infoDb.def().getSchemaUpdates().get(0));
        }
        catch (SQLiteException e) {
            s_log.severe(e.getMessage());
        }

    }

    /**
     * Creates and returns an instance of this class configured appropriately for the specified
     * database definition class.  The database schema as defined in the specified Def class 
     * will be created and/or migrated as needed when this class is instantiated.
     * @param defClass A class that extends SQLite.Def and creates a versioned schema when constructed
     * @return an instance of SQLite
     * @throws SQLiteException
     */
    public static <T extends Def> SQLite db(Class<T> defClass) throws SQLiteException {
        return db(defClass, null);
    }
    
    /**
     * @see SQLite db(Class<T> defClass)
     * @param dbFile The file on disk that holds the SQLite data
     * @throws SQLiteException
     */
    public static <T extends Def> SQLite db(Class<T> defClass, File dbFile) throws SQLiteException {
        Map<File, SQLite> fileDbMap = s_dbMap.get(defClass);
        if (fileDbMap == null) {
            fileDbMap = new HashMap<File, SQLite>();
            s_dbMap.put(defClass, fileDbMap);
        }

        Def def = null;
        try {
            def = defClass.newInstance();
        }
        catch (InstantiationException e) {
            throw new SQLiteException(SQLiteConstants.SQLITE_ERROR, "Unable to instantiate: " + e.getMessage());
        }
        catch (IllegalAccessException e) {
            throw new SQLiteException(SQLiteConstants.SQLITE_ERROR, "Illegal access of: " + e.getMessage());
        }

        if (dbFile == null) {
            dbFile = new File(SQLITE_HOME, def.getName());
        }

        SQLite db = fileDbMap.get(dbFile);
        if (db == null) {
            db = new SQLite(def, dbFile);
            db.migrate();
            fileDbMap.put(dbFile, db);
        }
        return db;
    }

    /**
     * Returns an instance of SQLite for the metadata db manged by this class.  Contains versions, etc.
     * @return
     */
    public static SQLite info() {
        return s_infoDb;
    }

    /** ~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~ */

    private Def def;
    private File _dbFile = null;
    private SQLiteQueue _q = null;

    /**
     * Construct an instance of SQLite for the specified database Def and File
     * @param dbDef
     * @param dbFile
     */
    public SQLite(Def dbDef, File dbFile) {
        this.def = dbDef;
        this._dbFile = dbFile;
        open();
    }

    public Def def() { return def; }

    public File file() { return _dbFile; }

    public void open() {
    	if (_q != null && !_q.isStopped()) {
    		return;
    	}
        this._q = new SQLiteQueue(this._dbFile);
        this._q.start();
    }
    
    public int version() throws SQLiteException {
        List<Record> list = info().query(new SQL("select version from versions where db_name = ?").set(1, def.getName()));
        if (list != null && list.size() > 0) {
            return list.get(0).getInt("version");
        }
        else {
            info().execute(new Tx()
                    .add(new SQL("insert into versions (db_name, version) values (?, 0)").set(1, def.getName()))
            );
            return 0;
        }
    }

    public void migrate() throws SQLiteException {
        int v = version();
        if (v < def.getSchemaUpdates().size()) {
            for (int i = v; i < def.getSchemaUpdates().size(); i++) {
                execute(def.getSchemaUpdates().get(i));
                info().execute(new SQL("update versions set version = ? where db_name = ?").set(1, i + 1).set(2, def.getName()));
            }
        }
    }

    public List<Record> query(final SQL sql) throws SQLiteException {

        if (!_dbFile.exists()) {
            return new ArrayList<Record>();
        }
        if (_q.isStopped()) {
        	throw new SQLiteException(SQLiteConstants.SQLITE_ERROR, this.def().getName() + " has been closed and the job q is stopped.");
        }
        return _q.execute(new SQLiteJob<List<Record>>() {
        	protected List<Record> job(SQLiteConnection connection) throws SQLiteException {
                List<Record> list = new ArrayList<Record>();
                SQLiteStatement st = connection.prepare(sql.getSql());
                try {
	                while (st.step()) {
	                    Record r = new Record();
	                    for (int i = 0; i < st.columnCount(); i++) {
	                        r.set(st.getColumnName(i), st.columnValue(i));
	                    }
	                    list.add(r);
	                }
                }
                finally {
                	st.dispose();
                }
                return list;
        	}
        }).complete();
        
    }

    public List<List<Record>> execute(final Tx tx) throws SQLiteException {

        if (_q.isStopped()) {
        	throw new SQLiteException(SQLiteConstants.SQLITE_ERROR, this.def().getName() + " has been closed and the job q is stopped.");
        }

        return _q.execute(new SQLiteJob<List<List<Record>>>() {
        	protected List<List<Record>> job(SQLiteConnection connection) throws SQLiteException {
    	
		    	List<List<Record>> list = new ArrayList<List<Record>>();
		        for (int i = 0; i < tx.size(); i++) {
		            String sql = tx.get(i).getSql();
//		            s_log.info(sql);
		            if (sql.trim().toLowerCase().startsWith("select")) {
		                List<Record> list2 = new ArrayList<Record>();
		                SQLiteStatement st = connection.prepare(sql);
		                try {
			                while (st.step()) {
			                    Record r = new Record();
			                    for (int ii = 0; ii < st.columnCount(); ii++) {
			                        r.set(st.getColumnName(ii), st.columnValue(ii));
			                    }
			                    list2.add(r);
			                }
			                list.add(list2);
		                }
		                finally {
		                	st.dispose();
		                }
		            }
		            else {
		                connection.exec(sql);
		            }
		        }
		        return list;
		        
        	}
        }).complete();
    }
    public List<Record> execute(final SQL st) throws SQLiteException {

        if (_q.isStopped()) {
        	throw new SQLiteException(SQLiteConstants.SQLITE_ERROR, this.def().getName() + " has been closed and the job q is stopped.");
        }

        return _q.execute(new SQLiteJob<List<Record>>() {
        	protected List<Record> job(SQLiteConnection connection) throws SQLiteException {
    	
        		String sql = st.getSql();
//        		s_log.info(sql);

        		List<Record> list2 = new ArrayList<Record>();
	            if (sql.trim().toLowerCase().startsWith("select")) {
	                SQLiteStatement st = connection.prepare(sql);
	                try {
		                while (st.step()) {
		                    Record r = new Record();
		                    for (int ii = 0; ii < st.columnCount(); ii++) {
		                        r.set(st.getColumnName(ii), st.columnValue(ii));
		                    }
		                    list2.add(r);
		                }
	                }
	                finally {
	                	st.dispose();
	                }
	            }
	            else {
	                connection.exec(sql);
	            }

	            return list2;

        	}
        }).complete();
        	
	}

    /**
     * Convenience method that executes an Insert Transaction and just returns the first  
     * List of Records
     * @param tx
     * @return
     * @throws SQLiteException
     */
    public List<Record> insert(Insert tx) throws SQLiteException {
        List<List<Record>> list  = execute(tx);
        if (list.size() > 0) {
            return list.get(0);
        }
        else {
            return new ArrayList<Record>();
        }
    }

    /**
     * Waits for all pending jobs to finish and then shuts down the job q.  
     * @throws InterruptedException
     */
    public void close() throws InterruptedException {
        if (_q.isStopped()) {
        	return;
        }
    	_q.stop(true).join();
    }

    /** ~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~ */

    public static class Record {
        private Map<String, Object> _map = new HashMap<String, Object>();
        private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S z");

        public Map<String, Object> getRecord() {
            return _map;
        }

        public void set(String field, Object value) {
            if (value instanceof Date) {
                value = sdf.format((Date)value);
            }
            _map.put(field, value);
        }

        public Double getDouble(String field) {
            Object value = _map.get(field);
            if (value == null) {
                return null;
            }
            return Double.valueOf(value.toString());
        }
        public Integer getInt(String field) {
            Object value = _map.get(field);
            if (value == null) {
                return null;
            }
            return Integer.valueOf(value.toString());
        }
        public Long getLong(String field) {
            Object value = _map.get(field);
            if (value == null) {
                return null;
            }
            return Long.valueOf(value.toString());
        }
        public String getString(String field) {
            Object value = _map.get(field);
            if (value == null) {
                return null;
            }
            return value.toString();
        }
        public Date getDate(String field) throws ParseException {
            String value = getString(field);
            if (value == null) {
                return null;
            }
            return sdf.parse(value);
        }
    }

    /** ~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~ */

    /** 
     * The SqlStatement class allows parameterized sql statements to be assembled separately from a
     * database connection. Parameters are specified as question marks ("?") in the sql statement.
     * Parameters are then "set" by specifying a 1 based index and a value.
     */
    public static class SQL
    {

        private StringBuffer sql1 = new StringBuffer();
        private Map<Integer, String> values = new HashMap<Integer, String>();
        private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S z");

        public SQL() {}
        public SQL(String sql) {
            sql1.append(sql);
        }

        public void chop(String d) {
            if (sql1.length() > 0 && sql1.lastIndexOf(d) > 0) {
                if (sql1.substring(sql1.lastIndexOf(d)).equals(d)) {
                    sql1.delete(sql1.lastIndexOf(d), sql1.length());
                }
            }
        }

        /**
         * The set method allows a parameter value to be specified.  The index starts at 1.
         * @param idx
         * @param obj
         */
        public SQL set(int idx, Object obj) {

            if (obj == null) {
                return this;
            }

            String value = obj.toString();
            if (obj instanceof String) {
                value = value.replaceAll("\\\\", "\\\\\\\\");
                value = value.replaceAll("\\?", "\\<Q\\>");
                value = value.replaceAll(Pattern.quote("'"), Matcher.quoteReplacement("''"));
                value = "'" + value + "'";
            }
            else if (obj instanceof Date) {
//                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                value = "'" + sdf.format((Date)obj) + "'";
            }
            else if (obj instanceof Boolean) {
                if ((Boolean)obj) {
                    value = "'true'";
                }
                else {
                    value = "'false'";
                }
            }
            else if (obj instanceof SQL) {
                SQL sql = (SQL)obj;
                value = sql.getSql();
            }

            values.put(idx, value);

            return this;

        }

        /**
         * Returns the sql statement with parameters replaced with the set values.
         * @return
         */
        public String getSql() {

            StringBuffer sql2 = new StringBuffer(sql1);

            int count = 0;
            int p1 = -1;
            int p2 = -1;
            while ((p1 = sql2.indexOf("?", p2)) > -1 ) {
                count++;
                String value = values.get(count);
                if (value == null) {
                    value = "NULL";
                }
                sql2.replace(p1, p1 + 1, value);
                p2 = p1 + 1;
            }

            String sql = sql2.toString();
            sql = sql.replaceAll(Pattern.quote("<Q>"), Matcher.quoteReplacement("?"));

            return sql;
        }

        /**
         * Used to build the sql statement.  Appends the specified string to the current statement.
         * @param s
         * @return
         */
        public SQL append(String s) {
            sql1.append(s);
            return this;
        }

        /**
         * Resets the parameters.
         */
        public SQL reset() {
            this.values.clear();
            return this;
        }

        @Override
        public String toString()
        {
            return this.getSql();
        }


    }

    /** ~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~ */

    public static class Tx {

        private List<SQL> _stmts = new ArrayList<SQL>();

        public Tx add(SQL sql) {
            _stmts.add(sql);
            return this;
        }

        public int size() {
            return _stmts.size() + 2;
        }

        public SQL get(int i) {
            if (i == 0) {
                return new SQL("BEGIN");
            }
            else if (i == (size() - 1)) {
                return new SQL("COMMIT");
            }
            else {
                return _stmts.get(i - 1);
            }
        }
    }

    public static class Insert extends Tx {
        private final String NEXT_VAL = "net.glasmann.base.SQLite.nextValue";
        private String table;
        private Map<String, Object> fields = new HashMap<String, Object>();

        public Insert(String table) {
            this.table = table;
        }

        public Insert field(String name, Object value) { fields.put(name, value); return this;}
        public Insert field(String name) { fields.put(name, NEXT_VAL); return this;}

        public Insert build() {

            // build the insert statement
            SQL sql1 = new SQL();
            sql1.append("insert into ");
            sql1.append(table).append(" ");
            sql1.append("(");
            for (String name : fields.keySet()) {
                sql1.append(name);
                sql1.append(",");
            }
            sql1.chop(",");
            sql1.append(") values (");
            for (String name : fields.keySet()) {
                sql1.append("?");
                sql1.append(",");
            }
            sql1.chop(",");
            sql1.append(")");
            int c = 0;
            for (String name : fields.keySet()) {
                c++;
                Object value = fields.get(name);
                if (NEXT_VAL.equals(value.toString())) {
                    value = new SQL("(select max(" + name + ") from " + table + ") + 1");
                }
                sql1.set(c, value);
            }
            add(sql1);

            // build the select statement
            SQL sql2 = new SQL("select * from " + table + " where ROWID = last_insert_rowid();");
            add(sql2);

            return this;
        }
    }

    public static class Upsert extends Tx {

        private String table;
        private Map<String, Object> keys = new HashMap<String, Object>();
        private Map<String, Object> insertFields = new HashMap<String, Object>();
        private Map<String, Object> updateFields = new HashMap<String, Object>();

        public Upsert(String table) {
            this.table = table;
        }

        public Upsert field(String name, Object value) {
            return field(name, value, false);
        }
        public Upsert field(String name, Object value, boolean onlyOnInsert) {
            if (onlyOnInsert) {
                insertFields.put(name, value);
            }
            else {
                insertFields.put(name, value);
                updateFields.put(name, value);
            }
            return this;
        }

        public Upsert key(String name, Object value) {
            keys.put(name, value);
            return this;
        }

        public Upsert build() {

            // build the insert statement
            SQL sql1 = new SQL();
            sql1.append("insert or ignore into ");
            sql1.append(table).append(" ");
            sql1.append("(");
            for (String name : keys.keySet()) {
                sql1.append(name);
                sql1.append(",");
            }
            for (String name : insertFields.keySet()) {
                sql1.append(name);
                sql1.append(",");
            }
            sql1.chop(",");
            sql1.append(") values (");
            for (String name : keys.keySet()) {
                sql1.append("?");
                sql1.append(",");
            }
            for (String name : insertFields.keySet()) {
                sql1.append("?");
                sql1.append(",");
            }
            sql1.chop(",");
            sql1.append(")");
            int c = 0;
            for (String name : keys.keySet()) {
                c++;
                sql1.set(c, keys.get(name));
            }
            for (String name : insertFields.keySet()) {
                c++;
                sql1.set(c, insertFields.get(name));
            }
            add(sql1);

            // build the update statement
            SQL sql2 = new SQL();
            sql2.append("update ");
            sql2.append(table).append(" ");
            sql2.append("set ");
            for (String name : updateFields.keySet()) {
                sql2.append(name);
                sql2.append(" = ?,");
            }
            sql2.chop(",");
            sql2.append(" where ");
            for (String name : keys.keySet()) {
                sql2.append(name);
                sql2.append(" = ? and ");
            }
            sql2.chop(" and ");
            c = 0;
            for (String name : updateFields.keySet()) {
                c++;
                sql2.set(c, updateFields.get(name));
            }
            for (String name : keys.keySet()) {
                c++;
                sql2.set(c, keys.get(name));
            }
            add(sql2);

            return this;
        }
    }

    /** ~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~ */

    public static abstract class Def {
        private String name;
        private List<Tx> schemaUpdates = new ArrayList<Tx>();

        protected Def schema(int version, SQL st) {
            if (schemaUpdates.size() < version) {
                for (int i = schemaUpdates.size(); i < version; i++) {
                    schemaUpdates.add(new Tx());
                }
            }
            Tx tx = schemaUpdates.get(version - 1);
            tx.add(st);
            return this;
        }

        public List<Tx> getSchemaUpdates() {
            return schemaUpdates;
        }
        public String getName() {
            return name;
        }
        protected void setName(String name) {
            this.name = name;
        }
    }

    /** ~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~'~.~ */

    private static class Info extends Def {
        public Info() {
            setName("database_info");
            schema(1, new SQL()
                .append("create table if not exists versions ( ")
                .append("db_name text not null, ")
                .append("version integer not null, ")
                .append("PRIMARY KEY (db_name) ")
                .append(") ")
            );
        }
    }

}
