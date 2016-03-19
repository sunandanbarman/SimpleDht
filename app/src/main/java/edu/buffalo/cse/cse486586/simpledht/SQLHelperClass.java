package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.*;
import android.util.Log;

/**
 * Created by sunandan on 24 Feb 16
 * Database code referred from :
 * http://developer.android.com/training/basics/data-storage/databases.html
 */
class SQLHelperClass extends  SQLiteOpenHelper{
    private String TAG = SQLHelperClass.class.getName();
    private static String DB_TABLE = "Data";
    private static String DB_NAME  = "SimpleDht.db";
    public static int DB_VERSION = 1;

    private static SQLHelperClass instance_;
    // private static int COLUMN_ID = 0;
    // private static String COMMA_SEP  = ",";
    /***
     * column names
     */
    //private static String COLUMN_ID_ = "ID";
    private static String COLUMN_KEY = "key";
    private static String COLUMN_VAL = "value";
    private static String TEXT_TYPE  = " TEXT ";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE " + DB_TABLE + " ( " + COLUMN_KEY + " STRING PRIMARY KEY,"
                    + COLUMN_VAL + TEXT_TYPE + " )"
            ;
    /**
     * Create a helper object to create, open, and/or manage a database.
     * This method always returns very quickly.  The database is not actually
     * created or opened until one of {@link #getWritableDatabase} or
     * {@link #getReadableDatabase} is called.
     *
     * @param context to use to open or create the database
     *
     * Hide away the constructor to use a single instance of SQLite DB
     *
     * @param context
     */
    private SQLHelperClass(Context context) {
        super(context, DB_NAME, null, DB_VERSION);
        Log.e(TAG, "Class created");
    }

    /**
     * Singleton instance returned in a thread-safe manner
     * @param context
     * @return
     */
    public static synchronized SQLHelperClass getInstance(Context context) {

        if (instance_ == null) {
            instance_ = new SQLHelperClass(context.getApplicationContext());
        }
        return instance_;
    }


    @Override
    public void onCreate(SQLiteDatabase db) {

        db.execSQL(SQL_CREATE_TABLE);
        Log.e(TAG,"Database created");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

    }

    /**
     * Use this method to insert the values into DB
     * @param cv
     * @return
     */
    public long insertValues(ContentValues cv) {
        // COLUMN_ID++;
        // Gets the data repository in write mode
        SQLiteDatabase db = this.getWritableDatabase();

        // Insert the new row, returning the primary key value of the new row
        // insertWithOnConflict takes care to replace the row in case value already exists
        // ensuring key=value is always up-to date
        long newRowID;
        newRowID =db.insertWithOnConflict(DB_TABLE,null,cv,SQLiteDatabase.CONFLICT_REPLACE);
        db.close();
        return newRowID;
    }

    public Cursor getData(String[] projection, String selection, String[] selectionArgs,
                          String sortOrder) {
        SQLiteDatabase db = this.getReadableDatabase();
        Cursor c = db.query(DB_TABLE,projection,selection,selectionArgs,null,null,sortOrder);

        return c;
    }

}

