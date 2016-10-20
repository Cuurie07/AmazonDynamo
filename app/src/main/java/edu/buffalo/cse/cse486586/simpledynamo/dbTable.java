package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.sqlite.SQLiteDatabase;
import android.util.Log;

/**
 * Created by anjali on 27/04/16.
 */
public class dbTable {

    public static final String TABLE_NAME = "FirstTable";
    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";

    private static final String DATABASE_CREATE = "CREATE TABLE "
            + TABLE_NAME
            + "("
            + KEY_FIELD + " TEXT PRIMARY KEY, "
            + VALUE_FIELD + " TEXT"
            + ");";

    public static void onCreate(SQLiteDatabase database) {
        database.execSQL(DATABASE_CREATE);
    }

    public static void onUpgrade(SQLiteDatabase database, int oldVersion,
                                 int newVersion) {
        Log.w(dbTable.class.getName(), "Upgrading database from version "
                + oldVersion + " to " + newVersion
                + ", which will destroy all old data");
        database.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
        onCreate(database);
    }
}
