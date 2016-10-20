package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by anjali on 27/04/16.
 */
public class DBHelper extends SQLiteOpenHelper {

    private static final String DATABASE_NAME = "PA4";
    private static final int DATABASE_VERSION = 2;

    public static final String TABLE_NAME = "FirstTable";
    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";


    private static final String DATABASE_CREATE = "CREATE TABLE "
            + TABLE_NAME
            + "("
            + KEY_FIELD + " TEXT, "
            + VALUE_FIELD + " TEXT"
            + ");";

    public DBHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    // Method is called during creation of the database
    @Override
    public void onCreate(SQLiteDatabase database) {
        database.execSQL(DATABASE_CREATE);
    }

    // Method is called during an upgrade of the database,
    // e.g. if you increase the database version
    @Override
    public void onUpgrade(SQLiteDatabase database, int oldVersion,
                          int newVersion) {
        database.execSQL("DROP TABLE IF EXISTS"+TABLE_NAME);
        onCreate(database);
    }
}

