package edu.buffalo.cse.cse486586.simpledht;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    private static final String TAG = SimpleDhtProvider.class.getName();


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        synchronized (this) {
            Log.e(TAG, "To insert in DB Fn In ContentProvider:" + values.toString());


            if (SimpleDhtActivity.sql.insertValues(values) == -1) {
                Log.e(TAG, "Insertion into db failed for values :" + values.toString());
            }
            Log.e("insert", values.toString());
            return uri;
        }
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        synchronized(this) {
            Log.e(TAG,"query is "  + selection);
            if (selection.equalsIgnoreCase("@")) {
                Log.e(TAG,"To retrieve all the messages from system ");

                /*if (selectionArgs != null) {
                    for(String s :selectionArgs) {
                        Log.e(TAG,"selectionArgs " + s);
                    }

                } else {
                    Log.e(TAG,"selectionArgs  is NULL");
                }*/
                selection = null;
                selectionArgs = null;
                sortOrder = null;

            }
            Cursor c = SimpleDhtActivity.sql.getData(projection, selection,selectionArgs, sortOrder);

            //Log.e("query", selection);
            return c;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub


        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
