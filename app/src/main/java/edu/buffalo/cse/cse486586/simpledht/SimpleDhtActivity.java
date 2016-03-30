package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {
    public static SQLHelperClass sql;
    static final int SERVER_PORT = 10000;
    static final String TAG      = SimpleDhtActivity.class.getName();
    private static final String LDumpSelection = "@";
    public static final String KEY_FIELD   = "key";
    public static final String VALUE_FIELD = "value";
    public static final String PORT        = "port";


    public static SimpleDhtActivity singleActivity;
    public static Uri contentURI;
    public Cursor resultCursor;
    public static TextView tv;

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    public static SimpleDhtActivity getInstance() {
        return singleActivity;
    }
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        Log.e(TAG, "Application onCreate");
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);

        contentURI = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        //Log.e(TAG, "Local port found " + myPort);

        /**Create event handlers here**/
        tv = (TextView) findViewById(R.id.textView1);

        tv.setMovementMethod(new ScrollingMovementMethod());

        findViewById(R.id.btnTest).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));

        final TextView editText = (TextView) findViewById(R.id.editText);
        findViewById(R.id.btnSend).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                ContentValues cv = new ContentValues();
                cv.put(KEY_FIELD, editText.getText().toString());
                cv.put(VALUE_FIELD, editText.getText().toString());
                editText.setText("");

                Uri uri = getContentResolver().insert(contentURI,cv);
                if (uri == null) {
                    Log.e(TAG,"insert Failed for btnSend");
                }
            }
        });
        findViewById(R.id.btnClear).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                tv.setText("");
            }
        });
        findViewById(R.id.btnQuery).setOnClickListener(
            new View.OnClickListener() {
                @Override
                public  void onClick(View v) {
                    resultCursor = getContentResolver().query(contentURI,null,
                                    editText.getText().toString(),null,null);
                    if(resultCursor == null ) {
                        Log.e(TAG,"Data not found");
                    }
                    resultCursor.close();
                    /*String key  = resultCursor.getString(0);
                    String value= resultCursor.getString(1);
                    tv.append("\n**** Specific Found****");
                    tv.append("\n" + key + "\n");
                    tv.append(value);
                    resultCursor.close();*/
                }
        });

        findViewById(R.id.btnLDump).setOnClickListener(
                new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        resultCursor = getContentResolver().query(contentURI, null,
                                LDumpSelection, null, null);
                        if (resultCursor == null) {
                            Log.e(TAG, "Result null");

                        }

                        int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                        int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
                        if (keyIndex == -1 || valueIndex == -1) {
                            Log.e(TAG, "Wrong columns");
                            resultCursor.close();

                        }

                        resultCursor.moveToFirst();
                        tv.append("************LDump starts******\n");
                        while (!resultCursor.isAfterLast()) {
                            String key = resultCursor.getString(0);
                            String val = resultCursor.getString(1);
                            Log.e(TAG, " key :" + key + " value :" + val);
                            tv.append(key + "=" + val);
                            tv.append("\n");
                            resultCursor.moveToNext();
                        }
                        tv.append("\n************LDump ends******");
                        Log.e(TAG, "Found rows are :" + String.valueOf(resultCursor.getCount()));

                        resultCursor.close();
                    }
                });

        findViewById(R.id.btnGDump).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

            }
        });

        sql = SQLHelperClass.getInstance(getApplicationContext());
        if (singleActivity == null)
            singleActivity = this;
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

    public void setText(final String value){
        runOnUiThread(new Runnable() {
            @Override
            public void run() {
                tv.append(value);
                tv.append("\n************\n");
            }
        });
    }


}
