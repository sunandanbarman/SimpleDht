package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.widget.TextView;

/**
 * Created by sunandan on 3/18/16.
 */
public class LDump implements View.OnClickListener {
    private static String TAG;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static final String LDumpSelection = "@";
    private static final String GDumpSelection = "*";

    private final TextView mTextView;
    private final ContentResolver mContentResolver;
    private final Uri mUri;


    public LDump(TextView _tv, ContentResolver _cr) {
        TAG = SimpleDhtActivity.TAG;
        mTextView = _tv;
        mContentResolver = _cr;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public void onClick(View v) {
        new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
    }

    private class Task extends AsyncTask<Void, String, Void> {
        protected void onProgressUpdate(String...strings) {
            mTextView.append(strings[0]);
            mTextView.append("\n");
            return;
        }


        private boolean testQuery() {
            try {

                Cursor resultCursor = mContentResolver.query(mUri, null,
                        LDumpSelection, null, null);
                if (resultCursor == null) {
                    Log.e(TAG, "Result null");
                    throw new Exception();
                }

                int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
                if (keyIndex == -1 || valueIndex == -1) {
                    Log.e(TAG, "Wrong columns");
                    resultCursor.close();
                    throw new Exception();
                }

                resultCursor.moveToFirst();

                while(!resultCursor.isAfterLast()) {
                    String key = resultCursor.getString(0);
                    String val = resultCursor.getString(1);
                    Log.e(TAG, " key :" + key + " value :" + val);
                    publishProgress(key + "=" + val);
                    resultCursor.moveToNext();
                }
                Log.e(TAG, String.valueOf(resultCursor.getCount()));

                resultCursor.close();
            } catch (Exception e) {
                return false;
            }

            return true;
        }

        @Override
        protected Void doInBackground(Void... params) {
            testQuery();
            return null;
        }
    }


}
