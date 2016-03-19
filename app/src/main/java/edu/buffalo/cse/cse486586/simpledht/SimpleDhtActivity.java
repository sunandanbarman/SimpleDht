package edu.buffalo.cse.cse486586.simpledht;

import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {
    public static SQLHelperClass sql;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);

        /**Create event handlers here**/
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.btnTest).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));

        findViewById(R.id.btnLDump).setOnClickListener(
                new LDump(tv, getContentResolver()));

        sql = SQLHelperClass.getInstance(getApplicationContext());
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
