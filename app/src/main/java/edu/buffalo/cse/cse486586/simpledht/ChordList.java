package edu.buffalo.cse.cse486586.simpledht;

import android.util.Log;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

/**
 * Created by sunandan on 3/24/16.
 */
public class ChordList<String> extends TreeSet<String> {
    String myPort;
    ChordList() {

    }
    @Override
    public boolean add(String obj) {
        Log.e("AddToTreeSet", obj.toString());
        return super.add(obj);
    }
    public String getPredecessor(String port) {
        Log.e("getPredecessor","to find for " + port);
        if (this.contains(port)) {

            if (this.first().equals(port)) {
                Log.e("getPredecessor","pred is " + this.last());
                return this.last();
            }
            if (this.size() == 2) {
                if (this.last().equals(port)) {
                    Log.e("getPredecessor","pred is " + this.first());
                    return this.first();
                }
            }
            Log.e("getPredecessor","pred is " + this.lower(port));
            return this.lower(port);
        }
        Log.e("getPredecessor","port not found");
        return null;
    }
    /**
     *
     * @param port
     * @return
     */
    public String getSuccessor(String port) {
        Log.e("getSuccessor","to find for " + port);
        if (this.contains(port)) {
            //if (this.size() == 2 || port.equals("11124"))
            {
                if (this.last().equals(port)) {
                    Log.e("getSuccessor","succ is " + this.first());
                    return this.first();
                }
            }
            Log.e("getSuccessor","succ is " + this.higher(port));
            return this.higher(port);
        }
        Log.e("getSuccessor","to find for " + port);
        return null;
    }
}
