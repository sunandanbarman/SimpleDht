package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    public static String TAG  = SimpleDhtProvider.class.getName();

    public static String predHash = "", succHash = ""; //String having hash(predPort) / hash(succPort)
    private static Map<String,String> predAllInRing, succAllInRing; //TODO : Remove later; Contains list of <port,Pred> and <predWithHash,Succ>
    public static Set<String> msgSentForLookup;
    public static int predPort, succPort;
    private static String node_id = "";
    public static String myPort = "";

    public static String ACK = "ACK";
    public static String INSERT_LOOKUP= "INSERT_LOOKUP";
    public static String QUERY_LOOKUP = "QUERY_LOOKUP";
    public static String ALIVE        = "ALIVE";
    public static String NODE_ADDED   = "ADDED";
    public static String UPDATE_SUCC  = "UPDATE_SUCC";

    public static final int TIMEOUT          = 1000;
    public static final int MAX_MSG_LENGTH   = 512;
    public static final int SERVER_PORT      = 10000;
    public static final int NODE_JOINER_PORT = 11108;
    public static final int[] AVD_ID         = {5554,5556,5558,5560,5562};
    public static final int[] REMOTE_PORT    = {11108,11112,11116,11120,11124};
    public static SimpleDhtProvider singleInstance;

    public ChordList<String> chordList;

    /*%%%%%%%%%%%%%%%%%%%%% HELPER FUNCTIONS START %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
    public String getMyPort() {
        TelephonyManager tel = (TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);

        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        return String.valueOf((Integer.parseInt(portStr) * 2));
    }
    private static boolean isFirstNode() {
        if (Integer.valueOf(myPort) < predPort) {
            return true;
        }
        return false;
    }
    public static boolean doLookup(String hashKey) {
        if (predHash.equals("") && succHash.equals("") ) {
            Log.e(TAG,"No predecessor and successor, hence inserting within own provider");
            return true;
        }
        Log.e(TAG,"predHash " + predHash + " node_id " + node_id);
        Log.e(TAG," hashCompare :" + hashKey.compareTo(predHash));
        Log.e(TAG," hashCompare :" + hashKey.compareTo(node_id));
        if (hashKey.compareTo(predHash)  >= 1 && ( hashKey.compareTo(node_id)  <= 0) ) {
            Log.e(TAG,"In doLookup found true");
            return true;
        }
        if (predHash.equalsIgnoreCase(succHash)) {
            Log.e(TAG,"predHash == succHash");
            if (hashKey.compareTo(predHash) >= 1 && hashKey.compareTo(succHash) >= 1) { // hashKey greater than both
                Log.e(TAG,"hashKey greater than both predHash and succHash");
                return true;
            }
        }
        /*if (isFirstNode()) {
            Log.e(TAG,"This is the first node !");
            if (hashKey.compareTo(predHash) > 0) {
                Log.e(TAG,"insertTrue 1 in isFirstNode");
                return true;
            }
            if (hashKey.compareTo(node_id) <= 0 ) {
                Log.e(TAG,"insertTrue 2 in isFirstNode");
                return true;
            }
        }*/
        return false;
    }
    private void createServerSocket() {
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket " + e.getMessage());
            return;
        }

    }
    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
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
    public long insertIntoDatabase(ContentValues cv) {
        if (SimpleDhtActivity.sql.insertValues(cv) == -1) {
            Log.e(TAG, "Insertion into db failed for values :" + cv.toString());
            return -1;
        }
        Log.e(TAG,"Database insertion success for values " + cv.toString());
        SimpleDhtActivity.getInstance().setText(cv.toString());
        return 0;
    }
    // uri.toString() --> content://edu.buffalo.cse.cse486586.simpledht.provider
    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        //Log.e(TAG,uri.toString());
        String port = "";
        String key  = values.get(SimpleDhtActivity.KEY_FIELD).toString();
        String value= values.get(SimpleDhtActivity.VALUE_FIELD).toString();

        if (values.get("port") != null) {
            port    = values.get(SimpleDhtActivity.PORT).toString();
            values.remove(SimpleDhtActivity.PORT);
        }
        Log.e(TAG,"cv =" + values.toString());
        Log.e(TAG,"insertBegin, port :" + port);
        Log.e(TAG,"insertBegin, key " + key);
        Log.e(TAG,"insertBegin, value " + value);
        String hashKey = "";
        try {
            hashKey = genHash(key);
            Log.e(TAG,"hashKey in insert " + hashKey);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
        //synchronized (this)
        {
            Log.e(TAG,"Content values -->" + values.get(SimpleDhtActivity.KEY_FIELD) +  " = "  + values.get(SimpleDhtActivity.VALUE_FIELD));
            Log.e(TAG, "To insert in DB Fn In ContentProvider:" + values.toString());

            if (doLookup(hashKey)) { // value can be inserted into this node
                if (insertIntoDatabase(values) == -1) {
                    return null;
                } else {
                    Log.e(TAG,"Database insertion success for values " + values.toString());
                    SimpleDhtActivity.getInstance().setText(values.toString());
                    return uri;
                }
/*
                if (insertIntoDatabase(values) == -1) {
                    Log.e(TAG, "Insertion into db failed for values :" + values.toString());
                    return null;
                }else {
                    Log.e(TAG,"Database insertion success for values " + values.toString());
                    SimpleDhtActivity.getInstance().setText(values.toString());
                    //SimpleDhtActivity.updateTextView(values);
                    return uri;
                }
*/
            }
            //else if ( msgSentForLookup.contains(new StringBuilder(key).append(":").append(value).toString())) {
/*
            else if (message) {
                Log.e(TAG,"Message returned to originator... Insert in DB");
                //msgSentForLookup.remove(new StringBuilder(key).append(":").append(value).toString());
                if (SimpleDhtActivity.sql.insertValues(values) == -1) {
                    Log.e(TAG, "Insertion into db failed for values :" + values.toString());
                    return null;
                }else {
                    Log.e(TAG,"Database insertion success for values " + values.toString());
                    SimpleDhtActivity.getInstance().setText(values.toString());
                    //SimpleDhtActivity.updateTextView(values);
                    return uri;
                }
            }
*/
            else {
                Log.e(TAG, "Lookup required for this message");
                if (port.equals("")) {
                    port = myPort;
                }
                Log.e(TAG,"succPort " + String.valueOf(succPort));
                Message message = new Message(key, value, hashKey, INSERT_LOOKUP, port, String.valueOf(succPort), "dummy", "dummy");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                msgSentForLookup.add(new StringBuilder(key).append(":").append(value).toString());
            }

            Log.e("insert", values.toString());
            return uri;
        }
    }
    private void getPredAndSucc()  {
        predAllInRing= new HashMap<String, String>();
        succAllInRing= new HashMap<String, String>();

        for(int i= 11124; i >= NODE_JOINER_PORT; i = i - 4) {
            if (i == NODE_JOINER_PORT) {
                predAllInRing.put(String.valueOf(i),"11124");
                break;
            }
            predAllInRing.put(String.valueOf(i),String.valueOf(i-4));
        }
        Log.e(TAG,"predAllInRing ");
        for(Map.Entry<String,String> entry : predAllInRing.entrySet()) {
            Log.e(TAG," key " + entry.getKey() + " = " + entry.getValue());

        }

        for(Integer i = NODE_JOINER_PORT; i <= 11124; i = i+4) {
            if (i == 11124) {
                succAllInRing.put(String.valueOf(i),String.valueOf(NODE_JOINER_PORT));
                break;
            }
            succAllInRing.put(String.valueOf(i),String.valueOf(i+4));

        }
        Log.e(TAG,"succAllInRing ");
        for(Map.Entry<String,String> entry : succAllInRing.entrySet()) {
            Log.e(TAG," key " + entry.getKey() + " = " + entry.getValue());

        }


        /*succAllInRing.put("11108","11112");
        succAllInRing.put("11112","11116");
        succAllInRing.put("11116","11120");
        succAllInRing.put("","");
        succAllInRing.put("","");*/
        /*predPort = Integer.valueOf(myPort) - 4;
        succPort = Integer.valueOf(myPort) + 4;
        if (Integer.valueOf(myPort) == NODE_JOINER_PORT) {
            predPort = 11124;
        }
        predWithHash.put(predPort,genHash(String.valueOf(predPort)));
        if (Integer.valueOf(myPort) == 11124) {
            succPort = NODE_JOINER_PORT;
        }
        succWithHash.put(succPort,genHash(String.valueOf(succPort)));*/
    }
    private void sendAliveMessageToAVD0() {
        Log.e(TAG, "sendAliveMessageToAVD0 by " + myPort);
        Message message = new Message("key","value","hash",ALIVE,myPort,String.valueOf(NODE_JOINER_PORT),"dummy","dummy");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);
    }
    public void sendNodeAddedMessage(Message message) {
        Log.e(TAG, "Node " + message.remotePort + " is added. To send ACK to it");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
        if (chordList.size() >2 ) //inform the pred of new joinee to update its successor as well
        {
            Message temp = new Message(message);
            temp.messageType= UPDATE_SUCC;
            temp.succPort   = message.remotePort;
            temp.remotePort = chordList.getPredecessor(message.remotePort);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,temp);
        }
    }
    @Override
    public boolean onCreate() {
        Log.e(TAG,"SimpleDhtProvider onCreate");
        myPort = getMyPort();
        Log.e(TAG, "myPort " + myPort);
        msgSentForLookup = new HashSet<String>();
        // TODO Auto-generated method stub
        createServerSocket();
        //getPredAndSucc();
        if (myPort.equalsIgnoreCase(String.valueOf(NODE_JOINER_PORT))) {
            predHash = "";
            succHash = "";
            chordList = new ChordList<String>();
            chordList.add(myPort);
        }


        try {

            node_id = genHash(String.valueOf(Integer.valueOf(myPort) / 2));
            Log.e(TAG, "node_id " + node_id);
/*
            if (predAllInRing.size() > 0 ) {
                predHash = genHash(String.valueOf(Integer.valueOf(predAllInRing.get(myPort)) / 2 ));
            } else {
                predHash = "";
            }
            if (succAllInRing.size() >0 ) {
                succHash = genHash(String.valueOf(Integer.valueOf(succAllInRing.get(myPort)) / 2));
            } else {
                succHash = "";
            }
            Log.e(TAG,"predHash " + predHash);
            Log.e(TAG,"succHash " + succHash);
*/

            Log.e(TAG,"for 5554 :" + genHash("5554"));
            Log.e(TAG,"for 5556 :" + genHash("5556"));
            Log.e(TAG, "for 5558 :" + genHash("5558"));
            Log.e(TAG,"for 5560 :" + genHash("5560"));
            Log.e(TAG, "for 5562 :" + genHash("5562"));

            if (!(myPort.equalsIgnoreCase(String.valueOf(NODE_JOINER_PORT))) )
                sendAliveMessageToAVD0();
            /*Log.e(TAG,"predWithHash starts");
            for(Map.Entry<Integer,String> entry : predWithHash.entrySet()) {
                Log.e(TAG,"key " + entry.getKey() + " : " + entry.getValue());
            }
            Log.e(TAG,"succWithHash starts");
            for(Map.Entry<Integer,String> entry : succWithHash.entrySet()) {
                Log.e(TAG,"key " + entry.getKey() + " : " + entry.getValue());
            }*/

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (singleInstance == null) {
            singleInstance = this;
        }
       /* try {
            //getPredAndSucc();
            Log.e(TAG, "predHash :" + predWithHash.get(predPort));
            Log.e(TAG,"succHash :" + succWithHash.get(predPort));

            Log.e(TAG,"node_id :" + node_id);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();

        }*/

        /*if (myPort.equalsIgnoreCase("11108")) { //initially no other AVD are alive
            predPort = 11108;
            succPort = 11108;

            try {
                predWithHash.put(predPort,genHash(String.valueOf(predPort)));
                succWithHash.put(succPort,genHash(String.valueOf(succPort)));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

        } else { //send ALIVE message to AVD0 i.e. port 11108
            Message message = new Message("","",node_id,ALIVE,Integer.valueOf(myPort),NODE_JOINER_PORT);
            //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);

        }*/


        return true;
    }

    private Cursor returnAllLocalData(String[] projection) {
        Cursor c;
        c = SimpleDhtActivity.sql.getData(projection,null,null,null);
        return c;
    }
    /**
     * NOTE : No synchronization is required for ContentProvider as the underlying SQLite provides thread-safety.
     * @param uri
     * @param projection
     * @param selection
     * @param selectionArgs
     * @param sortOrder
     * @return
     */
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        Cursor c;
        boolean bReturnFromLocalOnly = false;
        String hashKey;
        try {
            hashKey = genHash(selection);
            if (!doLookup(hashKey)) {
                Message message = new Message();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);
                Thread.sleep(1500);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
        // TODO Auto-generated method stub
        //synchronized(this)
        {
            Log.e(TAG,"query is "  + selection);
            Log.e(TAG,"projection " + projection + " selection " + selection);
            if (selectionArgs != null) {
                for(String s:selectionArgs) {
                    Log.e(TAG," s =" + s);
                }
            }
            bReturnFromLocalOnly = selection.equalsIgnoreCase("@");
            if (!bReturnFromLocalOnly && selection.equalsIgnoreCase("*")) {
                if (predHash.equals("") && succHash.equals(""))  {
                    bReturnFromLocalOnly = true;
                }
            }

            if (bReturnFromLocalOnly) {
               c= returnAllLocalData(projection);
            } else {

                c = SimpleDhtActivity.sql.getData(projection, "key=?",new String[]{selection}, sortOrder);
            }

            return c;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub


        return 0;
    }
    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public static SimpleDhtProvider getInstance() {
        return singleInstance;
    }
    public void setPredAndSuccHash(String predPort_,String succPort_) {
        predPort = Integer.valueOf(predPort_);
        succPort = Integer.valueOf(succPort_);

        try {
            predHash = genHash(String.valueOf(predPort / 2));
            succHash = genHash(String.valueOf(succPort / 2));
        } catch(NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }
    public void setPredAndSucc(Message message) {
        if (message.messageType.equals(NODE_ADDED))
        {

            predPort = Integer.valueOf(message.predPort);
            succPort = Integer.valueOf(message.succPort);
            Log.e("setPredAndSucc","updating pred and succ " + predPort + " : " + succPort);
            try {
                predHash = genHash(String.valueOf(predPort / 2));
                succHash = genHash( String.valueOf(succPort /2) );
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            Log.e(TAG,"predHash and succHash set in setPredAndSucc " + predHash + " : " + succHash);
        }
        else {
            Log.e(TAG,"message.Type is not node_added :" + message.messageType);
        }
    }
    public void updateSuccessor(Message message) {
        if (message.messageType.equalsIgnoreCase(UPDATE_SUCC)) {
            succPort = Integer.valueOf(message.succPort);
            Log.e("updateSuccessor","updating succ " + " : " + succPort);
            try {
                succHash = genHash(String.valueOf(succPort/2));
            } catch(Exception e) {
                e.printStackTrace();
            }
        } else {
            Log.e(TAG,"message.type is not UPDATE_SUCC " + message.messageType);
        }
    }
}
