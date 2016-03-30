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
import android.database.MatrixCursor;
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

    //public static String ACK = "ACK";
    public static String ALIVE        = "ALIVE";
    public static String NODE_ADDED   = "ADDED";
    public static String UPDATE_SUCC  = "UPDATE_SUCC";

    public static String INSERT_LOOKUP= "INSERT_LOOKUP"; // used to ask for lookup to find proper position in chord ring
    public static String QUERY_LOOKUP = "QUERY_LOOKUP";  // used by originator for both "@" and "*" query

    public static String QUERY_DONE    = "QUERY_DONE";    // indicate the originator that the query was completed; tell them the actual portLocation
    public static String QUERY_GET_DATA= "QUERY_GET_DATA";// indicate the process to return local data (specified in "key", can be "@"s )
    public static String QRY_DATA_DONE = "QRY_DATA_DONE"; // indicate the key is found in local DB; inform remote avd of the result
    public static String ALIVE_NODES   = "ALIVE_NODES";
    public static String ALIVE_NODES_RESP = "ALIVE_NODES_RESP";
    public static String EMPTY         = "EMPTY";
    public static String DB_ROW_COUNT  = "DB_ROW_COUNT";
    public static String DB_ROW_COUNT_FOUND  = "DB_ROW_COUNT_FOUND";

    public static final int TIMEOUT          = 1000;
    public static final int MAX_MSG_LENGTH   = 4096; //HUGE: as the whole DB data is dumped  as a string
    public static final int SERVER_PORT      = 10000;
    public static final int NODE_JOINER_PORT = 11108;
    //public static final int[] AVD_ID         = {5554,5556,5558,5560,5562};
    public static final int[] REMOTE_PORT    = {11108,11112,11116,11120,11124};
    public static SimpleDhtProvider singleInstance;
    public static Map<String,String> hashWithPortMap;
    public static Map<String,String> portWithHashMap;

    public ChordList<String> chordList;
    public final static Object lock =  new Object();

    /*Helpers for query() operation*/
    public static boolean queryDone = false;
    public static String portLocation; // sent by AVD0 informing the AVD of the actual location of the key
    public static String queryKey, queryValue; // stores the result sent by remote avd after querying its database

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

        if (predHash.equalsIgnoreCase(succHash)) {
            Log.e(TAG,"predHash == succHash");
            if (hashKey.compareTo(predHash) >= 1 && hashKey.compareTo(succHash) >= 1) { // hashKey greater than both
                Log.e(TAG,"hashKey greater than both predHash and succHash");
                return true;
            }
        }
        if (hashKey.compareTo(predHash) > 0 && hashKey.compareTo(node_id) <= 0) {
            return true;
        }
        if (hashKey.compareTo(node_id)> 0 && hashKey.compareTo(predHash) > 0 && node_id.compareTo(predHash) < 0) { //first node, hashkey greater than all nodes
            return  true;
        }
        if (hashKey.compareTo(node_id) < 0 && hashKey.compareTo(predHash) <0 && node_id.compareTo(predHash) < 0 ) { //first node, hashKey lesser than all nodes
            return true;
        }
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

        hashKey = genHash(key);
        Log.e(TAG,"hashKey in insert " + hashKey);
        Log.e(TAG,"Content values -->" + values.get(SimpleDhtActivity.KEY_FIELD) +  " = "  + values.get(SimpleDhtActivity.VALUE_FIELD));
        Log.e(TAG, "To insert in DB Fn In ContentProvider:" + values.toString());

        if (doLookup(hashKey)) { // value can be inserted into this node
            if (insertIntoDatabase(values) == -1) {
                return null;
            } else {
                Log.e(TAG, "Database insertion success for values " + values.toString());
                //SimpleDhtActivity.getInstance().setText(values.toString());
                return uri;
            }
        }
        else {
            Log.e(TAG, "Lookup required for this message");
            if (port.equals("")) {
                port = myPort;
            }
            Log.e(TAG,"succPort " + String.valueOf(succPort));
            Log.e(TAG, "predPort " + String.valueOf(predPort));
            Message message = new Message(key, value, hashKey, INSERT_LOOKUP, port, String.valueOf(succPort), "dummy", "dummy");
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            //msgSentForLookup.add(new StringBuilder(key).append(":").append(value).toString());
        }

        Log.e("insert", values.toString());
        return uri;
    }
    /*private void getPredAndSucc()  {
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
    }*/
    private void sendAliveMessageToAVD0() {
        Log.e(TAG, "sendAliveMessageToAVD0 by " + myPort);
        String hash = genHash(String.valueOf(Integer.valueOf(myPort)/2));

        Message message = new Message("key","value","hash",ALIVE,myPort,String.valueOf(NODE_JOINER_PORT),"dummy","dummy");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
    }
    public void sendMessageToRemotePort(Message message) {
        if (message.messageType == NODE_ADDED)
            Log.e(TAG, "Node " + message.remotePort + " is added. To send ACK to it");
        else
            Log.e(TAG,"To send messsage to " + message.remotePort + " messageType " + message.messageType);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
    }

    private void createHashWithPortMap() {
        hashWithPortMap = new HashMap<String, String>();
        portWithHashMap = new HashMap<String, String>();
        String hash = "";
        for(int port : REMOTE_PORT) {
            hash = genHash(String.valueOf(port/2));
            hashWithPortMap.put(hash,String.valueOf(port));
            portWithHashMap.put(String.valueOf(port),hash);
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
        if (singleInstance == null) {
            singleInstance = this;
        }
        //lock = new Object();
        node_id = genHash(String.valueOf(Integer.valueOf(myPort) / 2));
        Log.e(TAG, "node_id " + node_id);
        if (myPort.equalsIgnoreCase(String.valueOf(NODE_JOINER_PORT))) {
            createHashWithPortMap();
            predHash = "";
            succHash = "";
            chordList = new ChordList<String>();
            chordList.add(portWithHashMap.get(myPort));
        }

        Log.e(TAG,"for 5554 :" + genHash("5554"));
        Log.e(TAG,"for 5556 :" + genHash("5556"));
        Log.e(TAG, "for 5558 :" + genHash("5558"));
        Log.e(TAG,"for 5560 :" + genHash("5560"));
        Log.e(TAG, "for 5562 :" + genHash("5562"));

        if (!(myPort.equalsIgnoreCase(String.valueOf(NODE_JOINER_PORT))) )
            sendAliveMessageToAVD0();
        return true;
    }

    public Cursor returnLocalData(String selection) {
        Cursor c;
        if (selection == null || selection.equalsIgnoreCase(SimpleDhtActivity.GDumpSelection))
            c = SimpleDhtActivity.sql.getData(null,null,null,null);
        else
            c=  SimpleDhtActivity.sql.getData(null, "key=?",new String[]{selection}, null);
        return c;
    }

    private void waitForResponse() {
        while(!queryDone) {
            synchronized (lock) {
                try {
                    lock.wait(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
        queryDone = false;
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
        Cursor c = null;
        //boolean bReturnFromLocalOnly = false;

        Log.e(TAG,"uri :" + uri);
        Log.e(TAG,"query is "  + selection);
        //if only 1 avd in chord ring, return all local data in case of "@" OR "*" queries
        if (predHash.equalsIgnoreCase("") && succHash.equalsIgnoreCase("") &&
            (  selection.equalsIgnoreCase(SimpleDhtActivity.LDumpSelection) || selection.equalsIgnoreCase(SimpleDhtActivity.GDumpSelection))) {
            selection = SimpleDhtActivity.LDumpSelection;
        }
        if (selection.equalsIgnoreCase(SimpleDhtActivity.LDumpSelection)) { //Local dump
            Log.e(TAG,"Local dump starts");
            c = returnLocalData(null);
        }
        else if (selection.equalsIgnoreCase(SimpleDhtActivity.GDumpSelection)) { //global dump
            Log.e(TAG,"Global dump starts");
            Message message = new Message(selection,"dummy","dummy",ALIVE_NODES,myPort,String.valueOf(NODE_JOINER_PORT),"dummy","dummy");
            new ClientTask().execute(message);
            //try {
            MatrixCursor matrixCursor = new MatrixCursor(new String[]{SimpleDhtActivity.KEY_FIELD,SimpleDhtActivity.VALUE_FIELD});
            waitForResponse();
/*
            while(!queryDone) {
                synchronized (lock) {
                    lock.wait(1000);
                }

            }
*/
            queryDone = false;
            Log.e(TAG,"Response of alive nodes " + SimpleDhtProvider.queryKey);
            String[] aliveNodes = SimpleDhtProvider.queryKey.split(":");
            StringBuilder sKeyList = new StringBuilder();
            StringBuilder sValList = new StringBuilder();
            for(String sAlivePort : aliveNodes) {
                Log.e(TAG,"sAlive " + sAlivePort);
                message.remotePort = sAlivePort;
                message.originPort = myPort;
                message.messageType= QUERY_GET_DATA;
                message.key        = SimpleDhtActivity.GDumpSelection; // get all data

                new ClientTask().execute(message);
                waitForResponse();
                //Log.e(TAG, "Db rows from  " + message.remotePort + " is " + SimpleDhtProvider.queryKey);


                /*int rowCount = Integer.valueOf(SimpleDhtProvider.queryKey);
                if (rowCount > 0) {
                    SimpleDhtProvider.queryKey = "";
                    for(int i=0; i < rowCount; i++) {
                        Log.e(TAG,"ask for " + i + " row from " + message.remotePort );
                        new ClientTask().execute(message);
                        Log.e(TAG,"query result for " + i + " row is " + SimpleDhtProvider.queryKey + "=" + SimpleDhtProvider.queryValue);
                        String[] results = new String[]{SimpleDhtProvider.queryKey,SimpleDhtProvider.queryValue};
                        matrixCursor.addRow(results);
                    }

                } else {
                    Log.e(TAG,"row count from  " + message.remotePort + " is zero");
                }*/

                Log.e(TAG, "all keys from " + message.remotePort + " is " + SimpleDhtProvider.queryKey);
                Log.e(TAG, "all values from " + message.remotePort + " is " + SimpleDhtProvider.queryValue);
                if (!SimpleDhtProvider.queryKey.equalsIgnoreCase(SimpleDhtProvider.EMPTY)) {
                    String[] keyArr = SimpleDhtProvider.queryKey.split(":");
                    String[] valArr = SimpleDhtProvider.queryValue.split(":");

                    for(int i=0; i < keyArr.length; i++) {
                        String[] results  = new String[]{keyArr[i],valArr[i]};
                        matrixCursor.addRow(results);
                    }
                } else {
                    Log.e(TAG,"No data in " + message.remotePort);
                }
                //sKeyList = sKeyList.append(message.key.split(":"));
                //sValList = sValList.append(message.value.split(":"));
            }

            //String[] results = new String[]{sKeyList.toString(), sValList.toString()};

            //matrixCursor.addRow(results);
            return matrixCursor;
/*
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
*/
        }
        else{
            Log.e(TAG, "Ask to avd0");
            c = SimpleDhtActivity.sql.getData(null, "key=?",new String[]{selection}, null);
            Log.e(TAG,"Rows in DB ==>" + String.valueOf(c.getCount()));

            if ((c== null) || (c.getCount() == 0)) { //not found in local database, ask avd0 for actual location
                Log.e(TAG,"To forward message to " + NODE_JOINER_PORT + " to find port location");
                Message message = new Message(selection,"dummy","dummy",QUERY_LOOKUP,myPort,String.valueOf(NODE_JOINER_PORT),"dummy","dummy");
                new ClientTask().execute(message);
                waitForResponse();
                Log.e(TAG,"Actual location of message :" + SimpleDhtProvider.portLocation);
/*
                try {
                    while(!queryDone)
                    { //wait here until server reports back
                        synchronized (lock) {
                            lock.wait(1000); // UNBOUNDED LOCK WILL CAUSE APPLICATION TO HANG
                        }
                    }
                    queryDone = false;


                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
*/
                message.remotePort = SimpleDhtProvider.portLocation;
                message.originPort = myPort;
                message.messageType= SimpleDhtProvider.QUERY_GET_DATA;
                message.key        = selection;

                new ClientTask().execute(message);
/*
                try
                {
*/
/*
                    while(!queryDone) {
                        synchronized (lock) {
                            lock.wait(1000); // UNBOUNDED LOCK WILL CAUSE APPLICATION TO HANG
                        }
                    }
*/
//;                   queryDone = false;
                waitForResponse();
                Log.e(TAG, "Found key " + SimpleDhtProvider.queryKey + " : " + SimpleDhtProvider.queryValue);
                SimpleDhtActivity.getInstance().setText("\n*** QUERY RESULTS **** ="  + SimpleDhtProvider.queryKey + " :: " + SimpleDhtProvider.queryValue + "\n");

                String[] results = new String[]{SimpleDhtProvider.queryKey, SimpleDhtProvider.queryValue};
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{SimpleDhtActivity.KEY_FIELD,SimpleDhtActivity.VALUE_FIELD});
                matrixCursor.addRow(results);
                //c = matrixCursor;

                return matrixCursor;
/*
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
*/

            } else {

//                String[] results = new String[]{SimpleDhtProvider.queryKey, SimpleDhtProvider.queryValue};
//                MatrixCursor matrixCursor = new MatrixCursor(results);
                //c = matrixCursor;

                Log.e(TAG,"Found in local DB itself !");

            }
        }

        return c;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub


        return 0;
    }
    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
    public String genHash(String input) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        } catch(NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static SimpleDhtProvider getInstance() {
        return singleInstance;
    }
    public void setPredAndSuccHash(String predPort_,String succPort_) {
        predPort = Integer.valueOf(predPort_);
        succPort = Integer.valueOf(succPort_);

        predHash = portWithHashMap.get(predPort_);
        succHash = portWithHashMap.get(succPort_);

    }
    public void setPredAndSucc(Message message) {
        if (message.messageType.equals(NODE_ADDED))
        {

            predPort = Integer.valueOf(message.predPort);
            succPort = Integer.valueOf(message.succPort);
            Log.e("setPredAndSucc","updating pred and succ " + predPort + " : " + succPort);
            predHash = genHash(String.valueOf(predPort / 2));
            succHash = genHash(String.valueOf(succPort / 2));
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
    public  String getListOfAliveNodes() {
        StringBuilder result = new StringBuilder();
        for(String sTemp : chordList) {
            sTemp = hashWithPortMap.get(sTemp);
            result = result.append(sTemp).append(":");
        }
        return result.toString();
    }
/*
    public void notifyFoundPortMessage() {
        lock.notifyAll();
    }
*/

}
