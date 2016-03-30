package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.database.Cursor;
import android.os.AsyncTask;
import android.util.Log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.HashSet;
import java.util.TreeSet;

/**
 * Created by sunandan on 3/19/16.
 */
    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
public class ServerTask extends AsyncTask<ServerSocket, String, Void> {
    public void printTreeSet(ChordList<String> treeSet) {
        Log.e("printTreeSet","******  begin ******");
        for(String port : treeSet) {
            Log.e("printTreeSet","port " + port);
        }
        Log.e("printTreeSet","******* end  *****");
    }

    private String findLocationStoringTheMessage(Message message) {
        String key     = message.key;
        String hashKey = SimpleDhtProvider.getInstance().genHash(key);
        if (hashKey.compareTo(SimpleDhtProvider.getInstance().chordList.first()) < 0) {
            return  SimpleDhtProvider.hashWithPortMap.get(SimpleDhtProvider.getInstance().chordList.first());
        } else if (hashKey.compareTo(SimpleDhtProvider.getInstance().chordList.last()) > 0 ) {
            return SimpleDhtProvider.hashWithPortMap.get(SimpleDhtProvider.getInstance().chordList.first());
        } else {
            return SimpleDhtProvider.hashWithPortMap.get(SimpleDhtProvider.getInstance().chordList.ceiling(hashKey));
        }
    }
    private HashSet<String> getNodesWithNewPredAndSucc(ChordList<String> oldChordList) {
        HashSet<String> result = new HashSet<String>();
        String tempPred, tempSucc;

        Log.e("getNodesWithNewPredSucc","***************** begins *********");
        for(String s:oldChordList) {
            Log.e("getNodesWithNewPredSucc",s);
        }
        Log.e("getNodesWithNewPredSucc","***************** ends   **********");
        for(String node :SimpleDhtProvider.getInstance().chordList) {
            Log.e("getNodesWithNewPredSucc", "node =" + node);
            String port = SimpleDhtProvider.getInstance().hashWithPortMap.get(node);
            //String avdID = SimpleDhtProvider.getInstance().hashWithPortMap.get(node);
            Log.e("getNodesWithNewPredSucc"," port =" + port);
            if (!oldChordList.contains(node))
            {

                Log.e("getNodesWithNewPredSucc","New node " +  port + " found");
                result.add(node);
            } else {
                tempSucc = oldChordList.getSuccessor(node);
                tempPred = oldChordList.getPredecessor(node);

                if (!tempSucc.equalsIgnoreCase(SimpleDhtProvider.getInstance().chordList.getSuccessor(node))) { //successor is different
                    Log.e("getNodesWithNewPredSucc",port + " has new successor ");
                    result.add(node);
                }
                if (!tempPred.equalsIgnoreCase(SimpleDhtProvider.getInstance().chordList.getPredecessor(node))) { //pred is different
                    Log.e("getNodesWithNewPredSucc",port + " has new predecessor ");
                    result.add(node);
                }
            }
        }
        return result;
    }

    /**
     * Does a lookup on the message's hashkey and determines whether the message to this node, or to forward to successor node for further lookups
     * @return
     */
    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        String TAG = SimpleDhtProvider.TAG;
        SimpleDhtProvider provider = SimpleDhtProvider.singleInstance;
        Log.e(TAG, "Withing ServerTask doInBackground function");
        ServerSocket serverSocket = sockets[0];
        Socket clientSocket;
        InputStream inputStream;
        DataInputStream dataInputStream;
        OutputStream outputStream;
        DataOutputStream dataOutputStream;
        byte[] msgIncoming = new byte[SimpleDhtProvider.MAX_MSG_LENGTH];
        try {
            while(true) {
                clientSocket = serverSocket.accept();
                //Log.e(TAG,"clientSocket accepted from  " + clientSocket.getRemoteSocketAddress());
                try {
                    inputStream = clientSocket.getInputStream();
                    dataInputStream = new DataInputStream(inputStream);
                    dataInputStream.read(msgIncoming);

                    Message message = new Message(new String(msgIncoming));
                    if (!msgIncoming.equals(null) && msgIncoming.equals("")) {
                        Log.e(TAG,"msgReceived " + msgIncoming);
                    }
                    clientSocket.close(); //release the connection, process the message
                    if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.ALIVE)) {
                        Log.e(TAG, "ALIVE message found from " + message.originPort);
                        Log.e(TAG, "to add " + message.originPort);

                        ChordList<String> tempList = new ChordList<String>();
                        tempList.addAll(new TreeSet<String>(SimpleDhtProvider.getInstance().chordList));
                        Log.e(TAG, "\ntempList starts*******");
                        printTreeSet(tempList);
                        Log.e(TAG, "\ntempList ends*******");
                        SimpleDhtProvider.getInstance().chordList.add(SimpleDhtProvider.getInstance().portWithHashMap.get(message.originPort));

                        //Log.e(TAG, String.valueOf(SimpleDhtProvider.getInstance().chordList.add(String.valueOf(message.originPort))));
                        //printTreeSet(SimpleDhtProvider.getInstance().chordList);
                        HashSet<String> nodeHashList = getNodesWithNewPredAndSucc(tempList);
                        for (String node: nodeHashList) {
                            String predPort = SimpleDhtProvider.getInstance().hashWithPortMap.get(SimpleDhtProvider.getInstance().chordList.getPredecessor(node));
                            String succPort = SimpleDhtProvider.getInstance().hashWithPortMap.get(SimpleDhtProvider.getInstance().chordList.getSuccessor(node));
                            String nodePort = SimpleDhtProvider.getInstance().hashWithPortMap.get(node);
                            Log.e(TAG,"node " + nodePort + " pred & succ changed !");
                            Log.e(TAG,"new Pred " + predPort);
                            Log.e(TAG, "new succ " + succPort);
                            if (!nodePort.equalsIgnoreCase(SimpleDhtProvider.myPort)) {
                                Message nodeAdded_msg = new Message("dummy","dummy","dummy",
                                        SimpleDhtProvider.getInstance().NODE_ADDED,
                                        SimpleDhtProvider.getInstance().myPort, //origin port
                                        nodePort,                     //remote port
                                        predPort, //pred
                                        succPort);  //succ

                                SimpleDhtProvider.getInstance().sendMessageToRemotePort(nodeAdded_msg);
                            } else {
                                SimpleDhtProvider.getInstance().setPredAndSuccHash(predPort,succPort);
                                Log.e(TAG, "pred & succ for OWN is " + SimpleDhtProvider.predPort + " : " + SimpleDhtProvider.succPort);
                            }
                        }

                        Log.e(TAG, "message created for ACK alive");




                        //SimpleDhtProvider.singleInstance.chordList.add(message.originPort);
                    }
                    else if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.INSERT_LOOKUP)) {
                        Log.e(TAG, "Insert_Lookup request found from  " + message.originPort);
                        ContentValues cv = new ContentValues();
                        cv.put(SimpleDhtActivity.KEY_FIELD,message.key);
                        cv.put(SimpleDhtActivity.VALUE_FIELD, message.value);


/*
                        if (message.originPort.equalsIgnoreCase(SimpleDhtProvider.getInstance().myPort)) {
                            //message has returned, hence insert directly; no need to check
                            Log.e(TAG,"Message has returned to origin. InsertIntoDatabase");
                            SimpleDhtProvider.getInstance().insertIntoDatabase(cv);
                        } else
*/
                        {
                            Log.e(TAG,"diff. origin;search continues...");
                            cv.put(SimpleDhtActivity.PORT, message.originPort);
                            SimpleDhtProvider.getInstance().insert(SimpleDhtActivity.contentURI, cv);
                        }

                    }
                    else if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.QUERY_LOOKUP)) {
                        Log.e(TAG,"Query_Lookup request found from  " + message.originPort);
                        String portLocation = findLocationStoringTheMessage(message);
                        //reply to origin port with found portLocation
                        Log.e(TAG,"actual port location is " + portLocation);
                        message.remotePort= message.originPort; //reply to sender about it
                        message.messageType= SimpleDhtProvider.QUERY_DONE;
                        message.originPort = portLocation;      // this is the location of DB; receiver extracts this for further lookup
                        //Log.e(TAG,"message.originPort is now " + message.originPort);
                        SimpleDhtProvider.getInstance().sendMessageToRemotePort(message);

                    } else if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.QUERY_DONE)) {
                        Log.e(TAG,"Received reply from AVD0 about portLocation " + message.originPort);
                        SimpleDhtProvider.portLocation = message.originPort;
                        synchronized (SimpleDhtProvider.lock) {
                            SimpleDhtProvider.queryDone = true;
                            SimpleDhtProvider.lock.notifyAll();
                        }

                    } else if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.QUERY_GET_DATA)) {
                        Log.e(TAG,"QUERY_GET_DATA, To query " + message.key);
                        Cursor cursor = SimpleDhtProvider.getInstance().returnLocalData(message.key);

                        Log.e(TAG,"Rows found :" + cursor.getCount());
                        cursor.moveToFirst();
                        if (!(cursor.isFirst() && cursor.isLast())) {
                            Log.e(TAG, "Wrong number of rows");
                            //resultCursor.close();
                            //throw new Exception();
                        }
                        message.messageType = SimpleDhtProvider.QRY_DATA_DONE;
                        int keyIndex   = cursor.getColumnIndex(SimpleDhtActivity.KEY_FIELD);
                        int valueIndex = cursor.getColumnIndex(SimpleDhtActivity.VALUE_FIELD);

                        Log.e(TAG,"keyIndex " + keyIndex);
                        Log.e(TAG,"valueIndex " + valueIndex);

                        message.key         = cursor.getString(keyIndex);
                        message.value       = cursor.getString(valueIndex);

                        Log.e(TAG,"DB key " + message.key);
                        Log.e(TAG,"DB val " + message.value);

                        message.remotePort  = message.originPort;
                        message.originPort  = SimpleDhtProvider.myPort;
                        SimpleDhtProvider.getInstance().sendMessageToRemotePort(message);
                    }
                    else if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.QRY_DATA_DONE)) {
                        Log.e(TAG,"Result of QRY_DATA_DONE from " + message.remotePort + " is " + message.value);
                        SimpleDhtProvider.queryKey  = message.key;
                        SimpleDhtProvider.queryValue= message.value;
                        synchronized (SimpleDhtProvider.lock) {
                            SimpleDhtProvider.queryDone = true;
                            SimpleDhtProvider.lock.notifyAll();
                        }
                    }
                    else if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.getInstance().NODE_ADDED)) {
                        Log.e(TAG,"Node added ACK found, pred and succ are" + message.predPort + " : " + message.succPort);
                        SimpleDhtProvider.getInstance().setPredAndSucc(message);
                    }
                    else if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.getInstance().UPDATE_SUCC)) {
                        Log.e(TAG,"successor Update to "+ message.succPort);
                        SimpleDhtProvider.getInstance().updateSuccessor(message);
                    }

                } catch(SocketTimeoutException e) {
                    e.printStackTrace();
                } catch(StreamCorruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } /*catch(EOFException e) {

                    }*/ catch (Exception e) {
                    e.printStackTrace();
                }


            }
        } catch(Exception e) {

        }
        return null;
    }
}
