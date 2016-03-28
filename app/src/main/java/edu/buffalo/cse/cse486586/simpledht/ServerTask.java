package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
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
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
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
        /*for(String node : oldChordList) {
            String avdID = SimpleDhtProvider.getInstance().hashWithPortMap.get(node);
            Log.e("getNodesWithNewPredSucc","avdID in =" + avdID);
            tempSucc = SimpleDhtProvider.getInstance().chordList.getSuccessor(avdID);
            tempPred = SimpleDhtProvider.getInstance().chordList.getPredecessor(avdID);

            if (!tempSucc.equalsIgnoreCase(oldChordList.getSuccessor(avdID))) { //successor is different
                Log.e("getNodesWithNewPredSucc",avdID + " has new successor ");
                result.add(String.valueOf(Integer.valueOf(avdID)*2));
            }
            if (!tempPred.equalsIgnoreCase(oldChordList.getPredecessor(avdID))) { //pred is different
                Log.e("getNodesWithNewPredSucc",avdID + " has new predecessor ");
                result.add(String.valueOf(Integer.valueOf(avdID)*2));
            }
        }*/
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
                Log.e(TAG,"clientSocket accepted from  " + clientSocket.getRemoteSocketAddress());
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

                                SimpleDhtProvider.getInstance().sendNodeAddedMessage(nodeAdded_msg);
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


                        if (message.originPort.equalsIgnoreCase(SimpleDhtProvider.getInstance().myPort)) {
                            //message has returned, hence insert directly; no need to check
                            Log.e(TAG,"Message has returned to origin. InsertIntoDatabase");
                            SimpleDhtProvider.getInstance().insertIntoDatabase(cv);
                        } else {
                            Log.e(TAG,"diff. origin;search continues...");
                            cv.put(SimpleDhtActivity.PORT, message.originPort);
                            SimpleDhtProvider.getInstance().insert(SimpleDhtActivity.contentURI, cv);
                        }

                    }
                    else if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.QUERY_LOOKUP)) {
                        Log.e(TAG,"Query_Lookup request found from  " + message.originPort);
                    }
                    else if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.getInstance().NODE_ADDED)) {
                        Log.e(TAG,"Node added ACK found, pred and succ are" + message.predPort + " : " + message.succPort);
                        SimpleDhtProvider.getInstance().setPredAndSucc(message);
                    }
                    else if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.getInstance().UPDATE_SUCC)) {
                        Log.e(TAG,"successor Update to "+ message.succPort);
                        SimpleDhtProvider.getInstance().updateSuccessor(message);
                    }
                        /* A message is received, check its hash*/
/*
                    if (SimpleDhtProvider.doLookup(""))
                    { // message belongs to this node, let the client know
                        outputStream = clientSocket.getOutputStream();
                        dataOutputStream = new DataOutputStream(outputStream);
                        //Message msgACK   = new Message(message.key,message.value,message.hashKey, ACK, Integer.valueOf(myPort));
                        //String replyACK  = msgACK.deconstructMessage();
                        //dataOutputStream.write(replyACK.getBytes());

                        clientSocket.close();
                    } else {


                        clientSocket.close();
                    }
*/

                    //clientSocket.close();
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
