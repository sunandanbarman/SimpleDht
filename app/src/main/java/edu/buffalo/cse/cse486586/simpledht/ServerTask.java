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

/**
 * Created by sunandan on 3/19/16.
 */
    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
public class ServerTask extends AsyncTask<ServerSocket, String, Void> {
    public void printTreeSet() {
        Log.e("printTreeSet","******  begin ******");
        for(String port : SimpleDhtProvider.singleInstance.chordList) {
            Log.e("printTreeSet","port " + port);
        }
        Log.e("printTreeSet","******* end  *****");
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
                        Log.e(TAG, String.valueOf(SimpleDhtProvider.getInstance().chordList.add(String.valueOf(message.originPort))));
                        printTreeSet();
                        Message nodeAdded_msg = new Message("","","",
                                                            SimpleDhtProvider.getInstance().NODE_ADDED,
                                                            SimpleDhtProvider.getInstance().myPort,message.originPort,
                                                            SimpleDhtProvider.getInstance().chordList.getPredecessor(String.valueOf(message.originPort)),
                                                            SimpleDhtProvider.getInstance().chordList.getSuccessor(String.valueOf(message.originPort))
                                                            );
                        Log.e(TAG, "message created for ACK alive");
                        SimpleDhtProvider.getInstance().sendNodeAddedMessage(nodeAdded_msg);
                        SimpleDhtProvider.getInstance().setPredAndSuccHash(SimpleDhtProvider.getInstance().chordList.getPredecessor(SimpleDhtProvider.getInstance().myPort),
                                SimpleDhtProvider.getInstance().chordList.getSuccessor(SimpleDhtProvider.getInstance().myPort));

                        Log.e(TAG, "pred & succ for OWN is " + SimpleDhtProvider.predPort + " : " + SimpleDhtProvider.succPort);
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
