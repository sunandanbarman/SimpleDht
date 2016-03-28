package edu.buffalo.cse.cse486586.simpledht;

import android.os.AsyncTask;
import android.util.Log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * Created by sunandan on 3/19/16.
 */
public class ClientTask extends AsyncTask<Message,Void,Void> {
    @Override
    public Void doInBackground(Message...messages) {
        String TAG = SimpleDhtProvider.TAG;
        Message message = messages[0];
        if (message.messageType == null || message.messageType.equalsIgnoreCase("")) {
            Log.e(SimpleDhtActivity.TAG, "messageType is either blank or NULL");
            return null;
        }
        Log.e(TAG,"to send message to " + message.remotePort + " messsageType " + message.messageType);
/*
        Log.e(SimpleDhtProvider.TAG,"location :" + Arrays.asList(SimpleDhtProvider.REMOTE_PORT).get(message.originPort));
        if (!Arrays.asList(SimpleDhtProvider.REMOTE_PORT).contains(message.originPort)) {
            Log.e(SimpleDhtActivity.TAG,"originPort " + message.originPort + " not valid !");
            return null;
        }
*/
        Socket socket;
        InputStream inputStream;
        DataInputStream dataInputStream;
        OutputStream outputStream;
        DataOutputStream dataOutputStream;
        String remotePort;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.valueOf(message.remotePort));
            Log.e(TAG,"LocalSocketAddress :" + socket.getLocalSocketAddress().toString());
            socket.setSoTimeout(SimpleDhtProvider.TIMEOUT);
            outputStream    = socket.getOutputStream();
            dataOutputStream= new DataOutputStream(outputStream);
            String tempMsg  = message.deconstructMessage();

            Log.e(TAG,"tempMsg :" + tempMsg);
            dataOutputStream.write(tempMsg.getBytes());
            dataOutputStream.flush();

            socket.close();
        } catch(SocketTimeoutException ex) {
            ex.printStackTrace();
        } catch (UnknownHostException ex) {
            ex.printStackTrace();
        } catch( IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }
 }
