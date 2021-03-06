package edu.buffalo.cse.cse486586.simpledht;

import android.database.Cursor;
import android.util.Log;

/**
 * Similiar to GroupMessenger2, this class creates a message with fields
 * 1. msgType 2. originPort 3. hash(key) 4. key 5. value 6. messageType
 * For sending and receiving, we use the deConstruct and reConstruct procedures
 * Created by sunandan on 3/18/16.
 */
public class Message {
    public String key,value, hashKey, messageType;
    public String originPort, remotePort;
    public String predPort, succPort; //use only in case of NODE_ADDED message

    private Cursor resultCursor;
    Message() {

    }
    Message(Message m1) {

        this.key  = m1.key;
        this.value= m1.value;
        this.hashKey     = m1.hashKey;
        this.messageType = m1.messageType;
        this.originPort  = m1.originPort;
        this.remotePort  = m1.remotePort;
        this.predPort    = m1.predPort;
        this.succPort    = m1.succPort;
        //this.resultCursor= null;
    }
    Message(String text) {
        this.reconstructMessage(text);
    }
    Message(String key, String value, String hashKey, String messageType, String originPort,String remotePort, String predPort, String succPort) {
        this.key  = key;
        this.value= value;
        this.hashKey = hashKey;
        this.messageType = messageType;
        this.originPort  = originPort;
        this.remotePort  = remotePort;
        this.predPort    = predPort;
        this.succPort    = succPort;
    }
    /** split the incomingMessage and fill in the details in "this" object**/
    public void reconstructMessage(String incomingMessage) {

        String[] msgs = incomingMessage.split(";");
        try {
            this.key = msgs[0];
            this.value = msgs[1];
            this.hashKey = msgs[2];
            this.messageType= msgs[3];
            this.originPort = msgs[4];
            this.remotePort = msgs[5];
            this.predPort   = msgs[6];
            this.succPort   = msgs[7];
        }
        catch(Exception ex) {
            Log.e("excce",incomingMessage);
            ex.printStackTrace();
        }
    }

    /** Use only in client-server communication**/
    public String deconstructMessage() {
        return  this.key           + ";"
                + this.value       + ";"
                + this.hashKey     + ";"
                + this.messageType + ";"
                + this.originPort  + ";"
                + this.remotePort  + ";"
                + this.predPort    + ";"
                + this.succPort    + ";";
    }
    /**
     *
     * @return
     */
    public String toString() {
        return  "key   :"       + this.key     + ";"
                + "value:"     + this.value  + ";"
                + "hashKey:"     + this.hashKey  + ";"
                + "messageType:"      + this.messageType + ";"
                + "originPort:"     + this.originPort  + ";"
                + "remotePort:"     + this.remotePort  + ";"
                + "remotePort:"     + this.predPort  + ";"
                + "remotePort:"     + this.succPort  + ";";
    }

}
