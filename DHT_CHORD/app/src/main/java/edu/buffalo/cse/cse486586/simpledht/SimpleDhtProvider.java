package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {

    static final int SERVER_PORT = 10000;
    static final String[] REMOTE_PORTS={"11108","11112","11116","11120","11124"};
    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    private String nodeName;
    private String nodeId;
    public String nextNodeName=null;
    public String prevNodeName=null;
    public String nextNodeId=null;
    public String prevNodeId=null;
    public Uri nodeUri;
    public ArrayList<String> localKeyList=new ArrayList<String>();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    public Cursor returnCursor=null;
    public MatrixCursor retMat=null;
    public String queryKey=null;
    public String valueKey=null;
    public MatrixCursor globalMatrixCursor=null;
    public Cursor queryAllResult=null;
    public String queryAllOriginNode=null;
    public String globalQueryLookupNode=null;
    public String deleteOriginNode=null;


    public class ChordNode implements Comparable<ChordNode>
    {

        public String nodeId;
        public String nodeName;
        public String prevId=null;
        public String nextId=null;
        public String nextNodeName=null;
        public String prevNodeName=null;
        public HashMap<String,String> storage;
        public  HashMap<String,String> nodeHash;
        //public MatrixCursor returnCursor=new MatrixCursor();
        @Override
        public int compareTo(ChordNode next) {

            if(this.nodeId.compareTo(next.nodeId)<0) {
                return -1;
            }
            else if(this.nodeId.compareTo(next.nodeId)>0)
            {
                return 1;
            }
            return 0;
        }

    }


    //ConcurrentHashMap<String,ChordNode> chordHashMap=new ConcurrentHashMap<String, ChordNode>();
    ArrayList<ChordNode> chordList=new ArrayList<ChordNode>();


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if(selection.equals("@"))
        {
            for (int i=0;i<localKeyList.size();i++)
            {
                getContext().deleteFile(localKeyList.remove(i));
            }
            localKeyList=null;
        }
        else if(selection.equals("*") && nextNodeName==null)
        {
            for (int i=0;i<localKeyList.size();i++)
            {
                getContext().deleteFile(localKeyList.remove(i));
            }
            localKeyList=null;

        }
        else if(selection.equals("*") && nextNodeName!=null)
        {
            for (int i=0;i<localKeyList.size();i++)
            {
                getContext().deleteFile(localKeyList.remove(i));
            }
            localKeyList=null;

            String deleteAll="DELETE-"+nodeName+"-"+nextNodeName;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,deleteAll,nextNodeName);
        }
        else {
            localKeyList.remove(selection);
            getContext().deleteFile(selection);
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        if(nextNodeName==null) {
            FileOutputStream outputStream;
            String filename = values.get("key").toString();

            String value = values.get("value").toString();
            try {
                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                localKeyList.add(filename);
                outputStream.write(value.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }
            Log.v("insert", values.toString());
            return uri;
        }
        else
        {
            FileOutputStream outputStream;
            String filename = values.get("key").toString();
            String value = values.get("value").toString();
            try
            {
                String keyHash=genHash(filename);
                if((keyHash.compareTo(prevNodeId)>0) && (keyHash.compareTo(nodeId)<=0))
                {
                    try {
                        outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                        localKeyList.add(filename);
                        outputStream.write(value.getBytes());
                        Log.e(TAG,"Key Inserted: "+filename+" "+keyHash+" Value inserted: "+value);
                        outputStream.close();
                    } catch (Exception e) {
                        Log.e(TAG, "File write failed");
                    }
                    return uri;
                }
                else if((keyHash.compareTo(nodeId)>0)&& (keyHash.compareTo(prevNodeId)>0) && (nodeId.compareTo(prevNodeId)<0))
                {
                    try {
                        outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                        localKeyList.add(filename);
                        outputStream.write(value.getBytes());
                        Log.e(TAG,"Key Inserted: "+filename+" "+keyHash+" Value inserted: "+value);
                        outputStream.close();
                    } catch (Exception e) {
                        Log.e(TAG, "File write failed");
                    }
                    return uri;
                }
                else if((keyHash.compareTo(nodeId)<=0)&& (keyHash.compareTo(prevNodeId)<0) && (nodeId.compareTo(prevNodeId)<0))
                {
                    try {
                        outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                        localKeyList.add(filename);
                        outputStream.write(value.getBytes());
                        Log.e(TAG,"Key Inserted: "+filename+" "+keyHash+" Value inserted: "+value);
                        outputStream.close();
                    } catch (Exception e) {
                        Log.e(TAG, "File write failed");
                    }
                    return uri;
                }
                else if((keyHash.compareTo(nodeId)>0)&& (nodeId.compareTo(nextNodeId)>0))
                {
                    String toNode=String.valueOf(Integer.parseInt(nextNodeName)*2);
                    String message;
                    message="INSERTLOOKUP-"+nodeName+"-"+toNode+"-"+filename+"-"+value;


                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, toNode);
                }
                else if((keyHash.compareTo(nodeId)>0))
                {
                    String toNode=String.valueOf(Integer.parseInt(nextNodeName)*2);
                    String message;
                    message="INSERTLOOKUP-"+nodeName+"-"+toNode+"-"+filename+"-"+value;


                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, toNode);
                }
                else if((keyHash.compareTo(prevNodeId)<=0) && (keyHash.compareTo(nodeId)<0) && (nodeId.compareTo(prevNodeId)<0))
                {
                    try {
                        outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                        localKeyList.add(filename);
                        outputStream.write(value.getBytes());
                        Log.e(TAG,"Key Inserted: "+filename+" "+keyHash+" Value inserted: "+value);
                        outputStream.close();
                    } catch (Exception e) {
                        Log.e(TAG, "File write failed");
                    }
                    return uri;
                }
                else if((keyHash.compareTo(prevNodeId)<=0) && (keyHash.compareTo(nodeId)<0))
                {
                    String toNode=String.valueOf(Integer.parseInt(prevNodeName)*2);
                    String message;
                    message="INSERTLOOKUP-"+nodeName+"-"+toNode+"-"+filename+"-"+value;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, toNode);
                }
                else
                {
                    Log.e(TAG, "Key Hash Not Satisfying any condition KEY: "+filename+" VALUE: "+value);

                }
            }
            catch (NoSuchAlgorithmException e)
            {
                Log.e(TAG, "Key Hash generation failed");
                return null;
            }




        }
        return null;

    }


    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public boolean onCreate() {

        // TODO Auto-generated method stub
        //Code taken from PA1
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        final String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        nodeName=portStr;

        nodeUri=buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");



        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            serverSocket.setSoTimeout(0);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {

            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        try
        {
            nodeId=genHash(nodeName);
        }
        catch (NoSuchAlgorithmException e)
        {
            Log.e(TAG, "Node Id Hash generation failed");
            return false;
        }
        if(nextNodeName==null)
        {
            Log.e(TAG,"nextnode name null working");
        }
        if (myPort.equals(REMOTE_PORTS[0]))
        {
            Log.e(TAG,"Inside myport if");
            ChordNode firstNode=new ChordNode();
            firstNode.nextId=null;
            firstNode.prevId=null;
            firstNode.nodeId=nodeId;
            firstNode.nodeName=nodeName;
            firstNode.nextNodeName=null;
            firstNode.prevNodeName=null;
            //chordHashMap.put(nodeName,firstNode);
            chordList.add(firstNode);
            Log.e(TAG,"After First list add- Chord Size "+chordList.size());
        }

        //else if(chordList.size()>0)
        else if(nextNodeName==null)
        {
            String message="JOIN-"+nodeName+"-"+nodeId;
            if(nodeName!=String.valueOf(Integer.parseInt(REMOTE_PORTS[0])/2))
            {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, myPort);
            }

        }
        else
        {
            Log.e(TAG,"Chord Size "+chordList.size()+" "+nextNodeName+"port name"+myPort);

            return true;
        }
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        if(selection.contains("@") ||(selection.contains("*")&& nextNodeName==null))
        {
            Log.e(TAG,"QUERY -- Inside @* :"+selection);
            // Referred https://developer.android.com/reference/android/database/MatrixCursor.html
            String[] columnNames = {"key", "value"};
            MatrixCursor matrixCursor = new MatrixCursor(columnNames);
            try {
                String value="";
                String msg;
                for (int i=0;i<localKeyList.size();i++)
                {
                    String key=localKeyList.get(i);
                    FileInputStream inputStream = getContext().openFileInput(key);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                    /*while ((msg = reader.readLine()) != null) {
                        value += msg;

                    }*/
                    value=reader.readLine();
                    MatrixCursor.RowBuilder cursorRows = matrixCursor.newRow();
                    cursorRows.add("key", key);
                    cursorRows.add("value", value.toString());
                    reader.close();
                }
                queryKey=selection;
                valueKey=value.toString();

            }
            catch (IOException e)
            {
                Log.e(TAG,"IO exception in query @*");
            }


            returnCursor=matrixCursor;
            return matrixCursor;
        }

        else if((selection.contains("*")&& nextNodeName!=null))
        {
            Log.e(TAG,"QUERY -- Inside * nextNodeName!=null :"+selection);
            String[] columnNames = {"key", "value"};
            MatrixCursor matrixCursor = new MatrixCursor(columnNames);
            try {
                String value="";
                String msg;
                for (int i=0;i<localKeyList.size();i++)
                {
                    String key=localKeyList.get(i);
                    FileInputStream inputStream = getContext().openFileInput(key);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                    value=reader.readLine();
                    MatrixCursor.RowBuilder cursorRows = matrixCursor.newRow();
                    cursorRows.add("key", key);
                    cursorRows.add("value", value.toString());
                    reader.close();
                }
                queryKey=selection;
                valueKey=value.toString();
                Log.e(TAG,"Local * all data collected in: "+nodeName+" : "+queryKey+"-"+valueKey);
                Log.e(TAG,"Local * all data collected in: "+nodeName+" : "+matrixCursor.getCount());
                if (queryAllOriginNode == null) {
                    queryAllOriginNode = nodeName;
                    Log.e(TAG, "Setting query ALl origin Node First time from" + nodeName);
                }
                if(queryAllOriginNode.equals(nextNodeName))
                {
                    Log.e(TAG,"Next node name is the origin- returning cursor");
                    queryAllOriginNode=null;
                    return matrixCursor;
                }
            }
            catch (IOException e)
            {
                Log.e(TAG,"IO exception in query @*");
            }
            if (queryAllOriginNode == null) {
                queryAllOriginNode = nodeName;
                Log.e(TAG, "Setting query ALl origin Node First time from" + nodeName);
            }
            if(!queryAllOriginNode.equals(nextNodeName))
            {
                Log.e(TAG,"Inside * equals for next node "+nodeName);
                Log.e(TAG,"Origin Node: "+queryAllOriginNode+" next node name: "+nextNodeName+" current node:"+nodeName);
                MatrixCursor matrixcursor1;

                matrixcursor1 = query_next_new(nodeName, nextNodeName, queryAllOriginNode);
                Log.e(TAG,"Below query Next");
                if(matrixcursor1.getCount()>0) {
                    matrixcursor1.moveToFirst();
                    int keyIndex = matrixcursor1.getColumnIndex("key");
                    int valueIndex = matrixcursor1.getColumnIndex("value");

                    String returnKey = matrixcursor1.getString(keyIndex);
                    String returnValue = matrixcursor1.getString(valueIndex);
                    MatrixCursor.RowBuilder newCursorRows = matrixCursor.newRow();
                    newCursorRows.add("key", returnKey);
                    newCursorRows.add("value", returnValue);

                    for (int i = 1; i < matrixcursor1.getCount(); i++) {
                        matrixcursor1.moveToNext();
                        returnKey = matrixcursor1.getString(keyIndex);
                        returnValue = matrixcursor1.getString(valueIndex);
                        newCursorRows = matrixCursor.newRow();
                        newCursorRows.add("key", returnKey);
                        newCursorRows.add("value", returnValue);
                    }
                }
               //globalMatrixCursor = matrixCursor;
            }
            Log.e(TAG,"Above GMC SET");
            globalMatrixCursor = matrixCursor;
            if(globalMatrixCursor==null)
            {
                Log.e(TAG,"GMC is null");
            }
            Log.e(TAG,"Below GMC SET");
            Log.e(TAG,"Final return matrix cursor count:"+matrixCursor.getCount());
           /* matrixCursor.moveToFirst();
            String returnKey = matrixCursor.getString(0);
            String returnValue = matrixCursor.getString(1);
            Log.e(TAG,"FINAL MAT: First-"+returnKey+"-"+returnValue);*/

            for(int m=0;m<matrixCursor.getCount();m++)
            {
                matrixCursor.moveToNext();
                String returnKey = matrixCursor.getString(0);
                String returnValue = matrixCursor.getString(1);
                Log.e(TAG,"FINAL MAT:" + m+"-"+returnKey+"-"+returnValue);

            }

            queryAllOriginNode=null;
            return matrixCursor;
        }
        else {
            FileInputStream inputStream;
            String[] columnNames = {"key", "value"};
            MatrixCursor matrixCursor = new MatrixCursor(columnNames);
            Log.e(TAG, "QUERY-- Inside Query Else");
            if (localKeyList.size() != 0) {
                for (int i = 0; i < localKeyList.size(); i++) {
                    if (localKeyList.get(i).equals(selection)) {

                        try {
                            String value = "";
                            String msg;
                            inputStream = getContext().openFileInput(selection);
                            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                        /*while ((msg = reader.readLine()) != null) {
                            value += msg;

                        }*/
                            value = reader.readLine();

                            MatrixCursor.RowBuilder cursorRows = matrixCursor.newRow();
                            cursorRows.add("key", selection);
                            cursorRows.add("value", value.toString());
                            Log.e(TAG, "QUERY-MATRIX: " + selection + "-" + value.toString());
                            reader.close();
                        } catch (Exception e) {
                            Log.e(TAG, "File read failed");
                        }


                        Log.v("query", selection);
                        return matrixCursor;
                    }
                    if (i == localKeyList.size() - 1) {
                        Log.e(TAG, "QUERY-- has locallist but no value in it");
                        try {
                            String keyHash = genHash(selection);
                            int dstPort=0;
                            if((keyHash.compareTo(nodeId)>0 && keyHash.compareTo(nextNodeId)<=0)||(keyHash.compareTo(nodeId)>0 && nodeId.compareTo(nextNodeId)>0)
                                    ||(keyHash.compareTo(nodeId) < 0 && nodeId.compareTo(nextNodeId) > 0 && keyHash.compareTo(nextNodeId)<0))
                            {
                                dstPort=Integer.parseInt(nextNodeName);
                            }

                            else
                            {
                                int queryNextPort=Integer.parseInt(nextNodeName)*2;
                                Log.e(TAG,"Next Lookup Port: "+nextNodeName +" key Hash="+ keyHash);
                                String message="QUERYLOOKUPNEXT-"+nodeName+"-"+keyHash+"-"+nodeName;
                                try {
                                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            queryNextPort);
                                    socket.setSoTimeout(0);
                                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                                    writer.println(message);
                                    writer.flush();

                                    InputStreamReader isr = new InputStreamReader(socket.getInputStream());
                                    BufferedReader reader = new BufferedReader(isr);
                                    String messageReceived = reader.readLine();
                                    if (messageReceived==null)
                                    {
                                        Log.e(TAG,"QUERYLOOKUPNEXT-No message Received");

                                    }
                                    else
                                    {
                                        String[] msg=messageReceived.split("-");

                                            dstPort=Integer.parseInt(msg[1]);


                                    }
                                }
                                catch (UnknownHostException e) {
                                    Log.e(TAG, "QUERYLOOKUPNEXT UnknownHostException");
                                } catch (IOException e) {
                                    Log.e(TAG, "QUERYLOOKUPNEXT socket IOException");
                                }
                            }
                            Log.e(TAG,"Lookedup DST PORT "+dstPort);
                            String sendToNode = String.valueOf(dstPort* 2);
                            String messageQuery;
                            messageQuery = "QUERY-" + nodeName + "-" + sendToNode + "-" + selection;
                            try {
                                String cursorValues = new queryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageQuery, sendToNode).get();

                                if(cursorValues==null)
                                {
                                    Log.e(TAG, "QUERY--valueCursor is null");
                                }
                                String[] returnValues=cursorValues.split("-");
                                Log.e(TAG, "QUERY--cursorValues"+" KEY:"+returnValues[0] +" Value: "+returnValues[1]);
                                MatrixCursor.RowBuilder cursorRows = matrixCursor.newRow();
                                cursorRows.add("key", returnValues[0]);
                                cursorRows.add("value", returnValues[1]);
                                return matrixCursor;



                            }
                            catch(ExecutionException e)
                            {
                                Log.e(TAG, "Query--EXECUTION EXCEPTION");
                            }
                            catch(InterruptedException e){
                                Log.e(TAG, "Query--INTERRUPTED EXCEPTION");
                            }

                        }
                        catch(NoSuchAlgorithmException e)
                        {
                            Log.e(TAG, "Query--NO Such algo exception ");
                        }

                    }
                }
            }
            else
            {
                Log.e(TAG, "Query--HAS no locallist");
                try {
                    String keyHash = genHash(selection);
                    int dstPort=0;
                    if((keyHash.compareTo(nodeId)>0 && keyHash.compareTo(nextNodeId)<=0)||(keyHash.compareTo(nodeId)>0 && nodeId.compareTo(nextNodeId)>0)
                            ||(keyHash.compareTo(nodeId) < 0 && nodeId.compareTo(nextNodeId) > 0 && keyHash.compareTo(nextNodeId)<0))
                    {
                        dstPort=Integer.parseInt(nextNodeName);
                    }
                    else
                    {
                        int queryNextPort=Integer.parseInt(nextNodeName)*2;
                        String message="QUERYLOOKUPNEXT-"+nodeName+"-"+keyHash+"-"+nodeName;
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    queryNextPort);
                            socket.setSoTimeout(0);
                            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                            writer.println(message);
                            writer.flush();

                            InputStreamReader isr = new InputStreamReader(socket.getInputStream());
                            BufferedReader reader = new BufferedReader(isr);
                            String messageReceived = reader.readLine();
                            if (messageReceived==null)
                            {
                                Log.e(TAG,"QUERYLOOKUPNEXT-No message Received");

                            }
                            else
                            {
                                String[] msg=messageReceived.split("-");

                                dstPort=Integer.parseInt(msg[1]);


                            }
                        }
                        catch (UnknownHostException e) {
                            Log.e(TAG, "QUERYLOOKUPNEXT UnknownHostException");
                        } catch (IOException e) {
                            Log.e(TAG, "QUERYLOOKUPNEXT socket IOException");
                        }
                    }

                    Log.e(TAG,"Lookedup DST PORT "+dstPort);
                    String sendToNode = String.valueOf(dstPort* 2);
                    String messageQuery;
                    messageQuery = "QUERY-" + nodeName + "-" + sendToNode + "-" + selection;
                    try {
                        String cursorValues = new queryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageQuery, sendToNode).get();

                        if(cursorValues==null)
                        {
                            Log.e(TAG, "QUERY--valueCursor is null");
                        }
                        String[] returnValues=cursorValues.split("-");
                        Log.e(TAG, "QUERY--cursorValues"+" KEY:"+returnValues[0] +" Value: "+returnValues[1]);
                        MatrixCursor.RowBuilder cursorRows = matrixCursor.newRow();
                        cursorRows.add("key", returnValues[0]);
                        cursorRows.add("value", returnValues[1]);
                        return matrixCursor;



                    }
                    catch(ExecutionException e)
                    {
                        Log.e(TAG, "Query--EXECUTION EXCEPTION");
                    }
                    catch(InterruptedException e){
                        Log.e(TAG, "Query--INTERRUPTED EXCEPTION");
                    }
                }
                catch(NoSuchAlgorithmException e)
                {
                    Log.e(TAG, "Query--NO Such algo exception ");
                }
            }
            return matrixCursor;
        }

    }
    public MatrixCursor query_next_new(String current,String next, String origin)
    {
        String[] columnNames = {"key", "value"};
        MatrixCursor matrixCursor = new MatrixCursor(columnNames);
        Log.e(TAG,"In query_next");
        String message="QUERYNEXTALL-"+current+"-"+next+"-*"+"-"+origin;

            int queryNextPort = Integer.parseInt(next)*2;
            int queryPortOrigin = Integer.parseInt(queryAllOriginNode)*2;
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        queryNextPort);
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                writer.println(message);
                Log.e(TAG, "query_next_new - QUERYNEXTALL message sent from "+nodeName+" to "+queryNextPort+" origin "+queryPortOrigin);
                writer.flush();
                InputStreamReader isr = new InputStreamReader(socket.getInputStream());
                BufferedReader reader = new BufferedReader(isr);
                String messageReceived = reader.readLine();
                if(messageReceived==null)
                {
                    Log.e(TAG,"Null message received in new socket");

                }
                else
                {
                    String[] msg=messageReceived.split("-");
                    if(msg[0].equals("QUERYNEXTALLRETMSG"))
                    {

                        MatrixCursor allreturnedCursor = new MatrixCursor(columnNames);
                        Log.e(TAG,"Received QUERYNEXTALLRETMSG msg length"+msg.length);

                        if(msg.length>3) {
                            for (int i = 3; i < msg.length; i += 2) {
                                MatrixCursor.RowBuilder cursorRows = allreturnedCursor.newRow();
                                cursorRows.add("key", msg[i]);
                                cursorRows.add("value", msg[i + 1]);
                            }
                            Log.e(TAG,"ALL returned cursor Count "+allreturnedCursor.getCount());
                        }
                        matrixCursor=allreturnedCursor;
                    }

                }


                //socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return matrixCursor;
        }


    public int queryLookup(String nodeName,String keyHash,String originNode) {
        int dstPort = 0;

        if ((keyHash.compareTo(nodeId) > 0 && keyHash.compareTo(nextNodeId) <= 0) || (keyHash.compareTo(nodeId) > 0 && nodeId.compareTo(nextNodeId) > 0)
                ||(keyHash.compareTo(nodeId) < 0 && nodeId.compareTo(nextNodeId) > 0 && keyHash.compareTo(nextNodeId)<0))
        {
            dstPort = Integer.parseInt(nextNodeName);
        } else {
            int queryNextPort = Integer.parseInt(nextNodeName) * 2;
            String message = "QUERYLOOKUPNEXT-" + nodeName + "-" + keyHash + "-" + originNode;
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        queryNextPort);
                socket.setSoTimeout(0);
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                writer.println(message);
                writer.flush();

                InputStreamReader isr = new InputStreamReader(socket.getInputStream());
                BufferedReader reader = new BufferedReader(isr);
                String messageReceived = reader.readLine();
                if (messageReceived == null) {
                    Log.e(TAG, "queryLookUP Func-No message Received");

                } else {
                    String[] msg = messageReceived.split("-");

                    dstPort = Integer.parseInt(msg[1]);


                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "queryLookUP Func UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "queryLookUP Func socket IOException");
            }



        }
        return dstPort;
    }
    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            try {
                serverSocket.setSoTimeout(0);
                while (true) //loop added to send multiple messages
                {
                    Socket socket = serverSocket.accept();

                    InputStreamReader isr = new InputStreamReader(socket.getInputStream());

                    BufferedReader reader = new BufferedReader(isr);

                    String messageReceived = reader.readLine();
                    Log.e(TAG,"Below Readerline : "+messageReceived);
                    if (messageReceived == null) {
                        Log.e(TAG, "No Message received from client");
                    } else {
                        String message[] = messageReceived.split("-");
                        Log.e(TAG, messageReceived + " " + message[0] + " " + message[1] + " " + message[2]);
                        String requestType = message[0];

                        if (requestType.equals("JOIN") && message.length==3) {
                            Log.e(TAG,"Server Side - JOIN message Received");
                            String sentByNodeName = message[1];
                            String sentByNodeId = message[2];
                            Log.e(TAG,"Name: "+sentByNodeName+" ID: "+sentByNodeId);
                            String portNo;
                            String[] valueReturned;
                            if(chordList.size()>0) {
                                valueReturned = addToChord(sentByNodeName, sentByNodeId);
                                Log.e(TAG, sentByNodeName + "added to the ring - CHord Size now" + chordList.size());

                                String nodeAddedMsg = "ADDED-" + sentByNodeName + "-" + String.valueOf(valueReturned[0]) + "-" + String.valueOf(valueReturned[1]) + "-" + String.valueOf(valueReturned[2]) + "-" + String.valueOf(valueReturned[3]);

                                portNo = String.valueOf((Integer.parseInt(sentByNodeName) * 2));
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodeAddedMsg, portNo);

                                if(chordList.size()>1)
                                {
                                    for (int j=0;j<chordList.size();j++)
                                    {
                                        String finalAddedMsg = "ADDED-" + chordList.get(j).nodeName + "-" + chordList.get(j).nextId + "-" + chordList.get(j).nextNodeName + "-" + chordList.get(j).prevId + "-" + chordList.get(j).prevNodeName;
                                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, finalAddedMsg, chordList.get(j).nodeName);


                                    }
                                }


                                Log.e(TAG,"CHord list size now:"+chordList.size());
                            }









                        }
                        else if(requestType.equals("ADDED") && message.length==6)
                        {
                            Log.e(TAG,"Server side - ADDED received");
                            String tmpNextNodeId=message[2];
                            String tmpNextNodeName=message[3];
                            String tmpPrevNodeId=message[4];
                            String tmpPrevNodeName=message[5];
                            nextNodeId=tmpNextNodeId;
                            nextNodeName=tmpNextNodeName;
                            prevNodeId=tmpPrevNodeId;
                            prevNodeName=tmpPrevNodeName;
                            Log.e(TAG,"Server side- ADDED end-nextnodename "+nextNodeName+":"+nextNodeId+" prevnodeName "+prevNodeName+":"+prevNodeId);

                        }
                        else if(requestType.equals("INSERTLOOKUP") && message.length==5)
                        {
                            //message="INSERTLOOKUP-"+nodeName+"-"+toNode+"-"+filename+"-"+value;
                            String key=message[3];
                            String value=message[4];
                            ContentValues values=new ContentValues();
                            values.put("key",key);
                            values.put("value",value);
                            insert(nodeUri,values);

                        }
                        else if(requestType.equals("QUERYLOOKUPNEXT"))
                        {
                            //String message="QUERYLOOKUPNEXT-"+nodeName+"-"+keyHash+"-"+nodeName;
                            if(globalQueryLookupNode==null)
                            {
                                globalQueryLookupNode=message[3];

                            }
                            Log.e(TAG,"Before querylookup call");
                            int dstPort=queryLookup(message[1],message[2],message[3]);
                            Log.e(TAG,"After querylookup call - dstport"+dstPort);
                            String lookupRetMsg="QUERYLOOKUPNEXTRET-"+dstPort;
                            Log.e(TAG,"lookup ret msg: "+lookupRetMsg);
                            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                            writer.println(lookupRetMsg);



                        }
                        else if(requestType.equals("QUERY"))
                        {
                            Cursor resultCursor;
                            Log.e(TAG,"Server side- inside QUERY");
                            //message = "QUERY-" + nodeName + "-" + toNode + "-" + selection;
                            String key=message[3];
                            /*public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                                String sortOrder)*/

                            resultCursor=query(nodeUri,null,key,null,null);
                            resultCursor.moveToFirst();
                            int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                            int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);

                            String returnKey = resultCursor.getString(keyIndex);
                            String returnValue = resultCursor.getString(valueIndex);
                            Log.e(TAG,"return key: "+returnKey+" return Value "+returnValue );
                            Log.e(TAG,"QUERYALL-SUB-RECEIVED from "+message[1]);
                            String queryReturnedMsg="QUERYRETURNED-"+returnKey+"-"+returnValue+"-"+message[1];
                            //String querryAllReturnmsg="QUERYALLRET"+queryAllOriginNode;

                            Log.e(TAG,"queryReturnedMsg: "+queryReturnedMsg);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryReturnedMsg,nodeName);



                        }
                        else if(requestType.equals("QUERYRETURNED"))
                        {
                            Log.e(TAG,"QUERYRETURNED - INSIDE SERVER SIDE-setting values");
                            String key=message[1];
                            String value=message[2];
                            queryKey=key;
                            valueKey=value;


                        }
                        //String message="QUERYNEXTALL-"+current+"-"+next+"-*"+"-"+origin;
                        else if(requestType.equals("QUERYNEXTALL"))
                        {
                            Cursor resultCursor;
                            String key=message[3];
                            if(queryAllOriginNode==null)
                            {
                                queryAllOriginNode=message[4];
                                Log.e(TAG,"QUERYNEXTALL -Server side- Setting global origin in"+nodeName+" as "+message[4]);
                            }
                            resultCursor=query(nodeUri,null,key,null,null);
                            Log.e(TAG,"Count of resultCursor: "+resultCursor.getCount());
                            String queryAllMsg="QUERYNEXTALLRETMSG-"+queryAllOriginNode+"-"+prevNodeName;
                            if(resultCursor.getCount()>0) {
                                Log.e(TAG,"resultCursor Iteration 1");
                                resultCursor.moveToFirst();
                                int keyIndex = resultCursor.getColumnIndex("key");
                                int valueIndex = resultCursor.getColumnIndex("value");

                                String returnKey = resultCursor.getString(keyIndex);
                                String returnValue = resultCursor.getString(valueIndex);
                                queryAllMsg += "-" + returnKey + "-" + returnValue;


                                for (int i = 1; i < resultCursor.getCount(); i++) {
                                    Log.e(TAG,"resultCursor Iteration: "+i+1);
                                    resultCursor.moveToNext();
                                    returnKey = resultCursor.getString(keyIndex);
                                    returnValue = resultCursor.getString(valueIndex);
                                    queryAllMsg += "-" + returnKey + "-" + returnValue;
                                }
                            }
                            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                            writer.println(queryAllMsg);


                        }
                        else if(requestType.equals("QUERYALL"))
                        {
                            Cursor resultCursor;
                            Log.e(TAG,"Server side- inside QUERY");
                            //message = "QUERY-" + nodeName + "-" + toNode + "-" + selection;
                            String key=message[3];
                            if(queryAllOriginNode==null)
                            {
                                queryAllOriginNode=message[4];
                                Log.e(TAG,"QUERYALL -Server side- Setting global origin in"+nodeName+" as "+message[4]);
                            }
                            /*public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                                String sortOrder)*/
                            resultCursor=query(nodeUri,null,key,null,null);
                            Log.e(TAG,"Count of resultCursor: "+resultCursor.getCount());
                            queryAllResult=resultCursor;
                            String queryAllMsg="QUERYALLRETMSG-"+queryAllOriginNode+"-"+prevNodeName;
                            if(resultCursor.getCount()>0) {
                                Log.e(TAG,"resultCursor Iteration 1");
                                resultCursor.moveToFirst();
                                int keyIndex = resultCursor.getColumnIndex("key");
                                int valueIndex = resultCursor.getColumnIndex("value");

                                String returnKey = resultCursor.getString(keyIndex);
                                String returnValue = resultCursor.getString(valueIndex);
                                queryAllMsg += "-" + returnKey + "-" + returnValue;

                                for (int i = 1; i < resultCursor.getCount()-1; i++) {
                                    Log.e(TAG,"resultCursor Iteration: "+i+1);
                                    resultCursor.moveToNext();
                                    returnKey = resultCursor.getString(keyIndex);
                                    returnValue = resultCursor.getString(valueIndex);
                                    queryAllMsg += "-" + returnKey + "-" + returnValue;
                                }
                            }
                            new allClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,queryAllMsg,nodeName);

                        }

                        else if(requestType.equals("QUERYALLRETMSG"))
                        {
                            Log.e(TAG,"Received QUERYALL RET msg length");
                            String[] columnNames = {"key", "value"};
                            MatrixCursor allreturnedCursor = new MatrixCursor(columnNames);


                            int receivedStringLength=message.length;
                            Log.e(TAG,"Received msg length"+receivedStringLength);

                            if(receivedStringLength>3) {
                                for (int i = 3; i < receivedStringLength; i += 2) {
                                    MatrixCursor.RowBuilder cursorRows = allreturnedCursor.newRow();
                                    cursorRows.add("key", message[i]);
                                    cursorRows.add("value", message[i + 1]);
                                }
                            }


                            Log.e(TAG,"Above GMC in QUERYALLRETMSG");
                            Log.e(TAG,"All returned Cursor length:"+allreturnedCursor.getCount());
                            globalMatrixCursor=allreturnedCursor;
                        }
                        else if(requestType.equals("DELETE"))
                        {
                            Log.e(TAG,"Received DELETE Message");
                            delete(nodeUri,"@",null);
                            if(deleteOriginNode==null)
                            {
                                deleteOriginNode=message[1];
                            }
                            if(deleteOriginNode.equals(nextNodeName))
                            {
                                return null;
                            }
                            delete(nodeUri,"@",null);
                            String deleteMsg="DELETE-"+deleteOriginNode+"-"+nextNodeName;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,deleteMsg,nextNodeName);
                        }


                    }
                }
            }
            catch(IOException e)
            {
                Log.e(TAG, "ServerTask IO Exception Maybe timeout");

            }
        return null;
        }
    }

    public MatrixCursor query_next(String current,String next, String origin)
    {
        String[] columnNames = {"key", "value"};
        MatrixCursor matrixCursor = new MatrixCursor(columnNames);
        Log.e(TAG,"In query_next");
        String message="QUERYALL-"+current+"-"+next+"-*"+"-"+origin;
        try {
            matrixCursor = new getAllClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, next).get();
        }
        catch(ExecutionException e)
        {
            Log.e(TAG, "QueryALL--EXECUTION EXCEPTION");
        }
        catch(InterruptedException e){
            Log.e(TAG, "QueryALL--INTERRUPTED EXCEPTION");
        }

        return matrixCursor;
    }

    private  class allClientTask extends  AsyncTask<String, Void, Void>
    {
        @Override
        protected Void doInBackground(String... msgs)
        {
            String msgToSend = msgs[0];
            String[] msgClientReceived = msgToSend.split("-");
            if (msgClientReceived.length == 0) {
                Log.e(TAG, "getAllClientTask No Message Received");
            }

            int queryPortold = Integer.parseInt(msgClientReceived[2])*2;
            int queryPortOrigin = Integer.parseInt(queryAllOriginNode)*2;
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        queryPortOrigin);
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                writer.println(msgToSend);
                Log.e(TAG,"allClientTask- received msg="+msgToSend);
                Log.e(TAG, "allClientTask - QUERYALLRETMSG message sent from "+nodeName+" to "+queryPortOrigin);
                writer.flush();
                //socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }

    private class allClientTskCursor extends AsyncTask<Cursor, Void, Void> {

        @Override
        protected Void doInBackground(Cursor...cursor) {

            Cursor allRetCursor=cursor[0];
            if (allRetCursor == null) {
                Log.e(TAG, "allClientTask No Cursor Received");
            }

                int insertLookupPort = Integer.parseInt(prevNodeName)*2;
                Log.e(TAG, "allClientTask QUERYALL---" + insertLookupPort);
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            insertLookupPort);
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    writer.println(allRetCursor);
                    Log.e(TAG, "getAllClientTask - QUERYALL message sent from "+nodeName);
                    writer.flush();
                    //socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }
            return null;
        }
    }
    private class getAllClientTask extends AsyncTask<String, Void, MatrixCursor> {

        @Override
        protected MatrixCursor doInBackground(String... msgs) {

            String msgToSend = msgs[0];
            String[] msgClientReceived = msgToSend.split("-");
            if (msgClientReceived.length == 0) {
                Log.e(TAG, "getAllClientTask No Message Received");
            }
            Log.e(TAG, "getAllClientTask message " + msgToSend);
            Log.e(TAG, "getAllClientTask message0 " + msgClientReceived[0]);
            if (msgClientReceived[0].equals("QUERYALL")) {
                int insertLookupPort = Integer.parseInt(msgClientReceived[2])*2;
                Log.e(TAG, "getAllClientTask QUERYALL---" + msgToSend);
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            insertLookupPort);
                    socket.setSoTimeout(0);
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    writer.println(msgToSend);
                    Log.e(TAG, "getAllClientTask - QUERYALL message sent from "+nodeName+" to:"+insertLookupPort);

                    Log.e(TAG,"globalMatrixCursor is not null");

                    writer.flush();
                    if(nodeName.equals("5554")) {
                        while (globalMatrixCursor == null) {
                            continue;
                        }
                    }
                    Log.e(TAG,"globalMatrixCursor is not null ");
                    //socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }

            MatrixCursor tmpCursor=globalMatrixCursor;
            if(globalMatrixCursor!=null)
            {
                Log.e(TAG,"globalMatrixCursor is not null 2");
            }
            globalMatrixCursor=null;
            if(tmpCursor==null) {
                Log.e(TAG, "tmpCursor NULL");
            }
            return tmpCursor;
        }
    }
    public String[] addToChord(String sentByNodeName,String sentByNodeId)
    {
        Log.e(TAG,"JOIN - add to chord called for "+sentByNodeName);
        String[] returnValue=new String[4];
        ChordNode newNode=new ChordNode();
        newNode.nodeId=sentByNodeId;
        newNode.nodeName=sentByNodeName;

        ChordNode firstNode=chordList.get(0);
        //ChordNode firstNode=chordHashMap.get("5554");


        if(firstNode.nextId==null)
        {
            firstNode.nextId=newNode.nodeId;
            firstNode.prevId=newNode.nodeId;
            firstNode.prevNodeName=newNode.nodeName;
            firstNode.nextNodeName=newNode.nodeName;
            newNode.nextId=firstNode.nodeId;
            newNode.prevId=firstNode.nodeId;
            newNode.nextNodeName=firstNode.nodeName;
            newNode.prevNodeName=firstNode.nodeName;
            chordList.add(newNode);
            Collections.sort(chordList);
            returnValue[0]=newNode.nextId;
            returnValue[1]=newNode.nextNodeName;
            returnValue[2]=newNode.prevId;
            returnValue[3]=newNode.prevNodeName;
            //chordHashMap.put(sentByNodeName,newNode);
        }

        else
        {

            Log.e(TAG,"Inside ADD Else");
            for(int i=0;i<chordList.size();i++)
            {
             String tempNextNodeId=chordList.get(i).nodeId;

                if(newNode.nodeId.compareTo(tempNextNodeId)>0)
                {
                    Log.e(TAG,"Iteration "+i+" tmp: "+tempNextNodeId+" newnodeID: "+newNode.nodeId);
                    if(i!=(chordList.size()-1)) {
                        continue;
                    }
                   else
                    {
                        Log.e(TAG,"Inside IF - newnnodeID greater than last node");
                        newNode.nextId=chordList.get(0).nodeId;
                        newNode.nextNodeName=chordList.get(0).nodeName;
                        newNode.prevId=chordList.get(i).nodeId;
                        newNode.prevNodeName=chordList.get(i).nodeName;
                        chordList.get(i).nextNodeName=newNode.nodeName;
                        chordList.get(i).nextId=newNode.nodeId;
                        chordList.get(0).prevNodeName=newNode.nodeName;
                        chordList.get(0).prevId=newNode.nodeId;
                        chordList.add(newNode);
                        returnValue[0]=newNode.nextId;
                        returnValue[1]=newNode.nextNodeName;
                        returnValue[2]=newNode.prevId;
                        returnValue[3]=newNode.prevNodeName;
                        Collections.sort(chordList);
                        break;


                    }
                }
                else
                {
                    Log.e(TAG,"Inside ADD 2nd Else");
                    if(i==0)
                    {
                        Log.e(TAG,"Newnode ID lesser than 0th ID");
                        newNode.nextId=chordList.get(0).nodeId;
                        newNode.nextNodeName=chordList.get(0).nodeName;
                        newNode.prevId=chordList.get(chordList.size()-1).nodeId;
                        newNode.prevNodeName=chordList.get(chordList.size()-1).nodeName;
                        chordList.get(chordList.size()-1).nextNodeName=newNode.nodeName;
                        chordList.get(chordList.size()-1).nextId=newNode.nodeId;
                        chordList.get(0).prevNodeName=newNode.nodeName;
                        chordList.get(0).prevId=newNode.nodeId;
                        chordList.add(newNode);
                        returnValue[0]=newNode.nextId;
                        returnValue[1]=newNode.nextNodeName;
                        returnValue[2]=newNode.prevId;
                        returnValue[3]=newNode.prevNodeName;
                        Collections.sort(chordList);
                        break;
                    }
                    chordList.get(i-1).nextNodeName=newNode.nodeName;
                    chordList.get(i-1).nextId=newNode.nodeId;
                    chordList.get(i).prevNodeName=newNode.nodeName;
                    chordList.get(i).prevId=newNode.nodeId;
                    newNode.nextId=chordList.get(i).nodeId;
                    newNode.nextNodeName=chordList.get(i).nodeName;
                    newNode.prevId=chordList.get(i-1).nodeId;
                    newNode.prevNodeName=chordList.get(i-1).nodeName;
                    chordList.add(newNode);
                    returnValue[0]=newNode.nextId;
                    returnValue[1]=newNode.nextNodeName;
                    returnValue[2]=newNode.prevId;
                    returnValue[3]=newNode.prevNodeName;
                    Collections.sort(chordList);
                    break;

                }

            }

        }

        Log.e(TAG,"Inside add end "+returnValue[0]+" "+returnValue[1]+" "+returnValue[2]+" "+returnValue[3]);
        return returnValue;
    }

    private class queryClientTask extends AsyncTask<String, Void, String> {

        @Override
        protected String doInBackground(String... msgs) {

            String msgToSend = msgs[0];
            String[] msgClientReceived = msgToSend.split("-");
            if (msgClientReceived.length == 0) {
                Log.e(TAG, "ClientTask No Message Received");
            }
            Log.e(TAG, "queryClientTask message " + msgToSend);
            Log.e(TAG, "queryClientTask message " + msgClientReceived[0]);
            if (msgClientReceived[0].equals("QUERY")) {
                int insertLookupPort = Integer.parseInt(msgClientReceived[2]);
                Log.e(TAG, "queryClientTask QUERY---" + msgToSend);
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            insertLookupPort);
                    socket.setSoTimeout(0);
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    writer.println(msgToSend);
                    Log.e(TAG, "queryClientTask - QUERY message sent from "+nodeName);

                    while(valueKey==null)
                    {
                    continue;
                    }

                    writer.flush();
                    //socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }

            String returnString=queryKey+"-"+valueKey;
            queryKey=null;
            valueKey=null;
            Log.e(TAG, "queryClientTask returnValue"+returnString);
            return returnString;
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            String msgToSend = msgs[0];
            String[] msgClientReceived=msgToSend.split("-");
            if(msgClientReceived.length==0)
            {
                Log.e(TAG, "ClientTask No Message Received");
            }
            Log.e(TAG, "ClientTask message "+msgToSend);
            Log.e(TAG, "ClientTask message "+msgClientReceived[0]);
            if(msgClientReceived[0].equals("JOIN")) {
                Log.e(TAG, "ClientTask message- Inside JOIN IF");
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORTS[0]));
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    writer.println(msgToSend);
                    Log.e(TAG,"Client Side - JOIN message sent");
                    writer.flush();
                    //socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }
            else if(msgClientReceived[0].equals("ADDED"))
            {
                Log.e(TAG,"Inside ADDED- msgClientReceived[1]-"+msgClientReceived[1]);
                int addedPort=Integer.parseInt(msgClientReceived[1])*2;
                Log.e(TAG,"Inside ADDED- dstPort-"+addedPort);
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            addedPort);
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    writer.println(msgToSend);
                    Log.e(TAG,"Client Side - ADDED message sent");
                    writer.flush();
                    //socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }
            //message="INSERT-"+nodeName+"-"+toNode+"-"+filename+"-"+value;
            else if(msgClientReceived[0].equals("INSERTLOOKUP"))
            {
                int insertLookupPort=Integer.parseInt(msgClientReceived[2]);
                Log.e(TAG,"Clientside INSERTLOOKUP--"+msgToSend);
                Log.e(TAG,"Clientside INSERTLOOKUP port--"+insertLookupPort);
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            insertLookupPort);
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    writer.println(msgToSend);
                    Log.e(TAG,"Client Side - INSERTLOOKUP message sent");
                    writer.flush();
                    //socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }
            //message = "QUERY-" + nodeName + "-" + toNode + "-" + selection;
            else if(msgClientReceived[0].equals("QUERY"))
            {
                int insertLookupPort=Integer.parseInt(msgClientReceived[2]);
                Log.e(TAG,"Clientside QUERY---"+msgToSend);
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            insertLookupPort);
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    writer.println(msgToSend);
                    Log.e(TAG,"Client Side - QUERY message sent");
                    writer.flush();
                    //socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }

            }
            else if(msgClientReceived[0].equals("QUERYRETURNED"))
            {
                int insertLookupPort=Integer.parseInt(msgClientReceived[3])*2;
                Log.e(TAG,"QUERYRETURNED---"+msgToSend);
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            insertLookupPort);
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    writer.println(msgToSend);
                    Log.e(TAG, "Client Side - QUERYRETURNED message sent to "+msgClientReceived[3]);
                    writer.flush();
                    //socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }

            }
            else if(msgClientReceived[0].equals("DELETE"))
            {
                int deletePort=Integer.parseInt(msgClientReceived[2])*2;
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),deletePort);
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    writer.println(msgToSend);
                    Log.e(TAG,"Client Side - DELETE message sent");
                    writer.flush();
                    //socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }

            }

            return null;
        }
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
