package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
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

public class SimpleDynamoProvider extends ContentProvider {

	static final int SERVER_PORT = 10000;
	static final String[] REMOTE_PORTS={"11108","11112","11116","11120","11124"};
	static final String[] NODE_LIST={"5562","5556","5554","5558","5560"};
	static final int FIRSTNODE=0;
	static final int LASTNODE=4;
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	public String nodeName;
	public String nodeId;
	public String nextNodeName=null;
	public String nextNodeId=null;
	public String previousNodeName=null;
	public String previousNodeId=null;
	public String replica1=null;
	public String replica2=null;
	public String replica1Status=null;
	public String replica2Status=null;
	public boolean globalInit=false;

	public Uri nodeUri;
	public ArrayList<String> localKeyList=new ArrayList<String>();
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	ArrayList<Node> dynamoNodeList=new ArrayList<Node>();




	public class Node implements Comparable<Node>
	{

		public String nodeId;
		public String nodeName;
		public String prevId=null;
		public String nextId=null;
		public String nextNodeName=null;
		public String prevNodeName=null;
		@Override

		public int compareTo(Node next) {

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


	public void initDynamoList()
	{
		for(int i=0;i<5;i++)
		{
			Node newNode = new Node();
			newNode.nodeName = NODE_LIST[i];
			try {
				newNode.nodeId = genHash(newNode.nodeName);
				if(i!=0 && i!= 4) {

					newNode.nextNodeName = NODE_LIST[i + 1];
					newNode.nextId = genHash(newNode.nextNodeName);
					newNode.prevNodeName = NODE_LIST[i - 1];
					newNode.prevId=genHash(newNode.prevNodeName);
				}
				else
				{
					if(i==0)
					{
						newNode.nextNodeName = NODE_LIST[i + 1];
						newNode.nextId = genHash(newNode.nextNodeName);
						newNode.prevNodeName = NODE_LIST[LASTNODE];
						newNode.prevId=genHash(newNode.prevNodeName);

					}
					else if(i==4)
					{
						newNode.nextNodeName = NODE_LIST[FIRSTNODE];
						newNode.nextId = genHash(newNode.nextNodeName);
						newNode.prevNodeName = NODE_LIST[i-1];
						newNode.prevId=genHash(newNode.prevNodeName);
					}
				}
				if(nodeName.equals(newNode.nodeName))
				{
					nodeId=newNode.nodeId;
					nextNodeName=newNode.nextNodeName;
					nextNodeId=newNode.nextId;
					previousNodeName=newNode.prevNodeName;
					previousNodeId=newNode.prevId;
				}
				dynamoNodeList.add(newNode);
			}
			catch(NoSuchAlgorithmException e)
			{
				Log.e(TAG,"Exception in node creation");
			}

		}
		Log.e(TAG,"DYNAMOLIST CREATED");
		for (int i=0;i<5;i++)
		{
			Log.e(TAG,"DYNAMOLIST: "+dynamoNodeList.get(i).nodeName+"-"+dynamoNodeList.get(i).nextNodeName+"-"+dynamoNodeList.get(i).prevNodeName);
		}

	}

	public class ReplicaObject
	{
		public Socket socket;
		public String message;
		public String replicaPort1;
		public String replicaPort2;
		public String key;
		public String value;
		public ReplicaObject(Socket socket1, String message1, String key,String value,String port1,String port2)
		{
			this.socket=socket1;
			this.message=message1;
			this.replicaPort1=port1;
			this.replicaPort2=port2;
			this.key=key;
			this.value=value;
		}

	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}




	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {

		while(!globalInit)
		{
			if(globalInit)
			{
				break;
			}
		}

		/*try {
			Thread.sleep(100);
		}
		catch(InterruptedException e)
		{
			Log.e(TAG,"InterruptedException exception in query sleep");
		}*/



		String filename = values.get("key").toString();
		String value = values.get("value").toString();
		Log.e(TAG,"INSERT request received for KEY: "+filename+" VALUE "+value);
		int currentNodeIndex=-1;
		String replicaNode1;
		String replicaNode2;


		FileOutputStream outputStream;
		try {
			String keyHash=genHash(filename);

			if (((keyHash.compareTo(previousNodeId) > 0) && keyHash.compareTo(nodeId) <= 0)||
				((keyHash.compareTo(nodeId)>0)&& (keyHash.compareTo(previousNodeId)>0) && (nodeId.compareTo(previousNodeId)<0))||
					((keyHash.compareTo(nodeId)<=0)&& (keyHash.compareTo(previousNodeId)<0) && (nodeId.compareTo(previousNodeId)<0))
					)
			{
					outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
					localKeyList.add(filename);
					outputStream.write(value.getBytes());
					Log.e(TAG, "Key Inserted: " + filename + " " + keyHash + " Value inserted: " + value);
					outputStream.close();
					replicaNode1 = nextNodeName;
					for (int i = 0; i < 5; i++) {
						if (nodeName.equals(NODE_LIST[i])) {
							currentNodeIndex = i;
						}
					}
					if (currentNodeIndex < 3) {
						replicaNode2 = NODE_LIST[currentNodeIndex + 2];
					}
					else if (currentNodeIndex == 3)
					{
						replicaNode2=NODE_LIST[FIRSTNODE];
					}
					else if(currentNodeIndex == 4)
					{
						replicaNode2=NODE_LIST[1];
					}
					else
					{
						replicaNode2="ERROR";
					}
					Log.e(TAG,"ReplicasOnly2: "+replicaNode1+" "+replicaNode2);
					insertToReplicas(filename, value, replicaNode1, replicaNode2);
			}
			else
			{
				String insertToNode;
				int insertToNodeIndex=-1;
				for(int i=0;i<dynamoNodeList.size();i++)
				{
					if(
					(keyHash.compareTo(dynamoNodeList.get(i).prevId)>0 && keyHash.compareTo(dynamoNodeList.get(i).nodeId)<=0)||
					((keyHash.compareTo(dynamoNodeList.get(i).nodeId)>0)&& (keyHash.compareTo(dynamoNodeList.get(i).prevId)>0) && (dynamoNodeList.get(i).nodeId.compareTo(dynamoNodeList.get(i).prevId)<0))||
					((keyHash.compareTo(dynamoNodeList.get(i).nodeId)<=0)&& (keyHash.compareTo(dynamoNodeList.get(i).prevId)<0) && (dynamoNodeList.get(i).nodeId.compareTo(dynamoNodeList.get(i).prevId)<0))

							)
					{
						insertToNode=dynamoNodeList.get(i).nodeName;
						for (int j = 0; j < 5; j++) {
							if (insertToNode.equals(NODE_LIST[i])) {
								insertToNodeIndex = i;
							}
						}
						if (insertToNodeIndex < 3) {
							replicaNode1= NODE_LIST[insertToNodeIndex + 1];
							replicaNode2 = NODE_LIST[insertToNodeIndex + 2];
						}
						else if (insertToNodeIndex == 3)
						{
							replicaNode1= NODE_LIST[insertToNodeIndex + 1];
							replicaNode2=NODE_LIST[FIRSTNODE];
						}
						else if(insertToNodeIndex == 4)
						{
							replicaNode1=NODE_LIST[FIRSTNODE];
							replicaNode2=NODE_LIST[1];
						}
						else
						{
							replicaNode1="ERROR";
							replicaNode2="ERROR";
						}
						Log.e(TAG,"Replicas3: "+insertToNode+" "+replicaNode1+" "+replicaNode2);
						replica1=replicaNode1;
						replica2=replicaNode2;
						//insertToNodes(filename, value, insertToNode,replicaNode1, replicaNode2);
						Log.e(TAG,"INSERT_Sending to coordinator--"+insertToNode+" from "+nodeName);
						sendToCoordinator(filename, value, insertToNode,replicaNode1, replicaNode2);

					}
				}
			}



		}
		catch (NoSuchAlgorithmException e)
		{
			Log.e(TAG, "Key Hash failed in insert");
		}
		catch (Exception e)
		{
			Log.e(TAG, "File write failed in insert");
		}
		Log.v("insert", values.toString());
		return uri;
	}

	public Void sendToCoordinator(String key,String value,String insertToNode,String replicaNode1,String replicaNode2)
	{
		String message="COORDINATE-"+key+"-"+value+"-"+replicaNode1+"-"+replicaNode2+"-"+nodeName;
		String message2="REPLICATE2-"+key+"-"+value+"-"+nodeName;
		String coordinatorPort=String.valueOf(Integer.parseInt(insertToNode)*2);

		try {
			boolean status3=new InsertFailTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message2, replicaNode2).get();
			//new InsertFailTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message2, replicaNode2);
			boolean status1 = new InsertCoordinatorTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, coordinatorPort).get();

			//new InsertCoordinatorTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, coordinatorPort);
			//insertToReplicas(key,value,replicaPort1,replicaPort2);
			boolean status2=new InsertFailTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message2, replicaNode1).get();
			//new InsertFailTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message2, replicaNode1);

			/*if(status1!=true)
			{
				Log.e(TAG,"SENDTOCOORDINATOR -- Insert failed in sendToCoordinator");
			}
			else
			{
				Log.e(TAG,"SENDTOCOORDINATOR -- Insert Succeded in sendToCoordinator");
			}*/
		}
		catch (ExecutionException e)
		{
			Log.e(TAG,"Execution exception thrown in insertToReplicas--"+e);
		}
		catch(InterruptedException e)
		{
			Log.e(TAG,"InterruptedException thrown in insertToReplicas--"+e);
		}



		return null;
	}
	public Void insertToNodes(String key,String value,String insertToNode,String replicaNode1,String replicaNode2)
	{
		String insertToNodePort=String.valueOf(Integer.parseInt(insertToNode)*2);
		String replicaPort1=String.valueOf(Integer.parseInt(replicaNode1)*2);
		String replicaPort2=String.valueOf(Integer.parseInt(replicaNode2)*2);
		String message="REPLICATE-"+key+"-"+value+"-"+nodeName;
		new InsertReplicaTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message,insertToNodePort);
		new InsertReplicaTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message,replicaPort1);
		new InsertReplicaTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message,replicaPort2);
		return null;
	}
	public Boolean insertReplica(String key,String value)
	{
		FileOutputStream outputStream;
		try {
			outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
			if(!localKeyList.contains(key)) {
				localKeyList.add(key);
			}
			outputStream.write(value.getBytes());
			Log.e(TAG, "Replica Key inserted: " + key + " Value inserted: " + value);
			outputStream.close();
		}
		catch (Exception e)
		{
			Log.e(TAG, "File write failed in insert");
			return false;
		}

		return true;
	}

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
		else {
			localKeyList.remove(selection);
			getContext().deleteFile(selection);
		}


		return 0;
	}

	public Boolean insertRecoveryReplica(String key,String value)
	{
		FileOutputStream outputStream;
		try {

			if(!localKeyList.contains(key)) {
				localKeyList.add(key);
				outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
				outputStream.write(value.getBytes());
				Log.e(TAG, "Replica Key inserted: " + key + " Value inserted: " + value);
				outputStream.close();
			}
		}
		catch (Exception e)
		{
			Log.e(TAG, "File write failed in insert");
			return false;
		}

		return true;
	}

	public Void insertToReplicas(String key,String value,String replicaNode1,String replicaNode2)
	{
		String replicaPort1=String.valueOf(Integer.parseInt(replicaNode1)*2);
		String replicaPort2=String.valueOf(Integer.parseInt(replicaNode2)*2);
		String message="REPLICATE-"+key+"-"+value;
		try {
			boolean status2 = new InsertReplicaTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, replicaPort2).get();
			boolean status1 = new InsertReplicaTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, replicaPort1).get();

			//new InsertReplicaTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, replicaPort2);
			//new InsertReplicaTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, replicaPort1);

		}
		catch (ExecutionException e)
		{
			Log.e(TAG,"Execution exception thrown in insertToReplicas--"+e);
		}
		catch(InterruptedException e)
		{
			Log.e(TAG,"InterruptedException thrown in insertToReplicas--"+e);
		}



		return null;
	}

	private class InsertCoordinatorTask extends AsyncTask<String, Void, Boolean>
	{
		@Override
		protected Boolean doInBackground(String... msgs) {
			String msgToSend=msgs[0];
			String statusMsg="";
			//String statusMsg1="";
			//String statusMsg2="";
			int replicaPort=Integer.parseInt(msgs[1]);
			Log.e(TAG,"InsertReplicaTask msg-"+msgToSend+" ToPort-"+replicaPort);
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),replicaPort);
				socket.setSoTimeout(2000);
				PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
				writer.println(msgToSend);
				writer.flush();
				InputStreamReader isr = new InputStreamReader(socket.getInputStream());
				BufferedReader reader = new BufferedReader(isr);
				statusMsg = reader.readLine();
				reader.close();
			}
			catch (UnknownHostException e) {
				Log.e(TAG, "INSERT REPLICA UnknownHostException");
				return false;
			} catch (IOException e) {
				Log.e(TAG, "INSERT REPLICA socket IOException");
				return false;
			}

			if(statusMsg!=null) {
				if (statusMsg.equals("SUCCESS")) {
					return true;
				}
				return false;
			}
			else {
				return false;
			}
			/*if(!replica1.equals(nodeName))
			{

				while (replica1Status == null) {
					continue;
				}
			}
			if(!replica2.equals(nodeName))
			{
				while (replica2Status == null) {
					continue;
				}
			}

			replica1=null;
			replica2=null;
			replica1Status=null;
			replica2Status=null;*/


		}

	}

	private class InsertFailTask extends AsyncTask<String, Void, Boolean>
	{
		@Override
		protected Boolean doInBackground(String... msgs) {
			String msgToSend=msgs[0];
			int replicaPort=Integer.parseInt(msgs[1])*2;
			Log.e(TAG,"InsertFailTask-"+msgToSend+" ToPort-"+replicaPort);
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),replicaPort);
				PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
				writer.println(msgToSend);
				writer.flush();
			}
			catch (UnknownHostException e) {
				Log.e(TAG, "INSERT REPLICA UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "INSERT REPLICA socket IOException");
			}
			return true;
		}
	}
	private class InsertReplicaTask extends AsyncTask<String, Void, Boolean>
	{
		@Override
		protected Boolean doInBackground(String... msgs) {
			String msgToSend = msgs[0];
			String statusMsg = "";
			int replicaPort = Integer.parseInt(msgs[1]);
			Log.e(TAG, "InsertReplicaTask msg-" + msgToSend + " ToPort-" + replicaPort);
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), replicaPort);
				PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
				writer.println(msgToSend);
				writer.flush();
				InputStreamReader isr = new InputStreamReader(socket.getInputStream());
				BufferedReader reader = new BufferedReader(isr);
				statusMsg = reader.readLine();
				reader.close();
			} catch (UnknownHostException e) {
				Log.e(TAG, "INSERT REPLICA UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "INSERT REPLICA socket IOException");
			}
			if (statusMsg != null) {
				if (statusMsg.equals("SUCCESS")) {
					return true;
				}
				return false;
			} else {
				return false;
			}
		}
	}

	private class InsertReplicaFromCoordTask extends AsyncTask<ReplicaObject, Void, Void>
	{
		@Override
		protected Void doInBackground(ReplicaObject... replicaObjects) {
			Log.e(TAG,"Inside InsertReplicaFromCoordTask "+nodeName);
			ReplicaObject replicaObject=replicaObjects[0];
			String msgToSend=replicaObject.message;
			int replicaPort1=Integer.parseInt(replicaObject.replicaPort1);
			int replicaPort2=Integer.parseInt(replicaObject.replicaPort2);
			String statusMsg1="";
			String statusMsg2="";
			String key=replicaObject.key;
			String value=replicaObject.value;
			boolean insertCoordinator= insertReplica(key,value);


			Log.e(TAG,"InsertReplicaFromCoordTask msg-"+msgToSend+" ToPort-"+replicaPort1);
			try {
				Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),replicaPort1);
				PrintWriter writer = new PrintWriter(socket1.getOutputStream(), true);
				writer.println(msgToSend);
				writer.flush();
				InputStreamReader isr = new InputStreamReader(socket1.getInputStream());
				BufferedReader reader = new BufferedReader(isr);
				statusMsg1 = reader.readLine();
				reader.close();
			}
			catch (UnknownHostException e) {
				Log.e(TAG, "InsertReplicaFromCoordTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "InsertReplicaFromCoordTask socket IOException");
			}

			try {
				Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),replicaPort2);
				PrintWriter writer = new PrintWriter(socket2.getOutputStream(), true);
				writer.println(msgToSend);
				writer.flush();
				InputStreamReader isr = new InputStreamReader(socket2.getInputStream());
				BufferedReader reader = new BufferedReader(isr);
				statusMsg2 = reader.readLine();
				reader.close();
			}
			catch (UnknownHostException e) {
				Log.e(TAG, "InsertReplicaFromCoordTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "InsertReplicaFromCoordTask socket IOException");
			}

			if(statusMsg1!=null&&statusMsg2!=null) {
				if (insertCoordinator == true && statusMsg1.equals("REPDONE") && statusMsg2.equals("REPDONE")) {
					try {
						String msgFromCoord = "SUCCESS";
						Socket replicaSocket = replicaObject.socket;
						PrintWriter writer = new PrintWriter(replicaSocket.getOutputStream(), true);
						writer.println(msgFromCoord);
						writer.flush();
					} catch (UnknownHostException e) {
						Log.e(TAG, "InsertReplicaFromCoordTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "InsertReplicaFromCoordTask socket IOException");
					}
				}
			}
			else
			{

				try {
					String msgFromCoord = "FAILURE";
					Socket replicaSocket = replicaObject.socket;
					PrintWriter writer = new PrintWriter(replicaSocket.getOutputStream(), true);
					writer.println(msgFromCoord);
					writer.flush();
				} catch (UnknownHostException e) {
					Log.e(TAG, "InsertReplicaFromCoordTask UnknownHostException");
				} catch (IOException e) {
					Log.e(TAG, "InsertReplicaFromCoordTask socket IOException");
				}
			}

			return null;
		}
	}

	private class NodeRecoveryTask extends AsyncTask<String, Void, Boolean> {
		@Override
		protected Boolean doInBackground(String... msgs) {
			boolean status=false;
			String failedNode=msgs[0];
			int failedNodeIndex=-1;
			String recoveryMessage="";
			String recoveryReplyMessage1=null;
			String recoveryReplyMessage2=null;
			String recoveryReplyMessage3=null;
			String recoveryReplyMessage4=null;

			String pred1="";
			String succ1="";
			String pred2="";
			String succ2="";

			for (int i = 0; i < 5; i++) {
				if (failedNode.equals(NODE_LIST[i])) {
					failedNodeIndex = i;
				}
			}
			if(failedNodeIndex==0)
			{
				pred1=NODE_LIST[LASTNODE];
				pred2=NODE_LIST[3];
			}
			else if(failedNodeIndex==1)
			{
				pred1=NODE_LIST[0];
				pred2=NODE_LIST[LASTNODE];
			}
			else
			{
				pred1=NODE_LIST[failedNodeIndex-1];
				pred2=NODE_LIST[failedNodeIndex-2];
			}
			succ1=NODE_LIST[(failedNodeIndex+1)%5];
			succ2=NODE_LIST[(failedNodeIndex+2)%5];

			int pred1Port=Integer.parseInt(pred1)*2;
			int pred2Port=Integer.parseInt(pred2)*2;;
			int succ1Port=Integer.parseInt(succ1)*2;;
			int succ2Port=Integer.parseInt(succ2)*2;;
			try {
				recoveryMessage="RECOVER-"+failedNode+"-"+previousNodeName+"-pred";
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), pred1Port);
				PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
				writer.println(recoveryMessage);
				writer.flush();
				InputStreamReader isr = new InputStreamReader(socket.getInputStream());
				BufferedReader reader = new BufferedReader(isr);
				recoveryReplyMessage1 = reader.readLine();
				reader.close();
			} catch (UnknownHostException e) {
				Log.e(TAG, "RECOVERTASK UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "RECOVERTASK socket IOException");
			}

			try {
				recoveryMessage="RECOVER-"+failedNode+"-"+previousNodeName+"-pred";
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), pred2Port);
				PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
				writer.println(recoveryMessage);
				writer.flush();
				InputStreamReader isr = new InputStreamReader(socket.getInputStream());
				BufferedReader reader = new BufferedReader(isr);
				recoveryReplyMessage2 = reader.readLine();
				reader.close();
			} catch (UnknownHostException e) {
				Log.e(TAG, "RECOVERTASK UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "RECOVERTASK socket IOException");
			}
			try {
				recoveryMessage="RECOVER-"+failedNode+"-"+previousNodeName+"-succ";
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), succ1Port);
				PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
				writer.println(recoveryMessage);
				writer.flush();
				InputStreamReader isr = new InputStreamReader(socket.getInputStream());
				BufferedReader reader = new BufferedReader(isr);
				recoveryReplyMessage3 = reader.readLine();
				reader.close();
			} catch (UnknownHostException e) {
				Log.e(TAG, "RECOVERTASK UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "RECOVERTASK socket IOException");
			}

			try {
				recoveryMessage="RECOVER-"+failedNode+"-"+previousNodeName+"-succ";
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), succ2Port);
				PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
				writer.println(recoveryMessage);
				writer.flush();
				InputStreamReader isr = new InputStreamReader(socket.getInputStream());
				BufferedReader reader = new BufferedReader(isr);
				recoveryReplyMessage4 = reader.readLine();
				reader.close();
			} catch (UnknownHostException e) {
				Log.e(TAG, "RECOVERTASK UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "RECOVERTASK socket IOException");
			}

			if(recoveryReplyMessage1!=null) {
				String[] recReply1 = recoveryReplyMessage1.split("-");
				if (recReply1.length > 2) {
					for (int i = 1; i < recReply1.length; i += 2) {
						String key = recReply1[i];
						String value = recReply1[i + 1];
						if (key != null & value != null) {
							Log.e(TAG,"Insert To replica call in rec1");
							insertRecoveryReplica(key, value);
						}
					}
				}
			}
			if(recoveryReplyMessage2!=null) {
				String[] recReply2 = recoveryReplyMessage2.split("-");
				if (recReply2.length > 2) {
					for (int i = 1; i < recReply2.length; i += 2) {
						String key = recReply2[i];
						String value = recReply2[i + 1];
						if (key != null & value != null) {
							Log.e(TAG,"Insert To replica call in rec2");
							insertRecoveryReplica(key, value);
						}
					}
				}
			}

			if(recoveryReplyMessage3!=null) {
				String[] recReply3 = recoveryReplyMessage3.split("-");
				if (recReply3.length > 2) {
					for (int i = 1; i < recReply3.length; i += 2) {
						String key = recReply3[i];
						String value = recReply3[i + 1];
						if (key != null & value != null) {
							Log.e(TAG,"Insert To replica call in rec3");
							insertRecoveryReplica(key, value);
						}
					}
				}
			}


				if (recoveryReplyMessage4 != null) {
					String[] recReply4 = recoveryReplyMessage4.split("-");
					if (recReply4.length > 2) {
						for (int i = 1; i < recReply4.length; i += 2) {
							String key = recReply4[i];
							String value = recReply4[i + 1];
							if (key != null & value != null) {
								Log.e(TAG, "Insert To replica call in rec4");
								insertRecoveryReplica(key, value);
							}
						}
					}
				}



			globalInit=true;
			return true;
		}
	}

	@Override
	public boolean onCreate() {
		//Code taken from PA1 - Code used to obtain the current node name
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		final String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		nodeName=portStr;

		//Creating the dynamo list
		initDynamoList();



		Log.e(TAG,"AFTER init:NodeName "+nodeName+" Prev "+previousNodeName+" next "+nextNodeName);
		nodeUri=buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

		try {

			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			serverSocket.setSoTimeout(0);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {

			Log.e(TAG, "Can't create a ServerSocket "+e);
			return false;
		}

		//failure detection
		if(getContext().fileList()!=null && getContext().fileList().length>0) {
			Log.e(TAG, "node is recovering from failure");
			new NodeRecoveryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodeName);

		}
		else
		{
			globalInit=true;
		}
			/*
			try {
				new NodeRecoveryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodeName).get();

			} catch (InterruptedException e) {
				Log.e(TAG, "oncreate interrupted excpetion");
			} catch (ExecutionException e) {
				Log.e(TAG, "oncreate ExecutionException");
			}
		}*/

		//}
		/*else
		{
			try {
				FileOutputStream outputStream;
				outputStream = getContext().openFileOutput("dummyFile", Context.MODE_PRIVATE);
				outputStream.write("dummy".getBytes());
				Log.e(TAG, "Dummy File Created");
				outputStream.close();
			}
			catch (Exception e)
			{
				Log.e(TAG, "File write failed in insert");
			}
		}*/


		return true;
	}

	public Void sendToReplicas(Socket socket,String replicateMsg,String key, String value,String port1,String port2)
	{

		ReplicaObject replicaObject=new ReplicaObject(socket,replicateMsg,key,value,port1,port2);
		new InsertReplicaFromCoordTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replicaObject);

		return null;
	}

	private class ReplicationStatusTask extends AsyncTask<String, Void, Void>
	{
		@Override
		protected Void doInBackground(String... msgs) {
			Log.e(TAG,"Inside ReplicationStatusTask");
			String originNode=msgs[0];
			if(originNode.equals(nodeName))
			{
				return null;
			}
			else{
			int originPort=Integer.parseInt(originNode)*2;
			String statusMsg="REPDONE-"+nodeName;
			Log.e(TAG,"ReplicationStatusTask msg-"+statusMsg+" ToPort-"+originPort);


				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), originPort);
					PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
					writer.println(statusMsg);
					writer.flush();
				} catch (UnknownHostException e) {
					Log.e(TAG, "INSERT REPLICA UnknownHostException");
				} catch (IOException e) {
					Log.e(TAG, "INSERT REPLICA socket IOException");
				}

				return null;
			}
		}
	}

	private class RecoverPredTask extends AsyncTask<ReplicaObject, Void, Void> {
		@Override
		protected Void doInBackground(ReplicaObject... replicaObjects) {
			String recoveryRetMsg="RECRET";
			FileInputStream inputStream;
			ReplicaObject failedObject=replicaObjects[0];
			String failedNode=failedObject.replicaPort1;
			for(int i=0;i<localKeyList.size();i++) {
				try {
					String key=localKeyList.get(i);
					String keyHash = genHash(key);

					if (((keyHash.compareTo(previousNodeId) > 0) && keyHash.compareTo(nodeId) <= 0) ||
							((keyHash.compareTo(nodeId) > 0) && (keyHash.compareTo(previousNodeId) > 0) && (nodeId.compareTo(previousNodeId) < 0)) ||
							((keyHash.compareTo(nodeId) <= 0) && (keyHash.compareTo(previousNodeId) < 0) && (nodeId.compareTo(previousNodeId) < 0))
							) {

						try {
							String value = "";
							inputStream = getContext().openFileInput(key);
							BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
							value = reader.readLine();
							reader.close();
							recoveryRetMsg+="-"+key+"-"+value;
						} catch (UnknownHostException e) {
							Log.e(TAG, "RecoverPredTask  UnknownHostException");
						} catch (IOException e) {
							Log.e(TAG, "RecoverPredTask socket IOException");
						}

					}


				} catch (NoSuchAlgorithmException e) {
					Log.e(TAG, "RecoverPredTask key hash exception");
				}
			}

			try {

				Socket failedNodeSocket = failedObject.socket;
				PrintWriter writer = new PrintWriter(failedNodeSocket.getOutputStream(), true);
				writer.println(recoveryRetMsg);
				writer.flush();
			}
			catch (UnknownHostException e) {
				Log.e(TAG, "RecoverPredTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "RecoverPredTask socket IOException");
			}



			return null;
		}
	}

	private class RecoverSuccTask extends AsyncTask<ReplicaObject, Void, Void> {
		@Override
		protected Void doInBackground(ReplicaObject... replicaObjects) {
			String recoveryRetMsg="RECRET";
			FileInputStream inputStream;
			ReplicaObject failedObject=replicaObjects[0];
			String failedNode=failedObject.replicaPort1;
			String failedPrevNode=failedObject.replicaPort2;
			String failedNodeId="";
			String failedPrevId="";
			int currentNodeIndex=-1;
			try {
				failedNodeId = genHash(failedNode);
				failedPrevId=genHash(failedPrevNode);
			}
			catch (NoSuchAlgorithmException e)
			{
				Log.e(TAG,"KeyhashException");
			}

			for(int i=0;i<localKeyList.size();i++) {
				try {
					String key=localKeyList.get(i);
					String keyHash = genHash(key);

					if (((keyHash.compareTo(failedPrevId) > 0) && keyHash.compareTo(failedNodeId) <= 0) ||
							((keyHash.compareTo(failedNodeId) > 0) && (keyHash.compareTo(failedPrevId) > 0) && (failedNodeId.compareTo(failedPrevId) < 0)) ||
							((keyHash.compareTo(failedNodeId) <= 0) && (keyHash.compareTo(failedPrevId) < 0) && (failedNodeId.compareTo(failedPrevId) < 0))
							) {

						try {
							String value = "";
							inputStream = getContext().openFileInput(key);
							BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
							value = reader.readLine();
							reader.close();
							recoveryRetMsg+="-"+key+"-"+value;
						} catch (UnknownHostException e) {
							Log.e(TAG, "RecoverSuccTask  UnknownHostException");
						} catch (IOException e) {
							Log.e(TAG, "RecoverSuccTask socket IOException");
						}

					}


				} catch (NoSuchAlgorithmException e) {
					Log.e(TAG, "RecoverSuccTask key hash exception");
				}
			}

			try {

				Socket failedNodeSocket = failedObject.socket;
				PrintWriter writer = new PrintWriter(failedNodeSocket.getOutputStream(), true);
				writer.println(recoveryRetMsg);
				writer.flush();
			}
			catch (UnknownHostException e) {
				Log.e(TAG, "RecoverSuccTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "RecoverSuccTask socket IOException");
			}
			return null;

		}
		}






	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			try {
				//serverSocket.setSoTimeout(0);
				while (true) //loop added to send multiple messages
				{
					Socket socket = serverSocket.accept();
					InputStreamReader isr = new InputStreamReader(socket.getInputStream());

					BufferedReader reader = new BufferedReader(isr);

					String messageReceived = reader.readLine();
					Log.e(TAG,"ServerTask below Readerline : "+messageReceived);
					if (messageReceived == null) {
						Log.e(TAG, "No Message received from client");
					}
					else {
						String message[] = messageReceived.split("-");
						String requestType = message[0];
						if (requestType.equals("REPLICATE"))
						{
							String key=message[1];
							String value=message[2];
							boolean insertStatus= insertReplica(key,value);
							String insertStatusMsg;
							if(insertStatus==true)
							{
								insertStatusMsg="SUCCESS";
							}
							else
							{
								insertStatusMsg="FAILED";
							}
							PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
							writer.println(insertStatusMsg);
							writer.flush();

						}

						else if(requestType.equals("REPLICATE2"))
						{
							String key=message[1];
							String value=message[2];
							boolean insertStatus= insertReplica(key,value);
							Log.e(TAG,"Replicate2 insertStatus="+insertStatus);


						}
						else if(requestType.equals("COORDINATE"))
						{
							//"COORDINATE-"+key+"-"+value+"-"+replicaNode1+"-"+replicaNode2+originnode;
							String key=message[1];
							String value=message[2];
							String replicaPort1=String.valueOf(Integer.parseInt(message[3])*2);
							String replicaPort2=String.valueOf(Integer.parseInt(message[4])*2);
							String replicateMsg="COORDREPLICATE-"+key+"-"+value+"-"+nodeName+"-"+message[5];
							sendToReplicas(socket,replicateMsg,key,value,replicaPort1,replicaPort2);
							Log.e(TAG,"below sendtoreplica");
						}

						else if(requestType.equals("COORDREPLICATE"))
						{
							String key=message[1];
							String value=message[2];
							String originNode=message[3];
							boolean insertStatus= insertReplica(key,value);
							//ReplicaObject replicaObject=new ReplicaObject(socket,null,key,value,null,null);
							//new ReplicateTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,replicaObject);

							if(insertStatus==true)
							{
								String replicateStatusMsg="REPDONE";
								Log.e(TAG,"COORDREPLICATE-- Inside insert Status "+originNode);
								PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
								writer.println(replicateStatusMsg);
								writer.flush();
								Log.e(TAG,"COORDREPLICATE-- After ReplicationStatusTask call");
							}


						}

						else if(requestType.equals("RECOVER"))
						{
							String failedNode=message[1];
							String failedPrevNode=message[2];
							String recoveryType=message[3];

							if(recoveryType.equals("pred"))
							{
								ReplicaObject recoveryObject=new ReplicaObject(socket,null,null,null,failedNode,null);
								new RecoverPredTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,recoveryObject);
							}
							else if(recoveryType.equals("succ"))
							{
								ReplicaObject recoveryObject=new ReplicaObject(socket,null,null,null,failedNode,failedPrevNode);
								new RecoverSuccTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,recoveryObject);
							}



						}
						else if(requestType.equals("REPDONE"))
						{
							String fromReplicaNode=message[1];
							if(fromReplicaNode.equals(replica1))
							{
								replica1Status="DONE";
							}
							else if(fromReplicaNode.equals(replica2))
							{
								replica2Status="DONE";
							}

						}
						else if(requestType.equals("QUERY2"))
						{
							//"QUERY2-"+key;
							String key=message[1];
							String fromNode=message[2];
							String returnMsgFromReplica2=queryReplica2(key,fromNode);
							PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
							writer.println(returnMsgFromReplica2);
							writer.flush();
						}
						else if(requestType.equals("QUERYALL"))
						{
							String originNode=message[1];
							MatrixCursor allResultCursor=queryAll();
							String queryAllRetMsg="QUERYALLRET-"+nodeName;
							if(allResultCursor.getCount()>0) {
								Log.e(TAG,"resultCursor Iteration 1");
								allResultCursor.moveToFirst();
								int keyIndex = allResultCursor.getColumnIndex("key");
								int valueIndex = allResultCursor.getColumnIndex("value");

								String returnKey = allResultCursor.getString(keyIndex);
								String returnValue = allResultCursor.getString(valueIndex);
								queryAllRetMsg += "-" + returnKey + "-" + returnValue;


								for (int i = 1; i < allResultCursor.getCount(); i++) {
									Log.e(TAG,"resultCursor Iteration: "+i+1);
									allResultCursor.moveToNext();
									returnKey = allResultCursor.getString(keyIndex);
									returnValue = allResultCursor.getString(valueIndex);
									queryAllRetMsg += "-" + returnKey + "-" + returnValue;
								}
							}
							PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
							writer.println(queryAllRetMsg);

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


	public String queryReplica2(String key,String fromNode)
	{
		int queryReplica2Port=Integer.parseInt(fromNode)*2;
		if (localKeyList.size() != 0) {
			Log.e(TAG, "QUERYREPLICA2-- localkeylist has values");
			for (int j = 0; j < localKeyList.size(); j++) {
				if (localKeyList.get(j).equals(key)) {
					Log.e(TAG, "QUERYREPLICA2-- locallist has key");
					FileInputStream inputStream;
					try {
						String value = "";
						inputStream = getContext().openFileInput(key);
						BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
						value = reader.readLine();
						reader.close();
						String queryReturnMessage="QUERYRET2-"+key+"-"+value;
						Log.e(TAG, "QUERYREPLICA2--"+queryReturnMessage);

						return queryReturnMessage;

					}
					catch (UnknownHostException e) {
							Log.e(TAG, "queryReplica2  UnknownHostException");
						} catch (IOException e) {
							Log.e(TAG, "queryReplica2 socket IOException");
						}
					catch (Exception e) {
						Log.e(TAG, "queryReplica2 File read failed");
					}
					}
				}
			}
			else {
			Log.e(TAG, "QUERYREPLICA2-- localkeylist has no values");
			/*while (localKeyList.size() == 0) {
				Log.e(TAG, "Busy wait in query Replica2");
				if (localKeyList.size() > 0) {
					break;
				}
			}*/

			for (int j = 0; j < localKeyList.size(); j++) {
				if (localKeyList.get(j).equals(key)) {
					Log.e(TAG, "QUERYREPLICA2-- locallist has key");
					FileInputStream inputStream;
					try {
						String value = "";
						inputStream = getContext().openFileInput(key);
						BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
						value = reader.readLine();
						reader.close();
						String queryReturnMessage = "QUERYRET2-" + key + "-" + value;
						Log.e(TAG, "QUERYREPLICA2--" + queryReturnMessage);

						return queryReturnMessage;

					} catch (UnknownHostException e) {
						Log.e(TAG, "queryReplica2  UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "queryReplica2 socket IOException");
					} catch (Exception e) {
						Log.e(TAG, "queryReplica2 File read failed");
					}
				}

			}
		}

		return null;
	}

	public MatrixCursor queryAll()
	{
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
		}
		catch (IOException e)
		{
			Log.e(TAG,"IO exception in query @*");
		}
		return matrixCursor;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

		while(!globalInit)
		{
			if(globalInit)
			{
				break;
			}
		}

		try {
			Thread.sleep(300);
		}
		catch(InterruptedException e)
		{
			Log.e(TAG,"InterruptedException exception in query sleep");
		}

		Log.e(TAG,"Query Received for key"+selection);

		if(selection.contains("@")) //Querying for @ case
		{
			Log.e(TAG,"QUERY -- Inside @:"+selection);
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
					value=reader.readLine();
					MatrixCursor.RowBuilder cursorRows = matrixCursor.newRow();
					cursorRows.add("key", key);
					cursorRows.add("value", value.toString());
					reader.close();
				}
			}
			catch (IOException e)
			{
				Log.e(TAG,"IO exception in query @*");
			}
			return matrixCursor;
		}
		else if(selection.contains("*")) //Querying for "*" - Get values from the all the dynamo nodes
		{
			Log.e(TAG,"QUERY -- Inside *");
			String[] columnNames = {"key", "value"};
			MatrixCursor matrixCursor = new MatrixCursor(columnNames);
			try {
				String value = "";
				String msg;
				for (int i = 0; i < localKeyList.size(); i++) {
					String key = localKeyList.get(i);
					FileInputStream inputStream = getContext().openFileInput(key);
					BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
					value = reader.readLine();
					MatrixCursor.RowBuilder cursorRows = matrixCursor.newRow();
					cursorRows.add("key", key);
					cursorRows.add("value", value.toString());
					reader.close();
				}
			}
			catch (IOException e)
			{
				Log.e(TAG,"IO exception in query *");
			}
			String[] queryAllNodes=new String[4];
			for(int i=0;i<5;i++)
			{

				if(NODE_LIST[i].equals(nodeName))
				{
					continue;
				}

				else
				{
					MatrixCursor allMatrixCursor=queryTargetForALL(NODE_LIST[i]);
					if(allMatrixCursor.getCount()>0) {
						allMatrixCursor.moveToFirst();
						int keyIndex = allMatrixCursor.getColumnIndex("key");
						int valueIndex = allMatrixCursor.getColumnIndex("value");

						String returnKey = allMatrixCursor.getString(keyIndex);
						String returnValue = allMatrixCursor.getString(valueIndex);
						MatrixCursor.RowBuilder newCursorRows = matrixCursor.newRow();
						newCursorRows.add("key", returnKey);
						newCursorRows.add("value", returnValue);

						for (int j = 1; j < allMatrixCursor.getCount(); j++) {
							allMatrixCursor.moveToNext();
							returnKey = allMatrixCursor.getString(keyIndex);
							returnValue = allMatrixCursor.getString(valueIndex);
							newCursorRows = matrixCursor.newRow();
							newCursorRows.add("key", returnKey);
							newCursorRows.add("value", returnValue);
						}
					}
				}
			}

			return matrixCursor;
		}//Querying for * - end
		else // Querying for a single key  -- start
		{

			FileInputStream inputStream;
			String[] columnNames = {"key", "value"};
			MatrixCursor matrixCursor = new MatrixCursor(columnNames);
			String keyHash="";
			Log.e(TAG, "QUERY-- For a single key");
			try
			{
				keyHash=genHash(selection);
			}
			catch(NoSuchAlgorithmException e)
			{
				Log.e(TAG,"KeyHash failed in QUERY -- for a single key");
			}
			String queryToNode;
			String queryTargetNode=null;
			int queryToNodeIndex=-1;
			for(int i=0;i<dynamoNodeList.size();i++) {
				if (
						(keyHash.compareTo(dynamoNodeList.get(i).prevId) > 0 && keyHash.compareTo(dynamoNodeList.get(i).nodeId) <= 0) ||
								((keyHash.compareTo(dynamoNodeList.get(i).nodeId) > 0) && (keyHash.compareTo(dynamoNodeList.get(i).prevId) > 0) && (dynamoNodeList.get(i).nodeId.compareTo(dynamoNodeList.get(i).prevId) < 0)) ||
								((keyHash.compareTo(dynamoNodeList.get(i).nodeId) <= 0) && (keyHash.compareTo(dynamoNodeList.get(i).prevId) < 0) && (dynamoNodeList.get(i).nodeId.compareTo(dynamoNodeList.get(i).prevId) < 0))

						) {
					queryToNode = dynamoNodeList.get(i).nodeName;
					for (int j = 0; j < 5; j++) {
						if (queryToNode.equals(NODE_LIST[i])) {
							queryToNodeIndex = i;
						}
					}
					if (queryToNodeIndex < 3) {
						queryTargetNode = NODE_LIST[queryToNodeIndex + 2];
					} else if (queryToNodeIndex == 3) {
						queryTargetNode = NODE_LIST[FIRSTNODE];
					} else if (queryToNodeIndex == 4) {
						queryTargetNode = NODE_LIST[1];
					} else {
						Log.e(TAG, "QUERY -- for single key -- setting Error as target");
						queryTargetNode = "ERROR";
					}

				}
			}
				if(queryTargetNode==null)
				{
					Log.e(TAG,"QUERY -- for single key -- setting dummy target");
					queryTargetNode="ERROR";
				}

				Log.e(TAG,"QueryFirstTargetNode="+queryTargetNode);
				if(queryTargetNode.equals(nodeName))
				{
					Log.e(TAG, "QUERY-- For a single key -- Current Node is the target Node");
					if (localKeyList.size() != 0) {
						Log.e(TAG, "QUERY-- For a single key -- localkeylist has values");
						for (int j = 0; j < localKeyList.size(); j++) {
							if (localKeyList.get(j).equals(selection)) {

								try {
									String value = "";
									String msg;
									inputStream = getContext().openFileInput(selection);
									BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
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
						}
						Log.e(TAG,"QUERY-- For a single key -- localkeylist has  values but not the specific key");

						String querySecond=NODE_LIST[queryToNodeIndex];
						String queryValue=queryTargetNode(querySecond,selection);
						if(queryValue!=null) {
							if (queryValue.length() > 1) {
								Log.e(TAG, "QUERY-- For a single key -- 2ndQuery returned a value");
								String[] queryValues = queryValue.split("-");
								if (queryValues.length > 2) {
									MatrixCursor.RowBuilder cursorRows = matrixCursor.newRow();
									cursorRows.add("key", queryValues[1]);
									cursorRows.add("value", queryValues[2]);
									Log.e(TAG, "QUERY-MATRIX: " + queryValues[1] + "-" + queryValues[2]);
									return matrixCursor;
								} else {
									Log.e(TAG, "QUERY---- For a single key -- 2ndQuery  returned a value with length less than 2");
									return null;
								}
							}
						}
							Log.e(TAG, "QUERY---- For a single key -- 2ndQuery  returned a vnull value");
							return null;
					}
					else
					{
						Log.e(TAG,"QUERY-- For a single key -- localkeylist has no values");
						String querySecond=NODE_LIST[queryToNodeIndex];
						String queryValue=queryTargetNode(querySecond,selection);
						if(queryValue!=null) {
							if (queryValue.length() > 1) {
								Log.e(TAG, "QUERY-- For a single key -- 2ndQuery returned a value");
								String[] queryValues = queryValue.split("-");
								if(queryValues.length>2) {
									MatrixCursor.RowBuilder cursorRows = matrixCursor.newRow();
									cursorRows.add("key", queryValues[1]);
									cursorRows.add("value", queryValues[2]);
									Log.e(TAG, "QUERY-MATRIX: " + queryValues[1] + "-" + queryValues[2]);
									return matrixCursor;
								}
								else
								{
									Log.e(TAG, "QUERY---- For a single key -- 2ndQuery  returned a value with length less than 2");
									return null;
								}
							}
							Log.e(TAG, "QUERY---- For a single key -- 2ndQuery  returned a value with length less than 1");
							return null;
						}
						return null;
					}
				}
				//Querying the target node to get the value

				String queryReturnValue=queryTargetNode(queryTargetNode,selection);

				if(queryReturnValue==null)
				{
					Log.e(TAG,"QUERY single key 1st return value is null");
					String querySecond = NODE_LIST[queryToNodeIndex];
					if(querySecond.equals(nodeName))
					{
						Log.e(TAG, "QUERY-- For a single key -- Current Node is the 2nd query target Node");
						if (localKeyList.size() != 0) {
							Log.e(TAG, "QUERY-- For a single key -- localkeylist has values");
							for (int j = 0; j < localKeyList.size(); j++) {
								if (localKeyList.get(j).equals(selection)) {

									try {
										String value = "";
										String msg;
										inputStream = getContext().openFileInput(selection);
										BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
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
							}
							Log.e(TAG,"Wrong return for query");
							return matrixCursor;

						}

						else
						{
							Log.e(TAG, "QUERY-- For a single key -- Current Node is the 2nd query target Node-- has no locallist");
							return null;
						}
					}
					else
					{
						queryReturnValue=queryTargetNode(querySecond,selection);
						if(queryReturnValue!=null) {
							if (queryReturnValue.length() > 1) {
								Log.e(TAG, "QUERY-- For a single key -- 2ndQuery returned a value");
								String[] queryValues = queryReturnValue.split("-");
								if(queryValues.length>2) {
									MatrixCursor.RowBuilder cursorRows = matrixCursor.newRow();
									cursorRows.add("key", queryValues[1]);
									cursorRows.add("value", queryValues[2]);
									Log.e(TAG, "QUERY-MATRIX: " + queryValues[1] + "-" + queryValues[2]);
									return matrixCursor;
								}
								else
								{
									Log.e(TAG, "QUERY-- For a single key -- 2ndQuery  returned a value with length less than 2");
									return null;
								}
							}
							Log.e(TAG, "QUERY-- For a single key -- 2ndQuery  returned a value with length less than 1");
							return null;
						}
						else
						{
							Log.e(TAG, "QUERY-- For a single key -- 2ndQuery also returned null");
							String queryThird=NODE_LIST[(queryToNodeIndex+1)%5];
							if(queryThird.equals(nodeName))
							{
								if (localKeyList.size() != 0) {
									Log.e(TAG, "QUERY-- For a single key -- localkeylist has values");
									for (int j = 0; j < localKeyList.size(); j++) {
										if (localKeyList.get(j).equals(selection)) {

											try {
												String value = "";
												String msg;
												inputStream = getContext().openFileInput(selection);
												BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
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
									}
									Log.e(TAG,"Wrong return for query");
									return matrixCursor;

								}
								else
								{
									Log.e(TAG, "QUERY-- For a single key -- Current Node is not 3nd query target Node-- has no locallist");
									return null;
								}

							}
							else
							{
								Log.e(TAG, "QUERY-- For a single key --  3nd query ");
								//return null;
								queryReturnValue=queryTargetNode(queryThird,selection);
								if(queryReturnValue!=null) {
									if (queryReturnValue.length() > 1) {
										Log.e(TAG, "QUERY-- For a single key -- 2ndQuery returned a value");
										String[] queryValues = queryReturnValue.split("-");
										if(queryValues.length>2) {
											MatrixCursor.RowBuilder cursorRows = matrixCursor.newRow();
											cursorRows.add("key", queryValues[1]);
											cursorRows.add("value", queryValues[2]);
											Log.e(TAG, "QUERY-MATRIX: " + queryValues[1] + "-" + queryValues[2]);
											return matrixCursor;
										}
										else
										{
											Log.e(TAG, "QUERY-- For a single key -- 3rdQuery  returned a value with length < 2");
											return null;
										}
									}
									Log.e(TAG, "QUERY-- For a single key -- 2ndQuery  returned a value with length less than 1");
									return null;
								}
								else
								{
									Log.e(TAG, "QUERY-- For a single key -- 3rd Query  also returned null");
									return null;
								}




							}


						}
					}


				}
				else if(queryReturnValue.length()>2)
				{
					Log.e(TAG,"Query first node result is not null else"+queryReturnValue);
					String[] queryValues=queryReturnValue.split("-");
					if(queryValues.length>2) {
						MatrixCursor.RowBuilder cursorRows = matrixCursor.newRow();
						cursorRows.add("key", queryValues[1]);
						cursorRows.add("value", queryValues[2]);
						Log.e(TAG, "QUERY-MATRIX: " + queryValues[1] + "-" + queryValues[2]);
						return matrixCursor;
					}
					else
					{
						Log.e(TAG,"Query result is not null--but has no values-firstQueryRetValue "+queryReturnValue);
						String querySecond=NODE_LIST[queryToNodeIndex];
						Log.e(TAG,"query second node:"+querySecond);
						queryReturnValue=queryTargetNode(querySecond,selection);
						if(queryReturnValue!=null) {
							if (queryReturnValue.length() > 1) {
								Log.e(TAG, "QUERY-- For a single key -- 2ndQuery returned a value");
								String[] queryFinalValues = queryReturnValue.split("-");
								if(queryValues.length>2) {
									MatrixCursor.RowBuilder cursorRows = matrixCursor.newRow();
									cursorRows.add("key", queryValues[1]);
									cursorRows.add("value", queryValues[2]);
									Log.e(TAG, "QUERY-MATRIX: " + queryValues[1] + "-" + queryValues[2]);
									return matrixCursor;
								}
								else
								{
									Log.e(TAG, "QUERY-- For a single key -- 2ndQuery  returned a value with length less than 2");
									String queryThird=NODE_LIST[(queryToNodeIndex+1)%5];
									String queryFinalReturnValue=queryTargetNode(queryThird,selection);
									if(queryFinalReturnValue!=null) {
										if (queryFinalReturnValue.length() > 1) {
											Log.e(TAG, "QUERY-- For a single key -- 2ndQuery returned a value");
											String[] queryFinalValues2 = queryFinalReturnValue.split("-");
											if (queryFinalValues2.length > 2) {
												MatrixCursor.RowBuilder cursorRows = matrixCursor.newRow();
												cursorRows.add("key", queryFinalValues2[1]);
												cursorRows.add("value", queryFinalValues2[2]);
												Log.e(TAG, "QUERY-MATRIX: " + queryFinalValues2[1] + "-" + queryFinalValues2[2]);
												return matrixCursor;
											}
										}
									}

									Log.e(TAG, "Couldn't find the query value -- after third call");
									return null;
								}
							}
							Log.e(TAG, "QUERY-- For a single key -- 2ndQuery  returned a value with length less than 1");
							return null;
						}


						Log.e(TAG, "Couldn't find the query value");
						return null;

					}
				}
				else
				{
					Log.e(TAG,"Query result is null--final "+queryReturnValue);
					return null;
				}

		}  // Querying for a single key  -- end

	}


	public MatrixCursor queryTargetForALL(String targetNode)
	{
		String[] columnNames = {"key", "value"};
		MatrixCursor matrixCursor = new MatrixCursor(columnNames);
		Log.e(TAG,"In queryTargetForALL for Node "+targetNode);
		int queryAllTargetPort=Integer.parseInt(targetNode)*2;
		String queryAllReturnMessage=null;
		String message="QUERYALL-"+nodeName;
		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),queryAllTargetPort);
			PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
			writer.println(message);
			writer.flush();

			InputStreamReader isr = new InputStreamReader(socket.getInputStream());
			BufferedReader reader = new BufferedReader(isr);
			queryAllReturnMessage = reader.readLine();
			reader.close();
			if(queryAllReturnMessage==null)
			{
				Log.e(TAG,"Null message received in new socket");

			}
			else
			{
				String[] msg=queryAllReturnMessage.split("-");
				if(msg[0].equals("QUERYALLRET"))
				{

					MatrixCursor allreturnedCursor = new MatrixCursor(columnNames);
					Log.e(TAG,"Received QUERYALLRET msg length"+msg.length);

					if(msg.length>1) {
						for (int i = 2; i < msg.length; i += 2) {
							MatrixCursor.RowBuilder cursorRows = allreturnedCursor.newRow();
							cursorRows.add("key", msg[i]);
							cursorRows.add("value", msg[i + 1]);
						}
						Log.e(TAG,"ALL returned cursor Count "+allreturnedCursor.getCount());
					}
					matrixCursor=allreturnedCursor;
				}

			}

		}
		catch (UnknownHostException e) {
			Log.e(TAG, "ClientTask UnknownHostException");
		} catch (IOException e) {
			Log.e(TAG, "ClientTask socket IOException");
		}
		return matrixCursor;
	}
	public String queryTargetNode(String targetNode,String key)
	{
		int queryTargetPort=Integer.parseInt(targetNode)*2;
		String queryMessage="QUERY2-"+key+"-"+nodeName;
		String queryReturnMessage=null;
		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),queryTargetPort);
			PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
			writer.println(queryMessage);
			writer.flush();
			InputStreamReader isr = new InputStreamReader(socket.getInputStream());
			BufferedReader reader = new BufferedReader(isr);
			queryReturnMessage = reader.readLine();
			reader.close();

		}
		catch (UnknownHostException e) {
			Log.e(TAG, "QUERY2 TargetNode UnknownHostException");
		} catch (IOException e) {
			Log.e(TAG, "QUERY2 TargetNode socket IOException");
		}

		return queryReturnMessage;
	}
	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
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
