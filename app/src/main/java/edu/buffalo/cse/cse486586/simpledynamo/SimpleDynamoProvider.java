package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedList;


import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import java.util.*;
import android.database.MatrixCursor;




public class SimpleDynamoProvider extends ContentProvider {

	private DBHelper db;

	static String ports;


	String trial;

	private static final String TAG = SimpleDynamoProvider.class.getName();


	private static final String PROVIDER_NAME ="edu.buffalo.cse.cse486586.simpledynamo.provider";
	private static final String BASE_PATH = "MyTable";
	static final String URL = "content://" + PROVIDER_NAME + "/" + BASE_PATH;
	static final Uri CONTENT_URI = Uri.parse(URL);


	private SQLiteDatabase database;
	private static final String DATABASE_NAME = "PA4";

	private static final String JOIN = "join";

	String nodeid;

	Cursor c = null;

	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	static final int SERVER_PORT = 10000;

	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";

	static LinkedList<ListNode> l = new LinkedList<ListNode>();

	String meport;


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		SQLiteDatabase sqlDB = db.getWritableDatabase();

		String[] ll = {"5562","5556","5554","5558","5560","5562","5556"};

		String[] predar={"5560","5562","5556","5554","5558"};
		int rowsDeleted = 0;
		String star= "*";
		String at="@";
		if(selection.equalsIgnoreCase(star) || selection.equalsIgnoreCase(at)) {
			sqlDB.execSQL("DELETE * FROM " + dbTable.TABLE_NAME);

		}
		else
		{
			int back = 0;
			int index = 0;
			String belongsto = "nothing";


			try {
				for (int i = 0; i < 5; i++) {

					String mykey = genHash(selection);
					String hashport = genHash(ll[i]);
					String hashpred = genHash(predar[i]);
					if ((mykey.compareToIgnoreCase(hashpred) > 0) && (mykey.compareToIgnoreCase(hashport) <= 0)) {
						back = 1;
						Log.e(TAG, "key::" + selection + "belongs with me:" + ll[i]);
						belongsto = ll[i];
						index = i;
						break;
					}

				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			if (back == 0) {
				belongsto = "5562";
				index=0;
			}

			Log.e(TAG, "index to query from" + index);



			for (int j = index; j < ll.length; j++) {
				try {

					String remoteP = ll[j];
					int remotePort = Integer.parseInt(remoteP) * 2;
					Socket starsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							remotePort);

					StringBuffer msgbuff = new StringBuffer();
					msgbuff.append("just");
					msgbuff.append(",");
					msgbuff.append("fine");
					msgbuff.append(",");
					msgbuff.append("delete");
					msgbuff.append(",");
					msgbuff.append(selection);
					msgbuff.append(",");



					String msgToSend = msgbuff.toString();


					PrintWriter pw = new PrintWriter(starsocket.getOutputStream(), true);
					pw.println(msgToSend);

				} catch (UnknownHostException e) {
					Log.e(TAG, "ClientTask UnknownHostException");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException");
				}


			}
		//	sqlDB.execSQL("DELETE FROM " + dbTable.TABLE_NAME + " WHERE key=" + "'" + selection + "'");
		}

		getContext().getContentResolver().notifyChange(uri, null);
		return rowsDeleted;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub

		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub


		Log.e(TAG, "insert come");
		Log.e(TAG,"insert key:"+values.getAsString("key"));

		String[] ll = {"5562","5556","5554","5558","5560","5562","5556"};

		int[] pl={5558,5560,5562,5556,5554};

		String[] predar={"5560","5562","5556","5554","5558"};


		String mykey;
		String hashport;
		String hashpred;
		SQLiteDatabase sqlDB = db.getWritableDatabase();

		String[] argu = {values.getAsString("key")};
		int back=0;
		//int index=0;
		String po;
		int ad;


		for (int i = 0; i < 5; i++) {
			Log.e(TAG, "insert: for"+ll[i]);
			try {
				mykey = genHash(values.getAsString("key"));
				hashport = genHash(ll[i]);
				hashpred = genHash(predar[i]);
				if ((mykey.compareToIgnoreCase(hashpred) > 0) && (mykey.compareToIgnoreCase(hashport) <= 0)) {
					back = 1;
					Log.e(TAG, "key" + values.getAsString("key") + "belongs with me:" + l.get(i).getVal());
					Log.e(TAG, "came to insertfunc");
					Log.e(TAG, "index to replicate from:" + i);
					int j=0;
					while(j<3) {
						try{

							String remoteP = ll[j+i];
							int remotePort = Integer.parseInt(remoteP) * 2;
							Socket insertsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								remotePort);

						StringBuffer msgbuff = new StringBuffer();
						msgbuff.append("just");
						msgbuff.append(",");
						msgbuff.append("fine");
						msgbuff.append(",");
						msgbuff.append("insert");
						msgbuff.append(",");
						msgbuff.append(values.getAsString("key"));
						msgbuff.append(",");
						msgbuff.append(values.getAsString("value"));

						String msgToSend = msgbuff.toString();

						PrintWriter pw = new PrintWriter(insertsocket.getOutputStream(), true);
						pw.print(msgToSend);
						pw.flush();
						j++;
						pw.close();
						insertsocket.close();
					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException");
					}

					}
					//insertfunc(values.getAsString("key"), values.getAsString("value"), l.get(i).getVal());
					break;
				}


			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		if (back == 0) {
			Log.e(TAG,"did not match anyone");
			Log.e(TAG, "came to insertfunc");
			int j=0;
			while(j<3) {
				try{

					String remoteP = ll[j];
					int remotePort = Integer.parseInt(remoteP) * 2;
					Socket insertsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							remotePort);

					StringBuffer msgbuff = new StringBuffer();
					msgbuff.append("just");
					msgbuff.append(",");
					msgbuff.append("fine");
					msgbuff.append(",");
					msgbuff.append("insert");
					msgbuff.append(",");
					msgbuff.append(values.getAsString("key"));
					msgbuff.append(",");
					msgbuff.append(values.getAsString("value"));

					String msgToSend = msgbuff.toString();

					PrintWriter pw = new PrintWriter(insertsocket.getOutputStream(), true);
					pw.print(msgToSend);
					j++;
					pw.flush();
					pw.close();
					insertsocket.close();
				} catch (UnknownHostException e) {
					Log.e(TAG, "ClientTask UnknownHostException");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException");
				}

			}

		}
		//insertfunc(values.getAsString("key"), values.getAsString("value"), l.get(0).getVal());}
		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		Log.e(TAG,"on create");
		db = new DBHelper(getContext());
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(
				Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		ports=myPort;
		meport=portStr;

		ListNode l1=new ListNode("11120","11112" ,"5562");
		ListNode l2=new ListNode("11124","11108" ,"5556");
		ListNode l3=new ListNode("11112","11116" ,"5554");
		ListNode l4=new ListNode("11108","11120" ,"5558");
		ListNode l5 = new ListNode("11116","11124","5560");

		l.add(l1);
		l.add(l2);
		l.add(l3);
		l.add(l4);
		l.add(l5);

		Log.e(TAG, "port:" + portStr);

		try {
			nodeid = genHash(portStr);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html */

			ServerSocket serverSocket = new ServerSocket(SERVER_PORT,100);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
			Log.e(TAG, "Can't create a ServerSocket");
		}

		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, portStr, JOIN, nodeid);
		return true;

	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
									 String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

		c = null;
		SQLiteDatabase data = db.getWritableDatabase();
		String[] args = {selection};
		String star = "*";
		String at = "@";
		Log.e(TAG, "query called");
		Log.e(TAG, "query for:" + selection);

		String[] ll = {"5562","5556","5554","5558","5560","5562","5556"};

		String[] predar={"5560","5562","5556","5554","5558"};

		if (selection.equalsIgnoreCase(at)) {
			Log.e(TAG, "selection at");

			c = data.query(dbTable.TABLE_NAME, // a. table
					null, // b. column names to return
					null, // c. selections "where clause"
					null, // d. selections args "where values"
					null, // e. group by
					null, // f. having
					null, // g. order by
					null); // h. limit

			return c;
		} else if (selection.equalsIgnoreCase(star)) {
			Log.e(TAG, "selection star for 5");
			MatrixCursor mc1 = new MatrixCursor(new String[]{"key", "value"});
			for (int i = 0; i < 5; i++) {
				try {
					String remoteP = l.get(i).getVal();
					int remotePort1 = Integer.parseInt(remoteP) * 2;
					Socket starsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							remotePort1);

					StringBuffer msgbuff1 = new StringBuffer();
					msgbuff1.append("just");
					msgbuff1.append(",");
					msgbuff1.append("fine");
					msgbuff1.append(",");
					msgbuff1.append("squery");
					msgbuff1.append(",");
					msgbuff1.append(ports);


					String msgforstar = msgbuff1.toString();

					PrintWriter pw1 = new PrintWriter(starsocket.getOutputStream(), true);
					pw1.println(msgforstar);

					BufferedReader in1 = new BufferedReader(new InputStreamReader(starsocket.getInputStream()));
					String smsg = in1.readLine();
					try {
						if (smsg.equalsIgnoreCase("novalue")) {
							continue;
						} else {
							Log.e(TAG, "star cursor message:" + smsg);
							String[] totarr = smsg.split("/");
							String[] keyarr = totarr[0].split(",");
							String[] valarr = totarr[1].split(",");
							for (int j = 0; j < valarr.length; j++) {
								mc1.addRow(new String[]{keyarr[j], valarr[j]});
							}

						}
					} catch (NullPointerException n) {
						continue;
					}

				} catch (UnknownHostException e) {
					Log.e(TAG, "ClientTask UnknownHostException");
				} catch (IOException e) {
					e.printStackTrace();
					Log.e(TAG, "ClientTask socket IOException");
				}
			}
			return mc1;
		} else {

			Log.e(TAG, "entered else");
			c = data.query(dbTable.TABLE_NAME,
					null,
					"key=?",
					args,
					null,
					null,
					null);
			if (c.getCount() > 0) {
				c.moveToFirst();
				Log.e(TAG, "CURSOR VALUE:" + c.getString(0) + "," + c.getString(1));
				return c;
			} else{
				//Log.e(TAG, "entered else");
				Log.e(TAG, "came to queryfunc");

			int back = 0;
			int index = 0;
			String belongsto = "nothing";


			try {
				for (int i = 0; i < 5; i++) {

					Log.e(TAG, "query: for:" + i);
					String mykey = genHash(selection);
					String hashport = genHash(ll[i]);
					String hashpred = genHash(predar[i]);
					if ((mykey.compareToIgnoreCase(hashpred) > 0) && (mykey.compareToIgnoreCase(hashport) <= 0)) {
						back = 1;
						Log.e(TAG, "key::" + selection + "belongs with me:" + ll[i]);
						belongsto = ll[i];
						index = i;
						break;
					}

				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			if (back == 0) {
				belongsto = "5562";
				index = 0;
			}

			Log.e(TAG, "index to query from" + index);

			int j=0;

			while(j<3){
				try {

					String remoteP = ll[index+j];
					int remotePort = Integer.parseInt(remoteP) * 2;
					Socket starsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							remotePort);

					StringBuilder msgbuff = new StringBuilder();
					msgbuff.append("just");
					msgbuff.append(",");
					msgbuff.append("fine");
					msgbuff.append(",");
					msgbuff.append("query");
					msgbuff.append(",");
					msgbuff.append(selection);
					msgbuff.append(",");


					String msgToSend = msgbuff.toString();


					PrintWriter pw = new PrintWriter(starsocket.getOutputStream(), true);
					pw.println(msgToSend);

					BufferedReader in = new BufferedReader(new InputStreamReader(starsocket.getInputStream()));
					String msg = in.readLine();
					j++;
					try {
						if (msg.equalsIgnoreCase("novalue")) {
							continue;
						} else {
							Log.e(TAG, "cursor message:" + msg);
							String[] strarr = msg.split(",");
							MatrixCursor mc = new MatrixCursor(new String[]{"key", "value"});
							mc.addRow(new String[]{strarr[0], strarr[1]});
							return mc;
							//break;

						}
					} catch (NullPointerException n) {
						Log.e(TAG, "query: caught null");
						continue;
					}

				} catch (UnknownHostException e) {
					Log.e(TAG, "ClientTask UnknownHostException");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException");
				}


			}

		}
			}

		return null;
	}


	class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			ServerSocket serverSocket = sockets[0];
			Log.e(TAG, "ServerTask created at" + sockets[0].toString());

			try {
				while (true) {
					Socket socket = serverSocket.accept();
					Log.e(TAG, "ACCEPTED");
					try {
						BufferedReader in = new BufferedReader(
								new InputStreamReader(socket.getInputStream()));
						//while (true) {
						String input = in.readLine();

						String strRecieved = input.trim();
						String[] strsplit = strRecieved.split(",");
						Log.e(TAG, "server recieved:" + strRecieved);
						if((strsplit[2].equalsIgnoreCase("delete"))){
							SQLiteDatabase sqlDB = db.getWritableDatabase();
							String selection=strsplit[3];
							PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
							sqlDB.execSQL("DELETE FROM " + dbTable.TABLE_NAME + " WHERE key=" + "'" + selection + "'");

						}
						if (strsplit[2].equalsIgnoreCase("cursor")) {
							trial = strsplit[1];
						} else if (strsplit[2].equalsIgnoreCase("query")) {
							Log.e(TAG, "query called from server for:" + strsplit[3]);
							String key = strsplit[3];
							SQLiteDatabase data = db.getWritableDatabase();

							String selection = "key=" + "'" + key + "'";
							Cursor c1 = data.query(dbTable.TABLE_NAME,
									null,
									selection,
									null,
									null,
									null,
									null);
							PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
							if (c1 != null && c1.getCount() > 0) {

								c1.moveToFirst();
								StringBuffer msgbuff = new StringBuffer();

								while (!c1.isAfterLast()) {
									String k = c1.getString(0);
									msgbuff.append(k);
									msgbuff.append(",");
									String value = c1.getString(1);
									msgbuff.append(value);
									c1.moveToNext();
								}

								String msgTosend = msgbuff.toString();
								Log.e(TAG, "msgtosend in query:" + msgTosend);
								pw.println(msgTosend);

							} else {

								String msgTosend = "novalue";
								pw.println(msgTosend);

							}

						} else if (strsplit[2].equalsIgnoreCase("squery")) {

							Log.e(TAG, "squery called server");
							SQLiteDatabase data = db.getWritableDatabase();
							Cursor c2 = data.query(dbTable.TABLE_NAME, // a. table
									null, // b. column names to return
									null, // c. selections "where clause"
									null, // d. selections args "where values"
									null, // e. group by
									null, // f. having
									null, // g. order by
									null); // h. limit

							PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
							if (c2 != null && c2.getCount() > 0) {

								c2.moveToFirst();
								StringBuffer keybuff = new StringBuffer();
								StringBuffer valbuff = new StringBuffer();

								while (!c2.isAfterLast()) {
									String k = c2.getString(0);
									keybuff.append(k);
									keybuff.append(",");
									String value = c2.getString(1);
									valbuff.append(value);
									valbuff.append(",");
									c2.moveToNext();
								}

								String keystr = keybuff.toString();
								String valstr = valbuff.toString();

								StringBuffer totbuff = new StringBuffer();
								totbuff.append(keystr);
								totbuff.append("/");
								totbuff.append(valstr);

								String msgTosend = totbuff.toString();
								Log.e(TAG, "msgtosend in query:" + msgTosend);
								pw.println(msgTosend);

							} else {

								String msgTosend = "novalue";
								pw.println(msgTosend);

							}


						} else if (strsplit[2].equalsIgnoreCase("insert")) {
							Log.e(TAG, "insert called server");
							Log.e(TAG, "key:" + strsplit[3]);
							Log.e(TAG, "value:" + strsplit[4]);
							SQLiteDatabase sqlDB = db.getWritableDatabase();
							String[] argu = {strsplit[3]};
							ContentValues cv = new ContentValues();
							cv.put(KEY_FIELD, strsplit[3]);
							cv.put(VALUE_FIELD, strsplit[4]);
							Cursor c1 = sqlDB.query(dbTable.TABLE_NAME, null, "key=?", argu, null, null, null);
							if (c1.getCount() < 1) {
								sqlDB.insert(dbTable.TABLE_NAME, null, cv);
							} else {
								sqlDB.update(dbTable.TABLE_NAME, cv, "key=?", argu);
							}


						} else if (strsplit[2].equalsIgnoreCase("join")) {

							long start=System.currentTimeMillis();
								Log.e(TAG, "now will start recovery");
								startrecovery();
							long end=System.currentTimeMillis()-start;
							Log.e(TAG, "time to recover::" + end);

						} else if(strsplit[2].equalsIgnoreCase("getback")){

							Log.e(TAG, "reached server getback from::"+strsplit[3]);
							SQLiteDatabase data = db.getWritableDatabase();

							String query="SELECT * FROM " + dbTable.TABLE_NAME;
							Cursor c1=data.rawQuery(query,null);
							int count=c1.getCount();
							Log.e(TAG,"server getback count::"+count);
							PrintWriter pw=new PrintWriter(socket.getOutputStream(),true);

							if(c1!=null && count>0){

								c1.moveToFirst();
								StringBuilder keybuff=new StringBuilder();
								StringBuilder valbuff=new StringBuilder();

								while (!c1.isAfterLast()) {
									keybuff.append(c1.getString(0));
									keybuff.append(",");
									valbuff.append(c1.getString(1));
									valbuff.append(",");
									c1.moveToNext();
								}

								String keystr=keybuff.toString();
								String valstr=valbuff.toString();

								StringBuilder msgbuff=new StringBuilder();
								msgbuff.append(keystr);
								msgbuff.append("/");
								msgbuff.append(valstr);
								String msgTosend=msgbuff.toString();
								Log.e(TAG,"msgtosend while getting back:"+msgTosend);
								pw.println(msgTosend);

							}
							else{
								String msgTosend="goturbackbuddy";
								pw.println(msgTosend);

							}
						}

					} catch (IOException e) {
						Log.e(TAG, "Code to receive msg failed");
					}  finally {
						try {
							socket.close();
						} catch (IOException e) {
							Log.e(TAG, "Couldn't close a socket, what's going on?");
						}
					}
				}
			} catch (IOException e) {
				Log.e(TAG, "Code to receive msg failed after publish");
			}
            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
			return null;
		}

	}
	public synchronized void startrecovery() {

		
		long start=System.currentTimeMillis();

		int[] ll = {5562,5556,5554,5558,5560};

		int[] pl={5558,5560,5562,5556,5554};

		String[] predar={"5560","5562","5556","5554","5558"};

		int pred=0;
		int succ=0;
		int prepred=0;
		int index = 0;
		for (int i = 0; i < ll.length; i++) {

			if (ll[i]==Integer.parseInt(meport)) {
				index = i;
				break;
			}
		}

		Log.e(TAG, "startrecovery: index ::" + index);
		if (index - 1 >= 0 && index + 1 < ll.length) {
			pred = ll[index - 1];
			succ = ll[index + 1];
			prepred = pl[index];
		} else if (index - 1 < 0) {
			pred = ll[4];
			succ = ll[1];
			prepred = ll[3];

		} else if (index + 1 >= 5) {
			pred = ll[3];
			succ = ll[0];
			prepred = ll[2];

		}

		Log.e(TAG, "startrecovery: pred::" + pred);
		Log.e(TAG, "startrecovery: succ::" + succ);
		Log.e(TAG, "startrecovery: prepred::" + prepred);

		ArrayList<Integer> reco = new ArrayList<Integer>();

		reco.add(succ);
		reco.add(pred);

		long id = 0;
		SQLiteDatabase data = db.getWritableDatabase();


		for (int k = 0; k < reco.size(); k++) {

			int remotePort = reco.get(k) * 2;
			try {
				Socket sendsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						remotePort);


				Log.e(TAG, "getting back from " + remotePort);
                /*
                 * TODO: Fill in your client code that sends out a message.
                 */

				String msgToSend = "just,fine,getback,"+meport+",";
				
				PrintWriter pw = new PrintWriter(sendsocket.getOutputStream(), true);
				pw.println(msgToSend);


				BufferedReader in = new BufferedReader(new InputStreamReader(sendsocket.getInputStream()));
				String msg = "msgs";

				try {
					msg = in.readLine();
				} catch (NullPointerException n) {
					Log.e(TAG, "startrecovery: null caught in recovery");
					break;
				}

				Log.e(TAG,  "replied back::" + msg);

				if(msg==null || msg.equalsIgnoreCase("goturbackbuddy")){
					Log.e(TAG, "startrecovery: gotu" );
					break;
				}

				String mykey;
				String hashport;
				String hashpred;


				if (msg!=null && !(msg.equalsIgnoreCase("goturbackbuddy"))){

				String[] msgcol = msg.split("/");
					String[] keycol = msgcol[0].split(",");
				String[] valcol = msgcol[1].split(",");

				Log.e(TAG, "keycol size:" + keycol.length);
				Log.e(TAG, "valcol size" + valcol.length);


				if (k == 1) {

					Log.e(TAG, "startrecovery: k=1");

					try {
						for (int j = 0; j < keycol.length; j++) {
							mykey = genHash(keycol[j]);
							int back = 0;
							int belongsto=0;
							for (int i = 0; i < ll.length; i++) {
								hashport = genHash(Integer.toString(ll[i]));
								hashpred = genHash(predar[i]);
								if ((mykey.compareToIgnoreCase(hashpred) > 0) && (mykey.compareToIgnoreCase(hashport) <= 0)) {
									back = 1;
									Log.e(TAG, "key::" + keycol[j] +"val::"+valcol[j] + "belongs with me:" + ll[i]);
									belongsto = ll[i];
									break;
								}

							}
							if (belongsto==pred || belongsto==prepred) {

								Log.e(TAG, "inserted in my pred so inserted in me" + keycol[j]);
								ContentValues cv = new ContentValues();
								cv.put(KEY_FIELD, keycol[j]);
								cv.put(VALUE_FIELD, valcol[j]);
								String[] argu = {keycol[j]};
								Cursor c1 = data.query(dbTable.TABLE_NAME, null, "key=?", argu, null, null, null);
								if (c1.getCount() < 1) {
									data.insert(dbTable.TABLE_NAME, null, cv);
								} else {
									data.update(dbTable.TABLE_NAME, cv, "key=?", argu);
								}

							} else if (back == 0) {
								if (meport.equalsIgnoreCase("5562") || pred==5562 || prepred==5562) {
									Log.e(TAG, "taking unwanted pred ");
									ContentValues cv = new ContentValues();
									cv.put(KEY_FIELD, keycol[j]);
									cv.put(VALUE_FIELD, valcol[j]);
									String[] argu = {keycol[j]};
									Cursor c1 = data.query(dbTable.TABLE_NAME, null, "key=?", argu, null, null, null);
									if (c1.getCount() < 1) {
										data.insert(dbTable.TABLE_NAME, null, cv);
									} else {
										data.update(dbTable.TABLE_NAME, cv, "key=?", argu);
									}
								}
							}


						}
					} catch (NoSuchAlgorithmException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else if (k == 0) {
					Log.e(TAG, "startrecovery: k=0");

					Log.e(TAG, "added in pred list::succkey:" + keycol.length + "::succval::" + valcol.length);
					try {
						for (int j = 0; j < keycol.length; j++) {
							mykey = genHash(keycol[j]);
							int back = 0;
							int belongsto = 0;
							for (int i = 0; i < ll.length; i++) {
								hashport = genHash(Integer.toString(ll[i]));
								hashpred = genHash(predar[i]);
								if ((mykey.compareToIgnoreCase(hashpred) > 0) && (mykey.compareToIgnoreCase(hashport) <= 0)) {
									back = 1;
									Log.e(TAG, "key::" + keycol[j]+ "val::"+valcol[j]  + "belongs with me:" + ll[i]);
									belongsto = ll[i];
									break;
								}

							}
							if (belongsto==Integer.parseInt(meport)) {

								Log.e(TAG, "mine so inserted in me" + keycol[j]);
								ContentValues cv = new ContentValues();
								cv.put(KEY_FIELD, keycol[j]);
								cv.put(VALUE_FIELD, valcol[j]);

								String[] argu = {keycol[j]};
								Cursor c1 = data.query(dbTable.TABLE_NAME, null, "key=?", argu, null, null, null);
								if (c1.getCount() < 1) {
									data.insert(dbTable.TABLE_NAME, null, cv);
								} else {
									data.update(dbTable.TABLE_NAME, cv, "key=?", argu);
								}
							} else if (back == 0) {
								if (meport.equalsIgnoreCase("5562")) {
									Log.e(TAG, "taking unwanted pred ");
									ContentValues cv = new ContentValues();
									cv.put(KEY_FIELD, keycol[j]);
									cv.put(VALUE_FIELD, valcol[j]);
									String[] argu = {keycol[j]};
									Cursor c1 = data.query(dbTable.TABLE_NAME, null, "key=?", argu, null, null, null);
									if (c1.getCount() < 1) {
										data.insert(dbTable.TABLE_NAME, null, cv);
									} else {
										data.update(dbTable.TABLE_NAME, cv, "key=?", argu);
									}
								}
							}


						}
					} catch (NoSuchAlgorithmException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}

			}
			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");
			}
		}

		Log.e(TAG,"recovery ended");
		return;
	}

	class ClientTask extends AsyncTask<String, Void, Void> {


		@Override
		protected Void doInBackground(String... msgs) {

			Log.e(TAG, "client task created for" + msgs[0]);


			//myPort, portStr, JOIN, nodeid

			int remotePort = Integer.parseInt(msgs[0]);

			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						remotePort);
                /*
                 * TODO: Fill in your client code that sends out a message.*/

				String msgToSend = msgs[0]+","+msgs[1]+","+msgs[2];
				Log.e(TAG, "msg in client:" + msgToSend);

				PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
				pw.print(msgToSend);
				pw.flush();

				socket.close();
			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");
			}


			return null;
		}
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

class ListNode {
	public String pred ;
	public String succ;
	public String node;


	public ListNode(String p, String s,String n) {
		this.pred =p;
		this.succ =s;
		this.node=n;
	}

	public String getPred() {
		return pred;
	}

	public String getSucc(){
		return succ;
	}

	public String getVal(){
		return node;
	}

	public void setPred(String p){
		pred=p;
	}

	public void setSucc(String s){
		succ=s;
	}
}