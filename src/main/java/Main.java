import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main extends Thread  {

  public static final String ANSI_BLACK_BACKGROUND = "\u001B[40m";
  public static final String ANSI_RED_BACKGROUND = "\u001B[41m";
  public static final String ANSI_GREEN_BACKGROUND = "\u001B[42m";
  public static final String ANSI_YELLOW_BACKGROUND = "\u001B[43m";
  public static final String ANSI_BLUE_BACKGROUND = "\u001B[44m";
  public static final String ANSI_PURPLE_BACKGROUND = "\u001B[45m";
  public static final String ANSI_CYAN_BACKGROUND = "\u001B[46m";
  public static final String ANSI_WHITE_BACKGROUND = "\u001B[47m";



  static Vector<Socket> v = new Vector<>();
  static Boolean master = true;
  static int masterPort = -1;
  static String masterHost = "";
  static String replica = "";
  static int port = 0;
  static ConcurrentHashMap<Socket, Boolean> replicaSockets = new ConcurrentHashMap<>();
  static Socket masterSocket;
  static HashMap<String, String> map = new HashMap<>();
  static int countBytes = 0;
  static int receivedACKS = 0;
  static int countClient = 0;
  static int countInSyncReplicas = 0;
  static Vector<String> getAckCommand = new Vector<>();
  

  public synchronized void readCommand(InputStream in, Vector<String> command) throws IOException {
    //System.out.println(currentThread().getName()+ " trying to read commands");
    int x = 0;
    int tempcount = 0;
    tempcount++;
    char ch = (char)in.read();
    tempcount++;
    while(ch!='\r') {
      int x1 = (int)ch - (int)'0';
      x = x*10 + x1;
      ch = (char)in.read();
      tempcount++;
    }
    while(x-->0) {
      int skip = 2;
      while(skip-->0) {
        in.read();
        tempcount++;
      } 
      char ch1 = (char)in.read();
      tempcount++;
      int y = 0;
      while(ch1!='\r') {
        int y1 = (int)ch1 - (int)'0';
        y = y*10 + y1;
        ch1 = (char)in.read();
        tempcount++;
      }
      skip = 1;
      while(skip-->0) {
        in.read();
        tempcount++;
      }
      String s="";
      while(y-->0) {
        s=s+(char)in.read();
        tempcount++;
      }
      in.read();
      tempcount++;
      command.addElement(s);
    }
    int skip = 1;
    while(skip-->0) {
      in.read();
      tempcount++;
    }
    if(command.size()>1) {
      if(command.get(0).equalsIgnoreCase("SET")) {
        addToCountBytes(tempcount);
        //System.out.println(ANSI_BLUE_BACKGROUND+"It is set command which is sent to replica and hence increase countbytes by: "+tempcount+" and to: "+countBytes);
        
      }
    }
    //System.out.println("The last command was: "+command);
  }

  public static synchronized void addToCountBytes(int addValue) {
    countBytes += addValue;
  }

  public static String encodeRESPArr(String[] arr) {
    int n = arr.length;
    String send = "*" + n + "\r\n";
    for(String s:arr) {
      send += encodeRESP(s);
    }
    return send;
  }

  public static String encodeRESP(String s) {
    int size = s.length();
    String ret = "$" + size + "\r\n" + s + "\r\n";
    return ret;
  }

  public static String convertToBulkString(String s) {
    int n = s.length();
    return "$"+n+"\r\n"+s;
  }

  private static void sendToSocket(Socket s, String toSend) throws IOException {
    OutputStream os = new BufferedOutputStream(s.getOutputStream());
    os.write(toSend.getBytes());
    os.flush();  // Ensure data is sent immediately
  }

  public static synchronized void sendToReplica(Vector<String> command) throws IOException {
    String toSend1 = "";
    String arr[] = new String[command.size()];
    for(int i = 0;i<command.size();i++) {
      arr[i] = command.get(i);
    }
    toSend1 = encodeRESPArr(arr);
    final String toSend = toSend1;
    
    
    for(Socket s:replicaSockets.keySet()) {
      OutputStream os = s.getOutputStream();
      os.write(toSend.getBytes()); 
      os.flush();
    }
    // ExecutorService executor = Executors.newFixedThreadPool(Math.min(replicaSockets.size(), 10)); // Adjust pool size as needed
    // Sending data to each replica using parallel threads
    // for (Socket s : replicaSockets.keySet()) {
    //     executor.submit(() -> {
    //         try {
    //             sendToSocket(s, toSend);
    //         } catch (IOException e) {
    //             System.err.println("Error sending data to replica: " + s + " Error: " + e.getMessage());
    //         }
    //     });
    // }
    // Shut down the executor service
    // executor.shutdown();  



    if(arr[0].equalsIgnoreCase("SET") == false) {
      int tempadd = 0;
      for(int i = 0;i<toSend.length();i++) {
        char ch = toSend.charAt(i);
        if(ch=='\\')
          continue;
        else
          tempadd++;
      }
      addToCountBytes(tempadd);
    }
  }

  public static String toRESPInt(int x) {
    return ":+" + x + "\r\n";
  }

  public static synchronized void increaseInSyncReplicas() {
    countInSyncReplicas++;
  }

  public static synchronized void decreaseInSyncReplicas() {
    countInSyncReplicas--;
  }

  public static synchronized int getCountInSyncReplicas() {
    return countInSyncReplicas;
  }


  public void run() {
    Socket s = getSocket();
    try (InputStream in = s.getInputStream()) {
      OutputStream out = (s.getOutputStream());
      while(true) {
        char ch = (char)in.read();
        if(ch=='*') {
          Vector<String> command = new Vector<>();
          readCommand(in, command);
          //System.out.println("Received the following command: " + command + " from socket: " + currentThread().getName());
          if(command.get(0).equalsIgnoreCase("ECHO")) {
            //System.out.println("It is an ECHO command");
            String send = encodeRESP(command.get(1));
            out.write(send.getBytes());
            out.flush();
          }
          if(command.get(0).equalsIgnoreCase("PING")) {
            //System.out.println("It is an PING command");
            // sendToReplica(command);
            if(s.equals(masterSocket)==false) {
              out.write("+PONG\r\n".getBytes());
              out.flush();
            }
          }
          if(command.get(0).equalsIgnoreCase("SET")) {
            //System.out.println("Will try to send to all the replicas");
            sendToReplica(command);
            //System.out.println("It is a SET command");
            map.put(command.get(1), command.get(2));
            if(s.equals(masterSocket)==false) {// if request is not coming from the master socket but the client
              out.write(encodeRESP("OK").getBytes());
              out.flush();
              countInSyncReplicas = 0;
              for(Socket soc:replicaSockets.keySet()) {
                replicaSockets.put(soc, false);
                // decreaseInSyncReplicas();
              }
              
              sendToReplica(getAckCommand);
              // try {
              //   Thread.sleep(100);
              // } catch (InterruptedException e) {
              //   e.printStackTrace();
              // }
            }
            else { //if request is coming from the master socket
              //System.out.println("Received a command which was propagated by master which is: "+ command);
            }
            if(command.size()>3) {
              int ms = Integer.parseInt(command.get(4));
              Thread t = new Thread() {
                public void run() {
                  try {
                    Thread.sleep(ms);
                    map.remove(command.get(1));
                  } catch (InterruptedException e) {
                    //System.out.println(e);
                  }
                }
              };
              t.start();
            }
          }
          if(command.get(0).equalsIgnoreCase("GET")) {
            //System.out.println("It is a GET command");
            //System.out.println("Current map of data is: "+ map);
            //System.out.println("c1");
            String send = map.get(command.get(1));
            if(send==null || send.length()==0) {
              //System.out.println("Didn't find in the current map");
              out.write("$-1\r\n".getBytes());
              out.flush();
            }
            else {
              //System.out.println("Sending the following: "+send);
              out.write(encodeRESP(send).getBytes());
              out.flush();
            }
          }
          if(command.get(0).equalsIgnoreCase("INFO")) {
            if(command.get(1).equalsIgnoreCase("REPLICATION")) {
              if(master) {
                String send = "role:master\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0";
                String toSend = encodeRESP(send);
                out.write(toSend.getBytes());
                out.flush();
              }
              else {
                String send = "role:slave\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0";
                String toSend = encodeRESP(send);
                out.write(toSend.getBytes());
                out.flush();
              }
            }
          }
          if(command.get(0).equalsIgnoreCase("REPLCONF")) {
            if(command.get(1).equalsIgnoreCase("listening-port")) {
              //System.out.println("New replica added to the current master from port: " + s.getPort());
              if(countBytes==0) {
                replicaSockets.put(s,true);
                increaseInSyncReplicas();
              }
              else {
                replicaSockets.put(s,false);
                decreaseInSyncReplicas();
              }
              out.write("+OK\r\n".getBytes());
              out.flush();
            }
            else if(command.get(1).equalsIgnoreCase("capa")) {
              out.write("+OK\r\n".getBytes());
              out.flush();
            }
            else if(command.get(1).equalsIgnoreCase("GETACK")) {
              String count = "" + countBytes;
              String toSend[] = {"REPLCONF", "ACK", count};
              //System.out.println("Sending this to master: "+encodeRESPArr(toSend));
              out.write(encodeRESPArr(toSend).getBytes());
              out.flush();
            }
            else if(command.get(1).equalsIgnoreCase("ACK")) {
              //System.out.println("This is what replica sent as an ACK: "+command.toString());
              //System.out.println(countBytes);
              if(command.size()>2 && Integer.parseInt(command.get(2))==(countBytes-37)) {
                replicaSockets.put(s, true);
                increaseInSyncReplicas();
                //System.out.println("Ack received which is true and in sync replicas now are: "+countInSyncReplicas);
              }
            }
          }
          if(command.get(0).equalsIgnoreCase("PSYNC")) {
            String toSend = "+FULLRESYNC " + "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb" + " 0\r\n";
            out.write(toSend.getBytes());
            out.flush();
            byte[] contents = HexFormat.of().parseHex(
                "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2");
            out.write(("$"+contents.length+"\r\n").getBytes());
            out.flush();
            out.write(contents);
            out.flush();
          }
          if(command.get(0).equalsIgnoreCase("WAIT")) {
            //System.out.println("This is the WAIT command");
            int timeout = Integer.parseInt(command.get(1));
            try {
              Thread.sleep(timeout);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            String toSend = toRESPInt(getCountInSyncReplicas());
            out.write(toSend.getBytes());
            out.flush();
          }
        }
      }
    } catch (IOException e) {
      //System.out.println(e);
    }
  }

  public synchronized static void addSocket(Socket cs) {
    v.addElement(cs);
  }

  public synchronized static Socket getSocket() {
    while(v.isEmpty());
    Socket cs = v.get(v.size()-1);
    v.remove(v.size()-1);
    return cs;
  }


  public static void main(String[] args) throws UnknownHostException, IOException{
    //System.out.println("Logs from your program will appear here!");
    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    port = args.length==0?6379:Integer.parseInt(args[1]);
    countBytes = 0;
    getAckCommand.add("REPLCONF");
    getAckCommand.add("GETACK");
    getAckCommand.add("*");

    if(args.length>2 && args[2].equals("--replicaof")) { //this one is a slave and it will do handshake under this if
      master = false;
      //System.out.println(args[3]);
      masterHost = args[3].substring(0, args[3].length()-5);
      masterPort = Integer.parseInt(args[3].substring(args[3].length()-4));
      masterSocket = new Socket(masterHost, masterPort);
      OutputStream outMaster = (masterSocket.getOutputStream());
      InputStream inMaster = masterSocket.getInputStream();
      String[] arr = {"PING"};
      //System.out.println(encodeRESPArr(arr));
      outMaster.write(encodeRESPArr(arr).getBytes());
      outMaster.flush();
      int skip = 7;
      while(skip-->0) { //skip ok response
        inMaster.read();
      }
      String[] repl1 = {"REPLCONF", "listening-port", port+""};
      outMaster.write(encodeRESPArr(repl1).getBytes());
      outMaster.flush();
      skip = 5;
      while(skip-->0) { //skip ok response
        inMaster.read();
      }
      String[] repl2 = {"REPLCONF", "capa", "psync2"};
      outMaster.write(encodeRESPArr(repl2).getBytes());
      outMaster.flush();
      skip = 5;
      while(skip-->0) { //skip ok response
        inMaster.read();
      }
      String[] psync = {"PSYNC", "?", "-1"};
      outMaster.write(encodeRESPArr(psync).getBytes());
      outMaster.flush();
      skip=12;
      while(skip-->0){
        inMaster.read();
      }
      int repcount = 40;
      while(repcount-->0) {
        char ch = (char)inMaster.read();
        replica = replica+ch;
      }
      //System.out.println("replica id is: "+replica);
      skip = 4;
      while(skip-->0) {
        inMaster.read();
      }
      //System.out.println("This is a replica whose master is at port: " + masterPort);
      addSocket(masterSocket);
      Main t = new Main();
      t.start();
      //System.out.println("------------------------------------------");
      try {
        t.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);
      while(true) {
        clientSocket = serverSocket.accept();
        //System.out.println("Connection established");
        addSocket(clientSocket);
        Main t = new Main();
        countClient++;
        String name = "Client" + countClient;
        t.setName(name);
        t.start();
      }
    } catch (IOException e) {
      //System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) { 
        //System.out.println("IOException: " + e.getMessage());
      }
    }
    masterSocket.close();
  }
}
