import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Vector;

public class Main extends Thread  {

  static Vector<Socket> v = new Vector<>();
  static int size = 0;
  static Boolean master = true;
  static int masterPort = -1;
  static String masterHost = "";
  static String replica = "";
  static int port = 0;

  public void readCommand(InputStream in, Vector<String> command) throws IOException {
    int x = 0;
    char ch = (char)in.read();
    while(ch!='\r') {
      int x1 = (int)ch - (int)'0';
      x = x*10 + x1;
      ch = (char)in.read();
    }
    while(x-->0) {
      int skip = 2;
      while(skip-->0) {
        in.read();
      } 
      char ch1 = (char)in.read();
      int y = 0;
      while(ch1!='\r') {
        int y1 = (int)ch1 - (int)'0';
        y = y*10 + y1;
        ch1 = (char)in.read();
      }
      skip = 1;
      while(skip-->0) {
        in.read();
      }
      String s="";
      while(y-->0) {
        s=s+(char)in.read();
      }
      in.read();
      command.addElement(s);
    }
    int skip = 1;
    while(skip-->0) {
      in.read();
    }
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

  public void run() {
    Socket s = getSocket();
    HashMap<String, String> map = new HashMap<>();
    try (InputStream in = s.getInputStream()) {
      OutputStream out = (s.getOutputStream());
      // System.out.println(master);
      while(true) {
        char ch = (char)in.read();
        if(ch=='*') {
          Vector<String> command = new Vector<>();
          readCommand(in, command);

          System.out.println(command);
          if(command.get(0).equalsIgnoreCase("ECHO")) {
            System.out.println("It is an ECHO command");
            String send = encodeRESP(command.get(1));
            out.write(send.getBytes());
          }
          if(command.get(0).equalsIgnoreCase("PING")) {
            System.out.println("It is an PING command");
            out.write("+PONG\r\n".getBytes());
          }
          if(command.get(0).equalsIgnoreCase("SET")) {
            System.out.println("It is a SET command");
            map.put(command.get(1), command.get(2));
            out.write(encodeRESP("OK").getBytes());
            if(command.size()>3) {
              int ms = Integer.parseInt(command.get(4));
              Thread t = new Thread() {
                public void run() {
                  try {
                    Thread.sleep(ms);
                    map.remove(command.get(1));
                  } catch (InterruptedException e) {
                    System.out.println(e);
                  }
                }
              };
              t.start();
            }
          }
          if(command.get(0).equalsIgnoreCase("GET")) {
            System.out.println("It is a GET command");
            String send = map.get(command.get(1));
            if(send==null) {
              out.write("$-1\r\n".getBytes());
            }
            out.write(encodeRESP(send).getBytes());
          }
          if(command.get(0).equalsIgnoreCase("INFO")) {
            if(command.get(1).equalsIgnoreCase("REPLICATION")) {
              if(master) {
                String send = "role:master\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0";
                String toSend = encodeRESP(send);
                out.write(toSend.getBytes());
              }
              else {
                String send = "role:slave\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0";
                String toSend = encodeRESP(send);
                out.write(toSend.getBytes());
              }
            }
          }
          if(command.get(0).equalsIgnoreCase("REPLCONF")) {
            out.write("+OK\r\n".getBytes());
          }
          if(command.get(0).equalsIgnoreCase("PSYNC")) {
            String toSend = "+FULLRESYNC " + "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb" + " 0\r\n";
            out.write(toSend.getBytes());
          }
        }
      }
    } catch (IOException e) {
      System.out.println(e);
    }
  }

  public synchronized static void addSocket(Socket cs) {
    v.addElement(cs);
    size++;
  }

  public synchronized static Socket getSocket() {
    Socket cs = v.lastElement();
    v.remove(size-1);
    size--;
    return cs;
  }


  public static void main(String[] args) throws UnknownHostException, IOException{
    System.out.println("Logs from your program will appear here!");
    
    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    port = args.length==0?6379:Integer.parseInt(args[1]);


    if(args.length>2 && args[2].equals("--replicaof")) { //this one is a slave
      master = false;
      System.out.println(args[3]);
      masterHost = args[3].substring(0, args[3].length()-5);
      masterPort = Integer.parseInt(args[3].substring(args[3].length()-4));
      Socket masterSocket = new Socket(masterHost, masterPort);
      OutputStream outMaster = (masterSocket.getOutputStream());
      InputStream inMaster = masterSocket.getInputStream();
      String[] arr = {"PING"};
      System.out.println(encodeRESPArr(arr));
      outMaster.write(encodeRESPArr(arr).getBytes());
      int skip = 7;
      while(skip-->0) { //skip ok response
        inMaster.read();
      }
      String[] repl1 = {"REPLCONF", "listening-port", port+""};
      outMaster.write(encodeRESPArr(repl1).getBytes());
      skip = 5;
      while(skip-->0) { //skip ok response
        inMaster.read();
      }
      String[] repl2 = {"REPLCONF", "capa", "psync2"};
      outMaster.write(encodeRESPArr(repl2).getBytes());
      skip = 5;
      while(skip-->0) { //skip ok response
        inMaster.read();
      }
      String[] psync = {"PSYNC", "?", "-1"};
      outMaster.write(encodeRESPArr(psync).getBytes());
      skip=12;
      while(skip-->0){
        inMaster.read();
      }
      int repcount = 40;
      while(repcount-->0) {
        char ch = (char)inMaster.read();
        replica = replica+ch;
      }
      System.out.println("replica id is: "+replica);
      skip = 4;
      while(skip-->0) {
        inMaster.read();
      }
      masterSocket.close();
    }




    try {
      System.out.println("checkpoint 1");
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);
      while(true) {
        clientSocket = serverSocket.accept();
        System.out.println("Connection established");
        addSocket(clientSocket);
        Main t = new Main();
        t.start();
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) { 
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }
}
