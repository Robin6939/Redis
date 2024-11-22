import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Vector;

public class Main extends Thread  {

  static Vector<Socket> v = new Vector<>();
  static int size = 0;

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

  public String encodeRESP(String s) {
    int size = s.length();
    String ret = "$" + size + "\r\n" + s + "\r\n";
    return ret;
  }

  public void run() {
    Socket s = getSocket();
    HashMap<String, String> map = new HashMap<>();
    try (InputStream in = s.getInputStream()) {
      OutputStream out = (s.getOutputStream());
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
              out.write(encodeRESP("role:master").getBytes());
            }
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


  public static void main(String[] args){
    System.out.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = args.length==0?6379:Integer.parseInt(args[1]);
    try {
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
