import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Vector;

public class Main extends Thread  {

  static Vector<Socket> v = new Vector<>();
  static int size = 0;

  public void readCommand(InputStream in, Vector<String> command) throws IOException {
    int x = in.read() - (int)'0';
    // System.out.println("Size of the command: "+x);
    while(x-->0) {
      int skip = 5;
      while(skip-->0) {
        // System.out.println("Skip");
        in.read();
      }
      int y = in.read() - (int)'0';
      // System.out.println("Lenght of the next element in the command vector: "+y);
      skip = 4;
      while(skip-->0) {
        // System.out.println("Skip");
        in.read();
      }
      String s="";
      while(y-->0) {
        s=s+(char)in.read();
      }
      // System.out.println(s);
      command.addElement(s);
    }
    int skip = 4;
    while(skip-->0) {
      // System.out.println("Skip");
      in.read();
    }
  }

  public String encodeRESP(String s) {
    int size = s.length();
    String ret = "$" + size + "\\r\\n" + s + "\\r\\n";
    return ret;
  }

  public void run() {
    Socket s = getSocket();
    try (InputStream in = s.getInputStream()) {
      OutputStream out = (s.getOutputStream());
      while(true) {
        // in.read();
        // in.read();
        char ch = (char)in.read();
        System.out.println(ch);
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
        }
        else {
          System.out.println("This is the incorrect Command");
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
    int port = 6379;
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
