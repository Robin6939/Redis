import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Vector;

public class Main extends Thread  {

  static Vector<Socket> v = new Vector<>();
  static int size = 0;

  public void run() {
    Socket s = getSocket();
    try (InputStream in = s.getInputStream()) {
      OutputStream out = s.getOutputStream();
      while(true) {
        if(in.read()==42) {
          out.write("+PONG\r\n".getBytes());
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
      // System.out.println("Tyring to connect");
      // clientSocket = serverSocket.accept();
      // System.out.println("Connection established");
      // InputStream inputStream = (clientSocket.getInputStream());
      // OutputStream outputStream = clientSocket.getOutputStream();
      // while(true) {
      //   int line = inputStream.read();
      //   System.out.println("Data received : " + line);
      //   if(line==42)
      //     outputStream.write("+PONG\r\n".getBytes());
      // }

      while(true) {
        clientSocket = serverSocket.accept();
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
