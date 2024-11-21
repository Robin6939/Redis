import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
    System.out.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = 6379;
    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);
      System.out.println("Tyring to connect");
      clientSocket = serverSocket.accept();
      System.out.println("Connection established");
      InputStream inputStream = (clientSocket.getInputStream());
      OutputStream outputStream = clientSocket.getOutputStream();
      // DataOutputStream os = new DataOutputStream(outputStream);
      while(true) {
        int line = inputStream.read();
        System.out.println("Data received : " + line);
        if(line==42)
          outputStream.write("+PONG\r\n".getBytes());
        // os.writeUTF("+PONG\r\n");
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
