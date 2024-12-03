import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    static int port;
    static ExecutorService threadPool = Executors.newFixedThreadPool(10);
    static Boolean isMaster;
    static Socket masterSocket;
    static String replicationId = "";
    static AtomicInteger countBytes = new AtomicInteger(0);
    static ConcurrentHashMap<String, String> dataStore = new ConcurrentHashMap<>();
    static ConcurrentHashMap<Socket, Boolean> replicaSockets = new ConcurrentHashMap<>();
    static AtomicInteger inSyncReplicaCount = new AtomicInteger(0);





    public static void skip(InputStream is, int count) throws IOException {
        while(count-->0) {
            is.read();
        }
    }

    public static String toRESPInt(int x) {
        return ":+" + x + "\r\n";
    }

    public static String encodeRESP(String s) {
        int size = s.length();
        String ret = "$" + size + "\r\n" + s + "\r\n";
        return ret;
    }

    public static String encodeRESPArr(String[] arr) {
        int n = arr.length;
        String send = "*" + n + "\r\n";
        for(String s:arr) {
            send += encodeRESP(s);
        }
        return send;
    }

    public static void send(String[] command, OutputStream os) throws IOException {
        os.write(encodeRESPArr(command).getBytes());
    }

    public static void send(int integer, OutputStream os) throws IOException {
        os.write(toRESPInt(integer).getBytes());
    }

    public static void send(String word, OutputStream os) throws IOException {
        os.write(encodeRESP(word).getBytes());
    }

    public static void sendToReplicas(Vector<String> command) throws IOException {
        String arr[] = new String[command.size()];
        for(int i = 0;i<command.size();i++) {
          arr[i] = command.get(i);
        }
        String toSend = encodeRESPArr(arr);        
        
        for(Socket s:replicaSockets.keySet()) {
          OutputStream os = s.getOutputStream();
          send(arr, os);
        }
        if(arr[0].equalsIgnoreCase("SET") == false) {
          int tempadd = 0;
          for(int i = 0;i<toSend.length();i++) {
            char ch = toSend.charAt(i);
            if(ch=='\\')
              continue;
            else
              tempadd++;
          }
          countBytes.addAndGet(tempadd);
        }
      }

    public static void readCommand(InputStream in, Vector<String> command) throws IOException {
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
                countBytes.addAndGet(tempcount);            
            }
        }
    }






    public static void main(String args[]) throws IOException {
        if(args.length < 3) {
            setupMaster(args);
        }
        else
            setupSlave(args);
    }






    public static void setupSlave(String[] args) throws UnknownHostException, IOException {
        port = Integer.parseInt(args[1]);
        isMaster = false;
        String masterHost = args[3].substring(0, args[3].length()-5);
        int masterPort = Integer.parseInt(args[3].substring(args[3].length()-4));
        masterSocket = new Socket(masterHost, masterPort);
        handleHandshake();
        threadPool.submit(() -> {
            try {
                handleClients(masterSocket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public static void handleHandshake() throws IOException {
        InputStream is = masterSocket.getInputStream();
        OutputStream os = masterSocket.getOutputStream();

        String[] pingCommand = {"PING"};
        String[] handshakeCommand1 = {"REPLCONF", "listening-port", port+""};
        String[] handshakeCommand2 = {"REPLCONF", "capa", "psync2"};
        String[] handshakeCommand3 = {"PSYNC", "?", "-1"};

        send(pingCommand, os);
        skip(is, 7);
        send(handshakeCommand1, os);
        skip(is, 5);
        send(handshakeCommand2, os);
        skip(is, 5);
        send(handshakeCommand3, os);
        skip(is, 12);

        int replicationIdCount = 40;
        while(replicationIdCount-->0) {
            char ch = (char)is.read();
            replicationId = replicationId+ch;
        }
        skip(is, 4);
    }

    public static void setupMaster(String[] args) throws IOException {
        isMaster = true;
        port = args.length==0?6379:Integer.parseInt(args[1]);
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            while(true) {
                Socket socket = serverSocket.accept();
                threadPool.submit(()->{
                    try {
                        handleClients(socket);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
        catch (IOException e) {
            System.err.println("Server exception: " + e.getMessage());
        }

    }

    public static void handleClients(Socket socket) throws IOException {
        InputStream is = socket.getInputStream();
        OutputStream os = socket.getOutputStream();
        while(true) {
            char ch = (char)is.read();
            if(ch=='*') {
                Vector<String> command = new Vector<>();
                readCommand(is, command);
                switch (command.get(0)) {
                    case "ECHO":
                        handleEchoCommand(command, os);
                        break;
                    case "PING":
                        if(socket.equals(masterSocket)==false)
                            handlePingCommand(command, os);
                        break;
                    case "SET":
                        handleSetCommand(socket, command, os);
                        break;
                    case "GET":
                        handleGetCommand(command, os);
                        break;
                    case "REPLCONF":
                        handleReplconfCommand(socket, command, os);
                        break;
                    case "INFO":
                        handleInfoCommand(command, os);
                        break;
                    case "PSYNC":
                        handlePsyncCommand(command, os);
                        break;
                    case "WAIT":
                        handleWaitCommand(command, os);
                        break;
                    default:
                        break;
                }
            }
        }   
    }

    // Command Handlers
    public static void handleEchoCommand(Vector<String> command, OutputStream os) throws IOException {
        String response = encodeRESP(command.get(1));
        send(response, os);
    }

    public static void handlePingCommand(Vector<String> command, OutputStream os) throws IOException {
        send("PONG", os);
    }

    public static void handleGetCommand(Vector<String> command, OutputStream os) throws IOException {
        String key = command.get(1);
        String value = dataStore.getOrDefault(key, null);
        if (value == null) {
            send(-1, os);
        } else {
            send(value, os);
        }
    }

    public static void handleInfoCommand(Vector<String> command, OutputStream os) throws IOException {
        String info = isMaster ? "role:master" : "role:slave";
        info = info + "\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0";
        send(info, os);
    }

    public static void handlePsyncCommand(Vector<String> command, OutputStream os) throws IOException {
        String toSend = "+FULLRESYNC " + "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb" + " 0\r\n";
        send(toSend, os);
        byte[] contents = HexFormat.of().parseHex(
            "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2");
        os.write(("$"+contents.length+"\r\n").getBytes());
        os.write(contents);
    }

    public static void handleSetCommand(Socket s, Vector<String> command, OutputStream os) throws IOException {
        String key = command.get(1);
        String value = command.get(2);
        dataStore.put(key, value);
        sendToReplicas(command);
        if(s.equals(masterSocket)==false) {// if request is not coming from the master socket but the client
            send("OK", os);
            inSyncReplicaCount.set(0);
            for(Socket soc:replicaSockets.keySet()) {
              replicaSockets.put(soc, false);
            }
            Vector<String> getAckCommand = new Vector<>(Arrays.asList("REPLCONF", "GETACK", "*"));
            sendToReplicas(getAckCommand);
          }
          if(command.size()>3) {
            int ms = Integer.parseInt(command.get(4));
            Thread t = new Thread() {
              public void run() {
                try {
                  Thread.sleep(ms);
                  dataStore.remove(command.get(1));
                } catch (InterruptedException e) {
                  System.out.println(e);
                }
              }
            };
            t.start();
          }
    }

    public static void handleReplconfCommand(Socket s, Vector<String> command, OutputStream os) throws IOException {
        if(command.get(1).equalsIgnoreCase("listening-port")) {
            if(countBytes.get()==0) {
              replicaSockets.put(s,true);
              inSyncReplicaCount.incrementAndGet();
            }
            else {
              replicaSockets.put(s,false);
              inSyncReplicaCount.decrementAndGet();
            }
            send("OK", os);
          }
          else if(command.get(1).equalsIgnoreCase("capa")) {
            send("OK", os);
          }
          else if(command.get(1).equalsIgnoreCase("GETACK")) {
            String count = "" + countBytes.get();
            String toSend[] = {"REPLCONF", "ACK", count};
            send(toSend, os);
          }
          else if(command.get(1).equalsIgnoreCase("ACK")) {
            if(command.size()>2 && Integer.parseInt(command.get(2))==(countBytes.get()-37)) {
              replicaSockets.put(s, true);
              inSyncReplicaCount.incrementAndGet();
            }
          }
    }

    public static void handleWaitCommand(Vector<String> command, OutputStream os) throws IOException {
        int timeout = Integer.parseInt(command.get(1));
        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        send(inSyncReplicaCount.get(), os);
    }
}
