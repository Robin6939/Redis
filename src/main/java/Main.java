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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
    static ConcurrentHashMap<String, Vector<String>> streamIds = new ConcurrentHashMap<>();//key -> streamkey, value -> id
    static ConcurrentHashMap<String, ConcurrentHashMap<String, String>> streamStore = new ConcurrentHashMap<>();//key -> id, value -> key-val pairs
    static AtomicLong lastTimeId = new AtomicLong(-1);
    static AtomicLong lastSeqId = new AtomicLong(-1);




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
        os.flush();
    }

    public static void send(int integer, OutputStream os) throws IOException {
        os.write(toRESPInt(integer).getBytes());
        os.flush();
    }

    public static void send(String word, OutputStream os) throws IOException {
        os.write(encodeRESP(word).getBytes());
        os.flush();
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
        if(command.size()>0) {
            if(command.get(0).equalsIgnoreCase("SET") ||
             (command.size()>1 && command.get(1).equalsIgnoreCase("GETACK")) ) {
                countBytes.addAndGet(tempcount);
            }
        }
    }






    public static void main(String args[]) throws IOException {
        System.out.println("Program started successfully!!");
        if(args.length < 3) {
            setupMaster(args);
        }
        else
            setupSlave(args);
    }






    public static void setupSlave(String[] args) throws UnknownHostException, IOException {
        System.out.println("Setting up slave");
        port = Integer.parseInt(args[1]);
        isMaster = false;
        String masterHost = args[3].substring(0, args[3].length()-5);
        int masterPort = Integer.parseInt(args[3].substring(args[3].length()-4));
        masterSocket = new Socket(masterHost, masterPort);
        threadPool.submit(() -> {
            try {
                handleHandshake();
                handleClients(masterSocket);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        try(ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            while(true) {
                Socket socket = serverSocket.accept();
                threadPool.submit(()->{
                    try {
                        handleClients(socket);
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
    }

    public static void handleHandshake() throws IOException {
        System.out.println("Handshake started");

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
        System.out.println("Setting up master");
        isMaster = true;
        port = args.length==0?6379:Integer.parseInt(args[1]);
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            while(true) {
                Socket socket = serverSocket.accept();
                threadPool.submit(()->{
                    try {
                        handleClients(socket);
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
        catch (IOException e) {
            System.err.println("Server exception: " + e.getMessage());
        }

    }

    public static void handleClients(Socket socket) throws IOException, InterruptedException {
        InputStream is = socket.getInputStream();
        OutputStream os = socket.getOutputStream();
        while(true) {
            char ch = (char)is.read();
            if(ch=='*') {
                Vector<String> command = new Vector<>();
                readCommand(is, command);
                switch (command.get(0).toUpperCase()) {
                    case "ECHO":
                        handleEchoCommand(command, os);
                        break;
                    case "PING":
                        if(socket.equals(masterSocket)==false) //coming from client
                            handlePingCommand(command, os);
                        else
                            countBytes.addAndGet(14);
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
                    case "TYPE":
                        handleTypeCommand(command, os);
                        break;
                    case "XADD":
                        handleXaddCommand(command, os);
                        break;
                    case "XRANGE":
                        handleXrangeCommand(command, os);
                        break;
                    default:
                        break;
                }
            }
        }   
    }

    // Command Handlers
    public static void handleEchoCommand(Vector<String> command, OutputStream os) throws IOException {
        String response = (command.get(1));
        send(response, os);
    }

    public static void handlePingCommand(Vector<String> command, OutputStream os) throws IOException {
        send("PONG", os);
    }

    public static void handleGetCommand(Vector<String> command, OutputStream os) throws IOException, InterruptedException {
        String key = command.get(1);

        AtomicBoolean isSent = new AtomicBoolean(false);
        int timeout = 10;
        threadPool.submit(() -> {
            try {
                Thread.sleep(timeout);
                if(isSent.get()==false){
                    os.write("$-1\r\n".getBytes());
                    isSent.set(true);
                }
            } catch (InterruptedException | IOException e) {
                Thread.currentThread().interrupt();
            }
        });
        while(isSent.get()==false) {
            if(dataStore.getOrDefault(key, null)!=null) {
                send(dataStore.getOrDefault(key, null), os);
                isSent.set(true);
            }
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
            threadPool.submit(() ->{
                try {
                    Thread.sleep(100);
                    sendToReplicas(getAckCommand);
                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                }
            });
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
            String count = "" + (countBytes.get()-37);
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
        int timeout = Integer.parseInt(command.get(2));
        int rep = Integer.parseInt(command.get(1));
        AtomicBoolean isSent = new AtomicBoolean(false);
        threadPool.submit(() -> {
            try {
                Thread.sleep(timeout);
                if(isSent.get()==false){
                    send(inSyncReplicaCount.get(), os);
                    isSent.set(true);
                }
            } catch (InterruptedException | IOException e) {
                Thread.currentThread().interrupt();
            }
        });
        while(isSent.get()==false) {
            if(inSyncReplicaCount.get()>=rep) {
                send(inSyncReplicaCount.get(), os);
                isSent.set(true);
            }
        }
    }

    public static void handleTypeCommand(Vector<String> command, OutputStream os) throws IOException {
        String key = command.get(1);
        System.out.println(key);
        if(dataStore.getOrDefault(key, null)!=null)
            os.write("+string\r\n".getBytes());
        else if(streamIds.getOrDefault(key, null)!=null)
            os.write("+stream\r\n".getBytes());
        else
            os.write("+none\r\n".getBytes());
    }

    public static void handleXaddCommand(Vector<String> command, OutputStream os) throws IOException {
        System.out.println("This is an XADD command");
        String key = command.get(1); //key is always present
        System.out.println("The xadd command is: "+command);
        if(command.get(2).equals("*")) {// when entire id needs to be predicted by redis instance
            long currentTimeMillis = System.currentTimeMillis();
            String toReturn = currentTimeMillis+"-0";
            send(toReturn, os);

            if(streamIds.containsKey(key)) {
                streamIds.get(key).add(toReturn);
            }
            else {
                streamIds.put(key, new Vector<>(Arrays.asList(toReturn)));
            } // push id inside the key vector

            ConcurrentHashMap<String, String> values = new ConcurrentHashMap<>();
            for(int i=3;i<command.size();i+=2) {
                values.put(command.get(i), command.get(i+1));
            }
            streamStore.put(toReturn, values);//store all the key value pairs associated with that id

            lastSeqId.set(0);
            lastTimeId.set(currentTimeMillis);//update last seq and time id
        }
        else { //at least the time id is present
            String[] id = command.get(2).split("-"); 
            long timeId = Long.parseLong(id[0]);
            long seqId;
            if(id[1].equals("*")) {//seq id needs to be predicted by the redis instance
                if(lastTimeId.get() == timeId)
                    seqId = lastSeqId.get()+1;
                else
                    seqId = 0;//ignoring the case where the time id is lessthan the lastTimeId
                if(timeId==0 && seqId==0) {
                    seqId++;
                }
                String toReturn = timeId+"-"+seqId;
                os.write(("+"+toReturn+"\r\n").getBytes());

                if(streamIds.containsKey(key)) {
                    streamIds.get(key).add(toReturn);
                }
                else {
                    streamIds.put(key, new Vector<>(Arrays.asList(toReturn)));
                }  
                
                ConcurrentHashMap<String, String> values = new ConcurrentHashMap<>();
                for(int i=3;i<command.size();i+=2) {
                    values.put(command.get(i), command.get(i+1));
                }
                streamStore.put(toReturn, values);

                lastSeqId.set(seqId);
                lastTimeId.set(timeId);
            } else { //both the timeid and seqid is given where errors can also be there
                seqId = Long.parseLong(id[1]);
                String toReturn = command.get(2);
                if(toReturn.equals("0-0")) { //this is not allowed (0-0 is not but 1-0 is allowed)
                    os.write("-ERR The ID specified in XADD must be greater than 0-0\r\n".getBytes());
                }
                else if(timeId > lastTimeId.get()) { //time id greater than the last one
                    os.write(("+"+toReturn+"\r\n").getBytes()); 
                    lastTimeId.set(timeId);
                    lastSeqId.set(seqId);

                    if(streamIds.containsKey(key)) {
                        streamIds.get(key).add(toReturn);
                    }
                    else {
                        streamIds.put(key, new Vector<>(Arrays.asList(toReturn)));
                    } 

                    ConcurrentHashMap<String, String> values = new ConcurrentHashMap<>();
                    for(int i=3;i<command.size();i+=2) {
                        values.put(command.get(i), command.get(i+1));
                    }
                    streamStore.put(toReturn, values);
                }
                else if(timeId == lastTimeId.get() && (lastSeqId.get()<seqId)) { // if time id is same then the seq id needs to greater than the last one
                    os.write(("+"+toReturn+"\r\n").getBytes());
                    lastTimeId.set(timeId);
                    lastSeqId.set(seqId);

                    if(streamIds.containsKey(key)) {
                        streamIds.get(key).add(toReturn);
                    }
                    else {
                        streamIds.put(key, new Vector<>(Arrays.asList(toReturn)));
                    } 

                    ConcurrentHashMap<String, String> values = new ConcurrentHashMap<>();
                    for(int i=3;i<command.size();i+=2) {
                        values.put(command.get(i), command.get(i+1));
                    }
                    streamStore.put(toReturn, values);
                }
                else {
                    os.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".getBytes());
                }
            }
        }
    } 

    public static boolean isIdGreaterEqualTo(String s1, String s2) { //returns true if s1>s2
        String id1[] = s1.split("-");
        String id2[] = s2.split("-");
        if(id2.length==1) {
            if(Long.parseLong(id1[0])>Long.parseLong(id2[0]))
                return true;
            else
                return false;
        }
        else {
            if(Long.parseLong(id1[0])>Long.parseLong(id2[0]))
                return true;
            else if(Long.parseLong(id1[0])==Long.parseLong(id2[0]) && Long.parseLong(id1[1])>=Long.parseLong(id2[1]))
                return true;
            else
                return false;
        }
    }

    public static boolean isIdLesserEqualTo(String s1, String s2) { //returns true if s1>s2
        String id1[] = s1.split("-");
        String id2[] = s2.split("-");
        if(id2.length==1) {
            if(Long.parseLong(id1[0])<Long.parseLong(id2[0]))
                return true;
            else
                return false;
        }
        else {
            if(Long.parseLong(id1[0])<Long.parseLong(id2[0]))
                return true;
            else if(Long.parseLong(id1[0])==Long.parseLong(id2[0]) && Long.parseLong(id1[1])<=Long.parseLong(id2[1]))
                return true;
            else
                return false;
        }
    }

    public static void sendForXRange(Vector<String> validId, OutputStream os) throws IOException {
        int size = validId.size();
        os.write(("*"+size+"\r\n").getBytes());
        for(String id: validId) {
            int sizeMap = streamStore.get(id).size();
            os.write(("*"+sizeMap+"\r\n").getBytes());
            send(id, os);
            // os.write(("*"+sizeMap*2+"\r\n").getBytes());
            String keyVal[] = new String[sizeMap*2];
            int i=0;
            for(String key: streamStore.get(id).keySet()) {
                keyVal[i++] = (key);
                keyVal[i++] = (streamStore.get(id).get(key));
            }
            send(keyVal, os);
        }
    }

    public static void handleXrangeCommand(Vector<String> command, OutputStream os) throws IOException {
        String key = command.get(1);
        System.out.println(streamIds.get(key));
        Vector<String> validId = new Vector<>();
        for(String s:streamIds.get(key)) {
            if(isIdGreaterEqualTo(s, command.get(2)) && isIdLesserEqualTo(s, command.get(3))) {
                System.out.println(s+" : "+streamStore.get(s)); 
                validId.add(s);
            }
        }
        sendForXRange(validId, os);
    }
}
    