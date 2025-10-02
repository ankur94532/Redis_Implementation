import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Main {

  // ------------------ Simple KV with TTL ------------------
  static final class Key {
    volatile String value;
    volatile Instant expiry; // exclusive

    Key(String v, Instant e) {
      value = v;
      expiry = e;
    }
  }

  // DB state
  static final Map<String, Key> entries = new ConcurrentHashMap<>();

  // Replication bookkeeping
  static volatile boolean isReplica = false;
  static volatile String masterHost = null;
  static volatile int masterPort = -1;

  // All connected replicas (when we are master)
  static final Set<Socket> replicas = ConcurrentHashMap.newKeySet();

  // Server config
  static volatile int port = 6379;

  // constants
  static final String REPL_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

  public static void main(String[] args) throws Exception {
    parseArgs(args);

    // If we are a replica, connect to master and start replication reader
    if (isReplica) {
      startReplicaReplicationThread(masterHost, masterPort, port);
    }

    // Server loop
    try (ServerSocket ss = new ServerSocket(port)) {
      ss.setReuseAddress(true);
      while (true) {
        Socket s = ss.accept();
        new Thread(() -> {
          try {
            handleClient(s);
          } catch (IOException ignored) {
          } finally {
            closeQuiet(s);
          }
        }, "client-" + s.getRemoteSocketAddress()).start();
      }
    }
  }

  // ------------------ Client handling ------------------
  static void handleClient(Socket s) throws IOException {
    InputStream in = s.getInputStream();
    OutputStream out = s.getOutputStream();

    for (;;) {
      List<String> cmd = readRespArray(in); // blocks for one command
      if (cmd == null)
        break; // client closed
      // Special handling when a replica connects to us (we are master)
      if (!isReplica && isReplicaHandshakeCommand(cmd)) {
        processReplicaHandshakeCommand(cmd, s, out);
        continue;
      }
      execute(cmd, out, /* fromMaster */ false);
    }
  }

  // ------------------ Execute command ------------------
  static void execute(List<String> cmd, OutputStream out, boolean fromMaster) throws IOException {
    if (cmd.isEmpty())
      return;
    String op = cmd.get(0).toUpperCase(Locale.ROOT);

    switch (op) {
      case "PING": {
        if (!fromMaster) {
          out.write("+PONG\r\n".getBytes(StandardCharsets.US_ASCII));
        }
        return;
      }
      case "ECHO": {
        if (cmd.size() < 2) {
          if (!fromMaster)
            out.write("$-1\r\n".getBytes(StandardCharsets.US_ASCII));
          return;
        }
        byte[] b = cmd.get(1).getBytes(StandardCharsets.UTF_8);
        if (!fromMaster) {
          out.write(("$" + b.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
          out.write(b);
          out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
        }
        return;
      }
      case "INFO": { // handle: INFO or INFO REPLICATION
        if (!fromMaster) {
          if (isReplica) {
            byte[] body = "role:slave\r\n".getBytes(StandardCharsets.UTF_8);
            out.write(("$" + body.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
            out.write(body);
            out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
          } else {
            String body = "role:master\r\n" +
                "master_replid:" + REPL_ID + "\r\n" +
                "master_repl_offset:0\r\n";
            byte[] b = body.getBytes(StandardCharsets.UTF_8);
            out.write(("$" + b.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
            out.write(b);
            out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
          }
        }
        return;
      }
      case "SET": {
        if (cmd.size() < 3) {
          if (!fromMaster)
            out.write("-ERR wrong number of arguments for 'set'\r\n".getBytes());
          return;
        }
        String k = cmd.get(1), v = cmd.get(2);
        // Optional PX/EX (very minimal): SET k v PX ms
        Instant exp = Instant.ofEpochMilli(Long.MAX_VALUE);
        if (cmd.size() >= 5 && (cmd.get(3).equalsIgnoreCase("PX") || cmd.get(3).equalsIgnoreCase("EX"))) {
          long dur = Long.parseLong(cmd.get(4));
          if (cmd.get(3).equalsIgnoreCase("EX"))
            dur *= 1000;
          exp = Instant.now().plusMillis(dur);
        } else {
          exp = Instant.now().plusMillis(1_000_000_000L); // large default (compat with your earlier code)
        }
        entries.put(k, new Key(v, exp));

        if (!fromMaster) {
          out.write("+OK\r\n".getBytes(StandardCharsets.US_ASCII));
          // propagate to replicas
          propagateIfMaster(Arrays.asList("SET", k, v));
        }
        return;
      }
      case "INCR": {
        if (cmd.size() < 2) {
          if (!fromMaster)
            out.write("-ERR wrong number of arguments for 'incr'\r\n".getBytes());
          return;
        }
        String k = cmd.get(1);
        Key cur = entries.get(k);
        long val;
        if (cur == null || expired(cur)) {
          val = 1;
          entries.put(k, new Key(Long.toString(val), Instant.now().plusMillis(1_000_000_000L)));
        } else {
          try {
            val = Long.parseLong(cur.value) + 1;
            cur.value = Long.toString(val);
          } catch (NumberFormatException e) {
            if (!fromMaster)
              out.write("-ERR value is not an integer or out of range\r\n".getBytes());
            return;
          }
        }
        if (!fromMaster) {
          out.write((":" + Long.toString(val) + "\r\n").getBytes(StandardCharsets.US_ASCII));
          propagateIfMaster(Arrays.asList("INCR", k));
        }
        return;
      }
      case "GET": {
        if (cmd.size() < 2) {
          if (!fromMaster)
            out.write("$-1\r\n".getBytes(StandardCharsets.US_ASCII));
          return;
        }
        String k = cmd.get(1);
        Key cur = entries.get(k);
        if (cur == null || expired(cur)) {
          entries.remove(k);
          if (!fromMaster)
            out.write("$-1\r\n".getBytes(StandardCharsets.US_ASCII));
        } else {
          byte[] b = cur.value.getBytes(StandardCharsets.UTF_8);
          if (!fromMaster) {
            out.write(("$" + b.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
            out.write(b);
            out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
          }
        }
        return;
      }
      default: {
        if (!fromMaster) {
          out.write(("-ERR unknown command '" + cmd.get(0) + "'\r\n").getBytes(StandardCharsets.US_ASCII));
        }
      }
    }
  }

  static boolean expired(Key k) {
    return Instant.now().isAfter(k.expiry);
  }

  // ------------------ Master side: replica handshake handling ------------------
  static boolean isReplicaHandshakeCommand(List<String> a) {
    if (a.isEmpty())
      return false;
    String op = a.get(0).toUpperCase(Locale.ROOT);
    return op.equals("PING") || op.equals("REPLCONF") || op.equals("PSYNC");
  }

  static void processReplicaHandshakeCommand(List<String> a, Socket s, OutputStream out) throws IOException {
    String op = a.get(0).toUpperCase(Locale.ROOT);
    switch (op) {
      case "PING":
        out.write("+PONG\r\n".getBytes(StandardCharsets.US_ASCII));
        return;
      case "REPLCONF":
        // We just ACK any REPLCONF in this exercise
        out.write("+OK\r\n".getBytes(StandardCharsets.US_ASCII));
        return;
      case "PSYNC":
        // Send FULLRESYNC + fake RDB bulk
        out.write(("+FULLRESYNC " + REPL_ID + " 0\r\n").getBytes(StandardCharsets.US_ASCII));
        byte[] rdb = hex(
            "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2");
        out.write(("$" + rdb.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
        out.write(rdb);
        out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
        out.flush();

        // Keep this replica socket to stream future writes
        replicas.add(s);
        return;
    }
  }

  // ------------------ Replica side: connect & stream-applier ------------------
  static void startReplicaReplicationThread(String host, int mPort, int myPort) {
    Thread t = new Thread(() -> {
      Socket ms = null;
      try {
        ms = new Socket(host, mPort);
        ms.setTcpNoDelay(true);
        InputStream in = ms.getInputStream();
        OutputStream out = ms.getOutputStream();

        // 1) PING
        writeArray(out, Arrays.asList("PING"));
        readSimpleString(in); // +PONG

        // 2) REPLCONF listening-port <myPort>
        writeArray(out, Arrays.asList("REPLCONF", "listening-port", Integer.toString(myPort)));
        readSimpleString(in); // +OK

        // 3) REPLCONF capa eof
        writeArray(out, Arrays.asList("REPLCONF", "capa", "eof"));
        readSimpleString(in); // +OK

        // 4) PSYNC ? -1
        writeArray(out, Arrays.asList("PSYNC", "?", "-1"));

        // 5) +FULLRESYNC ... \r\n
        String full = readSimpleString(in); // e.g. FULLRESYNC <id> 0

        // 6) $<len>\r\n <rdb-bytes> \r\n
        int len = readBulkLen(in);
        readExactly(in, len); // discard RDB bytes
        expectCRLF(in);

        // 7) continuous command stream (arrays)
        for (;;) {
          List<String> cmd = readRespArray(in);
          if (cmd == null)
            break;
          // Apply immediately; do not reply
          execute(cmd, /* out */ nullStream(), /* fromMaster */ true);
        }
      } catch (IOException e) {
        // exit thread; harness will restart tests
      } finally {
        closeQuiet(ms);
      }
    }, "replication-reader");
    t.setDaemon(true);
    t.start();
  }

  static OutputStream nullStream() {
    return new OutputStream() {
      public void write(int b) {
      }
    };
  }

  // ------------------ Propagate writes to replicas ------------------
  static void propagateIfMaster(List<String> cmd) {
    if (isReplica)
      return;
    byte[] frame = serializeArray(cmd);
    for (Socket rs : new ArrayList<>(replicas)) {
      try {
        rs.getOutputStream().write(frame);
        rs.getOutputStream().flush();
      } catch (IOException e) {
        closeQuiet(rs);
        replicas.remove(rs);
      }
    }
  }

  // ------------------ RESP helpers ------------------
  static List<String> readRespArray(InputStream in) throws IOException {
    int start = in.read();
    if (start == -1)
      return null;
    if (start != '*')
      throw new IOException("Expected *");
    int n = Integer.parseInt(readLine(in)); // number of elements
    List<String> a = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      int sig = in.read();
      if (sig != '$')
        throw new IOException("Expected $");
      int len = Integer.parseInt(readLine(in));
      byte[] data = readExactly(in, len);
      expectCRLF(in);
      a.add(new String(data, StandardCharsets.UTF_8));
    }
    return a;
  }

  static void writeArray(OutputStream out, List<String> items) throws IOException {
    out.write(("*" + items.size() + "\r\n").getBytes(StandardCharsets.US_ASCII));
    for (String s : items) {
      byte[] b = s.getBytes(StandardCharsets.UTF_8);
      out.write(("$" + b.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
      out.write(b);
      out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
    }
    out.flush();
  }

  static byte[] serializeArray(List<String> items) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      baos.write(("*" + items.size() + "\r\n").getBytes(StandardCharsets.US_ASCII));
      for (String s : items) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        baos.write(("$" + b.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
        baos.write(b);
        baos.write("\r\n".getBytes(StandardCharsets.US_ASCII));
      }
      return baos.toByteArray();
    } catch (IOException impossible) {
      return new byte[0];
    }
  }

  static String readSimpleString(InputStream in) throws IOException {
    int sig = in.read();
    if (sig != '+')
      throw new IOException("Expected +");
    return readLine(in); // returns content without CRLF
  }

  static int readBulkLen(InputStream in) throws IOException {
    int sig = in.read();
    if (sig != '$')
      throw new IOException("Expected $");
    return Integer.parseInt(readLine(in));
  }

  static String readLine(InputStream in) throws IOException {
    StringBuilder sb = new StringBuilder(64);
    int prev = -1;
    for (;;) {
      int b = in.read();
      if (b == -1)
        throw new EOFException();
      if (prev == '\r' && b == '\n') {
        sb.setLength(sb.length() - 1); // drop \r
        return sb.toString();
      }
      sb.append((char) b);
      prev = b;
    }
  }

  static byte[] readExactly(InputStream in, int len) throws IOException {
    byte[] buf = new byte[len];
    int off = 0;
    while (off < len) {
      int n = in.read(buf, off, len - off);
      if (n < 0)
        throw new EOFException();
      off += n;
    }
    return buf;
  }

  static void expectCRLF(InputStream in) throws IOException {
    int r = in.read(), n = in.read();
    if (r != '\r' || n != '\n')
      throw new IOException("Expected CRLF");
  }

  static byte[] hex(String s) {
    int n = s.length();
    byte[] out = new byte[n / 2];
    for (int i = 0; i < n; i += 2) {
      out[i / 2] = (byte) Integer.parseInt(s.substring(i, i + 2), 16);
    }
    return out;
  }

  static void closeQuiet(Closeable c) {
    try {
      if (c != null)
        c.close();
    } catch (IOException ignored) {
    }
  }

  // ------------------ Args ------------------
  static void parseArgs(String[] args) {
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--port":
          if (i + 1 < args.length)
            port = Integer.parseInt(args[++i]);
          break;
        case "--replicaof":
          isReplica = true;
          // accept either 2 separate args or a quoted single arg
          if (i + 2 < args.length && !args[i + 1].contains(" ")) {
            masterHost = args[++i];
            masterPort = Integer.parseInt(args[++i]);
          } else {
            String joined = args[++i]; // e.g. "localhost 6379"
            String[] hp = joined.split("\\s+");
            masterHost = hp[0];
            masterPort = Integer.parseInt(hp[1]);
          }
          break;
      }
    }
  }
}
