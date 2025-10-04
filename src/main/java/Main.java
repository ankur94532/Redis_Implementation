import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class Key {
  public String value;
  public Instant time;

  public Key(String value, Instant time) {
    this.value = value;
    this.time = time;
  }
}

public class Main {
  static Map<String, Key> entries = new ConcurrentHashMap<>();
  static Map<String, List<String>> lists = new ConcurrentHashMap<>();
  static final Map<String, ArrayDeque<Waiter>> waitersByKey = new HashMap<>();
  static HashMap<String, HashMap<String, HashMap<String, String>>> streams = new HashMap<>();
  static final Object lock = new Object();
  static HashMap<String, Object> locks = new HashMap<>();
  static Map<Integer, ServerSocket> servers = new HashMap<>();
  static Map<Integer, Set<Socket>> slaves = new HashMap<>();
  static Map<String, String> configInfo = new HashMap<>();
  static int port = 6379;
  static int master = -1;
  static Map<Socket, Integer> lastAck = new ConcurrentHashMap<>();
  static File dbFile;
  static Map<Socket, Set<String>> subscibed = new HashMap<>();
  static Map<String, TreeMap<Double, TreeSet<String>>> scores = new HashMap<>();

  static final class Waiter {
    final Socket client;
    final OutputStream out;
    final String key;

    Waiter(Socket c, OutputStream o, String k) {
      this.client = c;
      this.out = o;
      this.key = k;
    }
  }

  static String hexConverter(List<String> hex) {
    String joined = String.join("", hex);
    byte[] key = HexFormat.of().parseHex(joined);
    String s = new String(key, StandardCharsets.UTF_8);
    return s;
  }

  static Instant hexLeToInstant(List<String> hex) {
    String joined = String.join("", hex);
    byte[] b = HexFormat.of().parseHex(joined); // must be 8 bytes
    if (b.length != 8)
      throw new IllegalArgumentException("Expected 8 bytes");
    long ms = ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getLong();
    return Instant.ofEpochMilli(ms);
  }

  static int decodeRdbLenHex(String hexBytes) {
    String[] p = hexBytes.trim().split("\\s+");
    if (p.length == 0 || p[0].isEmpty())
      throw new IllegalArgumentException("no bytes");
    int b0 = Integer.parseInt(p[0], 16) & 0xFF;
    int top = (b0 & 0xC0) >>> 6;

    if (top == 0b00) { // 1 byte (0..63)
      return b0 & 0x3F;
    } else if (top == 0b01) { // 2 bytes (64..16383)
      if (p.length < 2)
        throw new IllegalArgumentException("need 2 bytes");
      int b1 = Integer.parseInt(p[1], 16) & 0xFF;
      return ((b0 & 0x3F) << 8) | b1; // big-endian across the two
    } else if (top == 0b10) { // 5 bytes (>=16384)
      if (p.length < 5)
        throw new IllegalArgumentException("need 5 bytes");
      int b1 = Integer.parseInt(p[1], 16) & 0xFF;
      int b2 = Integer.parseInt(p[2], 16) & 0xFF;
      int b3 = Integer.parseInt(p[3], 16) & 0xFF;
      int b4 = Integer.parseInt(p[4], 16) & 0xFF;
      return (b1 << 24) | (b2 << 16) | (b3 << 8) | b4; // 32-bit BE
    } else { // 0b11 => special string encoding (INT8/16/32 or LZF), not a raw length
      throw new IllegalArgumentException("special-encoded string, not a raw length");
    }
  }

  public static void main(String[] args) throws IOException {
    System.out.println("Logs from your program will appear here!");
    if (args.length > 0 && args[0].equals("--port")) {
      port = Integer.parseInt(args[1]);
    }
    ServerSocket serverSocket;
    if (servers.containsKey(port)) {
      serverSocket = servers.get(port);
    } else {
      serverSocket = new ServerSocket(port);
      servers.put(port, serverSocket);
    }
    serverSocket.setReuseAddress(true);
    if (args.length > 2) {
      if (args[2].equals("--replicaof")) {
        master = Integer.parseInt(args[3].split(" ")[1]);
        new Thread(
            () -> {
              try (Socket masterSock = new Socket(args[3].split(" ")[0], master)) {
                byte[] buf = new byte[8192];
                int used = 0;
                OutputStream mout = masterSock.getOutputStream();
                mout.write(
                    "*1\r\n$4\r\nPING\r\n"
                        .getBytes(
                            java.nio.charset.StandardCharsets.US_ASCII));
                used += masterSock
                    .getInputStream()
                    .read(buf, used, buf.length - used);
                String data = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port"
                    + "\r\n$4\r\n"
                    + Integer.toString(port)
                    + "\r\n";
                mout.write(data.getBytes());
                used += masterSock
                    .getInputStream()
                    .read(buf, used, buf.length - used);
                mout.write(
                    "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n"
                        .getBytes());
                used += masterSock
                    .getInputStream()
                    .read(buf, used, buf.length - used);
                mout.write(
                    "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
                        .getBytes());
                int last = -1;
                int first = 0;
                while (true) {
                  if (used == buf.length) {
                    buf = java.util.Arrays.copyOf(buf, buf.length * 2);
                  }
                  int k = masterSock
                      .getInputStream()
                      .read(buf, used, buf.length - used);
                  if (last == -1) {
                    for (int i = used; i < used + k; i++) {
                      if (buf[i] < 0) {
                        last = i + 1;
                        first = 1;
                      }
                    }
                  }
                  if (last == -1) {
                    used += k;
                    continue;
                  }
                  if (k == -1) {
                    break;
                  }
                  List<String> commands = new ArrayList<>();
                  StringBuilder sb = new StringBuilder();
                  for (int i = first > 0 ? last : used; i < used + k;) {
                    if (buf[i] == 42
                        && i + 1 < used + k
                        && buf[i + 1] >= 48
                        && buf[i + 1] <= 57) {
                      i++;
                      while (i < used + k
                          && buf[i] >= 48
                          && buf[i] <= 57) {
                        i++;
                      }
                    } else if (buf[i] == 36
                        && i + 1 < used + k
                        && buf[i + 1] >= 48
                        && buf[i + 1] <= 57) {
                      i++;
                      while (i < used + k
                          && buf[i] >= 48
                          && buf[i] <= 57) {
                        i++;
                      }
                    } else if (buf[i] == 13) {
                      i++;
                    } else if (buf[i] == 10) {
                      if (sb.length() > 0) {
                        commands.add(sb.toString());
                      }
                      sb.setLength(0);
                      if (i + 1 == used + k
                          || (buf[i + 1] == 42
                              && i + 2 < used + k
                              && buf[i + 2] >= 48
                              && buf[i + 2] <= 57)) {
                        execute(
                            commands,
                            masterSock,
                            true,
                            used + k - last - 37);
                        commands.clear();
                      }
                      i++;
                    } else {
                      sb.append((char) buf[i]);
                      i++;
                    }
                  }
                  first = 0;
                  used += k;
                }
              } catch (IOException e) {
              } catch (InterruptedException e) {
              }
            })
            .start();
      } else {
        for (int i = 0; i < 4; i += 2) {
          String key = args[i];
          String value = args[i + 1];
          while (key.charAt(0) == '-') {
            key = key.substring(1);
          }
          configInfo.put(key, value);
        }
        dbFile = checkDirAndFile(args[1], args[3]);
        if (dbFile != null) {
          System.out.println("file present here");
          byte[] data = Files.readAllBytes(dbFile.toPath());
          String allHex = HexFormat.of().formatHex(data); // continuous hex
          List<String> input = new ArrayList<>();
          StringBuilder sb = new StringBuilder();
          for (int i = 0; i < allHex.length(); i += 2) {
            sb.append(allHex.charAt(i));
            sb.append(allHex.charAt(i + 1));
            input.add(sb.toString());
            sb.setLength(0);
          }
          for (int i = 0; i < input.size();) {
            if (input.get(i).equals("ff")) {
              break;
            }
            if (input.get(i).equals("fe")) {
              i += 2;
              continue;
            }
            if (input.get(i).equals("fb")) {
              i += 3;
              continue;
            }
            if (input.get(i).equals("fc")) {
              List<String> time = new ArrayList<>();
              i++;
              for (int j = i; j < i + 8; j++) {
                time.add(input.get(j));
              }
              i += 9;
              Instant expiry = hexLeToInstant(time);
              int len = 0;
              if (input.get(i).charAt(0) == '0') {
                len = decodeRdbLenHex(input.get(i));
                i++;
              } else if (input.get(i).charAt(0) == '4') {
                StringBuilder sbLen = new StringBuilder();
                sbLen.append(input.get(i));
                sbLen.append(" ");
                sbLen.append(input.get(i + 1));
                len = decodeRdbLenHex(sb.toString());
                i += 2;
              } else {
                StringBuilder sbLen = new StringBuilder();
                sbLen.append(input.get(i));
                sbLen.append(" ");
                sbLen.append(input.get(i + 1));
                sbLen.append(" ");
                sbLen.append(input.get(i + 2));
                sbLen.append(" ");
                sbLen.append(input.get(i + 3));
                sbLen.append(" ");
                sbLen.append(input.get(i + 3));
                i += 5;
              }
              time.clear();
              for (int j = i; j < i + len; j++) {
                time.add(input.get(j));
              }
              String key = hexConverter(time);
              i += len;
              if (input.get(i).charAt(0) == '0') {
                len = decodeRdbLenHex(input.get(i));
                i++;
              } else if (input.get(i).charAt(0) == '4') {
                StringBuilder sbLen = new StringBuilder();
                sbLen.append(input.get(i));
                sbLen.append(" ");
                sbLen.append(input.get(i + 1));
                len = decodeRdbLenHex(sb.toString());
                i += 2;
              } else {
                StringBuilder sbLen = new StringBuilder();
                sbLen.append(input.get(i));
                sbLen.append(" ");
                sbLen.append(input.get(i + 1));
                sbLen.append(" ");
                sbLen.append(input.get(i + 2));
                sbLen.append(" ");
                sbLen.append(input.get(i + 3));
                sbLen.append(" ");
                sbLen.append(input.get(i + 3));
                i += 5;
              }
              time.clear();
              for (int j = i; j < i + len; j++) {
                time.add(input.get(j));
              }
              i += len;
              String value = hexConverter(time);
              Key entry = new Key(value, expiry);
              entries.put(key, entry);
              continue;
            }
            if (input.get(i).equals("00")) {
              i++;
              int len = 0;
              if (input.get(i).charAt(0) == '0') {
                len = decodeRdbLenHex(input.get(i));
                i++;
              } else if (input.get(i).charAt(0) == '4') {
                StringBuilder sbLen = new StringBuilder();
                sbLen.append(input.get(i));
                sbLen.append(" ");
                sbLen.append(input.get(i + 1));
                len = decodeRdbLenHex(sb.toString());
                i += 2;
              } else {
                StringBuilder sbLen = new StringBuilder();
                sbLen.append(input.get(i));
                sbLen.append(" ");
                sbLen.append(input.get(i + 1));
                sbLen.append(" ");
                sbLen.append(input.get(i + 2));
                sbLen.append(" ");
                sbLen.append(input.get(i + 3));
                sbLen.append(" ");
                sbLen.append(input.get(i + 3));
                i += 5;
              }
              List<String> time = new ArrayList<>();
              for (int j = i; j < i + len; j++) {
                time.add(input.get(j));
              }
              String key = hexConverter(time);
              i += len;
              if (input.get(i).charAt(0) == '0') {
                len = decodeRdbLenHex(input.get(i));
                i++;
              } else if (input.get(i).charAt(0) == '4') {
                StringBuilder sbLen = new StringBuilder();
                sbLen.append(input.get(i));
                sbLen.append(" ");
                sbLen.append(input.get(i + 1));
                len = decodeRdbLenHex(sb.toString());
                i += 2;
              } else {
                StringBuilder sbLen = new StringBuilder();
                sbLen.append(input.get(i));
                sbLen.append(" ");
                sbLen.append(input.get(i + 1));
                sbLen.append(" ");
                sbLen.append(input.get(i + 2));
                sbLen.append(" ");
                sbLen.append(input.get(i + 3));
                sbLen.append(" ");
                sbLen.append(input.get(i + 3));
                i += 5;
              }
              time.clear();
              for (int j = i; j < i + len; j++) {
                time.add(input.get(j));
              }
              i += len;
              String value = hexConverter(time);
              Key entry = new Key(value, Instant.now().plusMillis(1_000_000_00000L));
              entries.put(key, entry);
              continue;
            }
            i++;
          }
        }
      }
    }
    try {
      while (true) {
        Socket clientSocket = serverSocket.accept();
        new Thread(() -> {
          try {
            handle(clientSocket);
          } catch (IOException | InterruptedException e) {
            e.printStackTrace();
          }
        }).start();
      }
    } finally {
      serverSocket.close();
    }
  }

  static File checkDirAndFile(String dirPath, String fileName) {
    Path dir = Paths.get(dirPath).toAbsolutePath().normalize();
    if (!Files.isDirectory(dir)) {
      return null;
    }
    Path file = dir.resolve(fileName).normalize();
    if (!Files.isRegularFile(file)) {
      return null;
    }
    return file.toFile();
  }

  static void handle(Socket client) throws IOException, InterruptedException {
    try {
      InputStream in = client.getInputStream();
      OutputStream out = client.getOutputStream();
      byte[] buf = new byte[8192];
      int used = 0;
      boolean multi = false;
      Deque<List<String>> queueCommands = new ArrayDeque<>();
      while (true) {
        if (used == buf.length) {
          buf = java.util.Arrays.copyOf(buf, buf.length * 2);
        }
        int k = in.read(buf, used, buf.length - used);
        if (k == -1) {
          break;
        }
        List<String> commands = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        for (int i = used; i < used + k;) {
          if (buf[i] == 42 && i + 1 < used + k && buf[i + 1] >= 48 && buf[i + 1] <= 57) {
            i++;
            while (i < used + k && buf[i] >= 48 && buf[i] <= 57) {
              i++;
            }
          } else if (buf[i] == 36 && i + 1 < used + k && buf[i + 1] >= 48 && buf[i + 1] <= 57) {
            i++;
            while (i < used + k && buf[i] >= 48 && buf[i] <= 57) {
              i++;
            }
          } else if (buf[i] == 13) {
            i++;
          } else if (buf[i] == 10) {
            if (sb.length() > 0) {
              commands.add(sb.toString());
            }
            sb.setLength(0);
            if (i + 1 == used + k) {
              break;
            }
            i++;
          } else {
            sb.append((char) buf[i]);
            i++;
          }
        }
        if (!commands.get(0).equalsIgnoreCase("wait")) {
          int last = 0;
          if (lastAck.containsKey(client)) {
            last = lastAck.get(client);
          }
          last++;
          lastAck.put(client, last);
        }
        used += k;
        if (commands.get(0).equalsIgnoreCase("discard")) {
          if (!multi) {
            out.write("-ERR DISCARD without MULTI\r\n".getBytes());
            continue;
          }
          multi = false;
          queueCommands.clear();
          out.write("+OK\r\n".getBytes(StandardCharsets.US_ASCII));
          continue;
        }
        if (commands.get(0).equalsIgnoreCase("exec")) {
          if (!multi) {
            out.write("-ERR EXEC without MULTI\r\n".getBytes());
            continue;
          }
          multi = false;
          if (queueCommands.isEmpty()) {
            out.write("*0\r\n".getBytes());
            continue;
          }
          out.write(("*" + queueCommands.size() + "\r\n").getBytes(StandardCharsets.US_ASCII));
          while (queueCommands.size() > 0) {
            execute(queueCommands.peekFirst(), client, false, used);
            queueCommands.pollFirst();
          }
          continue;
        }
        if (multi) {
          queueCommands.offerLast(commands);
          out.write("+QUEUED\r\n".getBytes(StandardCharsets.US_ASCII));
          continue;
        }
        if (commands.get(0).equalsIgnoreCase("multi")) {
          multi = true;
          out.write("+OK\r\n".getBytes(StandardCharsets.US_ASCII));
          continue;
        }
        execute(commands, client, false, used);
      }
    } catch (

    IOException ignored) {
    } finally {
      try {
        client.close();
      } catch (IOException ignore) {
      }
    }
  }

  // Parallel GETACK-and-wait for a single replica
  static int work(Socket client, int timeoutMs) {
    final long start = System.nanoTime();
    final long deadline = start + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    try {
      OutputStream out = client.getOutputStream();

      // Snapshot current ACK offset for this socket (default very small)
      final int before = lastAck.getOrDefault(client, Integer.MIN_VALUE);

      // Ask for ACKs
      out.write("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
          .getBytes(java.nio.charset.StandardCharsets.US_ASCII));
      out.flush();

      // Poll until ACK offset increases or we hit the deadline
      while (System.nanoTime() < deadline) {
        int now = lastAck.getOrDefault(client, Integer.MIN_VALUE);
        if (now > before)
          return 1; // got a newer ACK for this socket
        try {
          Thread.sleep(1);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return 0;
        }
      }
    } catch (IOException ioe) {
      return 0;
    }
    return 0; // timeout/no ACK
  }

  static void execute(List<String> commands, Socket client, boolean isMaster, int used)
      throws IOException, InterruptedException {

    /*
     * System.out.println("hi");
     * for (String str : commands) {
     * System.out.print(str + " ");
     * }
     * 
     * System.out.println();
     */

    OutputStream out = client.getOutputStream();
    if (subscibed.containsKey(client)) {
      if (!commands.get(0).equalsIgnoreCase("SUBSCRIBE") && !commands.get(0).equalsIgnoreCase("UNSUBSCRIBE")
          && !commands.get(0).equalsIgnoreCase("PING") && !commands.get(0).equalsIgnoreCase("QUIT")
          && !commands.get(0).equalsIgnoreCase("RESET")) {
        String data = "-ERR Can't execute '" + commands.get(0).toLowerCase() + "' in subscribed mode" + "\r\n";
        out.write(data.getBytes());
        return;
      }
      if (commands.get(0).equalsIgnoreCase("PING")) {
        out.write(("*2\r\n$4\r\npong\r\n$0\r\n\r\n").getBytes());
        return;
      }
    }
    if (commands.get(0).equalsIgnoreCase("zcard")) {
      if (!scores.containsKey(commands.get(1))) {
        out.write(":0\r\n".getBytes());
        return;
      }
      out.write((":" + scores.get(commands.get(1)).size() + "\r\n").getBytes());
    } else if (commands.get(0).equalsIgnoreCase("zrange")) {
      String key = commands.get(1);
      int start = Integer.parseInt(commands.get(2));
      int end = Integer.parseInt(commands.get(3));
      List<String> members = new ArrayList<>();
      if (!scores.containsKey(key)) {
        out.write("*0\r\n".getBytes());
        return;
      }
      for (Map.Entry<Double, TreeSet<String>> it : scores.get(key).entrySet()) {
        for (String member : it.getValue()) {
          members.add(member);
        }
      }
      if (members.size() == 0) {
        out.write("*0\r\n".getBytes());
        return;
      }
      System.out.println(members.size());
      start = Math.max(start, -members.size());
      while (start < 0) {
        start = (start + members.size()) % members.size();
      }
      while (end < 0) {
        end = (end + members.size()) % members.size();
      }
      if (start >= members.size() || start > end) {
        out.write("*0\r\n".getBytes());
        return;
      }
      end = Math.min(end, members.size() - 1);
      List<String> response = new ArrayList<>();
      for (int i = start; i <= end; i++) {
        response.add(members.get(i));
      }
      StringBuilder sb = new StringBuilder();
      sb.append("*");
      sb.append(response.size());
      sb.append("\r\n");
      for (String resp : response) {
        sb.append("$");
        sb.append(resp.length());
        sb.append("\r\n");
        sb.append(resp);
        sb.append("\r\n");
      }
      out.write(sb.toString().getBytes());
    } else if (commands.get(0).equalsIgnoreCase("zrank")) {
      String key = commands.get(1);
      String memeber = commands.get(2);
      if (!scores.containsKey(key)) {
        out.write("$-1\r\n".getBytes());
        return;
      }
      int rank = -1;
      int c = 0;
      for (Map.Entry<Double, TreeSet<String>> it : scores.get(key).entrySet()) {
        for (String member : it.getValue()) {
          if (member.equals(memeber)) {
            rank = c;
            break;
          }
          c++;
        }
        if (rank != -1) {
          break;
        }
      }
      if (rank == -1) {
        out.write("$-1\r\n".getBytes());
      } else {
        out.write((":" + rank + "\r\n").getBytes());
      }
    } else if (commands.get(0).equalsIgnoreCase("zadd")) {
      String key = commands.get(1);
      Double score = Double.parseDouble(commands.get(2));
      String member = commands.get(3);
      scores.putIfAbsent(key, new TreeMap<>());
      scores.get(key).putIfAbsent(score, new TreeSet<>());
      int present = 1;
      for (Map.Entry<Double, TreeSet<String>> it : scores.get(key).entrySet()) {
        if (it.getValue().contains(member)) {
          present = 0;
        }
      }
      scores.get(key).get(score).add(member);
      out.write((":" + present + "\r\n").getBytes());
    } else if (commands.get(0).equalsIgnoreCase("unsubscribe")) {
      if (subscibed.get(client).contains(commands.get(1))) {
        subscibed.get(client).remove(commands.get(1));
      }
      int len = subscibed.getOrDefault(client, new HashSet<>()).size();
      String data = "*3\r\n" + "$11\r\n" + "unsubscribe\r\n" + "$" + commands.get(1).length() + "\r\n"
          + commands.get(1) + "\r\n" + ":" + len
          + "\r\n";
      out.write(data.getBytes());
    } else if (commands.get(0).equalsIgnoreCase("publish")) {
      String channel = commands.get(1);
      String message = commands.get(2);
      String data = "*3\r\n$7\r\nmessage\r\n" + "$" + channel.length() + "\r\n" + channel + "\r\n$" + message.length()
          + "\r\n" + message + "\r\n";
      int count = 0;
      for (Map.Entry<Socket, Set<String>> entry : subscibed.entrySet()) {
        if (entry.getValue().contains(channel)) {
          entry.getKey().getOutputStream().write(data.getBytes());
          count++;
        }
      }
      out.write((":" + count + "\r\n").getBytes());
    } else if (commands.get(0).equalsIgnoreCase("subscribe")) {
      Set<String> channels = subscibed.getOrDefault(client, new HashSet<>());
      channels.add(commands.get(1));
      subscibed.put(client, channels);
      String response = "*3\r\n$9\r\nsubscribe\r\n" + "$" + commands.get(1).length() + "\r\n" + commands.get(1)
          + "\r\n" + ":" + channels.size() + "\r\n";
      out.write(response.getBytes());
    } else if (commands.get(0).equalsIgnoreCase("keys")) {
      String data = "*" + entries.size() + "\r\n";
      for (String str : entries.keySet()) {
        data += "$" + str.length() + "\r\n";
        data += str + "\r\n";
      }
      out.write(data.getBytes());
    } else if (commands.get(0).equalsIgnoreCase("config")) {
      String key = commands.get(2);
      String value = configInfo.get(key);
      String data = "*2\r\n" + "$" + key.length() + "\r\n" + key + "\r\n" + "$" + value.length() + "\r\n" + value
          + "\r\n";
      out.write(data.getBytes());
    } else if (commands.get(0).equalsIgnoreCase("wait")) {
      if (!slaves.containsKey(port) || slaves.get(port).isEmpty()) {
        out.write(":0\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        return;
      }
      if (!lastAck.containsKey(client)) {
        System.out.println("hi");
        out.write((":" + slaves.get(port).size() + "\r\n").getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        return;
      }
      int req = Integer.parseInt(commands.get(1));
      int timeout = Integer.parseInt(commands.get(2));
      Set<Socket> slaveSet = slaves.get(port);
      ExecutorService pool = Executors.newFixedThreadPool(Math.min(slaveSet.size(), 32));
      int count = 0;
      try {
        List<Callable<Integer>> tasks = new ArrayList<>();
        for (Socket skt : slaveSet) {
          tasks.add(() -> work(skt, timeout));
        }
        List<Future<Integer>> futures = pool.invokeAll(tasks, timeout, TimeUnit.MILLISECONDS);

        for (Future<Integer> f : futures) {
          try {
            if (!f.isCancelled()) {
              count += f.get(0, TimeUnit.MILLISECONDS);
              if (count == req) {
                break;
              }
            }
          } catch (Exception ignore) {
          }
        }
      } finally {
        pool.shutdownNow();
      }

      out.write((":" + count + "\r\n").getBytes(java.nio.charset.StandardCharsets.US_ASCII));
      return;
    } else if (commands.get(0).equalsIgnoreCase("psync")) {
      out.write("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n".getBytes());
      byte[] str = HexFormat.of().parseHex(
          "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2");
      out.write(("$" + str.length + "\r\n").getBytes());
      out.write(str);
      Set<Socket> slave = slaves.getOrDefault(port, new HashSet<>());
      slave.add(client);
      slaves.put(port, slave);
    } else if (commands.get(0).equalsIgnoreCase("REPLCONF")) {
      if (commands.size() == 3 && commands.get(1).equals("ACK")) {
        return;
      }
      if (commands.size() == 3 && commands.get(1).equals("GETACK") && commands.get(2).equals("*")) {
        String str = Integer.toString(used);
        String data = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n" + "$" + str.length() + "\r\n" + str + "\r\n";
        out.write(data.getBytes());
        out.flush();
        return;
      }
      out.write("+OK\r\n".getBytes());
    } else if (commands.get(0).equalsIgnoreCase("info")) {
      if (master != -1) {
        out.write("$10\r\nrole:slave\r\n".getBytes());
      } else {
        String body = "role:master\r\n" +
            "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n" +
            "master_repl_offset:0\r\n";

        byte[] b = body.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.write(("$" + b.length + "\r\n").getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        out.write(b);
        out.write("\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII));

      }
    } else if (commands.get(0).equalsIgnoreCase("echo")) {
      String p = commands.get(1);
      out.write(("$" + p.getBytes(StandardCharsets.UTF_8).length + "\r\n").getBytes(StandardCharsets.US_ASCII));
      out.write(p.getBytes(StandardCharsets.UTF_8));
      out.write("\r\n".getBytes(StandardCharsets.US_ASCII));

    } else if (commands.get(0).equalsIgnoreCase("ping")) {
      if (!isMaster) {
        out.write("+PONG\r\n".getBytes(StandardCharsets.US_ASCII));
      }
    } else if (commands.get(0).equalsIgnoreCase("set")) {
      if (commands.size() > 3) {
        Key key = new Key(commands.get(2), Instant.now().plusMillis(Long.parseLong(commands.get(4))));
        entries.put(commands.get(1), key);
      } else {
        Key key = new Key(commands.get(2), Instant.now().plusMillis(1_000_000_000L));
        entries.put(commands.get(1), key);
      }
      if (!isMaster) {
        out.write("+OK\r\n".getBytes(StandardCharsets.US_ASCII));
      }
    } else if (commands.get(0).equalsIgnoreCase("incr")) {
      if (!entries.containsKey(commands.get(1))) {
        Key key = new Key("1", Instant.now().plusMillis(1_000_000_000L));
        entries.put(commands.get(1), key);
        if (!isMaster) {
          out.write((":1" + "\r\n").getBytes(StandardCharsets.US_ASCII));
        }
        return;
      }
      Key key = entries.get(commands.get(1));
      Long val = -1L;
      try {
        val = Long.parseLong(key.value);
      } catch (NumberFormatException e) {
        if (!isMaster) {
          out.write("-ERR value is not an integer or out of range\r\n".getBytes());
        }
        return;
      }
      val++;
      key.value = Long.toString(val);
      entries.put(commands.get(1), key);
      if (!isMaster) {
        out.write((":" + val + "\r\n").getBytes(StandardCharsets.US_ASCII));
      }
    } else if (commands.get(0).equalsIgnoreCase("get")) {
      if (entries.containsKey(commands.get(1))) {
        Key key = entries.get(commands.get(1));
        if (Instant.now().isAfter(key.time)) {
          entries.remove(commands.get(1));
          out.write("$-1\r\n".getBytes(StandardCharsets.US_ASCII));
        } else {
          String p = key.value;
          byte[] b = p.getBytes(StandardCharsets.UTF_8);
          out.write(("$" + b.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
          out.write(b);
          out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
        }
      } else {
        out.write("$-1\r\n".getBytes(StandardCharsets.US_ASCII));
      }

    } else if (commands.get(0).equalsIgnoreCase("rpush")) {
      String name = commands.get(1);
      synchronized (lock) {
        List<String> entry = lists.get(name);
        if (entry == null)
          entry = new ArrayList<>();
        for (int i = 2; i < commands.size(); i++)
          entry.add(commands.get(i));
        lists.put(name, entry);
        lock.notifyAll();

        int len = entry.size();
        String p = Integer.toString(len);
        if (!isMaster) {
          out.write(":".getBytes(StandardCharsets.US_ASCII));
          out.write(p.getBytes(StandardCharsets.US_ASCII));
          out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
        }
      }

    } else if (commands.get(0).equalsIgnoreCase("lpush")) {
      String name = commands.get(1);
      synchronized (lock) {
        List<String> entry = lists.get(name);
        if (entry == null)
          entry = new ArrayList<>();
        for (int i = 2; i < commands.size(); i++)
          entry.add(0, commands.get(i));
        lists.put(name, entry);

        lock.notifyAll();

        int len = entry.size();
        String p = Integer.toString(len);
        if (!isMaster) {
          out.write(":".getBytes(StandardCharsets.US_ASCII));
          out.write(p.getBytes(StandardCharsets.US_ASCII));
          out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
        }
      }

    } else if (commands.get(0).equalsIgnoreCase("llen")) {
      List<String> entry = lists.get(commands.get(1));
      int len = (entry == null) ? 0 : entry.size();
      String p = Integer.toString(len);
      out.write(":".getBytes(StandardCharsets.US_ASCII));
      out.write(p.getBytes(StandardCharsets.US_ASCII));
      out.write("\r\n".getBytes(StandardCharsets.US_ASCII));

    } else if (commands.get(0).equalsIgnoreCase("lrange")) {
      String name = commands.get(1);
      int start = Integer.parseInt(commands.get(2));
      int end = Integer.parseInt(commands.get(3));
      List<String> list = lists.get(name);
      if (list == null || list.isEmpty()) {
        out.write("*0\r\n".getBytes(StandardCharsets.US_ASCII));
      } else {
        start = Math.max(start, -list.size());
        end = Math.max(end, -list.size());
        start = Math.min(start, list.size() - 1);
        end = Math.min(end, list.size() - 1);
        start = (start + list.size()) % list.size();
        end = (end + list.size()) % list.size();

        if (start >= list.size() || start > end) {
          out.write("*0\r\n".getBytes(StandardCharsets.US_ASCII));
        } else {
          int len = end - start + 1;
          out.write(("*" + len + "\r\n").getBytes(StandardCharsets.US_ASCII));
          for (int i = start; i <= end; i++) {
            String str = list.get(i);
            byte[] data = str.getBytes(StandardCharsets.UTF_8);
            out.write(("$" + data.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
            out.write(data);
            out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
          }
        }
      }

    } else if (commands.get(0).equalsIgnoreCase("lpop")) {
      String name = commands.get(1);
      List<String> list = lists.get(name);
      if (list == null || list.isEmpty()) {
        if (!isMaster) {
          out.write("$-1\r\n".getBytes(StandardCharsets.US_ASCII));
        }
      } else {
        if (commands.size() > 2) {
          int count = Math.min(Integer.parseInt(commands.get(2)), list.size());
          List<String> response = new ArrayList<>();
          while (count-- > 0) {
            response.add(list.remove(0));
          }
          respArray(out, response);
        } else {
          String str = list.remove(0);
          byte[] data = str.getBytes(StandardCharsets.UTF_8);
          if (!isMaster) {
            out.write(("$" + data.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
            out.write(data);
            out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
          }
        }
      }

    } else if (commands.get(0).equalsIgnoreCase("blpop")) {
      String key = commands.get(1);
      final double timeoutSecsD = Double.parseDouble(commands.get(2));
      final boolean waitForever = timeoutSecsD <= 0.0;

      final long deadline = waitForever
          ? Long.MAX_VALUE
          : System.nanoTime() + secsToNanos(timeoutSecsD);
      String popped = null;
      boolean timedOut = false;

      synchronized (lock) {
        List<String> list = lists.get(key);
        if (list != null && !list.isEmpty()) {
          popped = list.remove(0);
        } else {
          Waiter me = new Waiter(client, out, key);
          waitersByKey.computeIfAbsent(key, k -> new ArrayDeque<>()).addLast(me);

          for (;;) {
            long remaining = waitForever ? Long.MAX_VALUE : (deadline - System.nanoTime());
            if (!waitForever && remaining <= 0L) {
              Deque<Waiter> q = waitersByKey.get(key);
              if (q != null)
                q.remove(me);
              timedOut = true;
              break;
            }

            try {
              if (waitForever) {
                lock.wait();
              } else {
                long ms = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(remaining);
                int ns = (int) (remaining - java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(ms));
                lock.wait(ms, ns);
              }
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              Deque<Waiter> q = waitersByKey.get(key);
              if (q != null)
                q.remove(me);
              timedOut = true;
              break;
            }
            Deque<Waiter> q = waitersByKey.get(key);
            list = lists.get(key);
            if (q != null && q.peekFirst() == me && list != null && !list.isEmpty()) {
              q.pollFirst();
              popped = list.remove(0);
              break;
            }
          }
        }
      }
      if (!isMaster) {
        if (popped != null) {
          writeRespArray(out, java.util.List.of(key, popped));
        } else if (timedOut) {
          out.write("*-1\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        }
      }

    } else if (commands.get(0).equalsIgnoreCase("type")) {
      if (streams.containsKey(commands.get(1))) {
        out.write(("+stream\r\n").getBytes());
      } else if (entries.containsKey(commands.get(1))) {
        out.write(("+string\r\n").getBytes());
      } else {
        out.write(("+none\r\n").getBytes());
      }
    } else if (commands.get(0).equalsIgnoreCase("xadd")) {
      if (commands.get(2).equals("*")) {
        commands.set(2, generateUnixId());
      } else if (commands.get(2).split("-")[1].equals("*")) {
        String ids[] = commands.get(2).split("-");
        ids[1] = generateSeq(commands.get(1), ids[0]);
        StringBuilder builder = new StringBuilder();
        builder.append(ids[0]);
        builder.append("-");
        builder.append(ids[1]);
        commands.set(2, builder.toString());
      }
      if (check_0(commands.get(2))) {
        if (!isMaster) {
          out.write(("-ERR The ID specified in XADD must be greater than 0-0\r\n").getBytes());
        }
        return;
      }
      HashMap<String, HashMap<String, String>> newEntries = new HashMap<>();
      if (streams.containsKey(commands.get(1))) {
        newEntries = streams.get(commands.get(1));
        String last = "";
        for (Map.Entry<String, HashMap<String, String>> entry : newEntries.entrySet()) {
          last = entry.getKey();
        }
        if (check_inc(last, commands.get(2))) {
          if (!isMaster) {
            out.write(
                ("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                    .getBytes());
          }
          return;
        }
      }
      HashMap<String, String> entries = new HashMap<>();
      for (int i = 3; i < commands.size(); i += 2) {
        entries.put(commands.get(i), commands.get(i + 1));
      }
      newEntries.put(commands.get(2), entries);
      streams.put(commands.get(1), newEntries);
      String p = commands.get(2);
      byte[] b = p.getBytes(StandardCharsets.UTF_8);
      if (!isMaster) {
        out.write(("$" + b.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
        out.write(b);
        out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
      }
      if (locks.containsKey(commands.get(1))) {
        synchronized (locks.get(commands.get(1))) {
          locks.get(commands.get(1)).notifyAll();
        }
      }
    } else if (commands.get(0).equalsIgnoreCase("xrange")) {
      if (commands.get(2).equals("-")) {
        commands.set(2, "0-0");
      }
      if (commands.get(3).equals("+")) {
        long currentUnixTimeMillis = System.currentTimeMillis();
        commands.set(3, Long.toString(currentUnixTimeMillis) + "-0");
      }
      if (commands.get(2).split("-").length == 1) {
        commands.set(2, commands.get(2) + "-0");
      }
      if (commands.get(3).split("-").length == 1) {
        commands.set(3, commands.get(3) + "-1000000000");
      }
      findByRange(streams.get(commands.get(1)), commands.get(2), commands.get(3), out);
    } else if (commands.get(0).equalsIgnoreCase("xread")) {
      if (commands.get(1).equalsIgnoreCase("BLOCK")) {
        if (commands.size() == 6) {
          readBlock(Long.parseLong(commands.get(2)), commands.get(4), getLastStart(commands.get(4)), out);
          return;
        }
        readBlock(Long.parseLong(commands.get(2)), commands.get(4), commands.get(5), out);
        return;
      }
      int len = (commands.size() - 2) / 2;
      if (!isMaster) {
        out.write(("*" + len + "\r\n").getBytes());
      }
      for (int i = 2; i < 2 + len; i++) {
        readRange(commands.get(i), commands.get(i + len), out);
      }
    }
    if (!slaves.containsKey(port)) {
      return;
    }
    String str = commands.get(0);
    if (str.equalsIgnoreCase("set") || str.equalsIgnoreCase("incr") || str.equalsIgnoreCase("rpush")
        || str.equalsIgnoreCase("lpush") || str.equalsIgnoreCase("lpop") || str.equalsIgnoreCase("blpop")
        || str.equalsIgnoreCase("xadd")) {
      for (Socket socket : slaves.get(port)) {
        respArray(socket.getOutputStream(), commands);
      }
    }
  }

  static String getLastStart(String key) {
    String response = "0-0";
    if (streams.containsKey(key)) {
      HashMap<String, HashMap<String, String>> entries = streams.get(key);
      for (Map.Entry<String, HashMap<String, String>> it : entries.entrySet()) {
        response = it.getKey();
      }
    }
    System.out.println(response);
    return response;
  }

  static void readBlock(long timeoutMs, String key, String start, OutputStream out) throws IOException {
    final boolean waitForever = timeoutMs <= 0;
    final long deadline = waitForever
        ? Long.MAX_VALUE
        : System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(timeoutMs);

    final Object keyLock = locks.computeIfAbsent(key, k -> new Object());
    boolean hasData;
    System.out.println(waitForever);
    synchronized (keyLock) {
      while (check(key, start)) {
        if (waitForever) {
          try {
            keyLock.wait();
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        } else {
          long remaining = deadline - System.nanoTime();
          if (remaining <= 0L)
            break;
          long ms = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(remaining);
          int ns = (int) (remaining - java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(ms));
          try {
            keyLock.wait(ms, ns);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
      hasData = !check(key, start);
    }

    if (hasData) {
      out.write(("*1\r\n").getBytes());
      readRange(key, start, out);
    } else {
      out.write("*-1\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
    }
  }

  static boolean check(String id, String start) {
    int count = 0;
    HashMap<String, HashMap<String, String>> entries = streams.get(id);
    for (Map.Entry<String, HashMap<String, String>> it : entries.entrySet()) {
      if (checkRange(it.getKey(), start)) {
        count++;
        break;
      }
    }
    return count == 0;
  }

  static void readRange(String id, String start,
      OutputStream out) throws IOException {
    List<String> keys = new ArrayList<>();
    HashMap<String, HashMap<String, String>> entries = streams.get(id);
    for (Map.Entry<String, HashMap<String, String>> it : entries.entrySet()) {
      if (checkRange(it.getKey(), start)) {
        keys.add(it.getKey());
      }
    }
    out.write(("*2\r\n").getBytes());
    out.write(("$" + id.length() + "\r\n" + id + "\r\n").getBytes());
    out.write(("*" + keys.size() + "\r\n").getBytes());
    for (String key : keys) {
      HashMap<String, String> entry = entries.get(key);
      int len = 2 * entry.size();
      out.write(("*2" + "\r\n").getBytes());
      out.write(("$" + key.length() + "\r\n" + key + "\r\n").getBytes());
      out.write(("*" + len + "\r\n").getBytes());
      for (Map.Entry<String, String> it : entry.entrySet()) {
        out.write(("$" + it.getKey().length() + "\r\n" + it.getKey() + "\r\n").getBytes());
        out.write(("$" + it.getValue().length() + "\r\n" + it.getValue() + "\r\n").getBytes());
      }
    }
  }

  static void findByRange(HashMap<String, HashMap<String, String>> entries, String start, String end,
      OutputStream out) throws IOException {
    List<String> keys = new ArrayList<>();
    for (Map.Entry<String, HashMap<String, String>> it : entries.entrySet()) {
      if (checkRange(it.getKey(), start, end)) {
        keys.add(it.getKey());
      }
    }
    out.write(("*" + keys.size() + "\r\n").getBytes());
    for (String key : keys) {
      HashMap<String, String> entry = entries.get(key);
      int len = 2 * entry.size();
      out.write(("*2" + "\r\n").getBytes());
      out.write(("$" + key.length() + "\r\n" + key + "\r\n").getBytes());
      out.write(("*" + len + "\r\n").getBytes());
      for (Map.Entry<String, String> it : entry.entrySet()) {
        out.write(("$" + it.getKey().length() + "\r\n" + it.getKey() + "\r\n").getBytes());
        out.write(("$" + it.getValue().length() + "\r\n" + it.getValue() + "\r\n").getBytes());
      }
    }
  }

  static boolean checkRange(String key, String start, String end) {
    String[] inputs1 = key.split("-");
    String[] inputs2 = start.split("-");
    String[] inputs3 = end.split("-");
    Long x1 = Long.parseLong(inputs1[0]);
    Long y1 = Long.parseLong(inputs1[1]);
    Long x2 = Long.parseLong(inputs2[0]);
    Long y2 = Long.parseLong(inputs2[1]);
    Long x3 = Long.parseLong(inputs3[0]);
    Long y3 = Long.parseLong(inputs3[1]);
    if (x1 < x2 || x1 > x3) {
      return false;
    }
    if (x1 == x2 && y1 < y2) {
      return false;
    }
    if (x1 == x3 && y1 > y3) {
      return false;
    }
    return true;
  }

  static boolean checkRange(String key, String start) {
    String[] inputs1 = key.split("-");
    String[] inputs2 = start.split("-");
    Long x1 = Long.parseLong(inputs1[0]);
    Long y1 = Long.parseLong(inputs1[1]);
    Long x2 = Long.parseLong(inputs2[0]);
    Long y2 = Long.parseLong(inputs2[1]);
    if (x1 < x2) {
      return false;
    }
    if (x1 == x2 && y1 <= y2) {
      return false;
    }
    return true;
  }

  static String generateUnixId() {
    StringBuilder sb = new StringBuilder();
    long currentUnixTimeMillis = System.currentTimeMillis();
    sb.append(Long.toString(currentUnixTimeMillis));
    sb.append("-");
    sb.append("0");
    return sb.toString();
  }

  static String generateSeq(String key, String id) {
    if (streams.containsKey(key)) {
      String last = "";
      for (Map.Entry<String, HashMap<String, String>> entry : streams.get(key).entrySet()) {
        last = entry.getKey();
      }
      if (last.split("-")[0].equals(id)) {
        long k = Long.parseLong(last.split("-")[1]);
        k++;
        return Long.toString(k);
      }
      return "0";
    } else {
      if (id.equals("0")) {
        return "1";
      }
      return "0";
    }
  }

  static boolean check_0(String id) {
    String[] ids = id.split("-");
    if (ids[0].equals("0") && ids[1].equals("0")) {
      return true;
    }
    return false;
  }

  static boolean check_inc(String last, String id) {
    String[] input1 = last.split("-");
    String[] input2 = id.split("-");
    Long a = Long.parseLong(input1[0]);
    Long b = Long.parseLong(input2[0]);
    if (b < a) {
      return true;
    }
    if (b > a) {
      return false;
    }
    a = Long.parseLong(input1[1]);
    b = Long.parseLong(input2[1]);
    if (b > a) {
      return false;
    }
    return true;
  }

  static long secsToNanos(double secs) {
    if (secs >= (Long.MAX_VALUE / 1_000_000_000d))
      return Long.MAX_VALUE;
    long ns = (long) Math.round(secs * 1_000_000_000d);
    return Math.max(ns, 0L);
  }

  static void respArray(OutputStream out, List<String> response) throws IOException {
    out.write(("*" + response.size() + "\r\n").getBytes(StandardCharsets.US_ASCII));
    for (String s : response) {
      if (s == null) {
        out.write("$-1\r\n".getBytes(StandardCharsets.US_ASCII));
      } else {
        byte[] data = s.getBytes(StandardCharsets.UTF_8);
        out.write(("$" + data.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
        out.write(data);
        out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
      }
    }
  }

  static void writeRespArray(OutputStream out, List<String> items) throws IOException {
    out.write(("*" + items.size() + "\r\n").getBytes(StandardCharsets.US_ASCII));
    for (String s : items) {
      byte[] data = s.getBytes(StandardCharsets.UTF_8);
      out.write(("$" + data.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
      out.write(data);
      out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
    }
  }
}