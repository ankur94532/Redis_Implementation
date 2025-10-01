import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.time.Instant;

class Key {
  public String value;
  public Instant time;

  public Key(String value, Instant time) {
    this.value = value;
    this.time = time;
  }
}

public class Main {
  static Map<String, Key> entries = new HashMap<>();
  static Map<String, List<String>> lists = new HashMap<>();

  public static void main(String[] args) throws IOException {
    System.out.println("Logs from your program will appear here!");
    int port = 6379;
    final ServerSocket serverSocket = new ServerSocket(port);
    serverSocket.setReuseAddress(true);
    try {
      while (true) {
        Socket clientSocket = serverSocket.accept();
        new Thread(() -> {
          try {
            handle(clientSocket);
          } catch (IOException e) {
          }
        }).start();
      }
    } finally {
      serverSocket.close();
    }
  }

  static void handle(Socket client) throws IOException {
    try {
      InputStream in = client.getInputStream();
      OutputStream out = client.getOutputStream();
      byte[] buf = new byte[8192];
      int used = 0;
      while (true) {
        int n = in.read(buf, used, buf.length - used);
        if (n == -1) {
          break;
        }
        List<String> commands = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        for (int i = used; i < used + n;) {
          if (buf[i] == '*') {
            i++;
            while (i < used + n && buf[i] >= 48 && buf[i] <= 57) {
              i++;
            }
            continue;
          }
          if (buf[i] == '$') {
            if (sb.length() > 0) {
              commands.add(sb.toString());
              sb.setLength(0);
            }
            i++;
            while (i < used + n && buf[i] >= 48 && buf[i] <= 57) {
              i++;
            }
            continue;
          }
          if (buf[i] >= 65 && buf[i] <= 90) {
            sb.append((char) buf[i]);
          }
          if (buf[i] >= 97 && buf[i] <= 122) {
            sb.append((char) buf[i]);
          }
          if (buf[i] >= 48 && buf[i] <= 57) {
            sb.append((char) buf[i]);
          }
          if (buf[i] == 45) {
            sb.append((char) buf[i]);
          }
          i++;
        }
        if (sb.length() > 0) {
          commands.add(sb.toString());
        }
        /*
         * for (String command : commands) {
         * System.out.println(command);
         * }
         */
        used += n;
        if (commands.get(0).equalsIgnoreCase("echo")) {
          String p = commands.get(1);
          out.write(("$" + p.length() + "\r\n").getBytes());
          out.write(p.getBytes());
          out.write("\r\n".getBytes());
        } else if (commands.get(0).equalsIgnoreCase("ping")) {
          out.write("+PONG\r\n".getBytes());
        } else if (commands.get(0).equalsIgnoreCase("set")) {
          if (commands.size() > 3) {
            Key key = new Key(commands.get(2), Instant.now().plusMillis(Long.parseLong(commands.get(4))));
            entries.put(commands.get(1), key);
          } else {
            Key key = new Key(commands.get(2), Instant.now().plusMillis(1000000000));
            entries.put(commands.get(1), key);
          }
          out.write("+OK\r\n".getBytes());
        } else if (commands.get(0).equalsIgnoreCase("get")) {
          if (entries.containsKey(commands.get(1))) {
            Key key = entries.get(commands.get(1));
            if (Instant.now().isAfter(key.time)) {
              entries.remove(commands.get(1));
              out.write("$-1\r\n".getBytes());
            } else {
              String p = entries.get(commands.get(1)).value;
              out.write(("$" + p.length() + "\r\n").getBytes());
              out.write(p.getBytes());
              out.write("\r\n".getBytes());
            }
          } else {
            out.write("$-1\r\n".getBytes());
          }
        } else if (commands.get(0).equalsIgnoreCase("rpush")) {
          List<String> entry = new ArrayList<>();
          if (lists.containsKey(commands.get(1))) {
            entry = lists.get(commands.get(1));
          }
          for (int i = 2; i < commands.size(); i++) {
            entry.add(commands.get(i));
          }
          lists.put(commands.get(1), entry);
          int len = entry.size();
          String p = Integer.toString(len);
          out.write((":").getBytes());
          out.write(p.getBytes());
          out.write("\r\n".getBytes());
        } else if (commands.get(0).equalsIgnoreCase("lrange")) {
          String name = commands.get(1);
          int start = Integer.parseInt(commands.get(2));
          int end = Integer.parseInt(commands.get(3));
          if (!lists.containsKey(name)) {
            out.write("*0\r\n".getBytes());
          } else {
            List<String> list = lists.get(name);
            start = Math.max(start, -list.size());
            end = Math.max(end, -list.size());
            start = Math.min(start, list.size() - 1);
            end = Math.min(end, list.size() - 1);
            start = (start + list.size()) % list.size();
            end = (end + list.size()) % list.size();
            if (start >= list.size() || start > end) {
              out.write("*0\r\n".getBytes());
            } else {
              int len = end - start + 1;
              out.write(("*" + Integer.toString(len) + "\r\n").getBytes());
              for (int i = start; i <= end; i++) {
                String str = list.get(i);
                out.write(("$" + Integer.toString(str.length()) + "\r\n").getBytes());
                out.write((str + "\r\n").getBytes());
              }
            }
          }
        } else if (commands.get(0).equalsIgnoreCase("lpush")) {
          List<String> entry = new ArrayList<>();
          if (lists.containsKey(commands.get(1))) {
            entry = lists.get(commands.get(1));
          }
          for (int i = 2; i < commands.size(); i++) {
            entry.add(0, commands.get(i));
          }
          lists.put(commands.get(1), entry);
          int len = entry.size();
          String p = Integer.toString(len);
          out.write((":").getBytes());
          out.write(p.getBytes());
          out.write("\r\n".getBytes());
        } else if (commands.get(0).equalsIgnoreCase("llen")) {
          List<String> entry = new ArrayList<>();
          if (lists.containsKey(commands.get(1))) {
            entry = lists.get(commands.get(1));
          }
          int len = entry.size();
          String p = Integer.toString(len);
          out.write((":").getBytes());
          out.write(p.getBytes());
          out.write("\r\n".getBytes());
        } else if (commands.get(0).equalsIgnoreCase("lpop")) {
          if (!lists.containsKey(commands.get(1))) {
            out.write("$-1\r\n".getBytes());
          } else {
            if (commands.size() > 2) {
              int count = Integer.parseInt(commands.get(2));
              count = Math.min(count, lists.get(commands.get(1)).size());
              List<String> response = new ArrayList<>();
              while (count > 0) {
                response.add(lists.get(commands.get(1)).get(0));
                lists.get(commands.get(1)).remove(0);
                count--;
              }
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
            } else {
              String str = lists.get(commands.get(1)).get(0);
              lists.get(commands.get(1)).remove(0);
              out.write(("$" + Integer.toString(str.length()) + "\r\n").getBytes());
              out.write((str + "\r\n").getBytes());
            }
          }
        }
      }
    } catch (IOException ignored) {
    } finally {
      client.close();
    }
  }
}
