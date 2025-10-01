import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

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
  static Map<String, List<String>> lists = new ConcurrentHashMap<>();
  static final Map<String, ArrayDeque<Waiter>> waitersByKey = new HashMap<>();

  static final Object lock = new Object();

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
            e.printStackTrace();
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
            while (i < used + n && buf[i] >= '0' && buf[i] <= '9')
              i++;
            continue;
          }
          if (buf[i] == '$') {
            if (sb.length() > 0) {
              commands.add(sb.toString());
              sb.setLength(0);
            }
            i++;
            while (i < used + n && buf[i] >= '0' && buf[i] <= '9')
              i++;
            continue;
          }
          if ((buf[i] >= 'A' && buf[i] <= 'Z') ||
              (buf[i] >= 'a' && buf[i] <= 'z') ||
              (buf[i] >= '0' && buf[i] <= '9') ||
              buf[i] == '-') {
            sb.append((char) buf[i]);
          }
          i++;
        }
        if (sb.length() > 0)
          commands.add(sb.toString());
        used += n;

        if (commands.isEmpty())
          continue;

        if (commands.get(0).equalsIgnoreCase("echo")) {
          String p = commands.get(1);
          out.write(("$" + p.getBytes(StandardCharsets.UTF_8).length + "\r\n").getBytes(StandardCharsets.US_ASCII));
          out.write(p.getBytes(StandardCharsets.UTF_8));
          out.write("\r\n".getBytes(StandardCharsets.US_ASCII));

        } else if (commands.get(0).equalsIgnoreCase("ping")) {
          out.write("+PONG\r\n".getBytes(StandardCharsets.US_ASCII));

        } else if (commands.get(0).equalsIgnoreCase("set")) {
          if (commands.size() > 3) {
            Key key = new Key(commands.get(2), Instant.now().plusMillis(Long.parseLong(commands.get(4))));
            entries.put(commands.get(1), key);
          } else {
            Key key = new Key(commands.get(2), Instant.now().plusMillis(1_000_000_000L));
            entries.put(commands.get(1), key);
          }
          out.write("+OK\r\n".getBytes(StandardCharsets.US_ASCII));

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
            out.write(":".getBytes(StandardCharsets.US_ASCII));
            out.write(p.getBytes(StandardCharsets.US_ASCII));
            out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
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
            out.write(":".getBytes(StandardCharsets.US_ASCII));
            out.write(p.getBytes(StandardCharsets.US_ASCII));
            out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
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
            out.write("$-1\r\n".getBytes(StandardCharsets.US_ASCII));
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
              out.write(("$" + data.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
              out.write(data);
              out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
            }
          }

        } else if (commands.get(0).equalsIgnoreCase("blpop")) {
          String key = commands.get(1);
          final double timeoutSecsD = Double.parseDouble(commands.get(2));
          final boolean waitForever = timeoutSecsD <= 0.0;

          final long deadline = waitForever
              ? Long.MAX_VALUE
              : System.nanoTime() + secsToNanos(timeoutSecsD);
          System.out.println(secsToNanos(timeoutSecsD));
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

          if (popped != null) {
            writeRespArray(out, java.util.List.of(key, popped));
          } else if (timedOut) {
            out.write("*-1\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
          }

        }
      }
    } catch (IOException ignored) {
    } finally {
      try {
        client.close();
      } catch (IOException ignore) {
      }
    }
  }

  static long secsToNanos(double secs) {
    // clamp huge values
    if (secs >= (Long.MAX_VALUE / 1_000_000_000d))
      return Long.MAX_VALUE;
    long ns = (long) Math.round(secs * 1_000_000_000d); // keep fractions
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
