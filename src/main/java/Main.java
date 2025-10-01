import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
  static Map<String, String> entries = new HashMap<>();

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
        for (int i = used; i < used + n; i++) {
          if (buf[i] == '$') {
            if (sb.length() > 0) {
              commands.add(sb.toString());
              sb.setLength(0);
            }
          }
          if (buf[i] >= 65 && buf[i] <= 90) {
            sb.append((char) buf[i]);
          }
          if (buf[i] >= 97 && buf[i] <= 122) {
            sb.append((char) buf[i]);
          }
        }
        if (sb.length() > 0) {
          commands.add(sb.toString());
        }
        used += n;
        if (commands.get(0).equalsIgnoreCase("echo")) {
          String p = commands.get(1);
          out.write(("$" + p.length() + "\r\n").getBytes());
          out.write(p.getBytes());
          out.write("\r\n".getBytes());
        } else if (commands.get(0).equalsIgnoreCase("ping")) {
          out.write("+PONG\r\n".getBytes());
        } else if (commands.get(0).equalsIgnoreCase("set")) {
          entries.put(commands.get(1), commands.get(2));
          out.write("+OK\r\n".getBytes());
        } else if (commands.get(0).equalsIgnoreCase("get")) {
          if (entries.containsKey(commands.get(1))) {
            String p = entries.get(commands.get(1));
            out.write(("$" + p.length() + "\r\n").getBytes());
            out.write(p.getBytes());
            out.write("\r\n".getBytes());
          } else {
            out.write("$-1\r\n".getBytes());
          }
        }
      }
    } catch (IOException ignored) {
    } finally {
      client.close();
    }
  }
}
