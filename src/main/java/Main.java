import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Main {
  public static void main(String[] args) throws IOException {
    System.out.println("Logs from your program will appear here!");
    int port = 6379;
    final ServerSocket serverSocket = new ServerSocket(port);
    serverSocket.setReuseAddress(true);
    while (true) {
      Socket clientSocket = serverSocket.accept();
      new Thread(() -> {
        try {
          handle(clientSocket);
        } catch (IOException e) {
        }
      }).start();
    }
  }

  static void handle(Socket client) throws IOException {
    try {
      InputStream in = client.getInputStream();
      OutputStream out = client.getOutputStream();
      byte[] buf = new byte[1024];
      while (in.read(buf) != -1) {
        out.write("+PONG\r\n".getBytes(StandardCharsets.US_ASCII));
        out.flush();
      }
    } catch (IOException ignored) {
    } finally {
      client.close();
    }
  }
}
