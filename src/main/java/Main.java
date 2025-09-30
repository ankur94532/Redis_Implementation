import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args) throws IOException {
    System.out.println("Logs from your program will appear here!");
    int port = 6379;
    final ServerSocket serverSocket = new ServerSocket(port);
    serverSocket.setReuseAddress(true);
    Thread thread = new Thread(() -> {
      Socket clientSocket = null;
      try {
        clientSocket = serverSocket.accept();
        while (true) {
          byte[] input = new byte[1024];
          clientSocket.getInputStream().read(input);
          clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      } finally {
        try {
          if (clientSocket != null) {
            clientSocket.close();
          }
          serverSocket.close();
        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        }
      }
    });
  }
}
