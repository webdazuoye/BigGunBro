package network;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

public class TcpClient {
    private static final String IP = "192.168.161.120";
    private static final int PORT = 8080;

    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.out.println("我要把图片上传到服务器：" + IP);
        for(int i = 0;i <= 10;i++) {
            String filePath = "C:\\Users\\Sofia\\OneDrive\\桌面\\分割后的文件\\split" + i + ".jpg";
            try {
                //建立连接
                Socket socket = new Socket(IP, PORT);
                //读取图像文件上传
                FileInputStream fis = new FileInputStream(filePath);
                OutputStream os = socket.getOutputStream();

                String fileName = "split" + i + ".jpg";
                //重点：start
                //向服务器端传文件名
                DataOutputStream dataOutputStream = new DataOutputStream(os);
                dataOutputStream.writeUTF(fileName);
                dataOutputStream.flush();// 刷新流，传输到服务端

                byte[] buff = new byte[2 * 1024];
                int length = 0;

                while ((length = fis.read(buff)) != -1) {
                    os.write(buff, 0, length);
                }

                socket.shutdownOutput();

                //读取响应的数据
                InputStream is = socket.getInputStream();
                byte[] readBuff = new byte[1024];
                length = is.read(readBuff);
                String message = new String(readBuff, 0, length, StandardCharsets.UTF_8);
                System.out.println("服务器的响应结束：" + message);

            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}