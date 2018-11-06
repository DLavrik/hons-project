import java.io.IOException;
import java.io.*;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Scanner;
public class testclient {
    private Socket socket;
    private Scanner scanner;
        private Scanner scanner2;
    private BufferedReader in;
    private testclient(InetAddress serverAddress, int serverPort) throws Exception {
        this.socket = new Socket(serverAddress, serverPort);
        this.scanner = new Scanner(System.in);
	this.scanner2 = new Scanner(this.socket.getInputStream());
	this.in = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
	
    }
    private void start() throws IOException {
        String input;
	String inputLine;
        while (true) {
            input = scanner.nextLine();
            PrintWriter out = new PrintWriter(this.socket.getOutputStream(), true);
            out.println("I"+ deconstructNumber(2) + "aa" +deconstructNumber(1) + "1" + deconstructNumber(5) + "aaaaa");
            out.flush();
	    //inputLine = scanner2.nextLine();
	    //System.out.println("GOT " + inputLine);
	    while ((inputLine = in.readLine()) != null) {
	    	System.out.println("GOT " + inputLine);
	    	break;
	 	}
        }
    }
    private String deconstructNumber(int n)
    {
	byte first =(byte)( (n >> 24 )& 0xFF);
	byte second = (byte)((n >> 16) & 0xFF);
	byte third =(byte)( (n >> 8 )& 0xFF);
	byte fourth = (byte)(n & 0xFF);
	return "" + (char)first + (char)second + (char)third + (char)fourth;
    }
    public static void main(String[] args) throws Exception {
        testclient client = new testclient(
                InetAddress.getByName(args[0]), 
                Integer.parseInt(args[1]));
        
        System.out.println("\r\nConnected to Server: " + client.socket.getInetAddress());
        client.start();                
    }
}
