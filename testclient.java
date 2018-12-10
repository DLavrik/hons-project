import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.MappedByteBuffer;
import java.nio.ByteBuffer;
import java.io.*;

// bytes 0-4 is write index
// bytes 4-8 is read index

public class testclient {
    private static MappedByteBuffer req_b;
    private static MappedByteBuffer resp_b;
    private static int WriteIndex;
    private static int ReadIndex;
    private static int BufferSize;
    private static int BufferStartIndex;

    public static void main( String[] args ) throws Throwable {
	BufferSize = 20;
	BufferStartIndex = 8;

	
        File req_f = new File("req_buffer");
	File resp_f = new File("resp_buffer");
	ByteArrayOutputStream message = new ByteArrayOutputStream();
	ByteArrayOutputStream fullMessage = new ByteArrayOutputStream();
        FileChannel req_channel = FileChannel.open( req_f.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE );
	FileChannel resp_channel = FileChannel.open( resp_f.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE );

        req_b = req_channel.map( MapMode.READ_WRITE, 0, BufferSize + BufferStartIndex);
	resp_b = resp_channel.map( MapMode.READ_WRITE, 0 , BufferSize + BufferStartIndex);
	req_b.position(BufferStartIndex);
	resp_b.position(BufferStartIndex);
	updateReadIndex(BufferStartIndex);
	updateWriteIndex(BufferStartIndex);	

	message.write("R".getBytes(),0,1);
	message.write(deconstructNumber(4));
	message.write("test".getBytes(),0,4);
	message.write(deconstructNumber(1));
	message.write("0".getBytes(),0,1);
	fullMessage.write(deconstructNumber(message.toByteArray().length),0,4);
	message.writeTo(fullMessage);
	//for(int i = 0;i<1;i++)
	while(true)
	    {
		addToBuffer(fullMessage.toByteArray());
	    }

    }
    private static  byte[] deconstructNumber(int n)
    {
	byte[] bytes = ByteBuffer.allocate(4).putInt(n).array();
	return bytes;
    }
    private static void addToBuffer(byte[] byteArray)
    // writes byteArray starting at position WriteIndex, overlaps if reaches end of buffer
	
    {
	boolean added = false;
	while(!added)
	    {
		int bytesAvailable = getBytesAvailable();
		System.out.println("Bytes available: " + bytesAvailable + " \n");
		if(bytesAvailable >= byteArray.length)
		    {
			if(BufferSize + BufferStartIndex - WriteIndex <= byteArray.length) // need to overlap
			    {
				System.out.println("Splitting \n");
				req_b.put(byteArray,0, BufferSize + BufferStartIndex - WriteIndex);
				
				req_b.position(BufferStartIndex); // reset position to the beginning
				req_b.put(byteArray,BufferSize + BufferStartIndex - WriteIndex, byteArray.length - (BufferSize + BufferStartIndex - WriteIndex));
				updateWriteIndex(BufferStartIndex + byteArray.length - (BufferSize + BufferStartIndex - WriteIndex));
				added = true;
			    }
			else
			    {
				System.out.println("Writing normally");
				req_b.put(byteArray,0, byteArray.length);
				updateWriteIndex(WriteIndex + byteArray.length);
				added = true;
			    }
		    }
		else
		    {
			System.out.println("Not enough buffer space available, waiting for reader to advance");
		    }
	    }
    }
    private static int getBytesAvailable()
    {
	ReadIndex = getReadIndex();
	if(WriteIndex > ReadIndex)
	    {
		return BufferSize - (WriteIndex - ReadIndex);
	    }
	else if(WriteIndex < ReadIndex)
	    {
		return ReadIndex - WriteIndex;
	    }
	else return BufferSize;
    }
    private static void updateWriteIndex(int newIndex)
    {
	/*	if(newIndex >= BufferSize + BufferStartIndex)
	    {
		newIndex = BufferStartIndex;
		}*/
	WriteIndex = newIndex;
	int prevPosition = req_b.position();
	req_b.position(0);
	req_b.put(deconstructNumber(newIndex));
	req_b.position(prevPosition);
    }
    
    private static void updateReadIndex(int newIndex)
    {
	ReadIndex = newIndex;
	int prevPosition = req_b.position();
	req_b.position(4);
	req_b.put(deconstructNumber(newIndex));
	req_b.position(prevPosition);
    }

    private static int getReadIndex()
    {
	return req_b.getInt(4);
    }
}
