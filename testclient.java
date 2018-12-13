import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.MappedByteBuffer;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
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
    private static int ALLIGNMENT;
    private static boolean endOfBufferFlag;

    public static void main( String[] args ) throws Throwable {
	BufferSize = 200;
	BufferStartIndex = 8;
	ALLIGNMENT = 4;
	endOfBufferFlag = true;
	
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
		TimeUnit.SECONDS.sleep(1);
	    }

    }
    private static  byte[] deconstructNumber(int n)
    {
	byte[] bytes = ByteBuffer.allocate(4).putInt(n).array();
	return bytes;
    }

    private static int allign(int index)
    {
	int newIndex = index; 
	if(index % ALLIGNMENT != 0)
	    newIndex = index + ALLIGNMENT - (index % ALLIGNMENT);
	return newIndex; 
    }
    
    private static void addToBuffer(byte[] byteArray) throws InterruptedException	
    {
	boolean added = false;
	while(!added)
	    {
		int bytesAvailable = getBytesAvailable();
		int allignedBytesAvailable = bytesAvailable - (bytesAvailable % ALLIGNMENT);
		int bytesNeeded = allign(byteArray.length);
		if(allignedBytesAvailable >= bytesNeeded) // we can write to buffer
		    {
			System.out.println("Writing normally at " + WriteIndex);
			req_b.put(byteArray,0, byteArray.length);
			updateWriteIndex(WriteIndex + bytesNeeded);
			added = true;
		    }
		else if (!endOfBufferFlag) // we bumped into reader
		    {
			System.out.println("Reader has not advanced enough to write data");
			TimeUnit.SECONDS.sleep(1);
		    }
		else // end of buffer was reached, need to write from starting index again
		    {
			System.out.println("End of buffer reached, starting from beginning");
			markEnd(); //telling reader it should start reading from beginning again
			updateWriteIndex(BufferStartIndex);
			endOfBufferFlag = false;
			TimeUnit.SECONDS.sleep(1);
		    }			
	    }	    
    }

    private static void markEnd()
    {
	if(WriteIndex < BufferSize + BufferStartIndex) 
	    {
		System.out.println("Marking end at " + WriteIndex);
		req_b.put((byte)0xFF); //reader will see this byte and know it has to read from start again
	    }
    }
    
    private static int getBytesAvailable() //returns how many bytes left till end of buffer or the read index,
    {                                      //sets a flag telling which one marks the end of available bytes
	ReadIndex = getReadIndex();
	if(WriteIndex > ReadIndex)
	    {
		endOfBufferFlag = true;
		return BufferSize + BufferStartIndex - WriteIndex;
	    }
	else if(WriteIndex < ReadIndex)
	    {
		endOfBufferFlag = false;
		return ReadIndex - WriteIndex;
	    }
	else
	    {
		//need to check what happened before,
		//did reader catch up to the writer,
		//or vice versa
		if(!endOfBufferFlag)
		    
		    { // writer bumped into reader
			System.out.println("Bumped into reader");
			//	System.exit(0);
			return 0;
		    }
		else // reader caught up to writer
		    {
			System.out.println("Reader caught up");
			return BufferSize + BufferStartIndex - WriteIndex;
			
		    }
	    }
    }
    private static void updateWriteIndex(int newIndex)
    {
	WriteIndex = newIndex;
	//	int prevPosition = req_b.position();
	req_b.position(0);
	req_b.put(deconstructNumber(newIndex));
	req_b.position(newIndex);
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
