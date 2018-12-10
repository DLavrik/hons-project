/*
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db.rocksdb;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.Status;
import net.jcip.annotations.GuardedBy;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.InetAddress;
import java.net.Socket;
import java.util.Scanner;
import java.nio.ByteBuffer;
/**
 * RocksDB binding for <a href="http://rocksdb.org/">RocksDB</a>.
 *
 * See {@code rocksdb/README.md} for details.
 */
public class RocksDBClient extends DB {

  private Socket socket;
  private BufferedReader in;
  private DataInputStream is;
  private ByteArrayOutputStream message;
  private DataOutputStream out;
    private boolean debug;
  static final String PROPERTY_ROCKSDB_DIR = "rocksdb.dir";
  private static final String COLUMN_FAMILY_NAMES_FILENAME = "CF_NAMES";

  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBClient.class);

  @GuardedBy("RocksDBClient.class") private static Path rocksDbDir = null;
  @GuardedBy("RocksDBClient.class") private static RocksObject dbOptions = null;
  @GuardedBy("RocksDBClient.class") private static RocksDB rocksDb = null;
  @GuardedBy("RocksDBClient.class") private static int references = 0;

  private static final ConcurrentMap<String, ColumnFamily> COLUMN_FAMILIES = new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, Lock> COLUMN_FAMILY_LOCKS = new ConcurrentHashMap<>();

  @Override
  public void init() throws DBException {
    synchronized(RocksDBClient.class) {
	try{
	    this.socket = new Socket(InetAddress.getByName("localhost"), 8888);
	    message = new ByteArrayOutputStream();
	    out = new DataOutputStream(this.socket.getOutputStream());
	    //    in = new BufferedReader(new InputStreamReader(this.socket.getInputStream())); //bufferedinputstream
	    is = new DataInputStream(this.socket.getInputStream());
	    debug = true;
	}catch(Exception e){}
	
      if(rocksDb == null) {
        rocksDbDir = Paths.get(getProperties().getProperty(PROPERTY_ROCKSDB_DIR));
        LOGGER.info("RocksDB data dir: " + rocksDbDir);

        try {
          rocksDb = initRocksDB();
        } catch (final IOException | RocksDBException e) {
          throw new DBException(e);
        }
      }

      references++;
    }
  }

  /**
   * Initializes and opens the RocksDB database.
   *
   * Should only be called with a {@code synchronized(RocksDBClient.class)` block}.
   *
   * @return The initialized and open RocksDB instance.
   */
  private RocksDB initRocksDB() throws IOException, RocksDBException {
    if(!Files.exists(rocksDbDir)) {
      Files.createDirectories(rocksDbDir);
    }

    final List<String> cfNames = loadColumnFamilyNames();
    final List<ColumnFamilyOptions> cfOptionss = new ArrayList<>();
    final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();

    for(final String cfName : cfNames) {
      final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
          .optimizeLevelStyleCompaction();
      final ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(
          cfName.getBytes(UTF_8),
          cfOptions
      );
      cfOptionss.add(cfOptions);
      cfDescriptors.add(cfDescriptor);
    }

    final int rocksThreads = Runtime.getRuntime().availableProcessors() * 2;

    if(cfDescriptors.isEmpty()) {
      final Options options = new Options()
          .optimizeLevelStyleCompaction()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true)
          .setIncreaseParallelism(rocksThreads)
          .setMaxBackgroundCompactions(rocksThreads)
          .setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
      dbOptions = options;
      return RocksDB.open(options, rocksDbDir.toAbsolutePath().toString());
    } else {
      final DBOptions options = new DBOptions()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true)
          .setIncreaseParallelism(rocksThreads)
          .setMaxBackgroundCompactions(rocksThreads)
          .setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
      dbOptions = options;

      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
      final RocksDB db = RocksDB.open(options, rocksDbDir.toAbsolutePath().toString(), cfDescriptors, cfHandles);
      for(int i = 0; i < cfNames.size(); i++) {
        COLUMN_FAMILIES.put(cfNames.get(i), new ColumnFamily(cfHandles.get(i), cfOptionss.get(i)));
      }
      return db;
    }
  }

  @Override
  public void cleanup() throws DBException {
    super.cleanup();

    synchronized (RocksDBClient.class) {
      try {
        if (references == 1) {
          for (final ColumnFamily cf : COLUMN_FAMILIES.values()) {
            cf.getHandle().close();
          }

          rocksDb.close();
          rocksDb = null;

          dbOptions.close();
          dbOptions = null;

          for (final ColumnFamily cf : COLUMN_FAMILIES.values()) {
            cf.getOptions().close();
          }
          saveColumnFamilyNames();
          COLUMN_FAMILIES.clear();

          rocksDbDir = null;
        }

      } catch (final IOException e) {
        throw new DBException(e);
      } finally {
        references--;
      }
    }
  }


    
  @Override
  public Status read(final String table, final String key, final Set<String> fields,
      final Map<String, ByteIterator> result) {
      if(debug)   System.out.println("DOING READ");
       try{
	   message.write("R".getBytes(),0,1);
	   message.write(deconstructNumber(table.length()));
	   message.write(table.getBytes(),0,table.length());
	   message.write(deconstructNumber(key.length()));
	   message.write(key.getBytes(),0,key.length());
	   out.write(message.toByteArray());
	   message.reset();
	   
	   ByteArrayOutputStream response_msg = GetResponse();
	   byte[] response_array = response_msg.toByteArray();
	   byte[] raw_data = Arrays.copyOfRange(response_array, 2,response_array.length );
	   if(debug)	   System.out.println(Arrays.toString(response_array));
	   //deserializeValues(raw_data,fields,result);
	   return getStatus(response_msg.toByteArray());
      } catch(IOException e) {return Status.ERROR;}
       //deserializeValues(values, fields, result);
       //return Status.OK;
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
        final Vector<HashMap<String, ByteIterator>> result) {
     if(debug) System.out.println("DOING SCAN");
      try{
	  message.write("S".getBytes(),0,1);
	  message.write(deconstructNumber(table.length()));
	  message.write(table.getBytes(),0,table.length());
	  message.write(deconstructNumber(startkey.length()));
	  message.write(startkey.getBytes(),0,startkey.length());
	  message.write(deconstructNumber(recordcount));
	  message.write(deconstructNumber(fields.size()));
	  for(String field : fields)
	      {
		  message.write(deconstructNumber(field.length()));
		  message.write(field.getBytes(),0,field.length());
	      }
	  
	  out.write(message.toByteArray());
	  message.reset();

	  ByteArrayOutputStream response_msg = GetResponse();
	  return getStatus(response_msg.toByteArray());
      } catch(IOException e) {return Status.ERROR;}
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
   if(debug)   System.out.println("DOING UPDATE");
      try{
	  byte[] serialised = serializeValues(values);
	  if(debug)  System.out.println("size " + serialised.length);
	  message.write("U".getBytes(),0,1);
	  message.write(deconstructNumber(table.length()));
	  message.write(table.getBytes(),0,table.length());
	  message.write(deconstructNumber(key.length()));
	  message.write(key.getBytes(),0,key.length());
	  message.write(deconstructNumber(serialised.length));
	  message.write(serialised,0,serialised.length);
	  out.write(message.toByteArray());
	  message.reset();
	  ByteArrayOutputStream response_msg = GetResponse();
	  return getStatus(response_msg.toByteArray());
      } catch(IOException e) {return Status.ERROR;}
  }

    private byte[] deconstructNumber(int n)
    {
	byte[] bytes = ByteBuffer.allocate(4).putInt(n).array();
	return bytes;
    }
    public ByteArrayOutputStream GetResponse()throws IOException
    {
	int msg_size = is.readInt();
if(debug)System.out.println("MSG LENGTH " + msg_size);
	boolean finished = false;
	int offset = 0;
	int totalBytesRead = 0;
	byte[] buffer = new byte[1024];
	ByteArrayOutputStream result = new ByteArrayOutputStream();
	while(!finished)
	    {
		int bytesRead = is.read(buffer,0,1024);
		totalBytesRead += bytesRead;
		if(debug)System.out.println("BYTES READ " + bytesRead);
		if(debug)System.out.println("TOTAL BYTES READ " + totalBytesRead);
		result.write(buffer, 0 , bytesRead);
		if(totalBytesRead >= msg_size || bytesRead == -1) finished = true;
		
		if(debug)System.out.println("GOT " + new String(buffer));
		}
	    
	    if(debug)System.out.println("FINAL RESULT " + result.toString());
	    return result;
    }

    public Status getStatus(byte[] response)
    {
	if(debug)System.out.println("RESPONSE " + response);
	if (response[0] == 'O'){
	    if(debug)System.out.println("RETURNED OK");
	    return Status.OK;
	}
	else{
	    if(debug)System.out.println("RETURNED ERROR");
	    return Status.ERROR;
	}
    }
    
    @Override
    public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
	try{
	    if(debug)System.out.println("DOING INSERT");
	  byte[] serialised = serializeValues(values);
	 if(debug) System.out.println("size " + serialised.length);
	  message.write("I".getBytes(),0,1);
	  message.write(deconstructNumber(table.length()));
	  message.write(table.getBytes(),0,table.length());
	  message.write(deconstructNumber(key.length()));
	  message.write(key.getBytes(),0,key.length());
	  message.write(deconstructNumber(serialised.length));
	  message.write(serialised,0,serialised.length);
	  out.write(message.toByteArray());
	  message.reset();
	  
	  ByteArrayOutputStream response_msg = GetResponse();
	if(debug)  System.out.println(Arrays.toString(response_msg.toByteArray()));
	  return getStatus(response_msg.toByteArray());
      } catch(IOException e) {return Status.ERROR;}
  }

  @Override
  public Status delete(final String table, final String key) {
      if(debug)System.out.println("DOING DELETE");
      try{
	  message.write("D".getBytes(),0,1);
	  message.write(deconstructNumber(table.length()));
	  message.write(table.getBytes(),0,table.length());
	  message.write(deconstructNumber(key.length()));
	  message.write(key.getBytes(),0,key.length());
	  out.write(message.toByteArray());
	  message.reset();
	  
	  ByteArrayOutputStream response_msg = GetResponse();
	  return getStatus(response_msg.toByteArray());
      } catch(IOException e) {return Status.ERROR;}
  }

  private void saveColumnFamilyNames() throws IOException {
    final Path file = rocksDbDir.resolve(COLUMN_FAMILY_NAMES_FILENAME);
    try(final PrintWriter writer = new PrintWriter(Files.newBufferedWriter(file, UTF_8))) {
      writer.println(new String(RocksDB.DEFAULT_COLUMN_FAMILY, UTF_8));
      for(final String cfName : COLUMN_FAMILIES.keySet()) {
        writer.println(cfName);
      }
    }
  }

  private List<String> loadColumnFamilyNames() throws IOException {
    final List<String> cfNames = new ArrayList<>();
    final Path file = rocksDbDir.resolve(COLUMN_FAMILY_NAMES_FILENAME);
    if(Files.exists(file)) {
      try (final LineNumberReader reader =
               new LineNumberReader(Files.newBufferedReader(file, UTF_8))) {
        String line = null;
        while ((line = reader.readLine()) != null) {
          cfNames.add(line);
        }
      }
    }
    return cfNames;
  }

  private Map<String, ByteIterator> deserializeValues(final byte[] values, final Set<String> fields,
      final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);

    int offset = 0;
    while(offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      final String key = new String(values, offset, keyLen);
      offset += keyLen;

      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if(fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

    return result;
  }

  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try(final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for(final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(UTF_8);
        final byte[] valueBytes = value.getValue().toArray();
	//	System.out.println("KEYLENGTH " + keyBytes.length);
        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();
	//	System.out.println("VALUE LENGTH " + valueBytes.length);
        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }
    
  private void createColumnFamily(final String name) throws RocksDBException {
    COLUMN_FAMILY_LOCKS.putIfAbsent(name, new ReentrantLock());

    final Lock l = COLUMN_FAMILY_LOCKS.get(name);
    l.lock();
    try {
      if(!COLUMN_FAMILIES.containsKey(name)) {
        final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions().optimizeLevelStyleCompaction();
        final ColumnFamilyHandle cfHandle = rocksDb.createColumnFamily(
            new ColumnFamilyDescriptor(name.getBytes(UTF_8), cfOptions)
        );
        COLUMN_FAMILIES.put(name, new ColumnFamily(cfHandle, cfOptions));
      }
    } finally {
      l.unlock();
    }
  }

  private static final class ColumnFamily {
    private final ColumnFamilyHandle handle;
    private final ColumnFamilyOptions options;

    private ColumnFamily(final ColumnFamilyHandle handle, final ColumnFamilyOptions options) {
      this.handle = handle;
      this.options = options;
    }

    public ColumnFamilyHandle getHandle() {
      return handle;
    }

    public ColumnFamilyOptions getOptions() {
      return options;
    }
  }
}
