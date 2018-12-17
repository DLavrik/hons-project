//Example code: A simple server side code, which echos back the received message. 
//Handle multiple socket connections with select and fd_set on Linux 
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <string.h> //strlen 
#include <stdlib.h> 
#include <errno.h> 
#include <unistd.h> //close 
#include <arpa/inet.h> //close 
#include <sys/types.h> 
#include <sys/socket.h> 
#include <netinet/in.h> 
#include <sys/time.h> //FD_SET, FD_ISSET, FD_ZERO macros 
//#include "rocksdb/c.h"
#include <vector>
#include <map>
#include <set>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include <bitset>
//#include <unistd.h>
//#include <stdio.h>
//#include <stdlib.h>
//#include <sys/types.h>
#include <sys/stat.h>
//#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#define TRUE 1 
#define FALSE 0 
#define BUFFER_SIZE 200
#define BUFFER_START_INDEX 8
#define ALLIGNMENT 4
using namespace std;
using namespace rocksdb;
class ColumnFamily;

void deserializeValues(string values,set<string> ,map<string, string> &result);
string serializeValues( map<string, string> values);
vector<unsigned char> intToBytes(int);
int getSizeFromBuffer(char * buffer,int index);
int getSizeFromBuffer(vector<char> buffer,int index);
  
map<string, ColumnFamily> COLUMN_FAMILIES;
DB* db;
char * response;
bool debug = true;
class ColumnFamily {
    
private:
  
   ColumnFamilyHandle* handle;
   ColumnFamilyOptions options;
public:
  ColumnFamily(ColumnFamilyHandle* handle, ColumnFamilyOptions options) {
      this->handle = handle;
      this->options = options;
    }
  
  ColumnFamilyHandle* getHandle() {
      return handle;
    }

  ColumnFamilyOptions getOptions() {
      return options;
    }
};

class MMappedBuffer{
  
public:
  
  int fd;
  int result;
  void* buffer;
  char* chbuffer;
  int ReadIndex;
  int WriteIndex;
  bool endOfBufferFlag;
  MMappedBuffer(const char * filename)
  {
    fd = open(filename, O_RDWR | O_CREAT, (mode_t)0600);
    if (fd == -1) {
      perror("Error opening file for writing");
      exit(EXIT_FAILURE);
    }
    
    /* Stretch the file size to the size of the (mmapped) array of ints
     */
    result = lseek(fd, BUFFER_SIZE + BUFFER_START_INDEX, SEEK_SET);
    if (result == -1) {
      close(fd);
      perror("Error calling lseek() to 'stretch' the file");
      exit(EXIT_FAILURE);
    }
    
    result = write(fd, "", 1);
    if (result != 1) {
      close(fd);
      perror("Error writing last byte of the file");
      exit(EXIT_FAILURE);
    }

    buffer = mmap(0, BUFFER_SIZE + BUFFER_START_INDEX, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    
    if (buffer == MAP_FAILED) {
	close(fd);
	perror("Error mmapping the reponse file");
	exit(EXIT_FAILURE);
    }
    chbuffer = (char*) buffer;
    setReadIndex(BUFFER_START_INDEX - 1);
    setWriteIndex(BUFFER_START_INDEX);
    endOfBufferFlag = true;
  }

  ~MMappedBuffer()
  {
    if (munmap(buffer, BUFFER_SIZE) == -1) {
      perror("Error un-mmapping the response file");
      /* Decide here whether to close(fd) and exit() or not. Depends... */
    }
    close(fd);
  }

  void setReadIndex(int newIndex)
  {
    cout << "New read index " << newIndex << "\n";
    ReadIndex = newIndex;
    vector<unsigned char> num = intToBytes(newIndex);
    chbuffer[4] = num[0];
    chbuffer[5] = num[1];
    chbuffer[6] = num[2];
    chbuffer[7] = num[3];
  }

  void setWriteIndex(int newIndex)
  {
    WriteIndex = newIndex;
    vector<unsigned char> num = intToBytes(newIndex);
    
    chbuffer[0] = num[0]; 
    chbuffer[1] = num[1]; //make this atomic, only works on aligned values
    chbuffer[2] = num[2];
    chbuffer[3] = num[3];
  } 

  void checkEndMark()
  {
    cout<<"READ index at check end mark " << ReadIndex <<" Write index " <<WriteIndex << endl;
       if( (ReadIndex >= BUFFER_SIZE + BUFFER_START_INDEX - 1) || (chbuffer[ReadIndex+1] == 0xFF))
      {
	//setReadIndex(BUFFER_START_INDEX - 1);
	ReadIndex = BUFFER_START_INDEX - 1;
	cout<<"Local ReadIndex reset" <<endl;
      }
  }

  int allign(int index)
  {
    int newIndex = index; 
    if(index % ALLIGNMENT != 0)
      newIndex = index + ALLIGNMENT - (index % ALLIGNMENT);
    return newIndex; 
  }

  bool gotBytesToRead()
  {
    sleep(1);
    if(WriteIndex-1 == ReadIndex)
      return false;
    if((WriteIndex == BUFFER_START_INDEX) &&( ReadIndex == BUFFER_START_INDEX + BUFFER_SIZE - 1)){
      cout<<"special case no bytes left to read, " << WriteIndex << ", " << ReadIndex <<endl;
      // exit(EXIT_FAILURE);
      return false;
    }
    return true;
  }

  int getNextReadIndex()
  {
    if(ReadIndex < BUFFER_START_INDEX + BUFFER_SIZE - 1)
      return ReadIndex + 1;
    else return BUFFER_START_INDEX;
  }
  
  
  vector<char> readNext()
  {
    vector<char> result;
    WriteIndex = getWriteIndex();
    cout<<"WriteIndex: " << WriteIndex <<endl;
    if(gotBytesToRead())      
      {// this means we have some data to read
       // first we have to get the next request in vector form
	checkEndMark(); // check if we need to start reading from start again
	cout<<"Found unread data, write index: "<< WriteIndex << " , Read Index: " << ReadIndex <<endl;
	//exit(EXIT_FAILURE);
	int msgSize = getSizeFromBuffer(chbuffer, ReadIndex + 1);
	setReadIndex(ReadIndex + 4);
	cout<<"Message Size " << msgSize << endl;
	
	result.assign(chbuffer + ReadIndex + 1,chbuffer + ReadIndex + 1 + msgSize);
	setReadIndex(ReadIndex + allign(msgSize));
	  
      }
    else{
        cout<<"Nothing to read, read index " << ReadIndex <<endl;
    }
    return result;
  }

  void addToBuffer(string response)
    // writes response starting at position WriteIndex, overlaps if reaches end of buffer 
  {
    bool added = false;
	while(!added)
	  {
	    int bytesAvailable = getBytesAvailable();
	    int allignedBytesAvailable = bytesAvailable - (bytesAvailable % ALLIGNMENT);
	    int bytesNeeded = allign(response.length());
	    cout << "Alligned Bytes available: " << allignedBytesAvailable << " \n";
	    if(allignedBytesAvailable >= bytesNeeded)
	      {
		cout<< "Writing normally";
		copy(response.begin(),response.end(),chbuffer + WriteIndex);
		setWriteIndex(WriteIndex + response.length());
		added = true;
	      }
	    else if(!endOfBufferFlag)
	      {
		cout << "Reader has not advanced enough to write data\n";
	      }
	    else
	      {
		cout << "End of buffer reached, starting from beginning\n";
		markEnd();
		WriteIndex = BUFFER_START_INDEX;
	      }
	  }
  }
  
  void markEnd()
  {
    if(WriteIndex < BUFFER_SIZE + BUFFER_START_INDEX) 
      {
	cout << "Marking end at " << WriteIndex << "\n";
	chbuffer[WriteIndex] = 0xFF; //reader will see this byte and know it has to read from start again
      }
  }
  
  int getBytesAvailable()
    {
	ReadIndex = getReadIndex();
	if(WriteIndex > ReadIndex)
	    {
		return BUFFER_SIZE + BUFFER_START_INDEX - WriteIndex;
	    }
	else
	    {
		return ReadIndex - WriteIndex;
	    }
    }
  
  int getWriteIndex()
  {
    return getSizeFromBuffer(chbuffer, 0);
  }
  int getReadIndex()
  {
    return getSizeFromBuffer(chbuffer, 4);
  }
  
};

void createColumnFamily(string name){
  ColumnFamilyHandle* cf;
  Status s = db->CreateColumnFamily(ColumnFamilyOptions(), name, &cf);
  assert(s.ok());
  COLUMN_FAMILIES.insert(pair<string,ColumnFamily>(name,ColumnFamily(cf,ColumnFamilyOptions()) ) );
}


void saveColumnFamilyNames(){
  ofstream myfile ("/home/deividas/cfnames");
  
  if (myfile.is_open())
    {
      myfile << kDefaultColumnFamilyName + "\n";
      for (map<string, ColumnFamily>::iterator it= COLUMN_FAMILIES.begin(); it!=COLUMN_FAMILIES.end(); ++it){
	myfile << it->first + "\n";
   }
      myfile.close();
    }
}

int insert(string table, string key,string values) {
    if (COLUMN_FAMILIES.find(table) == COLUMN_FAMILIES.end()) {
    if(debug)  cout<<"Creating new column family " + table + "\n";
      createColumnFamily(table);
    }
    ColumnFamilyHandle * cf = COLUMN_FAMILIES.find(table)->second.getHandle();
    //  cout<<"Handle achieved\n";
    Status s = db->Put(WriteOptions(),cf, Slice(key), Slice(values));
    if(s.ok()){saveColumnFamilyNames(); return 0;}
    else return -1;
  }

int update(string table, string key,string values) {
  if (COLUMN_FAMILIES.find(table) == COLUMN_FAMILIES.end()) {
    if(debug)  cout<<"Creating new column family " + table + "\n";
      createColumnFamily(table);
    }
    ColumnFamilyHandle * cf = COLUMN_FAMILIES.find(table)->second.getHandle();
    //  cout<<"Handle achieved\n";
    string current_values;
    map<string, string> current_values_map;
    map<string, string> values_map;
    //cout<<"Getting values from database"<< endl;
    Status s = db->Get(ReadOptions(), cf, Slice(key), &current_values);
    // if(!s.ok()) return -1;
     set<string> empty;
     deserializeValues(current_values, empty, current_values_map);
     deserializeValues(values, empty,values_map);

     current_values_map.insert(values_map.begin(),values_map.end());
    
     s = db->Put(WriteOptions(),cf, Slice(key), Slice(serializeValues(current_values_map)));
     if(s.ok()){saveColumnFamilyNames(); return 0;}
     else return -1;
  }

map<string, string> scan(string table, string startkey, int iterations,set<string> fields) {
    if (COLUMN_FAMILIES.find(table) == COLUMN_FAMILIES.end()) {
    createColumnFamily(table);
    }
    
    ColumnFamilyHandle * cf = COLUMN_FAMILIES.find(table)->second.getHandle();

    map<string, string> result;
    int itercount = 0;
   if(debug) cout<<"Iterations: " <<iterations<<endl;
   if(debug)cout<<"startkey: " <<startkey<<endl;
    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
    for (it->Seek(startkey); it->Valid() && itercount < iterations; it->Next()) {
    if(debug)  cout<<"Seeking"<<endl;
      map<string, string> record;
      deserializeValues(it->value().ToString(), fields, record);
      //    cout<<recordfirst()<<endl;
      result.insert(record.begin(),record.end());
      itercount++;
    }
    assert(it->status().ok()); // Check for any errors found during the scan
    delete it;
    
    return result;
}



int delete_record(string table, string key) {

  if (COLUMN_FAMILIES.find(table) == COLUMN_FAMILIES.end()) {
    createColumnFamily(table);
  }

  ColumnFamilyHandle * cf = COLUMN_FAMILIES.find(table)->second.getHandle();
  db->Delete(WriteOptions(),cf,Slice(key));
  return 0;
}
  

string read(string table, string key) {
      if (COLUMN_FAMILIES.find(table) == COLUMN_FAMILIES.end()) {
      createColumnFamily(table);
      }

      ColumnFamilyHandle * cf = COLUMN_FAMILIES.find(table)->second.getHandle();
      string values;
      //string values;
    if(debug)  cout<<"Getting values from database"<< endl;
      Status s = db->Get(ReadOptions(), cf, Slice(key), &values);
      if(s.ok()){
if(debug)cout<<"is ok"<<endl;
	//cout<<"value size "<<values.ToString().length() <<endl;
	return values;
      }
      else return string("");
  }

string kDBPath = "/home/deividas/rocksdb-data";


vector<string> loadColumnFamilyNames(){
  string line;
  vector<string> res; 
  ifstream myfile ("/home/deividas/cfnames");
  if (myfile.is_open())
  {
    while ( getline (myfile,line) )
    {
      res.push_back(line);
    }
    myfile.close();
    return res;
  }
}



int getSizeFromBuffer(vector<char> buffer,int index)
{
  //return *reinterpret_cast<unsigned int*>(buffer.data() + index);
  //cout<<"last octet: " <<int((unsigned char)buffer[index+3]) <<endl;
  int size = int( (unsigned char)(buffer[index]) << 24 | (unsigned char)(buffer[index+1]) << 16 | (unsigned char)(buffer[index+2]) << 8 | (unsigned char)(buffer[index+3]) ); 
  return size;
}


int getSizeFromBuffer(string buffer,int index)
{
  //return *reinterpret_cast<unsigned int*>(buffer.data() + index);
  //cout<<"last octet: " <<int((unsigned char)buffer[index+3]) <<endl;
  int size = int( (unsigned char)(buffer[index]) << 24 | (unsigned char)(buffer[index+1]) << 16 | (unsigned char)(buffer[index+2]) << 8 | (unsigned char)(buffer[index+3]) ); 
  return size;
}

int getSizeFromBuffer(char * buffer,int index)
{
  //return *reinterpret_cast<unsigned int*>(buffer.data() + index);
  //cout<<"last octet: " <<int((unsigned char)buffer[index+3]) <<endl;
  int size = int( (unsigned char)(buffer[index]) << 24 | (unsigned char)(buffer[index+1]) << 16 | (unsigned char)(buffer[index+2]) << 8 | (unsigned char)(buffer[index+3]) ); 
  return size;
}
void deserializeValues(string values, set<string> fields, map<string, string> &result) {

    int offset = 0;
    while(offset < values.length()) {
      int keyLen = getSizeFromBuffer(values,offset);
    if(debug)  cout<<"DETECTED KEYLENGTH "  << keyLen<<endl;
      offset += 4;
      string key =  string(&values[0] + offset, &values[0] + offset + keyLen);
     if(debug) cout<<"DETECTED KEY "  << key<<endl;
      offset += keyLen;
      int valueLen = getSizeFromBuffer(values,offset);
    if(debug)  cout<<"DETECTED VALUE LENGTH "  << valueLen<<endl;
      offset += 4;
      string value =  string(&values[0] + offset, &values[0] + offset + valueLen);
     if(debug) cout<<"DETECTED VALUE "  << value<<endl;
      offset += valueLen;

      if(fields.size() == 0 || fields.find(key) !=fields.end()) {
	result.insert ( pair<string,string>(key,value) );
       }
    }
  }

vector<unsigned char> intToBytes(int paramInt)
{
  vector<unsigned char> arrayOfByte(4);
  for (int i = 0; i < 4; i++)
    arrayOfByte[3 - i] = (paramInt >> (i * 8));
  return arrayOfByte;
}

string serializeValues( map<string, string> values) {
  string result;
  map<string,string>::iterator it;
  for ( it = values.begin(); it != values.end(); it++ ) {
    vector<unsigned char> keyLength_v = intToBytes(it->first.length()); 
    string keyLength = string(keyLength_v.begin(), keyLength_v.end());
    vector<unsigned char> valueLength_v = intToBytes(it->second.length()); 
    string valueLength = string(valueLength_v.begin(), valueLength_v.end());
    result+= keyLength + it->first + valueLength + it->second;
  }
  return result;
    
}


string parseRequest(vector<char> buffer)
{
if(debug)  cout<<"PARSING REQUEST"<<endl;
  if(buffer.size() > 0)
    {
   if(debug)   cout<<"buffer accepted"<<endl;
      if(buffer[0] == 'I')
	{
	  int currIndex = 1;
	if(debug)  cout<<"INSERT"<<endl;
	  int size = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	  if(debug)cout<<"size "<< size<<endl;
	  string table(buffer.begin() + currIndex,buffer.begin() + currIndex + size );
	 if(debug) cout<<table<<endl;
	  currIndex +=size;
	  
	  size = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	 if(debug) cout<<"size "<< size<<endl;
	  string key(buffer.begin() + currIndex,buffer.begin() + currIndex + size );
	  if(debug)cout<<key<<endl;
	  currIndex +=size;

	  size = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	 if(debug) cout<<"size "<< size<<endl;
	  string values(buffer.begin() + currIndex,buffer.begin() + currIndex + size );
	 if(debug) cout<<values<<endl;
	  currIndex +=size;
	  //  cout<< "reached 1\n";
	  
	  const char *cstr = values.c_str();
	  //  cout<< "converted str\n";
	  int res = insert(table,key,values);
	  //	  cout<< "inserted\n";
	  if(res == 0)return "OK";
	  else return "ER";	  
	}
      else if(buffer[0] == 'D')
	{
	  int currIndex = 1;
	 if(debug) cout<<"DELETE"<<endl;
	  unsigned int size = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	 if(debug) cout<<"size "<< size<<endl;
	  string table(buffer.begin() + currIndex,buffer.begin() + currIndex + size );
	 if(debug) cout<<table<<endl;
	  currIndex +=size;
	  
	  size = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	 if(debug) cout<<"size "<< size<<endl;
	  string key(buffer.begin() + currIndex,buffer.begin() + currIndex + size );
	 if(debug) cout<<key<<endl;
	  currIndex +=size;
	  int res = delete_record(table,key);
	  if(res == 0){if(debug)cout<<"Deleted successfully "<<endl;return "OK";}
	  else return "ER";	  
	}
      else if(buffer[0] == 'R')
	{
	  int currIndex = 1;
	 if(debug) cout<<"READ"<<endl;
	  unsigned int size = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	 if(debug) cout<<"size "<< size<<endl;
	  string table(buffer.begin() + currIndex,buffer.begin() + currIndex + size );
	  if(debug)cout<<table<<endl;
	  currIndex +=size;
	  
	  size = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	  if(debug)cout<<"size "<< size<<endl;
	  string key(buffer.begin() + currIndex,buffer.begin() + currIndex + size );
	  if(debug)cout<<key<<endl;
	  currIndex +=size;

	  string res = read(table,key);
	  if(debug)cout<<"the value is " <<res <<endl;
	  auto * c = res.c_str();
	  //	  for (auto i = 0U; i < res.length(); ++i) {
	  // cout << hex << +c[i] << ' ' <<dec;
	  //}
	  string ret = "OK" + res;
	  if(res != "") return ret;
	  else return "ER";	  
	}
      else if(buffer[0] == 'U')
	{
	  int currIndex = 1;
	 if(debug) cout<<"UPDATE"<<endl;
	  int size = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	  if(debug)cout<<"size "<< size<<endl;
	  string table(buffer.begin() + currIndex,buffer.begin() + currIndex + size );
	if(debug)  cout<<table<<endl;
	  currIndex +=size;
	  
	  size = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	 if(debug) cout<<"size "<< size<<endl;
	  string key(buffer.begin() + currIndex,buffer.begin() + currIndex + size );
	if(debug)  cout<<key<<endl;
	  currIndex +=size;

	  size = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	 if(debug) cout<<"size "<< size<<endl;
	  string values(buffer.begin() + currIndex,buffer.begin() + currIndex + size );
	  if(debug)cout<<values<<endl;
	  currIndex +=size;
	  //  cout<< "reached 1\n"
	  //  cout<< "converted str\n";
	  int res = update(table,key,values);
	  //	  cout<< "updated\n";
	  if(res == 0)return "OK";
	  else return "ER";	  
	}

      else if(buffer[0] == 'S')
	{
	  int currIndex = 1;
	 if(debug) cout<<"SCAN"<<endl;
	  int size = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	 if(debug) cout<<"size "<< size<<endl;
	  string table(buffer.begin() + currIndex,buffer.begin() + currIndex + size );
	  if(debug)cout<<table<<endl;
	  currIndex +=size;
	  
	  size = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	  if(debug)cout<<"size "<< size<<endl;
	  string startkey(buffer.begin() + currIndex,buffer.begin() + currIndex + size );
	if(debug)  cout<<startkey<<endl;
	  currIndex +=size;
	  
	  int recordcount = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	  int fieldsize = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;

	  set<string> fields;
	  for(int i = 0;i < fieldsize; i++)
	    {
	      int fieldlength =  getSizeFromBuffer(buffer,currIndex);
	      currIndex+=4;
	      string field(buffer.begin() + currIndex,buffer.begin() + currIndex + fieldlength );
	     if(debug) cout<<"Field: " << field<<endl;
	      fields.insert(field);
	      currIndex +=fieldlength;	      
	    }
	 
	  map<string, string> result = scan(table,startkey,recordcount ,fields);
	  //	  cout<< "scan finished\n";
	  return "OK" + serializeValues(result) + "";
	  //else return "ER\n";	  
	}
      
    }
  else return "ER";
}

int main(int argc , char *argv[]) 
{
  vector<ColumnFamilyDescriptor> column_families;
  vector<string> cfnames = loadColumnFamilyNames();
  //for (auto & it : cfnames)
  for (auto it= cfnames.begin(); it!=cfnames.end(); ++it){
    column_families.push_back(ColumnFamilyDescriptor(*it, ColumnFamilyOptions()));
   }
  vector<ColumnFamilyHandle*> handles;
  Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  
  Status s = DB::Open(options, kDBPath, column_families, &handles, &db);
  assert(s.ok());
  
  for(int i = 0; i < cfnames.size(); i++) {
    COLUMN_FAMILIES.insert(pair<string,ColumnFamily>(cfnames[i], ColumnFamily(handles[i], ColumnFamilyOptions()) ) );
  }

  insert("test", "0", "abc");
  
  // setup is done
  
  MMappedBuffer req("req_buffer");
  MMappedBuffer resp("resp_buffer");

  while(true)
    {
      vector<char> request = req.readNext();
      sleep(1);
      //    cout<< "Request size " << request.size()<< endl;
      //cout<< "request: \n";
      
      //  for (int i = 0;i< request.size();i++)
      //	cout << request[i];
      /*
      if(request.size() > 0)
	{
	  cout<<"parsing"<<endl;
	  string response = parseRequest(request);
	  vector<unsigned char> msg_length = intToBytes(response.length()); 
	  string msgLength = string(msg_length.begin(), msg_length.end());
	  string final_response = msgLength + response;
	  cout<<"sending final response " << final_response <<endl;
	  resp.addToBuffer(final_response);
	  }
      */
    }
  //  for (auto a = request.begin(); a != request.end(); ++a)
    return 0;
}
  /*
  // for (map<string,ColumnFamily>::iterator it= COLUMN_FAMILIES.begin(); it!=COLUMN_FAMILIES.end(); ++it)
  // delete it->second.getHandle();
  
  //  delete db;

  int opt = TRUE; 
  int master_socket , addrlen , new_socket , client_socket[30] , 
    max_clients = 30 , activity, i , valread , sd; 
  int max_sd; 
  struct sockaddr_in address; 
  
  char buffer[1025]; //data buffer of 1K 
  vector<char> buffers[max_clients];
	//set of socket descriptors 
  fd_set readfds;
	
	
	//initialise all client_socket[] to 0 so not checked 
	for (i = 0; i < max_clients; i++) 
	{ 
		client_socket[i] = 0; 
	} 
		
	//create a master socket 
	if( (master_socket = socket(AF_INET , SOCK_STREAM , 0)) == 0) 
	{ 
		perror("socket failed"); 
		exit(EXIT_FAILURE); 
	} 
	
	//set master socket to allow multiple connections , 
	//this is just a good habit, it will work without this 
	if( setsockopt(master_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, 
		sizeof(opt)) < 0 ) 
	{ 
		perror("setsockopt"); 
		exit(EXIT_FAILURE); 
	} 
	
	//type of socket created 
	address.sin_family = AF_INET; 
	address.sin_addr.s_addr = INADDR_ANY; 
	address.sin_port = htons( PORT ); 
		
	//bind the socket to localhost port 8888 
	if (bind(master_socket, (struct sockaddr *)&address, sizeof(address))<0) 
	{ 
		perror("bind failed"); 
		exit(EXIT_FAILURE); 
	} 
	printf("Listener on port %d \n", PORT); 
		
	//try to specify maximum of 3 pending connections for the master socket 
	if (listen(master_socket, 3) < 0) 
	{ 
		perror("listen"); 
		exit(EXIT_FAILURE); 
	} 
		
	//accept the incoming connection 
	addrlen = sizeof(address); 
	puts("Waiting for connections ..."); 
		
	while(TRUE) 
	{ 
		//clear the socket set 
		FD_ZERO(&readfds); 
	
		//add master socket to set 
		FD_SET(master_socket, &readfds); 
		max_sd = master_socket; 
			
		//add child sockets to set 
		for ( i = 0 ; i < max_clients ; i++) 
		{ 
			//socket descriptor 
			sd = client_socket[i]; 
				
			//if valid socket descriptor then add to read list 
			if(sd > 0) 
				FD_SET( sd , &readfds); 
				
			//highest file descriptor number, need it for the select function 
			if(sd > max_sd) 
				max_sd = sd; 
		} 
	
		//wait for an activity on one of the sockets , timeout is NULL , 
		//so wait indefinitely 
		activity = select( max_sd + 1 , &readfds , NULL , NULL , NULL); 
	
		if ((activity < 0) && (errno!=EINTR)) 
		{ 
			printf("select error"); 
		} 
			
		//If something happened on the master socket , 
		//then its an incoming connection 
		if (FD_ISSET(master_socket, &readfds)) 
		{ 
			if ((new_socket = accept(master_socket, 
					(struct sockaddr *)&address, (socklen_t*)&addrlen))<0) 
			{ 
				perror("accept"); 
				exit(EXIT_FAILURE); 
			} 
			
			//inform user of socket number - used in send and receive commands 
			printf("New connection , socket fd is %d , ip is : %s , port : %d \n" , new_socket , inet_ntoa(address.sin_addr) , ntohs 
				(address.sin_port)); 
		
			//send new connection greeting message 
			//	if( send(new_socket, message, strlen(message), 0) != strlen(message) ) 
			//{ 
			  //	perror("send"); 
			  //} 
				
			//	puts("Welcome message sent successfully"); 
				
			//add new socket to array of sockets 
			for (i = 0; i < max_clients; i++) 
			{ 
				//if position is empty 
				if( client_socket[i] == 0 ) 
				{ 
					client_socket[i] = new_socket; 
					printf("Adding to list of sockets as %d\n" , i); 
						
					break; 
				} 
			} 
		} 
			
		//else its some IO operation on some other socket 
		for (i = 0; i < max_clients; i++) 
		{ 
			sd = client_socket[i]; 
			if (FD_ISSET( sd , &readfds)) 
			{ 
				//Check if it was for closing , and also read the 
				//incoming message
			  
				if ((valread = read( sd , buffer, 1024)) == 0) 
				{
					//Somebody disconnected , get his details and print 
					getpeername(sd , (struct sockaddr*)&address ,(socklen_t*)&addrlen); 
					printf("Host disconnected , ip %s , port %d \n" , 
						inet_ntoa(address.sin_addr) , ntohs(address.sin_port)); 
					cout<<"Closing socket\n";
					//Close the socket and mark as 0 in list for reuse 
					close( sd ); 
					client_socket[i] = 0; 
				} 
					
				else if(valread == -1){
				  printf("Weird disconnect\n");
				    close( sd ); 
				    client_socket[i] = 0;  
				  }
				else {

				if(debug)  cout<<"valread " <<valread<< "\n";
				  buffers[i].insert(buffers[i].end(),buffer,buffer+valread);
				if(debug) cout<<"inserted\n";
				  string response;
				  if(valread < 1024) { // if exact size, what then?
				    if(debug)  cout<<"Request complete:"<<endl;
				    //   for (auto a = buffers[i].begin(); a != buffers[i].end(); ++a)
				    //	 cout << *a;
				       response = parseRequest(buffers[i]);
				       buffers[i].clear();
				   }
				  //char * response_pointer = new char[response.size() + 1 ];
				  //response.copy(response_pointer, response.length());
				  //strcpy(response_pointer, response.c_str() + 3);
				  //strcpy(response_pointer, response.c_str());
				  // response.data();
				  vector<unsigned char> msg_length = intToBytes(response.length()); 
				  string msgLength = string(msg_length.begin(), msg_length.end());
				  
				  string final_response = msgLength + response;
				  char * response_pointer = &final_response[0];
			if(debug)	  cout<<"sending response "<<response_pointer<< endl;
			if(debug)	  cout<<"response in string format: "<<response<< endl;
			if(debug)	  cout<<"strlen " << response.length() <<endl;
				  
				    // printf("received: \n");
				    //printf("%s",(char*)buffer);
				  int bytesSent = 0; 
				  while(bytesSent < final_response.length())
				    {
				      int bytesToSend = final_response.length() - bytesSent;
				   if(debug)   cout<<"Bytes to send " << bytesToSend <<endl;
				      if(bytesToSend > 1024) bytesToSend = 1024;
				      send(sd , response_pointer + bytesSent , bytesToSend , 0 );
				      bytesSent += bytesToSend;
				    }
				  //  delete[] response_pointer;
				}
			} 
		} 
	} 
		
	return 0; 
} 
*/
