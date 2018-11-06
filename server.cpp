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
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include <bitset>
#include <unistd.h>
#define TRUE 1 
#define FALSE 0 
#define PORT 8888 
using namespace rocksdb;
class ColumnFamily;


std::map<std::string, ColumnFamily> COLUMN_FAMILIES;
DB* db;

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



void createColumnFamily(std::string name){
  ColumnFamilyHandle* cf;
  Status s = db->CreateColumnFamily(ColumnFamilyOptions(), name, &cf);
  assert(s.ok());
  COLUMN_FAMILIES.insert(std::pair<std::string,ColumnFamily>(name,ColumnFamily(cf,ColumnFamilyOptions()) ) );
}


void saveColumnFamilyNames(){
  std::ofstream myfile ("/home/deividas/cfnames");
  
  if (myfile.is_open())
    {
      myfile << kDefaultColumnFamilyName + "\n";
      for (std::map<std::string, ColumnFamily>::iterator it= COLUMN_FAMILIES.begin(); it!=COLUMN_FAMILIES.end(); ++it){
	myfile << it->first + "\n";
   }
      myfile.close();
    }
}

int insert(std::string table, std::string key,std::string values) {
    if (COLUMN_FAMILIES.find(table) == COLUMN_FAMILIES.end()) {
      std::cout<<"Creating new column family " + table + "\n";
      createColumnFamily(table);
      std::cout<<"Created \n";
    }
    ColumnFamilyHandle * cf = COLUMN_FAMILIES.find(table)->second.getHandle();
    //  std::cout<<"Handle achieved\n";
    Status s = db->Put(WriteOptions(),cf, Slice(key), Slice(values));
    if(s.ok()){saveColumnFamilyNames(); return 0;}
    else return -1;
  }


int delete_record(std::string table, std::string key) {

  if (COLUMN_FAMILIES.find(table) == COLUMN_FAMILIES.end()) {
    createColumnFamily(table);
  }

  ColumnFamilyHandle * cf = COLUMN_FAMILIES.find(table)->second.getHandle();
  db->Delete(WriteOptions(),cf,Slice(key));
  return 0;
}
  

std::string read(std::string table, std::string key) {
      if (COLUMN_FAMILIES.find(table) == COLUMN_FAMILIES.end()) {
      createColumnFamily(table);
      }

      ColumnFamilyHandle * cf = COLUMN_FAMILIES.find(table)->second.getHandle();
      std::string values;
      Status s = db->Get(ReadOptions(), cf, Slice(key), &values);
      if(s.ok())return values;
      else return std::string("");
  }

std::string kDBPath = "/home/deividas/rocksdb-data";


std::vector<std::string> loadColumnFamilyNames(){
  std::string line;
  std::vector<std::string> res; 
  std::ifstream myfile ("/home/deividas/cfnames");
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



unsigned int getSizeFromBuffer(std::vector<char> buffer,int index)
{
  //return *reinterpret_cast<unsigned int*>(buffer.data() + index);
  return ((unsigned int)buffer[index] << 24 | (unsigned int)buffer[index+1] << 16 | (unsigned int)buffer[index+2] << 8 | (unsigned int)buffer[index+3]);
}

char * parseRequest(std::vector<char> buffer)
{
  std::cout<<"PARSING REQUEST"<<std::endl;
  if(buffer.size() > 0)
    {
      std::cout<<"buffer accepted"<<std::endl;
      if(buffer[0] == 'I')
	{
	  int currIndex = 1;
	  std::cout<<"INSERT"<<std::endl;
	  unsigned int size = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	  std::cout<<"size "<< size<<std::endl;
	  std::string table(buffer.begin() + currIndex,buffer.begin() + currIndex + size );
	  std::cout<<table<<std::endl;
	  currIndex +=size;
	  
	  size = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	  std::cout<<"size "<< size<<std::endl;
	  std::string key(buffer.begin() + currIndex,buffer.begin() + currIndex + size );
	  std::cout<<key<<std::endl;
	  currIndex +=size;

	  size = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	  std::cout<<"size "<< size<<std::endl;
	  std::string values(buffer.begin() + currIndex,buffer.begin() + currIndex + size );
	  std::cout<<values<<std::endl;
	  currIndex +=size;
	  //  std::cout<< "reached 1\n";
	  
	  const char *cstr = values.c_str();
	  //  std::cout<< "converted str\n";
	  int res = insert(table,key,values);
	  //	  std::cout<< "inserted\n";
	  if(res == 0)return "OK\n";
	  else return "ER\n";	  
	}
      else if(buffer[0] == 'D')
	{
	  int currIndex = 1;
	  std::cout<<"DELETE"<<std::endl;
	  unsigned int size = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	  std::cout<<"size "<< size<<std::endl;
	  std::string table(buffer.begin() + currIndex,buffer.begin() + currIndex + size );
	  std::cout<<table<<std::endl;
	  currIndex +=size;
	  
	  size = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	  std::cout<<"size "<< size<<std::endl;
	  std::string key(buffer.begin() + currIndex,buffer.begin() + currIndex + size );
	  std::cout<<key<<std::endl;
	  currIndex +=size;

	  int res = delete_record(table,key);
	  if(res == 0)return "OK\n";
	  else return "ER\n";	  
	}
      else if(buffer[0] == 'R')
	{
	  int currIndex = 1;
	  std::cout<<"READ"<<std::endl;
	  unsigned int size = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	  std::cout<<"size "<< size<<std::endl;
	  std::string table(buffer.begin() + currIndex,buffer.begin() + currIndex + size );
	  std::cout<<table<<std::endl;
	  currIndex +=size;
	  
	  size = getSizeFromBuffer(buffer,currIndex);
	  currIndex+=4;
	  std::cout<<"size "<< size<<std::endl;
	  std::string key(buffer.begin() + currIndex,buffer.begin() + currIndex + size );
	  std::cout<<key<<std::endl;
	  currIndex +=size;

	  std::string res = read(table,key);
	  std::string ret = "OK" + res + "\n";
	  char final_ret[ret.size()+1];
	  strcpy(final_ret,ret.c_str());
	  if(res != "") return final_ret;
	  else return "ER\n";	  
	}
      
    }
  else return "ER\n";
}

int main(int argc , char *argv[]) 
{
  std::vector<ColumnFamilyDescriptor> column_families;
  std::vector<std::string> cfnames = loadColumnFamilyNames();
  //for (auto & it : cfnames)
  for (auto it= cfnames.begin(); it!=cfnames.end(); ++it){
    column_families.push_back(ColumnFamilyDescriptor(*it, ColumnFamilyOptions()));
   }
  std::vector<ColumnFamilyHandle*> handles;
  Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  
  Status s = DB::Open(options, kDBPath, column_families, &handles, &db);
  assert(s.ok());
  
  for(int i = 0; i < cfnames.size(); i++) {
    COLUMN_FAMILIES.insert(std::pair<std::string,ColumnFamily>(cfnames[i], ColumnFamily(handles[i], ColumnFamilyOptions()) ) );
  }

  

  // for (std::map<std::string,ColumnFamily>::iterator it= COLUMN_FAMILIES.begin(); it!=COLUMN_FAMILIES.end(); ++it)
  // delete it->second.getHandle();
  
  //  delete db;

  int opt = TRUE; 
  int master_socket , addrlen , new_socket , client_socket[30] , 
    max_clients = 30 , activity, i , valread , sd; 
  int max_sd; 
  struct sockaddr_in address; 
  
  char buffer[1025]; //data buffer of 1K 
  std::vector<char> buffers[max_clients];
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
					std::cout<<"Closing socket\n";
					//Close the socket and mark as 0 in list for reuse 
					close( sd ); 
					client_socket[i] = 0; 
				} 
					
				else if(valread == -1){
				  printf("Unexpected disconnect\n");
				    close( sd ); 
				    client_socket[i] = 0;  
				  }
				else {

				  std::cout<<"valread\n" <<valread<< "\n";
				  buffers[i].insert(buffers[i].end(),buffer,buffer+valread);
				  std::cout<<"inserted\n";
				  char * response;
				  if(valread < 1024) { // if exact size, what then?
				      std::cout<<"Request complete:"<<std::endl;
				       for (auto a = buffers[i].begin(); a != buffers[i].end(); ++a)
					 std::cout << *a;
				       response = parseRequest(buffers[i]);
				       buffers[i].clear();
				   }
				 
				    // printf("received: \n");
				    //printf("%s",(char*)buffer);
				  send(sd , response , strlen(response) , 0 );
				} 
			} 
		} 
	} 
		
	return 0; 
} 
