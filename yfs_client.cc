// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <algorithm>

yfs_client::yfs_client()
{
    ec = new extent_client();

}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "",0) != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
    return false;
}

bool
yfs_client::isdir(inum inum)
{
    return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
	size_t wb;		

	if(write(ino,0,size,"",wb) != OK){
		r = IOERR;
		goto release;
	}
release:
    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out,int type)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
	bool found = false;
	if(lookup(parent,name,found,ino_out) != OK){
		return IOERR;
	}
	if (found) return EXIST;
	
	if( ec->create(type, ino_out) != extent_protocol::OK){
		return IOERR;
	}
	
    std::string buf;
	fileinfo fin;
	if(getfile(parent,fin)!= OK){
		return IOERR;
	}
	if(read(parent,fin.size,0,buf) != OK){
		return IOERR;
	}

	std::string sname(name);
	std::string sinum = filename(ino_out);

	buf.append(" ");
	buf.append(sname);
	buf.append(" ");
	buf.append(sinum);

	size_t wb;
	if(write(parent,buf.size(),0,buf.c_str(),wb) != OK){
		return IOERR;
	}

    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
	std::list<dirent> list;
	
	if(readdir(parent,list)!= OK){
		return IOERR;
	}
    
	std::string sname(name);
	for( std::list<dirent>::iterator it = list.begin(); it !=list.end();it++){
		if(sname.compare((*it).name) == 0){
			found = true;
			ino_out = (*it).inum;
			return r;
		}
	}
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
	std::string buf;
	fileinfo fin;
	if(getfile(dir,fin)!= OK){
		return IOERR;
	}
	if(read(dir,fin.size,0,buf) != OK){
		return IOERR;
	}
	std::istringstream iss(buf);
	do
	{
		//parse string to list
		dirent d;
		iss >> d.name >> d.inum;
		list.push_back(d);
	} while (iss);
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: read using ec->get().
     */
	if(ec->get(ino,data) != extent_protocol::OK){
		return IOERR;
	}
	data = data.substr(off,size);
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
	std::string buf;
	fileinfo fin;
	if(getfile(ino,fin)!= OK){
		return IOERR;
	}
	if(read(ino,fin.size,0,buf) != OK){
		return IOERR;
	}

	std::string new_data(data);
	new_data = new_data.substr(0,size);
	int new_size = off+size;
	if(off > buf.size())
	{	
		char* new_buff = new char[new_size];
		for(int i=0;i<buf.size();i++)
			new_buff[i] = buf[i];
		for(int i=buf.size();i<off;i++)
			new_buff[i] = '\0';
		for(int i=off;i<off+size;i++)
			new_buff[i] = new_data[i-off];
		if(ec->put(ino,new_buff,new_size) != extent_protocol::OK){
			delete new_buff;
			return IOERR;
		}
		bytes_written = size;
		delete new_buff;
		return r;
	}else if(off + size > buf.size()) {
		buf.resize(off+size);
		buf.replace(off,size,new_data);
	}else{
		if(off == 0){
			buf.resize(off+size);
		}else{
			new_size = buf.size();
		}
		buf.replace(off,size,new_data);
	}

	bytes_written = size;
	
	if(ec->put(ino,buf,new_size) != extent_protocol::OK){
		return IOERR;
	}

    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
	bool found = false;
	inum ino_out;
	if(lookup(parent,name,found,ino_out) != OK)
		return IOERR;
	if(found){
		if(isdir(ino_out))
			return IOERR;
		ec->remove(ino_out);
		std::string buf;
		fileinfo fin;
		if(getfile(parent,fin)!= OK){
			return IOERR;
		}
		if(read(parent,fin.size,0,buf) != OK){
			return IOERR;
		}

		size_t pos = buf.find(name);
		size_t size = std::string(name).size() + filename(ino_out).size()+2;
		buf.erase(pos,size);

		size_t wb;
		if(write(parent,buf.size(),0,buf.c_str(),wb) != OK){
			return IOERR;
		}
	}

    return r;
}

