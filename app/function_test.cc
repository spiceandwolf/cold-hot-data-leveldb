#include <iostream>
#include <assert.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <Map>

#include "leveldb/db.h"
using namespace leveldb;
using namespace std;

int main(int argc, char** argv) {
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "D:\\workspace\\graduated design\\testdb", &db);
    assert(status.ok());

    std::ifstream infile; 
    infile.open("D:\\workspace\\graduated design\\doc\\result.txt");
    std::string line;
    std::map<std::string, std::string> res1;

    string val = "";
    //for(int i = 0; i < 1024; i++) val += "a";
    
    //这段代码注释掉后运行的是从本地文件恢复数据库的过程
    for (int i = 0; i < 1000000; i++) {  

        std::getline(infile, line);
        res1.insert(std::pair<std::string, std::string>(line, std::to_string(i)));
        
        status = db->Put(leveldb::WriteOptions(), /*"No." + std::to_string(i % 20000) + "'s key"*/line,
            /*"No." + std::to_string(i % 20000) + "'s value is " + */std::to_string(i) /*val*/);
        assert(status.ok()); 
        //std::cout<<"No." + std::to_string(i % 20000) + "'s value is " + std::to_string(i) << std::endl;
    }

    for(int i = 0; i < 2; i++) {
        std::getline(infile, line);
    } 
    
    std::string v2;
    int count = 0;
    int num = 0;

    for (int i = 0; i < 1000000; i++) {
        std::getline(infile, line);
        status = db->Get(leveldb::ReadOptions(), /*"No." + std::to_string(i % 20000) + "'s key"*/line, &v2);
        if (status.ok()) num++;
        //assert(status.ok());
        if (res1.find(line) != res1.end()) {
            if (status.ok()) count++;
            else std::cout<< line << " No." << v2 << " item is not found." << std::endl;
        } 
    }
     
    std::cout<< res1.size() << std::endl;
    std::cout<< count << num << std::endl;
 
    delete db;

}