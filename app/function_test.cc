#include <iostream>
#include <assert.h>
#include <string.h>

#include "leveldb/db.h"
using namespace leveldb;
using namespace std;

int main(int argc, char** argv) {
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "D:\\workspace\\graduated design\\testdb", &db);
    assert(status.ok());

    string val = "";
    for(int i = 0; i < 1024; i++) val += "a";
    
    //这段代码注释掉后运行的是从本地文件恢复数据库的过程
    for (int i = 0; i < 4000; i++) {
        
        status = db->Put(leveldb::WriteOptions(), "No." + std::to_string(i % 20000) + "'s key",
         "No." + std::to_string(i % 20000) + "'s value is " + /*std::to_string(i)*/ val);
        assert(status.ok());
    }

    std::string v2;
    status = db->Get(leveldb::ReadOptions(),"No.0's key", &v2);
    assert(status.ok());
    std::cout<<"the value is : "<<v2<<std::endl;
 
    delete db;
}