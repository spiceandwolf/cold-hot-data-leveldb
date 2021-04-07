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
    for (int i = 0; i < 10; i++) {
        
        status = db->Put(leveldb::WriteOptions(), std::to_string(i % 5), std::to_string(i));
        assert(status.ok());
    }

    std::string v2;
    status = db->Get(leveldb::ReadOptions(),"0", &v2);
    std::cout<<"v:"<<v2<<std::endl;
 
    delete db;
}