#include "storage_manager.h"

#include <typeinfo>
#include <iostream>

int main() {

  StorageManager sm("/home/spapadop/TileDB_Data");
  AttributeTile<int> at(0);


  try {
    sm.open_array("A", StorageManager::CREATE);
    sm.append_tile(&at, "A", "att1");
    //sm.open("A", StorageManager::CREATE);
  } catch(StorageManagerException& sme) {
    std::cout << sme.what() << "\n";
  } 

  return 0;
}
