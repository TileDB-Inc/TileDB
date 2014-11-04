#include "storage_manager.h"
#include <iostream>

int main() {
  // Create a storage manager. The input is where its workspace
  // will be created
  StorageManager sm("~/stavrospapadopoulos/TileDB/Data");

  // Create a dummy attribute tile of type float with id 0
  AttributeTile<float>* at = new AttributeTile<float>(0);
  
  // Create a dummy coordinate tile of type int with id 0 and 3 dimensions
  CoordinateTile<int>* coord = new CoordinateTile<int>(0, 3);

  try {
    // Open an array in CREATE mode
    sm.open_array("A", StorageManager::CREATE);

    // Sends a tile to the storage manager. Note that attribute tiles
    // must be sent along with the names of the array and attribute
    // they belong to.
    sm.append_tile(at, "A", "att1");
    
    // Sends a tile to the storage manager. Note that coordinate tiles
    // must be sent along with the name of the array they belong to.
    sm.append_tile(coord, "A");

    // Close the array
    sm.close_array("A");

  } catch(StorageManagerException& sme) {
    std::cout << sme.what() << "\n";
  } 

  return 0;
}
