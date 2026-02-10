add_library(cityhash102 src/city.cc)
target_include_directories(cityhash102 PUBLIC include)
target_include_directories(cityhash102 PRIVATE src)
