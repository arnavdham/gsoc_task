cmake_minimum_required(VERSION 3.10)
project(podio_to_parquet)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(podio REQUIRED)
find_package(ROOT REQUIRED)
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

include(${ROOT_USE_FILE})

# Include paths
include_directories("/home/arnavdham/gsoc_eval/EDM4hep/edm4hep")
include_directories("/home/arnavdham/gsoc_eval/EDM4hep/utils/include")
include_directories("/home/arnavdham/gsoc_eval/EDM4hep")
include_directories("/home/arnavdham/gsoc_eval/EDM4hep/build")

# Library paths
link_directories("/home/arnavdham/gsoc_eval/EDM4hep/build/edm4hep")
link_directories("/home/arnavdham/gsoc_eval/EDM4hep/build/utils")
link_directories("$ENV{CONDA_PREFIX}/lib")

add_executable(converter src/converter.cc)

target_link_libraries(converter 
    podio 
    podioRootIO           
    ${ROOT_LIBRARIES}     
    edm4hep               
    edm4hepUtils          
    Arrow::arrow_shared 
    Parquet::parquet_shared
)

add_executable(vector_converter src/vector_converter.cc)

target_link_libraries(vector_converter PRIVATE 
    podio
    podioRootIO
    ${ROOT_LIBRARIES}
    edm4hep
    edm4hepDict
    Arrow::arrow_shared
    Parquet::parquet_shared
)