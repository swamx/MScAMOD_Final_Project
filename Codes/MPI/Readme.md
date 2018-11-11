### Description of the directory:
The following directory contains MPI code for the project. There are two versions of MPI that have been implemented as follows:
  * Serial file read version: The data file is read first and than we process the same. *(File name: MPI_Sequential_read.cpp)*
  * Concurrent file read version: The data file is broken down to be read by individual processes. *(File name:  	MPI_Concurrent_read.cpp)*
Both of the above mentioned codes assumes that the number of processors are 4. They also make use of C++ v11 libraries.

#### Shell Script to execute MPI:
 * To compile the file: **mpicc -Wall programFile.cpp -o mpiLinkFile -lstdc++ -std=c++11**
 * To execute the file: **mpirun -n 4 mpiLinkFile**

**Please Note:** *You may need the set the file path for your data file. You may use any one of the NYC Taxi-Yellow cab dataset file which holds the records for Jan, 2009 to Dec, 2017 for the same.* 
