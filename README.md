# On studying the performance of Hadoop Map Reduce vs MPI for Aggregation Operations: A Big Data Challenge 

The following is a project accomplished as a past of MSc AMOD with Big Data Analytics stream. The objective of the work is to look into I/O for Map-Reduce and MPI paradigms. 

## Project Proposal:
Parallel and distributed computing have largely been utilized for solving problem involving large amount of data. The requirement today is to minimize the amount of time to complete a process. Two different computing techniques Map Reduce and MPI have found increased attention in recent times for two different groups of users; Map Reduce in business community and MPI in research community. Now the task of aggregation (i.e. sum, min, max, etc.) must be performed by both the scientific and business community for their relevant tasks. Aggregation tasks are usually used to summarize the data before proceeding with any type of analysis. The challenge is to determine the performance of MPI and Map Reduce for aggregation task. MPI is a paradigm for parallel processing while Map Reduce is a programming model. 

Therefore, MPI may be used to implement Map Reduce as done by Sandia Labs [1]. However, the Map Reduce using MPI (by Sandia Labs) lacks the fault tolerance and is not the same as Apache Hadoop implementation. Another key difference between MPI and Apache Hadoop Map Reduce is the usage of block storage. Apache Hadoop with large block size (minimum 64 MB) utilizes data localization for improving performance of operations. On the flipside, MPI doesn’t use data localization; instead it supports more flexible communication method than Map Reduce by moving data during communication. To cope up with the communication problem, Map Reduce provides a facility to improve the data communication, by using compression in intermediate stages as well as output to reduce network and I/O utilization.

The present work focuses on devising the optimal size of the block (i.e. 64 MB, 128 MB, and 256 MB), optimal size of the input split (i.e. 8 MB, 16 MB, 32 MB, and 64 MB) and optimal compression format (i.e. Snappy, Bzip2, and no compression) in intermediate and output stages for performing aggregate operation minimum for big data to calculate optimal time required for the operation and compare the same with the performance of MPI. To serve the purpose of the project, I would be using Taxi cab dataset from New York [3], which records taxi services from January 2009 to December 2017. The total size of the data is more than 150 GB. However, due to constraints of processing time we limit the data for MPI to 15 GB and about 120 GB for Apache Hadoop. To benchmark both the paradigms, an aggregation query like the following would be used: selecting minimum and maximum values of the taxi fare grouped by year and month. 
 
## Bibliography 
1) Map reduce, Sandia Labs, http://mapreduce.sandia.gov/ 
2) Hadoop Documentation, https://hadoop.apache.org/docs 
3) NYC Taxi & Limousine Commission, New York http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml 
4) Lee, K. H., Lee, Y. J., Choi, H., Chung, Y. D., & Moon, B. (2012). Parallel data processing with MapReduce: A Survey. ACM SIGMOD Record, 40(4), 11-20. 
5) Dittrich, J., & Quiané-Ruiz, J. A. (2012). Efficient big data processing in Hadoop MapReduce. Proceedings of the VLDB Endowment, 5(12), 2014-2015. 
6) Doulkeridis, C., & NØrvåg, K. (2014). A survey of large-scale analytical query processing in MapReduce. The VLDB Journal—The International Journal on Very Large Data Bases, 23(3), 355-380. 
7) Chen, C. P., & Zhang, C. Y. (2014). Data-intensive applications, challenges, techniques and technologies: A survey on Big Data. Information Sciences, 275, 314-347. 
8) Wes Kendall, Launching an Amazon EC2 MPI Cluster.  http://mpitutorial.com/tutorials/launching-an-amazon-ec2-mpi-cluster/ 
