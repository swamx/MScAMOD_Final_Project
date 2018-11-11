/*
Purpose: This file contains the MPI implementation of calculating minimum fare value by month and year by using concurrent data read.
Author: Devang Swami
Date: 10th Nov, 2018
Assumption: No of processors = 4 
Reference Code Utilized: https://stackoverflow.com/questions/13327127/mpi-io-reading-file-by-processes-equally-by-line-not-by-chunk-size
*/

//Include relavant libraries
#include <cstdio>
#include <cstring>
#include <sstream>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <cmath>
#include <mpi.h>

//Set the std namespace 
using namespace std;

//Function to decode date from a no between 1 to 108 to year-month value. A value of 1 represents Jan-2009.
string decode_Date(int date) {
	int month = (date % 12) + 1;
	int year = 2009 + (int)floor((date) / 12);
	string s = to_string(year) + "-" + to_string(month);
	return(s);
}

//Function to encode date from year-month to a no between 1 to 108. A value of 1 represents Jan-2009.
float encode_Date(string s) {
	int year, month;
	string q;
	int num_part = 0;
	stringstream  lineStream(s);
	while (getline(lineStream, q, '-')) {
		if (num_part == 0) {
			year = stoi(q);
		}
		else if (num_part == 1) {
			month = stoi(q);
		}
		else {
			break;
		}
		num_part++;
	}
	return(((year - 2009) * 12) + month);
}

//Begin the Main function
int main(int argc, char **argv) {
	//Variables for MPI file processes and offset
	MPI_File in;
	//MPI variable to store file size
	MPI_Offset filesize;
	//MPI variable to store local data size
	MPI_Offset localsize;
	//MPI variable to store the calculated value of start of the data file
	MPI_Offset start;
	//MPI variable to store the calculated value of end of the data file
	MPI_Offset end;
	//Iterators
	int i = 0, j = 0;
	//Path to the file
	char path[100] = "E://data.csv";
	//Store processor id and number of processes: For MPI 
	int my_id, num_procs;
	//Store error info from MPI processes
	int ierr;
	//Store the start and end time of processes
	double start_t, end_t;
	//Size of the overlap for the file split
	const int overlap = 250;
	
	/*Store minimum fare by processor id. 
	The 2nd demension has a size of 108 that denotes the year-month combination. 
	Assumed that there are 4 processors only.*/
	float MIN_FARE_P[4][108];
	//Store minimum fare after reducing it from all the processes
	float MIN_FARE[108];
	
	//Init MPI process
	ierr = MPI_Init(&argc, &argv);
	//Fetch process id and no of processes
	MPI_Comm_rank(MPI_COMM_WORLD, &my_id);
	MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
	//Start the timer for calculating processing time
	MPI_Barrier(MPI_COMM_WORLD);
	start_t = MPI_Wtime();
	//Init Minimum Fare variable to a very high value that is not expected in the dataset.
	for (int j = 0; j < 108; j++) {
		MIN_FARE_P[my_id][j] = 1000000.0;
		MIN_FARE[my_id] = 1000000.0;
	}
	
	//Open the data file in MPI to get it ready for reading
	ierr = MPI_File_open(MPI_COMM_WORLD, path, MPI_MODE_RDONLY, MPI_INFO_NULL, &in);
	//Fetch and store size of the file
	MPI_File_get_size(in, &filesize);
	//Calculate split size of the file
	localsize = filesize / num_procs;
	//Calculate start & end of the split for each process using process id
	start = my_id * localsize;
	end = start + localsize - 1;
	//Append the overlap to the end of the split size
	end += overlap;

	// For the last processor, the end is the end of the file 
	if (my_id == num_procs - 1) {
		end = filesize;
	}

	//Recalculate size of the local data
	localsize = end - start + 1;
	//Variable to store the chunk of data from file in each processor
	char *chunk = new char[localsize];

	//Read the content of the file in the chunk
	MPI_File_read_at_all(in, start, chunk, localsize, MPI_CHAR, MPI_STATUS_IGNORE);
	//Set EOF for the end of each chunk
	chunk[localsize] = '\0';
	//Stores data from one row
	char data[400];
	//Init iterators
	j = 0;
	//Loop over the data received in each process
	for (i = 0; i < localsize; i++) {
		//Identify that one row of data is complete when "\n" is encountered
		if (chunk[i] == '\n') {
			data[j] = '\n';
			//Convert data to a string for processing
			string s = data;
			//Convert data to string stream
			stringstream  lineStream(s);
			//Store value of each column
			string cell;
			//Keeps track of no of columns encountered
			int col_no = 0;
			//If flag dosenot remain 0 at end of the process than the data value is a bad value
			int flag = 0;
			//Store date and fare value
			float date, fare;
			
			//Process each column at a time 
			while (getline(lineStream, cell, ',')) {
				//If column is a date column
				if (col_no == 1) {
					try {
						date = encode_Date(cell.c_str());
						if (date > 0 && date < 109) {
							flag = 0;
						}
						else {
							flag = 1;
						}
					}
					catch (invalid_argument in_arg) {
						flag = 1;
					}
				}
				//If column is a fare column
				if (col_no == 16) {
					try {
						if (flag == 0) {
							fare = abs(stof(cell.c_str()));
							flag = 0;
						}
						else {
							flag = 1;
						}
					}
					catch (invalid_argument in_arg) {
						flag = 1;
					}
				}
				col_no++;
			}
			//Process if not a bad data point
			if (flag != 1) {
				if (fare > 0) {
					int ptr = date;
					//Date is btw 1 to 108 while array is btw 0 to 107. Hence, convert it to the right range.
					ptr--;
					if (MIN_FARE_P[my_id][ptr] > fare) {
						MIN_FARE_P[my_id][ptr] = fare;
					}
				}
			}
			
			//Resent the data index to 0. Marking that One row if complete and hence reuse the "data" variable to store new row
			j = 0;
		}
		//Keep fetching data until its not the end of the row
		else {
			data[j] = chunk[i];
			j++;
		}
	}

	//Reduce the minimum fare from all processes
	MPI_Reduce(&MIN_FARE_P[my_id], &MIN_FARE[my_id], 108, MPI_FLOAT, MPI_MIN, 0, MPI_COMM_WORLD);

	//Print the minimum fare and year-month combination
	if (my_id == 0) {
		cout << "Encoded Date\t Minimum Fare" << endl;
		for (int j = 0; j < 108; j++) {
			if (MIN_FARE[j] != 1000000.0) {
				cout << j + 1 << "\t" << decode_Date(j) << "\t\t" << MIN_FARE[j] << endl;
			}
		}
	}

	//Fetch end of time
	MPI_Barrier(MPI_COMM_WORLD);
	end_t = MPI_Wtime();
	//Print end of file 
	if (my_id == 0) {
		cout << "\nTime to complete the task was " << end_t - start_t << " s" << endl;
	}
	
	//Close MPI processes
	MPI_File_close(&in);
	ierr = MPI_Finalize();
	
	//return error if any
	return(ierr);
}