/*
Purpose: This file contains the MPI implementation of calculating minimum fare value by month and year by using sequential data read.
Author: Devang Swami
Date: 26th Oct, 2018
Assumption: No of processors = 4 
*/

//Include relavant libraries
#include <stdio.h>
#include <string.h>
#include <sstream>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <math.h>
#include <mpi.h>

//Set the namespace to std
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

//Find maximum value in the array: Used to find out the maximum size of the array required to store the scattered data
float max_value(int array1[]) {
	int l = sizeof(array1)/ sizeof(array1[0]);
	float max = 0;
	for (int i = 0; i < l; i++) {
		if (array1[i] >= max) {
			max = array1[i];
		}
	}
	return(max);
}

//Begin the Main function
int main(int argc, char** argv) {
	
	//Store processor id and number of processes: For MPI 
	int my_id, num_procs;
	//Store error info from MPI processes
	int ierr;
	//No of data points
	const int num_elements = 12854627;
	//Stores the data
	static float Data[num_elements][2];
	
	/*Store minimum fare by processor id. 
	The 2nd demension has a size of 108 that denotes the year-month combination. 
	Assumed that there are 4 processors only.*/
	float MIN_FARE_P[4][108];
	//Store minimum fare after reducing it from all the processes
	float MIN_FARE[108];
	//Store date and fare value
	float date, fare;
	
	//Variables to calculate and store the number of data points to be send to each process
	int displs[4] = { 0 };
	int send_counts[4] = { 0 };
	//Stores the maximum no of data points to be send 
	int max_vect_length = 0, max_send_counts = 0;
	
	//Iterator and row counter
	int i = 0, row_num = 0;
	//Stores the time taken by MPI processes
	double start, read_end, end;

	//Init MPI process
	ierr = MPI_Init(&argc, &argv);
	//Fetch and store the process id and number of processes
	MPI_Comm_rank(MPI_COMM_WORLD, &my_id);
	MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
	//Start the timer
	MPI_Wtime(MPI_COMM_WORLD);
	start = MPI_Wtime();
	
	//If it is the root process, fetch the data from the file and caluclate the data to be send to each process
	if (my_id == 0) {
		//Path of the file
		char filepath[100] = "//home//swami//basicPrograms//data.csv";
		//Create a to the data file
		ifstream  data(filepath);
		//Iterate and fetch each row at a time and store it in 'line' variable
		while (getline(data, line)) {
			//Create a stream from each row fetched
			stringstream  lineStream(line);
			//Store value of each column
			string cell;
			//Keeps track of no of columns encountered
			int col_no = 0;
			//If flag dosenot remain 0 at end of the process than the data value is a bad value
			int flag = 0;
			//Fetch one column at a time and store the value in 'cell' variable
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
					catch (invalid_argument in) {
						flag = 1;
					}
				}
				//If column is a fare column
				if (col_no == 16) {
					try {
						if (flag == 0) {
							fare = stof(cell.c_str());
							flag = 0;
						}
						else {
							flag = 1;
						}
					}
					catch (invalid_argument in) {
						flag = 1;
					}
				}
				col_no++;
			}
			//Process if not a bad data point
			if (flag != 1) {
				Data[i][0] = date;
				Data[i][1] = fare;
				//Increment data counter
				i++;
			}
			//Increment row counter
			row_num++;			
		}
		//Store time marking completion of the read operation
		MPI_Barrier(MPI_COMM_WORLD);
		read_end = MPI_Wtime();
		//Print information about the data
		cout << "No of samples collected = " << i << "\tfrom a total of " << row_num << " samples." << endl;
		cout << "No of processes: \t" << num_procs << endl;
		//Init Minimum Fare variable to a very high value that is not expected in the dataset.
		for (int j = 0; j < 108; j++) {
			MIN_FARE[j] = 1000000.0;
		}
		
		//Calculate no of data points to be send to each process
		for (int l = 0; l < i; l++) {
			send_counts[l % 4]+=2;
		}
		//Calculate displacements of the data array to be send to each process
		int sum = 0;
		for (int l = 0; l < 4; l++) {
			displs[l]=sum;
			sum += send_counts[l];
		}
		max_vect_length = i * 2;
	}
	//Calculate maximum send counts could be helpful if creating dynamic arrays
	//max_send_counts = max_value(send_counts);
	
	//Buffer to recieve scattered data
	static float rcv_buff[2800000][2];
	
	//Broadcast data that is required by other processes like no of data points, displacement information, and no of data points that each process would receive
	MPI_Bcast(&i, 1, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Bcast(&(send_counts[0]), 4, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Bcast(&(displs[0]), 4, MPI_INT, 0, MPI_COMM_WORLD);

	//Scatter the data
	MPI_Scatterv(&(Data[0][0]), send_counts, displs, MPI_FLOAT, &rcv_buff, send_counts[my_id], MPI_FLOAT, 0, MPI_COMM_WORLD);

	//max_send_counts = max_value(send_counts);
	//Init minimum fare
	for (int j = 0; j < 108; j++) {
		MIN_FARE_P[my_id][j] = 1000000.0;
	}
	
	//Calculate the minimum fare value by year-month
	for (int k = 0; k < (send_counts[my_id]/2); k++) {
		int pointer = (int) rcv_buff[k][0];
		pointer--;
		if (MIN_FARE_P[my_id][pointer] > rcv_buff[k][1] && rcv_buff[k][1] > 0) {
			MIN_FARE_P[my_id][pointer] = (float)rcv_buff[k][1];
		}
	}	
	
	//Reduce the minimum fare from all processes
	MPI_Reduce(&MIN_FARE_P[my_id], &MIN_FARE[my_id], 108, MPI_FLOAT, MPI_MIN, 0, MPI_COMM_WORLD);
	
	//Print the minimum fare and year-month combination
	if (my_id == 0) {
		cout << "Encoded Date\t Minimum Fare" << endl;
		for (int j = 0; j < 108; j++) {
			if (MIN_FARE[j] != 1000000.0) {
				cout << j+1 << "\t" << decode_Date(j) << "\t\t" << MIN_FARE[j] << endl;
			}
		}
	}
	
	//Fetch end of time
	MPI_Barrier(MPI_COMM_WORLD);
	end = MPI_Wtime();
	
	//Print end of file 
	if (my_id == 0) {
		cout << "\nTime to complete the task was " << end - start << " s, while data was read in " << read_end-start<<"s \n";
	}

	//Close MPI processes
	ierr = MPI_Finalize();

	//return error if any
	return(ierr);
}
