## Description of the directory:

The following directory contains the codes developed for Map Reduce Operation.
The code calculates the minimum and maximum fare by month and year.

## Shell Script:
### Compile Map Reduce Program:
* Comiple the program: $HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main AggregateFare.java
* Create a JAR: jar cf af.jar AggregateFare*.class

### Execute Map Reduce Job:
* Execute a Job: $HADOOP_HOME/bin/hadoop af.jar AggregateFare InputDirecotry OutputDirectory

## License Information:
Please note that the following code is distributed WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  
Please see the Repository License file for the applicable license.
