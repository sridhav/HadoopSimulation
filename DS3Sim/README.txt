/*******************************READ ME**********************/
/*******************************FILES**********************/
Two Files have been uploaded into the CSX Server. One is the DS3Sim.jar, second is the DS3Sim_Final.jar. 

DS3Sim_Final.jar is general file that needs to be submitted in CSX (Input should be physically given)

DS3Sim.jar is the Simulation file that is used to generate the graph (no input is required executes the 25 sequential 
and 25 parallel the Inputgenerator should be altered to get different configuarations, Output is presented in out.csv file)

Source code is also uploaded just check the folders DS3Sim and DS3Sim_Final


/*******************************EXECUTION**********************/
Execute using the following commands
	Java –jar DS3Sim_Final.jar
	Java –jar DS3Sim.jar
As an example an input file is given as example
	Java –jar DS3Sim_Final.jar < inp.txt

/********************************INPUTS********************/
The valid Inputs include
	1.	Volume of big data Which should be given in TB (an integer/long value)
	2. 	Velocity should be given in bps (an integer/long value)
	3.	Variety is also considered but I have took the data block size as variety 
		(variety takes the values from 1 to 8 and default value is 64 MB. 1 -> 2 MB, 
		2 -> 4 MB, 3 -> 8 MB, 4->16 MB, 5-> 32 MB, 6 -> 64 MB, 7->128 MB.
		MASTER NODE CONFIGUARATION
	4.	Master Node Memory High (should be an integer, the value is considered in GB)
	6.	Master Node Memory Low (should be an integer, the value is considered in GB)
	7.	Master Node CPU High (Should be an Integer, the value is considered in Ghz)
	8.	Master Node CPU Low (Should be an Integer, the value is considered in Ghz)
	9.	Number of Nodes (Should be an Integer)
	10. 	Nodes Connectivity (Should be an integer, Max = No of slaves, Min =0)
		0 = > parallel any value other than 0 means that those no of slaves are connected in sequence
		
		SLAVE NODE CONFIGUARATION
	11. 	Slave Node Memory High (should be an integer, the value is considered in GB)
	12.	Slave Node Memory Low (should be an integer, the value is considered in GB)
	13.	Random or cluster (values should be ‘r’ random and ‘c’ for cluster)
	14.	Slave Node CPU High (Should be an Integer, the value is considered in Ghz)
	15.	Slave Node CPU Low (Should be an Integer, the value is considered in Ghz)
	16.	Random or cluster (values should be ‘r’ random and ‘c’ for cluster)
	

