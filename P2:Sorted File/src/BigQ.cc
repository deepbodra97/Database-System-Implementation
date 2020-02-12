#include "BigQ.h"

class RecordComparator{
	ComparisonEngine comparisonEngine;
	OrderMaker *sortorder;

	public:
		RecordComparator(OrderMaker *sortorder){
			this->sortorder = sortorder;
		}

		bool operator()(Record *left, Record *right){
			return comparisonEngine.Compare(left, right, sortorder)>0;
		}
};


void *TwoPassMultiwayMergeSort (void *arg) {
		
}


BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runVectorlen) {
	// read data from in pipe sort them into runVectorlen pages
	Record recordFromPipe;
	Schema mySchema ("catalog", "nation");

	File runFile;
	Page tempPage;
	vector<Record*> runVector;

	runFile.Open(0, "./runFile.bin");

	int nPagesFilledForARun = 0;
	while(in.Remove(&recordFromPipe)){
		Record *copyOfRecordFromPipe = new Record();
		copyOfRecordFromPipe->Copy(&recordFromPipe);

		if(tempPage.Append(&recordFromPipe) == 1){ // record fits in the page
			runVector.push_back(copyOfRecordFromPipe); // push the record for this run
		} else{ // record does not fit in the page
			nPagesFilledForARun++;
			tempPage.EmptyItOut();
			if(nPagesFilledForARun == runVectorlen){ // if runVectorlen pages of records processed
				stable_sort(runVector.begin(), runVector.end(), RecordComparator(&sortorder)); // sort the records

				for(std::vector<Record*>::iterator it=runVector.begin(); it!=runVector.end(); ++it){ // print the records of this run
					(*it)->Print(&mySchema);
				}
				nPagesFilledForARun = 0;
				runVector.clear();
				//TODO: write out the records in the vector to the runFile
			}
		}
	}

	if(!runVector.empty()){
		stable_sort(runVector.begin(), runVector.end(), RecordComparator(&sortorder)); // sort the records

		for(std::vector<Record*>::iterator it=runVector.begin(); it!=runVector.end(); ++it){ // print the records of this run
			(*it)->Print(&mySchema);
		}
		nPagesFilledForARun = 0;
		runVector.clear();
		//TODO: write out the records in the vector to the runFile
	}

	

	
    // construct priority queue over sorted runVectors and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	out.ShutDown ();
}

BigQ::~BigQ () {
}