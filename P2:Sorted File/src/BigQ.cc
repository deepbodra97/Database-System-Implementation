#include "BigQ.h"

// A wrapper to be passed to the thread for 'Two Pass Multiway Merge Sort Thread'
class BigQMemberHolder{

	public:
		Pipe *in; //input pipe
		Pipe *out; // output pipe
		OrderMaker *sortorder; // ordermaker
		int runVectorlen; // run length: in pages
		File file; // file instance

		BigQMemberHolder(Pipe &in, Pipe &out, OrderMaker &sortorder, int runVectorlen){
			this->in = &in;
			this->out = &out;
			this->sortorder = &sortorder;
			this->runVectorlen = runVectorlen;
		}
};

// comparator class for Record
class RecordComparator{
	ComparisonEngine comparisonEngine; // comparison engine
	OrderMaker *sortorder; // sort order

	public:
		RecordComparator(OrderMaker sortorder){
			this->sortorder = &sortorder;
		}

		bool operator()(Record *left, Record *right){
			return comparisonEngine.Compare(left, right, sortorder)<0;
		}
};

// Instances of this class will be stored in the priority queue for phase 2 of 'Two Pass Multiway Merge Sort Thread'
class RunRecord{
	public:
		Record record; // record
		int runNumber; // run number of the record
};

// comparator class for RunRecord
class RunRecordComparator{
	ComparisonEngine comparisonEngine;
	OrderMaker *sortorder;

	public:
		RunRecordComparator(OrderMaker sortorder){
			this->sortorder = &sortorder;
		}

		bool operator()(RunRecord *left, RunRecord *right){
			return comparisonEngine.Compare(&(left->record), &(right->record), sortorder)>=0;
		}
};

// To track the information about the buffers in the phase 2 of 'Two Pass Multiway Merge Sort Thread'
class RunRecordMetaData{
	public:
		Page page; // page
		int currentPageNumber; // current page number
		int endPageNumber; // last page number
};

// 'Two Pass Multiway Merge Sort' Thread
void *TwoPassMultiwayMergeSort (void *arg) {
	BigQMemberHolder *bigQMemberHolder;
	bigQMemberHolder = (BigQMemberHolder*)arg;

	Record recordFromPipe; // record from input pipe

	File runFile; // file to store runs
	Page runPage; // page to store records for a run
	vector<Record*> runVector; // vector to sort records in memory

	Page tempPage; // temp page used to count 'runLength' records
	
	RecordComparator recordComparator = RecordComparator(*bigQMemberHolder->sortorder); // comparator

	runFile.Open(0, "./runFile.bin"); // open run file

	int nPagesFilledForARun = 0; // number of pages filled for the current run

	while(bigQMemberHolder->in->Remove(&recordFromPipe)){ // while there are records in the pipe
		Record *copyOfRecordFromPipe = new Record(); // to store copy of the record
		copyOfRecordFromPipe->Copy(&recordFromPipe); // copy the record from pipe

		if(tempPage.Append(&recordFromPipe) == 1){ // record fits in the temp page
			runVector.push_back(copyOfRecordFromPipe); // add the record in the vector for the current run
		} else{ // record does not fit in the page
			nPagesFilledForARun++; // increase page number
			tempPage.EmptyItOut(); // empty the temp page
			if(nPagesFilledForARun == bigQMemberHolder->runVectorlen){ // if runVectorlen pages of records processed
				stable_sort(runVector.begin(), runVector.end(), recordComparator); // sort the records
				for(std::vector<Record*>::iterator it=runVector.begin(); it!=runVector.end(); ++it){ // commit the sorted records to the file
					// (*it)->Print(&mySchema);
					if(runPage.Append(*it) == 0){
						// cout<<"runFile.GetLength() = "<<runFile.GetLength()<<endl;
						runFile.AddPage(&runPage, runFile.GetLength()-1<0?0:runFile.GetLength()-1);
						runPage.EmptyItOut();
						runPage.Append(*it);
					}
				}

				if(runPage.GetNumRecs()!=0){ // if the page is not full but still has records then also we must commit it to the file
					// cout<<"runFile.GetLength() = "<<runFile.GetLength()<<endl;
					runFile.AddPage(&runPage, runFile.GetLength()-1<0?0:runFile.GetLength()-1);
					runPage.EmptyItOut();
				}

				nPagesFilledForARun = 0; // set the number of pages filled to 0 for the next run
				runVector.clear(); // clear vector
			}
			tempPage.Append(&recordFromPipe);
			runVector.push_back(copyOfRecordFromPipe); // add the record that did not fit earlier as a part of the next run
		}
	}

	nPagesFilledForARun = 0;
	if(!runVector.empty()){
		stable_sort(runVector.begin(), runVector.end(), recordComparator); // sort the records

		for(std::vector<Record*>::iterator it=runVector.begin(); it!=runVector.end(); ++it){ // commit the sorted records to the file
			// (*it)->Print(&mySchema);
			if(runPage.Append(*it) == 0){
				// cout<<"runFile.GetLength() = "<<runFile.GetLength()<<endl;
				runFile.AddPage(&runPage, runFile.GetLength()-1<0?0:runFile.GetLength()-1);
				runPage.EmptyItOut();
				runPage.Append(*it);
			}
		}

		if(runPage.GetNumRecs()!=0){ // if the page is not full but still has records then also we must commit it to the file
			// cout<<"runFile.GetLength() = "<<runFile.GetLength()<<endl;
			runFile.AddPage(&runPage, runFile.GetLength()-1<0?0:runFile.GetLength()-1);
			runPage.EmptyItOut();
		}

		// free the Record objects lying around in the memory
		for(vector<Record *>::iterator it=runVector.begin();it!=runVector.end();++it){
	        delete *it;
	    }
		runVector.clear(); // free the space occupied by the vector
	}
	


	runFile.Close();


	
	// **********Phase 2**********
	
	runFile.Open(1, "./runFile.bin");
	int nPages = runFile.GetLength()-1;
	// cout<<"nPages:"<<nPages<<endl;
	priority_queue<RunRecord*, vector<RunRecord*>, RunRecordComparator> priorityQueue(*bigQMemberHolder->sortorder); // priority queue to merge the records

	if(nPages==0){
		cout<<"RunFile is empty! There are no pages in the runFile"<<endl;
	}else{

		// test
		// print entire runFile
		/*Schema mySchema("catalog", "partsupp");
		Page printPage;
		int printPageNumber=0;
		Record printRecord;
		for(int i=0; i<10; i++){
			runFile.GetPage(&printPage, printPageNumber);
			if(printPage.GetFirst(&printRecord) == 1){
				printRecord.Print(&mySchema);
			}
			while(printPage.GetFirst(&printRecord) == 1){
					
			}
			printPageNumber+=1;
		}*/

		int nRuns = ceil((float)nPages/bigQMemberHolder->runVectorlen); // total number of runs = ceil(number of pages/run length)
		// cout<<"nRuns:"<<nRuns<<endl;
		RunRecordMetaData runRecordMetaData[nRuns]; // acts as a buffer for the merge phase. new records will be  picked from this and added to the priority queue

		// init runRecordMetaData
		for(int i=0, startPageNumber=0; i<nRuns; i++, startPageNumber+=bigQMemberHolder->runVectorlen){
			runRecordMetaData[i].currentPageNumber = startPageNumber; // set start page number of i-th run
			runRecordMetaData[i].endPageNumber = startPageNumber+bigQMemberHolder->runVectorlen-1; // set end page number of i-th run
			runFile.GetPage(&runRecordMetaData[i].page, startPageNumber); // get the 1st page of i-th run
			if(i==nRuns-1 && nPages%bigQMemberHolder->runVectorlen!=0){ // if this is the last run
				runRecordMetaData[i].endPageNumber=runRecordMetaData[i].currentPageNumber+nPages%bigQMemberHolder->runVectorlen-1; // then the endPageNumber = start page number + (number of pages % runlength) - 1
			}
			RunRecord *runRecord = new RunRecord; // this record will be added to the priority queue
			runRecordMetaData[i].page.GetFirst(&(runRecord->record)); // get the first record from the page
			runRecord->runNumber = i; // set the run number
			priorityQueue.push(runRecord); // push the record to the priority queue
			// cout<<"(startPageNumber, endPageNumber):"<<runRecordMetaData[i].currentPageNumber<<","<<runRecordMetaData[i].endPageNumber<<endl;
		}

		
		while(!priorityQueue.empty()){ // while the queue has runrecords
			RunRecord *outputRunRecord = priorityQueue.top(); // get a pointer to the smallest runrecord in the queue
			priorityQueue.pop(); // remove the runrecord from the queue
			bigQMemberHolder->out->Insert(&(outputRunRecord->record)); // insert in the output pipe

			RunRecord *newRunRecord = new RunRecord; // the record after the popped record in the run will be stored in this
			// cout<<"outputRunRecord->runNumber"<<outputRunRecord->runNumber<<endl;
			newRunRecord->runNumber = outputRunRecord->runNumber; // set run number of this record equal to that of the popped record
			if(runRecordMetaData[outputRunRecord->runNumber].page.GetFirst(&(newRunRecord->record)) == 1){ // if this page for this run has more records then fetch the 1st record
				
				priorityQueue.push(newRunRecord); // push this record to the queue
			} else{ // if the page doesn't have more records
				runRecordMetaData[outputRunRecord->runNumber].currentPageNumber += 1; // increase the current page number by 1
				if(runRecordMetaData[outputRunRecord->runNumber].currentPageNumber <= runRecordMetaData[outputRunRecord->runNumber].endPageNumber){ // if we are not past the last page of the run
					
					runFile.GetPage(&(runRecordMetaData[outputRunRecord->runNumber].page), runRecordMetaData[outputRunRecord->runNumber].currentPageNumber); // get the next page
					runRecordMetaData[outputRunRecord->runNumber].page.GetFirst(&(newRunRecord->record)); // get the 1st record from this page
					priorityQueue.push(newRunRecord); // push it into the queue
				}
			}
		}
	}

	runFile.Close();
	remove("./runFile.bin");
	
	bigQMemberHolder->out->ShutDown();
}


BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runVectorlen) {
	// read data from in pipe sort them into runVectorlen pages
	BigQMemberHolder *bigQMemberHolder = new BigQMemberHolder(in, out, sortorder, runVectorlen);
		
	pthread_t threadTwoPassMultiwayMergeSort;
	pthread_create(&threadTwoPassMultiwayMergeSort, NULL, &TwoPassMultiwayMergeSort, (void*)bigQMemberHolder);
	
    // construct priority queue over sorted runVectors and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	// out.ShutDown ();
}

BigQ::~BigQ () {
}