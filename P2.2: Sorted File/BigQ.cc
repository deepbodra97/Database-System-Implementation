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
		int runLength; // length of this run in the number of pages
};

// 'Two Pass Multiway Merge Sort' Thread
void *TwoPassMultiwayMergeSort (void *arg) {

	/* PHASE 1: Split phase
	 * read data from in pipe sort them into runVectorlen pages
	 */

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


	vector<RunRecordMetaData*> runRecordMetaData; // acts as a buffer for the merge phase. new records will be  picked from this and added to the priority queue
	int nRuns = 0;

	while(bigQMemberHolder->in->Remove(&recordFromPipe)){ // while there are records in the pipe
		Record *copyOfRecordFromPipe = new Record(); // to store copy of the record
		copyOfRecordFromPipe->Copy(&recordFromPipe); // copy the record from pipe

		if(tempPage.Append(&recordFromPipe) == 1){ // record fits in the temp page
			runVector.push_back(copyOfRecordFromPipe); // add the record in the vector for the current run
		} else{ // record does not fit in the page
			nPagesFilledForARun++; // increase page number
			tempPage.EmptyItOut(); // empty the temp page
			if(nPagesFilledForARun == bigQMemberHolder->runVectorlen){ // if runVectorlen pages of records processed
				int currentRunLength = 0;

				stable_sort(runVector.begin(), runVector.end(), recordComparator); // sort the records
				for(std::vector<Record*>::iterator it=runVector.begin(); it!=runVector.end(); ++it){ // commit the sorted records to the file
					if(runPage.Append(*it) == 0){
						runFile.AddPage(&runPage, runFile.GetLength()-1<0?0:runFile.GetLength()-1);
						runPage.EmptyItOut();
						runPage.Append(*it);
						currentRunLength++;
					}
				}

				if(runPage.GetNumRecs()!=0){ // if the page is not full but still has records then also we must commit it to the file
					runFile.AddPage(&runPage, runFile.GetLength()-1<0?0:runFile.GetLength()-1);
					runPage.EmptyItOut();
					currentRunLength++;
				}
				RunRecordMetaData *newRunRecordMetaDatum = new RunRecordMetaData;
				if(nRuns == 0){
					newRunRecordMetaDatum->currentPageNumber = 0;
					newRunRecordMetaDatum->endPageNumber = currentRunLength-1;
				} else{
					newRunRecordMetaDatum->currentPageNumber = runRecordMetaData[nRuns-1]->endPageNumber+1;
					newRunRecordMetaDatum->endPageNumber = newRunRecordMetaDatum->currentPageNumber+currentRunLength-1;
				}
				runRecordMetaData.push_back(newRunRecordMetaDatum);

				nRuns++;
				nPagesFilledForARun = 0; // set the number of pages filled to 0 for the next run
				runVector.clear(); // clear vector
			}
			tempPage.Append(&recordFromPipe);
			runVector.push_back(copyOfRecordFromPipe); // add the record that did not fit earlier as a part of the next run
		}
	}

	nPagesFilledForARun = 0;
	runPage.EmptyItOut();
	tempPage.EmptyItOut();
	if(!runVector.empty()){
		stable_sort(runVector.begin(), runVector.end(), recordComparator); // sort the records

		int currentRunLength = 0;
		for(std::vector<Record*>::iterator it=runVector.begin(); it!=runVector.end(); ++it){ // commit the sorted records to the file
			if(runPage.Append(*it) == 0){
				runFile.AddPage(&runPage, runFile.GetLength()-1<0?0:runFile.GetLength()-1);
				runPage.EmptyItOut();
				runPage.Append(*it);
				currentRunLength++;
			}
		}

		if(runPage.GetNumRecs()!=0){ // if the page is not full but still has records then also we must commit it to the file
			runFile.AddPage(&runPage, runFile.GetLength()-1<0?0:runFile.GetLength()-1);
			runPage.EmptyItOut();
			currentRunLength++;
		}

		RunRecordMetaData *newRunRecordMetaDatum = new RunRecordMetaData;
		if(nRuns == 0){
			newRunRecordMetaDatum->currentPageNumber = 0;
			newRunRecordMetaDatum->endPageNumber = currentRunLength-1;
		} else{
			newRunRecordMetaDatum->currentPageNumber = runRecordMetaData[nRuns-1]->endPageNumber+1;
			newRunRecordMetaDatum->endPageNumber = newRunRecordMetaDatum->currentPageNumber+currentRunLength-1;
		}
		runRecordMetaData.push_back(newRunRecordMetaDatum);

		// free the Record objects lying around in the memory
		for(vector<Record *>::iterator it=runVector.begin();it!=runVector.end();++it){
	        delete *it;
	    }
	    nRuns++;
		runVector.clear(); // free the space occupied by the vector
	}


	nPagesFilledForARun = 0;
	if(!runVector.empty()){
		stable_sort(runVector.begin(), runVector.end(), recordComparator); // sort the records

		for(std::vector<Record*>::iterator it=runVector.begin(); it!=runVector.end(); ++it){ // commit the sorted records to the file
			bigQMemberHolder->out->Insert(*it);
		}
		// free the Record objects lying around in the memory
		for(vector<Record *>::iterator it=runVector.begin();it!=runVector.end();++it){
	        delete *it;
	    }
		runVector.clear(); // free the space occupied by the vector
	}
	


	runFile.Close();


	/* PHASE 2: Merge sorted Runs
	 * construct priority queue over sorted runVectors and dump sorted data into the out pipe
	 */
	runFile.Open(1, "./runFile.bin");
	int nPages = runFile.GetLength()-1;
	priority_queue<RunRecord*, vector<RunRecord*>, RunRecordComparator> priorityQueue(*bigQMemberHolder->sortorder); // priority queue to merge the records

	if(nPages==0){
		cout<<"RunFile is empty! There are no pages in the runFile"<<endl;
	}else{

		// init runRecordMetaData
		for(int i=0, startPageNumber=0; i<nRuns; i++, startPageNumber+=bigQMemberHolder->runVectorlen){
			runFile.GetPage(&runRecordMetaData[i]->page, runRecordMetaData[i]->currentPageNumber); // get the 1st page of i-th run
			RunRecord *runRecord = new RunRecord; // this record will be added to the priority queue
			runRecord->runNumber = i; // set the run number
			runRecordMetaData[i]->page.GetFirst(&(runRecord->record)); // get the first record from the page
			priorityQueue.push(runRecord); // push the record to the priority queue
		}
		
		while(!priorityQueue.empty()){ // while the queue has runrecords
			RunRecord *outputRunRecord = priorityQueue.top(); // get a pointer to the smallest runrecord in the queue
			priorityQueue.pop(); // remove the runrecord from the queue
			bigQMemberHolder->out->Insert(&(outputRunRecord->record)); // insert in the output pipe
			RunRecord *newRunRecord = new RunRecord; // the record after the popped record in the run will be stored in this
			newRunRecord->runNumber = outputRunRecord->runNumber; // set run number of this record equal to that of the popped record
			if(runRecordMetaData[outputRunRecord->runNumber]->page.GetFirst(&(newRunRecord->record)) == 1){ // if this page for this run has more records then fetch the 1st record
				
				priorityQueue.push(newRunRecord); // push this record to the queue
			} else{ // if the page doesn't have more records
				runRecordMetaData[outputRunRecord->runNumber]->currentPageNumber += 1; // increase the current page number by 1
				if(runRecordMetaData[outputRunRecord->runNumber]->currentPageNumber <= runRecordMetaData[outputRunRecord->runNumber]->endPageNumber){ // if we are not past the last page of the run
					runFile.GetPage(&runRecordMetaData[outputRunRecord->runNumber]->page, runRecordMetaData[outputRunRecord->runNumber]->currentPageNumber); // get the next page
					runRecordMetaData[outputRunRecord->runNumber]->page.GetFirst(&(newRunRecord->record)); // get the 1st record from this page
					priorityQueue.push(newRunRecord); // push it into the queue
				}
			}
		}
		
	}
	for(vector<RunRecordMetaData*>::iterator it=runRecordMetaData.begin();it!=runRecordMetaData.end();++it){
        delete *it;
    }
	runRecordMetaData.clear(); // free the space occupied by the vector

	runFile.Close(); // close runFile
	remove("./runFile.bin"); // remove runFile

	// finally shut down the out pipe
	bigQMemberHolder->out->ShutDown();
}


BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runVectorlen) {
	BigQMemberHolder *bigQMemberHolder = new BigQMemberHolder(in, out, sortorder, runVectorlen);
		
	pthread_t threadTwoPassMultiwayMergeSort;
	pthread_create(&threadTwoPassMultiwayMergeSort, NULL, &TwoPassMultiwayMergeSort, (void*)bigQMemberHolder);
}

BigQ::~BigQ () {
}

//Phase 1: test start
	// int nRuns = 0;
	// while(bigQMemberHolder->in->Remove(&recordFromPipe)){ // while there are records in the pipe
	// 	Record *copyOfRecordFromPipe = new Record(); // to store copy of the record
	// 	copyOfRecordFromPipe->Copy(&recordFromPipe); // copy the record from pipe

	// 	if(tempPage.Append(&recordFromPipe) == 1){ // record fits in the temp page
	// 		runVector.push_back(copyOfRecordFromPipe); // add the record in the vector for the current run
	// 	} else{ // record does not fit in the page
	// 		nPagesFilledForARun++; // increase page number
	// 		tempPage.EmptyItOut(); // empty the temp page
	// 		if(nPagesFilledForARun == bigQMemberHolder->runVectorlen){ // if runVectorlen pages of records processed

	// 			stable_sort(runVector.begin(), runVector.end(), recordComparator); // sort the records
	// 			for(std::vector<Record*>::iterator it=runVector.begin(); it!=runVector.end(); ++it){ // commit the sorted records to the file
	// 				bigQMemberHolder->out->Insert(*it);
	// 			}
	// 			cout<<"-----End of Run-----"<<endl;
	// 			nPagesFilledForARun = 0; // set the number of pages filled to 0 for the next run
	// 			runVector.clear(); // clear vector
	// 		}
	// 		tempPage.Append(&recordFromPipe);
	// 		runVector.push_back(copyOfRecordFromPipe); // add the record that did not fit earlier as a part of the next run
	// 	}
	// }


/*// test start
		// print heads of runs
		cout<<"Heads"<<endl;
		Schema mySchema("catalog", "orders");
		Page printPage;
		int printPageNumber=0;
		Record printRecord;
		for(int i=0; i<nRuns; i++){
			runFile.GetPage(&printPage, printPageNumber);
			if(printPage.GetFirst(&printRecord) == 1){
				printRecord.Print(&mySchema);
			}
			while(printPage.GetFirst(&printRecord) == 1){
					
			}
			printPageNumber+=1;
		}

		// test end*/

		// test start
		// print entire runFile
		/*cout<<"Entire runFile"<<endl;
		Schema mySchema("catalog", "orders");
		Page printPage;
		int printPageNumber=0;
		Record printRecord;
		runFile.GetPage(&printPage, printPageNumber);
		while(true){
			
			if(printPage.GetFirst(&printRecord) == 1){
				printRecord.Print(&mySchema);
			}else{
				printPageNumber+=1;
				if(printPageNumber<=nPages){
					runFile.GetPage(&printPage, printPageNumber);
				}
			}
		}*/ 

		// test end

// test start
		/*cout<<"Popped"<<endl;
		for(int i=0; i<nRuns; i++){
			priorityQueue.top()->record.Print(&mySchema);
			priorityQueue.pop();
		}*/
		// Schema mySchema("catalog", "orders");
		// test end
