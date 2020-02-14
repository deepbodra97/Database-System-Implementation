#include "BigQ.h"

class BigQMemberHolder{

	public:
		Pipe *in;
		Pipe *out;
		OrderMaker *sortorder;
		int runVectorlen;
		File file;

		BigQMemberHolder(Pipe &in, Pipe &out, OrderMaker &sortorder, int runVectorlen){
			this->in = &in;
			this->out = &out;
			this->sortorder = &sortorder;
			this->runVectorlen = runVectorlen;
		}
};

class RecordComparator{
	ComparisonEngine comparisonEngine;
	OrderMaker *sortorder;

	public:
		RecordComparator(OrderMaker sortorder){
			this->sortorder = &sortorder;
		}

		bool operator()(Record *left, Record *right){
			return comparisonEngine.Compare(left, right, sortorder)<0;
		}
};

class RunRecord{
	public:
		Record record;
		int runNumber;
};

class RunRecordComparator{
	ComparisonEngine comparisonEngine;
	OrderMaker *sortorder;

	public:
		RunRecordComparator(OrderMaker sortorder){
			this->sortorder = &sortorder;
		}

		bool operator()(RunRecord *left, RunRecord *right){
			return comparisonEngine.Compare(&(left->record), &(right->record), sortorder)<0;
		}
};

class RunRecordMetaData{
	public:
		Page page;
		int currentPageNumber;
		int endPageNumber;
};


void *TwoPassMultiwayMergeSort (void *arg) {
	BigQMemberHolder *bigQMemberHolder;
	bigQMemberHolder = (BigQMemberHolder*)arg;

	Record recordFromPipe;
	Schema mySchema ("catalog", "nation");

	File runFile;
	Page runPage;
	vector<Record*> runVector;

	Page tempPage;
	
	RecordComparator recordComparator = RecordComparator(*bigQMemberHolder->sortorder);

	runFile.Open(0, "./runFile.bin");

	int nPagesFilledForARun = 0;

	while(bigQMemberHolder->in->Remove(&recordFromPipe)){
		Record *copyOfRecordFromPipe = new Record();
		copyOfRecordFromPipe->Copy(&recordFromPipe);

		if(tempPage.Append(&recordFromPipe) == 1){ // record fits in the page
			runVector.push_back(copyOfRecordFromPipe); // push the record for this run
		} else{ // record does not fit in the page
			nPagesFilledForARun++;
			tempPage.EmptyItOut();
			if(nPagesFilledForARun == bigQMemberHolder->runVectorlen){ // if runVectorlen pages of records processed
				stable_sort(runVector.begin(), runVector.end(), recordComparator); // sort the records
				for(std::vector<Record*>::iterator it=runVector.begin(); it!=runVector.end(); ++it){ // print the records of this run
					// (*it)->Print(&mySchema);

					if(runPage.Append(*it) == 0){
						// cout<<"runFile.GetLength() = "<<runFile.GetLength()<<endl;
						runFile.AddPage(&runPage, runFile.GetLength()-1<0?0:runFile.GetLength()-1);
						runPage.EmptyItOut();
						runPage.Append(*it);
					}
				}

				if(runPage.GetNumRecs()!=0){
					// cout<<"runFile.GetLength() = "<<runFile.GetLength()<<endl;
					runFile.AddPage(&runPage, runFile.GetLength()-1<0?0:runFile.GetLength()-1);
					runPage.EmptyItOut();
				}

				nPagesFilledForARun = 0;
				runVector.clear();
			}
			tempPage.Append(&recordFromPipe);
			runVector.push_back(copyOfRecordFromPipe);
		}
	}

	nPagesFilledForARun = 0;
	if(!runVector.empty()){
		stable_sort(runVector.begin(), runVector.end(), recordComparator); // sort the records

		for(std::vector<Record*>::iterator it=runVector.begin(); it!=runVector.end(); ++it){ // print the records of this run
			// (*it)->Print(&mySchema);
			if(runPage.Append(*it) == 0){
				// cout<<"runFile.GetLength() = "<<runFile.GetLength()<<endl;
				runFile.AddPage(&runPage, runFile.GetLength()-1<0?0:runFile.GetLength()-1);
				runPage.EmptyItOut();
				runPage.Append(*it);
			}
		}

		if(runPage.GetNumRecs()!=0){
			// cout<<"runFile.GetLength() = "<<runFile.GetLength()<<endl;
			runFile.AddPage(&runPage, runFile.GetLength()-1<0?0:runFile.GetLength()-1);
			runPage.EmptyItOut();
		}

		nPagesFilledForARun = 0;
		// free the Record objects lying around in the memory
		for(vector<Record *>::iterator it=runVector.begin();it!=runVector.end();++it){
	        delete *it;
	    }
		runVector.clear();
	}
	runFile.Close();

	// Phase 2
	runFile.Open(1, "./runFile.bin");
	int nPages = runFile.GetLength()-1;

	priority_queue<RunRecord*, vector<RunRecord*>, RunRecordComparator> priorityQueue(*bigQMemberHolder->sortorder);

	if(nPages==0){
		cout<<"There are no pages in the runFile";
	}else{

		int nRuns = ceil((float)nPages/bigQMemberHolder->runVectorlen);
		
		RunRecordMetaData runRecordMetaData[nRuns];

		for(int i=0, startPageNumber=0; i<nRuns; i++, startPageNumber+=bigQMemberHolder->runVectorlen){
			cout<<"(startPageNumber, endPageNumber):"<<startPageNumber<<","<<startPageNumber+bigQMemberHolder->runVectorlen-1<<endl;
			runRecordMetaData[i].currentPageNumber = startPageNumber;
			runRecordMetaData[i].endPageNumber = startPageNumber+bigQMemberHolder->runVectorlen-1;
			runFile.GetPage(&runRecordMetaData[i].page, startPageNumber);
			if(i==nRuns-1 && nPages%bigQMemberHolder->runVectorlen!=0){
				runRecordMetaData[i].endPageNumber=nPages%bigQMemberHolder->runVectorlen;
			}
			RunRecord *runRecord = new RunRecord;
			runRecordMetaData[i].page.GetFirst(&(runRecord->record));
			runRecord->runNumber = i;
			priorityQueue.push(runRecord);
		}

		// priorityQueue.top()->record.Print(&mySchema);
		while(!priorityQueue.empty()){
			RunRecord *outputRunRecord = priorityQueue.top();
			priorityQueue.pop();
			bigQMemberHolder->out->Insert(&(outputRunRecord->record));

			RunRecord *newRunRecord = new RunRecord;
			if(runRecordMetaData[outputRunRecord->runNumber].page.GetFirst(&(newRunRecord->record)) == 1){
				newRunRecord->runNumber = outputRunRecord->runNumber;
				priorityQueue.push(newRunRecord);
			} else{
				if(runRecordMetaData[outputRunRecord->runNumber].currentPageNumber <= runRecordMetaData[outputRunRecord->runNumber].endPageNumber){
					runRecordMetaData[outputRunRecord->runNumber].currentPageNumber += 1;
					runFile.GetPage(&(runRecordMetaData[outputRunRecord->runNumber].page), runRecordMetaData[outputRunRecord->runNumber].currentPageNumber);
					runRecordMetaData[outputRunRecord->runNumber].page.GetFirst(&(newRunRecord->record));
					priorityQueue.push(newRunRecord);
				}
			}
		}
	}
	runFile.Close();
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