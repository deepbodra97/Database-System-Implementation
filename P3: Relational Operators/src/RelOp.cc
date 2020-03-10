#include "RelOp.h"

/**************************************************************************************************
SELECT PIPE
**************************************************************************************************/

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
	OperatorThreadMemberHolder *params = new OperatorThreadMemberHolder(NULL, &inPipe, NULL, &outPipe, &selOp, &literal, 0, 0, NULL, 0);
	pthread_create(&operatorThread, NULL, operate, (void*) params);
}

void SelectPipe::WaitUntilDone () {
	pthread_join (operatorThread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {
	return;	
}

void* SelectPipe::operate (void *arg) {
	OperatorThreadMemberHolder *params = (OperatorThreadMemberHolder*) arg;
	Record currentRecord;
	ComparisonEngine comparisonEngine;
	while(params->inPipe->Remove(&currentRecord)){
		if(comparisonEngine.Compare(&currentRecord, params->literal, params->selOp)){
			params->outPipe->Insert(&currentRecord);			
		}
	}
	params->outPipe->ShutDown();
}


/**************************************************************************************************
SELECT FILE
**************************************************************************************************/
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	OperatorThreadMemberHolder *params = new OperatorThreadMemberHolder(NULL, NULL, &inFile, &outPipe, &selOp, &literal, 0, 0, NULL, 0);
	pthread_create(&operatorThread, NULL, operate, (void*) params);
}

void SelectFile::WaitUntilDone () {
	pthread_join (operatorThread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
	return;
}

void* SelectFile::operate (void *arg) {
	OperatorThreadMemberHolder *params = (OperatorThreadMemberHolder*) arg;
	Record currentRecord;
	while(params->inFile->GetNext(currentRecord, *params->selOp, *params->literal)){
		params->outPipe->Insert(&currentRecord);
	}
	params->outPipe->ShutDown();
}


/**************************************************************************************************
DUPLICATE REMOVAL
**************************************************************************************************/
void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
	OperatorThreadMemberHolder *params = new OperatorThreadMemberHolder(&mySchema, &inPipe, NULL, &outPipe, NULL, NULL, 0, 0, NULL, this->runLength);
	pthread_create(&operatorThread, NULL, operate, (void*) params);
}

void DuplicateRemoval::WaitUntilDone () {
	pthread_join (operatorThread, NULL);
}

void DuplicateRemoval::Use_n_Pages (int runlen) {
	this->runLength = runlen;
	return;
}

void* DuplicateRemoval::operate (void *arg) {
	OperatorThreadMemberHolder *params = (OperatorThreadMemberHolder*) arg;

	OrderMaker sortOrder(params->mySchema);
	Pipe sortedPipe(PIPE_SIZE);
	BigQ bigQ(*params->inPipe, sortedPipe, sortOrder, params->runLength);

	Record currentRecord, nextRecord;
	ComparisonEngine comparisonEngine;

	if(sortedPipe.Remove(&currentRecord)){
	    while(sortedPipe.Remove(&nextRecord)){
    		if(comparisonEngine.Compare(&currentRecord, &nextRecord, &sortOrder)) {
        		params->outPipe->Insert(&currentRecord);
        		currentRecord.Consume(&nextRecord);
      		}
  		}
	    params->outPipe->Insert(&currentRecord);
	}
	params->outPipe->ShutDown();
}
