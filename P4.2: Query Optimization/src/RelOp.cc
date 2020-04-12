#include "RelOp.h"

void RelationalOp::WaitUntilDone() {
	pthread_join (operatorThread, NULL); 
}

void RelationalOp::Use_n_Pages (int runlen) {
	this->runLength = runlen;
	return;
}

/**************************************************************************************************
SELECT PIPE
**************************************************************************************************/

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
	RelationalOpThreadMemberHolder *params = new RelationalOpThreadMemberHolder(NULL, &inPipe, NULL, &outPipe, NULL, &selOp, &literal, 0, 0, NULL, this->runLength, NULL, NULL, NULL); // init thread params
	pthread_create(&operatorThread, NULL, Operate, (void*) params); // create thread
}

void* SelectPipe::Operate (void *arg) {
	RelationalOpThreadMemberHolder *params = (RelationalOpThreadMemberHolder*) arg; // parse thread params
	Record currentRecord;
	ComparisonEngine comparisonEngine;
	while(params->inPipe->Remove(&currentRecord)){ // fetch the next record from the pipe
		if(comparisonEngine.Compare(&currentRecord, params->literal, params->selOp)){ // does the current record match literal and cnf
			params->outPipe->Insert(&currentRecord); // if yes add the record to the output pipe			
		}
	}
	params->outPipe->ShutDown(); // shutdown output pipe
}


/**************************************************************************************************
SELECT FILE
**************************************************************************************************/
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	RelationalOpThreadMemberHolder *params = new RelationalOpThreadMemberHolder(NULL, NULL, &inFile, &outPipe, NULL, &selOp, &literal, 0, 0, NULL, this->runLength, NULL, NULL, NULL); // init thread params
	
	// debug 
	params->outputSchema = outputSchema;

	pthread_create(&operatorThread, NULL, Operate, (void*) params); // create thread

	// debug
	// PrintCache(params->outputCache, outputSchema);
}

void* SelectFile::Operate (void *arg) {
	cout<<"SelectFile output"<<endl;

	RelationalOpThreadMemberHolder *params = (RelationalOpThreadMemberHolder*) arg; // parse thread params
	Record currentRecord;
	
	while(params->inFile->GetNext(currentRecord, *params->selOp, *params->literal)){ // fetch the next record from the pipe that matches the literal and cnf
		
		// debug 
		// Record *r = new Record();
		// r->Copy(&currentRecord);
		// r->Print(params->outputSchema);
		// params->outputCache.push_back(r);
		// debug end

		params->outPipe->Insert(&currentRecord); // add the record to the output pipe
	}
	params->outPipe->ShutDown(); // shutdown output pipe
}



/**************************************************************************************************
PROJECT
**************************************************************************************************/
void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
	cout<<"Project Run: keepMe="<<*keepMe<<endl;
	RelationalOpThreadMemberHolder *params = new RelationalOpThreadMemberHolder(NULL, &inPipe, NULL, &outPipe, NULL, NULL, NULL, numAttsInput, numAttsOutput, keepMe, this->runLength, NULL, NULL, NULL); // init thread params
	
	// debug 
	params->outputSchema = outputSchema;

	pthread_create(&operatorThread, NULL, Operate, (void*) params); // create thread
}

void* Project::Operate (void *arg) {
	cout<<"Project output"<<endl;

	RelationalOpThreadMemberHolder *params = (RelationalOpThreadMemberHolder*) arg; // parse thread params
	
	// debug
		// int x=1;
		// params->keepMe = &x;
		cout<<"keepMe="<<endl;
		for(int* km = params->keepMe;km;km++){
			cout<<*km<<" ";
		}
		cout<<endl;
		cout<<"numAttsInput="<<params->numAttsInput<<endl;
		cout<<"numAttsOutput="<<params->numAttsOutput<<endl;


		// cout<<"keepMe="<<endl;
		// debug end


	Record currentRecord;
	while(params->inPipe->Remove(&currentRecord)){
		currentRecord.Project(params->keepMe, params->numAttsOutput, params->numAttsInput);

		// debug
		// Record *r = new Record();
		// r->Copy(&currentRecord);
		// cout<<"p:";
		// r->Print(params->outputSchema);
		// params->outputCache.push_back(r);
		// debug end	

		params->outPipe->Insert(&currentRecord);
	}
	params->outPipe->ShutDown(); // shutdown output pipe
}

/**************************************************************************************************
JOIN
**************************************************************************************************/
void generateRandomString(char *s, int len) {
    static const char alphaNum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        s[i] = alphaNum[rand() % (sizeof(alphaNum) - 1)];
    }
    s[len] = 0;
}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
	RelationalOpThreadMemberHolder *params = new RelationalOpThreadMemberHolder(NULL, &inPipeL, NULL, &outPipe, NULL, &selOp, &literal, 0, 0, NULL, this->runLength, NULL, NULL, &inPipeR); // init thread params
	
	// debug 
	params->outputSchema = outputSchema;

	pthread_create(&operatorThread, NULL, Operate, (void*) params); // create thread
}

void* Join::Operate (void *arg) {
	RelationalOpThreadMemberHolder *params = (RelationalOpThreadMemberHolder*) arg; // parse thread params
	OrderMaker leftOrderMaker, rightOrderMaker; // left and right ordermaker
	if (params->selOp->GetSortOrders(leftOrderMaker, rightOrderMaker)){ // if an acceptable ordering exists
		SortMergeJoin(params->inPipe, &leftOrderMaker, params->inPipeR, &rightOrderMaker, params->outPipe, params->selOp, params->literal, params->runLength, params); // the perform sort merge join
	} else{
		NestedLoopJoin(params->inPipe, params->inPipeR, params->outPipe, params->selOp, params->literal, params->runLength); // else perform nested loop join
	}
	params->outPipe->ShutDown(); // shutdown output pipe
}

void Join::SortMergeJoin(Pipe* leftPipe, OrderMaker* leftOrderMaker, Pipe* rightPipe, OrderMaker* rightOrderMaker, Pipe* outPipe, CNF* selOp, Record* literal, int runLength, RelationalOpThreadMemberHolder* params) {
	cout<<"SortMergeJoin output"<<endl;

	ComparisonEngine comparisonEngine;
	Pipe leftSortedPipe(PIPE_SIZE), rightSortedPipe(PIPE_SIZE);
	BigQ leftBigQ(*leftPipe, leftSortedPipe, *leftOrderMaker, runLength), rightBigQ(*rightPipe, rightSortedPipe, *rightOrderMaker, runLength); // start BigQ to get the records in sorted order
	Record recordFromLeft, recordFromRight, mergedRecord, previousRecord;
	FixedSizeRecordBuffer recordBuffer(runLength);
	bool leftNotEmpty = leftSortedPipe.Remove(&recordFromLeft), rightNotEmpty = rightSortedPipe.Remove(&recordFromRight);

	while(leftNotEmpty && rightNotEmpty) { // while there are records in both the sorted pipes
		int comparisonStatusForPipes = comparisonEngine.Compare(&recordFromLeft, leftOrderMaker, &recordFromRight, rightOrderMaker); // compare records based on respective ordermakers
		
		if (comparisonStatusForPipes<0){  // if left is smaller than right
			leftNotEmpty = leftSortedPipe.Remove(&recordFromLeft); // then advance left
		} else if (comparisonStatusForPipes>0){ // if right is smaller than left
			rightNotEmpty = rightSortedPipe.Remove(&recordFromRight); // advance right
		} else { // if left and right are equal
			recordBuffer.Clear(); // clear buffer
			for(previousRecord.Consume(&recordFromLeft); (leftNotEmpty=leftSortedPipe.Remove(&recordFromLeft)) && comparisonEngine.Compare(&previousRecord, &recordFromLeft, leftOrderMaker)==0; previousRecord.Consume(&recordFromLeft)){
	  			recordBuffer.Add(previousRecord); // accumulate records of the same value
	  		}
	  		recordBuffer.Add(previousRecord); // add the last record
	  		int comparisonStatusForBufferAndPipe;
			do { // Join records from the buffer one by one with the head of the pipe until the records from the buffer match with the head of the pipe
			  	for (Record *recordFromBuffer=recordBuffer.buffer; recordFromBuffer!=recordBuffer.buffer+(recordBuffer.numRecords); recordFromBuffer++) {
			  		if (comparisonEngine.Compare(recordFromBuffer, &recordFromRight, literal, selOp)) {   // if they match
			  			mergedRecord.MergeTheRecords(recordFromBuffer, &recordFromRight); // concatenates left and right by setting up attsToKeep
			  			
						// debug 
						// Record *r = new Record();
						// r->Copy(&mergedRecord);
						// r->Print(params->outputSchema);
						// params->outputCache.push_back(r);
						// debug end	

			  			outPipe->Insert(&mergedRecord);
			  		}
			  	}
			  	rightNotEmpty = rightSortedPipe.Remove(&recordFromRight);
			  	comparisonStatusForBufferAndPipe = comparisonEngine.Compare(recordBuffer.buffer, leftOrderMaker, &recordFromRight, rightOrderMaker);
	  		} while (rightNotEmpty && comparisonStatusForBufferAndPipe==0);    // read all records from right pipe with equal value
		}
	}
}

void Join::NestedLoopJoin(Pipe* leftPipe, Pipe* rightPipe, Pipe* outPipe, CNF* selOp, Record* literal, int runLength) {
	cout<<"SortMergeJoin output"<<endl;

	DBFile rightFile;
	PipeToFile(*rightPipe, rightFile);
	FixedSizeRecordBuffer leftBuffer(runLength);

	Record recordFromLeft;
	while(leftPipe->Remove(&recordFromLeft)){
		if (!leftBuffer.Add(recordFromLeft)) {  // if buffer is full
			JoinBufferWithFile(leftBuffer, rightFile, *outPipe, *literal, *selOp); // join records from buffer and right file
			leftBuffer.Clear(); // clear buffer
			leftBuffer.Add(recordFromLeft); // add this record now
		}
	}
	JoinBufferWithFile(leftBuffer, rightFile, *outPipe, *literal, *selOp); // join records from buffer and right file
	rightFile.Close(); // close rightFile
}

void Join::JoinBufferWithFile(FixedSizeRecordBuffer& recordBuffer, DBFile& file, Pipe& out, Record& literal, CNF& selOp) {
	ComparisonEngine cmp;
	Record merged;

	Record recordFromFile;
	file.MoveFirst();
	while(file.GetNext(recordFromFile)){
		for (Record *recordFromBuffer=recordBuffer.buffer; recordFromBuffer!=recordBuffer.buffer+(recordBuffer.numRecords); recordFromBuffer++) {
			if (cmp.Compare(recordFromBuffer, &recordFromFile, &literal, &selOp)) {
				merged.MergeTheRecords(recordFromBuffer, &recordFromFile); // concatenates left and right by setting up attsToKeep
				out.Insert(&merged);
			}
		}
	}
}

void Join::PipeToFile(Pipe& inPipe, DBFile& outFile) {
	int randomStringLen = 8;
	char randomString[randomStringLen];
	generateRandomString(randomString, randomStringLen); // generate a randomString of length=randomStringLens

	std::string fileName("join");
	fileName = fileName + randomString + ".bin"; // filename
	outFile.Create((char*)fileName.c_str(), heap, NULL);
	Record currentRecord;
	while (inPipe.Remove(&currentRecord)){
		outFile.Add(currentRecord); // add the record to the file
	}
}

bool FixedSizeRecordBuffer::Add (Record& addme) {
	if((size+=addme.GetLength())>capacity){ // if addMe cannot fit in the buffer
		return 0; // dont add to the buffer
  	}
	buffer[numRecords++].Consume(&addme); // else add to the buffer
	return 1;
}

void FixedSizeRecordBuffer::Clear () {
	size = 0;
	numRecords = 0;
}

FixedSizeRecordBuffer::FixedSizeRecordBuffer(int runLength) {
	numRecords = 0;
	size = 0;
	capacity = PAGE_SIZE*runLength; // capacty = page size * runLength
	buffer = new Record[PAGE_SIZE*runLength/sizeof(Record*)]; // allocate [(page size * runLength) / size of the record pointer ] bytes for the buffer
}

FixedSizeRecordBuffer::~FixedSizeRecordBuffer() {
	delete[] buffer; // free the buffer
}

/**************************************************************************************************
DUPLICATE REMOVAL
**************************************************************************************************/
void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
	RelationalOpThreadMemberHolder *params = new RelationalOpThreadMemberHolder(&mySchema, &inPipe, NULL, &outPipe, NULL, NULL, NULL, 0, 0, NULL, this->runLength, NULL, NULL, NULL);
	pthread_create(&operatorThread, NULL, Operate, (void*) params);
}

void* DuplicateRemoval::Operate (void *arg) {
	RelationalOpThreadMemberHolder *params = (RelationalOpThreadMemberHolder*) arg;

	OrderMaker sortOrder(params->mySchema);
	Pipe sortedPipe(PIPE_SIZE);
	BigQ *bigQ = new BigQ(*params->inPipe, sortedPipe, sortOrder, params->runLength);
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

/**************************************************************************************************
SUM
**************************************************************************************************/
void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
	RelationalOpThreadMemberHolder *params = new RelationalOpThreadMemberHolder(NULL, &inPipe, NULL, &outPipe, NULL, NULL, NULL, 0, 0, NULL, this->runLength, &computeMe, NULL, NULL);
	pthread_create(&operatorThread, NULL, Operate, (void*) params);
}

void* Sum::Operate (void *arg) {
	RelationalOpThreadMemberHolder *params = (RelationalOpThreadMemberHolder*) arg;
	if (params->function->GetReturnsIntType() == Int) CalculateSum<int>(params->inPipe, params->outPipe, params->function); // if int
	else CalculateSum<double>(params->inPipe, params->outPipe, params->function); // if double
	params->outPipe->ShutDown();
}

template <class T>
void Sum::CalculateSum(Pipe* inPipe, Pipe* outPipe, Function* function) {
	std::stringstream recordString;
	Record result;

	T sum = 0;
	Record currentRecord;
	while (inPipe->Remove(&currentRecord)){
		sum += function->Apply<T>(currentRecord);
	}
	
	Attribute attr;
	attr.name = "SUM";
	if(function->GetReturnsIntType() == Int){
		recordString << sum << "|";
		attr.myType = Int;
	} else{
		recordString <<sum << "|";
		attr.myType = Double;
	}
	
	Schema sumSchema("sum_schema", 1, &attr);
	result.ComposeRecord(&sumSchema, recordString.str().c_str());
	outPipe->Insert(&result);
}

/**************************************************************************************************
GROUPBY
**************************************************************************************************/
void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
	RelationalOpThreadMemberHolder *params = new RelationalOpThreadMemberHolder(NULL, &inPipe, NULL, &outPipe, NULL, NULL, NULL, 0, 0, NULL, this->runLength, &computeMe, &groupAtts, NULL);
	pthread_create(&operatorThread, NULL, Operate, (void*) params);
}

void* GroupBy::Operate (void *arg) {
	RelationalOpThreadMemberHolder *params = (RelationalOpThreadMemberHolder*) arg;

	cout<<"GroupBy ordermaker"<<endl;
	params->groupAtts->Print();

	if (params->function->GetReturnsIntType() == Int) MakeGroups<int>(params->inPipe, params->outPipe, params->groupAtts, params->function, params->runLength);
	else MakeGroups<double>(params->inPipe, params->outPipe, params->groupAtts, params->function, params->runLength);
	params->outPipe->ShutDown();
}

template <class T>
void GroupBy::MakeGroups(Pipe* inPipe, Pipe* outPipe, OrderMaker* orderMaker, Function* function, int runLength) {
	Pipe sortedPipe(PIPE_SIZE);
	BigQ bigQ(*inPipe, sortedPipe, *orderMaker, (int)runLength);
	Record currentRecord, nextRecord;
  	ComparisonEngine comparisonEngine;

	if(sortedPipe.Remove(&currentRecord)){  // currentRecord holds the  group
		T sum = function->Apply<T>(currentRecord);   // sum for current group
		while(sortedPipe.Remove(&nextRecord)){
			if(comparisonEngine.Compare(&currentRecord, &nextRecord, orderMaker)) {
				AddGroup(currentRecord, sum, outPipe, orderMaker, function);
				currentRecord.Consume(&nextRecord);
				sum = function->Apply<T>(currentRecord);
			} else{ 
				sum += function->Apply<T>(nextRecord);
			}
		}
		AddGroup(currentRecord, sum, outPipe, orderMaker, function);    // add last group in output pipe
	}
}

template <class T>
void GroupBy::AddGroup(Record& record, const T& sum, Pipe* outPipe, OrderMaker* orderMaker, Function* function) {
	record.Project(orderMaker->GetAtts(), orderMaker->GetNumAtts(), record.GetNumAtts());

	std::stringstream sumRecordStringStream;
	Record sumRecord;
	sumRecordStringStream<< sum << "|";

	Attribute attr;
	attr.name = "SUM";

	if(function->GetReturnsIntType() == Int){
		attr.myType = Int;
	}else {
		attr.myType = Double;
	}

	Schema sumSchema("sum_schema", 1, &attr);
	sumRecord.ComposeRecord(&sumSchema, sumRecordStringStream.str().c_str()); // create sum record

	Record mergedRecord;
	mergedRecord.MergeTheRecords(&sumRecord, &record); // merge sumrecord with record
	outPipe->Insert(&mergedRecord);
}

/**************************************************************************************************
WRITEOUT
**************************************************************************************************/
void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
	RelationalOpThreadMemberHolder *params = new RelationalOpThreadMemberHolder(&mySchema, &inPipe, NULL, NULL, outFile, NULL, NULL, 0, 0, NULL, this->runLength, NULL, NULL, NULL);
	pthread_create(&operatorThread, NULL, Operate, (void*) params);
}

void* WriteOut::Operate (void *arg) {
	RelationalOpThreadMemberHolder *params = (RelationalOpThreadMemberHolder*) arg;
	Record currentRecord;
	cout<<"RelOp:WriteOut"<<endl;
	params->mySchema->print();
	while(params->inPipe->Remove(&currentRecord)){
		// currentRecord.Print(params->mySchema);
		currentRecord.Write(params->outFile, params->mySchema);
	}
}