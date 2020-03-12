#include "RelOp.h"
/**************************************************************************************************
SELECT PIPE
**************************************************************************************************/

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
	OperatorThreadMemberHolder *params = new OperatorThreadMemberHolder(NULL, &inPipe, NULL, &outPipe, &selOp, &literal, 0, 0, NULL, 0, NULL, NULL, NULL);
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
	OperatorThreadMemberHolder *params = new OperatorThreadMemberHolder(NULL, NULL, &inFile, &outPipe, &selOp, &literal, 0, 0, NULL, 0, NULL, NULL, NULL);
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
PROJECT
**************************************************************************************************/
void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
	OperatorThreadMemberHolder *params = new OperatorThreadMemberHolder(NULL, &inPipe, NULL, &outPipe, NULL, NULL, numAttsInput, numAttsOutput, keepMe, 0, NULL, NULL, NULL);
	pthread_create(&operatorThread, NULL, operate, (void*) params);
}

void Project::WaitUntilDone () {
	pthread_join (operatorThread, NULL);
}

void Project::Use_n_Pages (int runlen) {
	return;
}

void* Project::operate (void *arg) {
	OperatorThreadMemberHolder *params = (OperatorThreadMemberHolder*) arg;
	Record currentRecord;
	while(params->inPipe->Remove(&currentRecord)){
		currentRecord.Project(params->keepMe, params->numAttsOutput, params->numAttsInput);
		params->outPipe->Insert(&currentRecord);
	}
	params->outPipe->ShutDown();
}

/**************************************************************************************************
JOIN
**************************************************************************************************/
void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
	OperatorThreadMemberHolder *params = new OperatorThreadMemberHolder(NULL, &inPipeL, NULL, &outPipe, &selOp, &literal, 0, 0, NULL, 0, NULL, NULL, &inPipeR);
	pthread_create(&operatorThread, NULL, operate, (void*) params);
}

void Join::WaitUntilDone () {
	pthread_join (operatorThread, NULL);
}

void Join::Use_n_Pages (int runlen) {
	this->runLength = runlen;
	return;
}

void* Join::operate (void *arg) {
	OperatorThreadMemberHolder *params = (OperatorThreadMemberHolder*) arg;
	OrderMaker orderLeft, orderRight;
	if (params->selOp->GetSortOrders(orderLeft, orderRight)){
		sortMergeJoin(params->inPipe, &orderLeft, params->inPipeR, &orderRight, params->outPipe, params->selOp, params->literal, params->runLength);
	} else{
		nestedLoopJoin(params->inPipe, params->inPipeR, params->outPipe, params->selOp, params->literal, params->runLength);
	}
	params->outPipe->ShutDown();
}

void Join::sortMergeJoin(Pipe* pleft, OrderMaker* orderLeft, Pipe* pright, OrderMaker* orderRight, Pipe* pout,
	CNF* sel, Record* literal, size_t runLen) {

	ComparisonEngine cmp;
	Pipe sortedLeft(PIPE_SIZE), sortedRight(PIPE_SIZE);
	BigQ qLeft(*pleft, sortedLeft, *orderLeft, runLen), qRight(*pright, sortedRight, *orderRight, runLen);
	Record fromLeft, fromRight, merged, previous;
	JoinBuffer buffer(runLen);

// two-way merge join
	for (bool moreLeft = sortedLeft.Remove(&fromLeft), moreRight = sortedRight.Remove(&fromRight); moreLeft && moreRight; ) {
		int result = cmp.Compare(&fromLeft, orderLeft, &fromRight, orderRight);
		if (result<0) moreLeft = sortedLeft.Remove(&fromLeft);
		else if (result>0) moreRight = sortedRight.Remove(&fromRight);
		else {       // equal attributes: fromLeft == fromRight ==> do joining
			buffer.clear();
			for(previous.Consume(&fromLeft); (moreLeft=sortedLeft.Remove(&fromLeft)) && cmp.Compare(&previous, &fromLeft, orderLeft)==0; previous.Consume(&fromLeft)){
				// FATALIF(!buffer.add(previous), "Join buffer exhausted.");   // gather records of the same value
	  			buffer.add(previous);
	  		}
	  		// FATALIF(!buffer.add(previous), "Join buffer exhausted.");     // remember the last one
	  		buffer.add(previous);
			do {       // Join records from right pipe
				Record rec;
	  			FOREACH(rec, buffer.buffer, buffer.nrecords)
		  		if (cmp.Compare(&rec, &fromRight, literal, sel)) {   // actural join
		  			merged.CrossProduct(&rec, &fromRight);
		  			pout->Insert(&merged);
		  		}
		  		END_FOREACH
	  		} while ((moreRight=sortedRight.Remove(&fromRight)) && cmp.Compare(buffer.buffer, orderLeft, &fromRight, orderRight)==0);    // read all records from right pipe with equal value
		}
	}
}

void Join::nestedLoopJoin(Pipe* pleft, Pipe* pright, Pipe* pout, CNF* sel, Record* literal, size_t runLen) {
	DBFile rightFile;
	dumpFile(*pright, rightFile);
	JoinBuffer leftBuffer(runLen);

	// nested loops join
	Record rec;
	while(pleft->Remove(&rec)){
		if (!leftBuffer.add(rec)) {  // buffer full ==> do join
			joinBuf(leftBuffer, rightFile, *pout, *literal, *sel);
			leftBuffer.clear();       // start next chunk of LEFT
			leftBuffer.add(rec);
		}
	}
	joinBuf(leftBuffer, rightFile, *pout, *literal, *sel);   // join the last buffer
	rightFile.Close();
}

void Join::joinBuf(JoinBuffer& buffer, DBFile& file, Pipe& out, Record& literal, CNF& selOp) {
	ComparisonEngine cmp;
	Record merged;

	Record fromFile, fromBuffer;

	while(file.GetNext(fromFile)){
		FOREACH(fromBuffer, buffer.buffer, buffer.nrecords)
		if (cmp.Compare(&fromBuffer, &fromFile, &literal, &selOp)) {   // actural join
			merged.CrossProduct(&fromBuffer, &fromFile);
			out.Insert(&merged);
		}
		END_FOREACH
	}
}

void gen_random(char *s, const int len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    s[len] = 0;
}

void Join::dumpFile(Pipe& in, DBFile& out) {
	const int RLEN = 10;
	char rstr[RLEN];
	// Rstring::gen(rstr, RLEN);  // need a random name otherwise two or more joins would crash
	gen_random(rstr, RLEN);
	std::string tmpName("join");
	tmpName = tmpName + rstr + ".tmp";
	out.Create((char*)tmpName.c_str(), heap, NULL);
	Record rec;
	while (in.Remove(&rec)) out.Add(rec);
}

JoinBuffer::JoinBuffer(size_t npages): size(0), capacity(PAGE_SIZE*npages), nrecords(0) {
  buffer = new Record[PAGE_SIZE*npages/sizeof(Record*)];
}

JoinBuffer::~JoinBuffer() { delete[] buffer; }

bool JoinBuffer::add (Record& addme) {
  if((size+=addme.GetLength())>capacity) return 0;
  buffer[nrecords++].Consume(&addme);
  return 1;
}

/**************************************************************************************************
DUPLICATE REMOVAL
**************************************************************************************************/
void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
	OperatorThreadMemberHolder *params = new OperatorThreadMemberHolder(&mySchema, &inPipe, NULL, &outPipe, NULL, NULL, 0, 0, NULL, this->runLength, NULL, NULL, NULL);
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
	OperatorThreadMemberHolder *params = new OperatorThreadMemberHolder(NULL, &inPipe, NULL, &outPipe, NULL, NULL, 0, 0, NULL, 0, &computeMe, NULL, NULL);
	pthread_create(&operatorThread, NULL, operate, (void*) params);
	// cout<<"Run:end\n";
}

void Sum::WaitUntilDone () {
	pthread_join (operatorThread, NULL);
	// cout<<"WaitUntilDone:end\n";
}

void Sum::Use_n_Pages (int runlen) {
	// cout<<"Use_n_Pages:end\n";
	return;
}

void* Sum::operate (void *arg) {
	// cout<<"operate:start\n";
	OperatorThreadMemberHolder *params = (OperatorThreadMemberHolder*) arg;
	if (params->function->GetReturnsIntType() == Int) calculateSum<int>(params->inPipe, params->outPipe, params->function);
	else calculateSum<double>(params->inPipe, params->outPipe, params->function);
	params->outPipe->ShutDown();
	// cout<<"operate:end\n";
}

template <class T>
void Sum::calculateSum(Pipe* inPipe, Pipe* outPipe, Function* function) {
	// cout<<"calculateSum:start\n";
	T sum=0;
	Record rec;
	while (inPipe->Remove(&rec)){
		sum += function->Apply<T>(rec);
	}
	Record result(sum);
	outPipe->Insert(&result);
	// cout<<"calculateSum:end\n";
}

/**************************************************************************************************
GROUPBY
**************************************************************************************************/
void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
	OperatorThreadMemberHolder *params = new OperatorThreadMemberHolder(NULL, &inPipe, NULL, &outPipe, NULL, NULL, 0, 0, NULL, 0, &computeMe, &groupAtts, NULL);
	pthread_create(&operatorThread, NULL, operate, (void*) params);
}

void GroupBy::WaitUntilDone () {
	pthread_join (operatorThread, NULL);
}

void GroupBy::Use_n_Pages (int runlen) {
	this->runLength = runlen;
	return;
}

void* GroupBy::operate (void *arg) {
	OperatorThreadMemberHolder *params = (OperatorThreadMemberHolder*) arg;
	if (params->function->GetReturnsIntType() == Int) doGroup<int>(params->inPipe, params->outPipe, params->groupAtts, params->function, params->runLength);
	else doGroup<double>(params->inPipe, params->outPipe, params->groupAtts, params->function, params->runLength);
}

template <class T>   // similar to duplicate elimination
void GroupBy::doGroup(Pipe* in, Pipe* out, OrderMaker* order, Function* func, size_t runLen) {
  Pipe sorted(PIPE_SIZE);
  BigQ biq(*in, sorted, *order, (int)runLen);
  Record cur, next;
  ComparisonEngine cmp;

  // TODO: why not shutting down the input pipe here??
  if(sorted.Remove(&cur)) {  // cur holds the current group
	T sum = func->Apply<T>(cur);   // holds the sum for the current group
	while(sorted.Remove(&next))
	  if(cmp.Compare(&cur, &next, order)) {
		putGroup(cur, sum, out, order);
		cur.Consume(&next);
		sum = func->Apply<T>(cur);
	  } else sum += func->Apply<T>(next);
	putGroup(cur, sum, out, order);    // put the last group into the output pipeline
  }
}