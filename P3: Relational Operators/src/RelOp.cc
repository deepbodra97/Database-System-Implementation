#include "RelOp.h"

void RelationalOp::WaitUntilDone() {
	pthread_join (operatorThread, NULL); 
}

int RelationalOp::create_joinable_thread(pthread_t *thread, void *(*start_routine) (void *), void *arg) {
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	int rc = pthread_create(thread, &attr, start_routine, arg);
	pthread_attr_destroy(&attr);
	return rc;
}

/**************************************************************************************************
SELECT PIPE
**************************************************************************************************/

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
	OperatorThreadMemberHolder *params = new OperatorThreadMemberHolder(NULL, &inPipe, NULL, &outPipe, NULL, &selOp, &literal, 0, 0, NULL, 0, NULL, NULL, NULL);
	// pthread_create(&operatorThread, NULL, operate, (void*) params);
	create_joinable_thread(&operatorThread, operate, params);
}

/*void SelectPipe::WaitUntilDone () {
	pthread_join (operatorThread, NULL);
}*/

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
	OperatorThreadMemberHolder *params = new OperatorThreadMemberHolder(NULL, NULL, &inFile, &outPipe, NULL, &selOp, &literal, 0, 0, NULL, 0, NULL, NULL, NULL);
	// pthread_create(&operatorThread, NULL, operate, (void*) params);
	create_joinable_thread(&operatorThread, operate, params);
}

/*void SelectFile::WaitUntilDone () {
	pthread_join (operatorThread, NULL);
}*/

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
	OperatorThreadMemberHolder *params = new OperatorThreadMemberHolder(NULL, &inPipe, NULL, &outPipe, NULL, NULL, NULL, numAttsInput, numAttsOutput, keepMe, 0, NULL, NULL, NULL);
	// pthread_create(&operatorThread, NULL, operate, (void*) params);
	create_joinable_thread(&operatorThread, operate, params);
}

/*void Project::WaitUntilDone () {
	pthread_join (operatorThread, NULL);
}*/

void Project::Use_n_Pages (int runlen) {
	return;
}

void* Project::operate (void *arg) {
	cout<<"operate:start\n";
	OperatorThreadMemberHolder *params = (OperatorThreadMemberHolder*) arg;
	Record currentRecord;
	while(params->inPipe->Remove(&currentRecord)){
		cout<<"operate:loop\n";
		currentRecord.Project(params->keepMe, params->numAttsOutput, params->numAttsInput);
		params->outPipe->Insert(&currentRecord);
	}
	params->outPipe->ShutDown();
	cout<<"operate:end\n";
}

/**************************************************************************************************
JOIN
**************************************************************************************************/
void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
	OperatorThreadMemberHolder *params = new OperatorThreadMemberHolder(NULL, &inPipeL, NULL, &outPipe, NULL, &selOp, &literal, 0, 0, NULL, 0, NULL, NULL, &inPipeR);
	// pthread_create(&operatorThread, NULL, operate, (void*) params);
	create_joinable_thread(&operatorThread, operate, params);
}

/*void Join::WaitUntilDone () {
	pthread_join (operatorThread, NULL);
}*/

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
	file.MoveFirst();
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




// // void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { 

// // 	this->inPipeL = &inPipeL;
// // 	this->inPipeR = &inPipeR;
// // 	this->outPipe = &outPipe;
// // 	this->selOp = &selOp;
// // 	this->literal = &literal;
// // 	pthread_create(&thread,NULL,jswpn,this);

// // }

// // void* Join::jswpn(void* arg){

// // 	Join *j = (Join *) arg;
// // 	j->join();	

// // }


// // void Join::join(){

// // 	/*OrderMaker *omL = new OrderMaker();
// // 	OrderMaker *omR = new OrderMaker();
// // 	if(selOp->GetSortOrders(*omL,*omR)==0){}
// // 	else{
		
// // 		Pipe *OL = new Pipe(100);
// // 		Pipe *OR = new Pipe(100);
// // 		BigQ L = BigQ (*inPipeL, *OL, *omL, nPages);
// // 		BigQ R = BigQ (*inPipeR, *OR, *omR, nPages);
// // 		Record *RL = new Record();
// // 		Record *RR = new Record();
// // 		int resultL = OL->Remove(RL);
// // 		int resultR = OR->Remove(RR);
// // 		while(resultL&&resultR){
			
// // 		}
	
// // 	}
// // 	*/


// //         Pipe *LO = new Pipe(100);
// //         Pipe *RO = new Pipe(100);

// //         OrderMaker *omL = new OrderMaker();
// //         OrderMaker *omR = new OrderMaker();
    
// // 	ComparisonEngine compEng;

// //         int sortMergeFlag = selOp->GetSortOrders(*omL, *omR);

// //         // if sort merge flag != 0 perform SORT-MERGE JOIN
// //         if (sortMergeFlag != 0) {

// //                 // sort left pipe
// //                 BigQ L(*inPipeL, *LO, *omL, this->nPages);

// //                 // sort right pipe
// //                 BigQ R(*inPipeR, *RO, *omR, this->nPages);

// //                 Record RL;
// //                 Record *RR = new Record();
// //                 vector<Record*> mergeVector; // to store records with same value of join attribute

// //                 int isLeftPipeEmpty = LO->Remove(&RL);
// //                 int isRightPipeEmpty = RO->Remove(RR);

// //                 int numLeftAttrs = RL.GetNumAtts();
// //                 int numRightAttrs = RR->GetNumAtts();

// //                 // array used as input to merge record function
// //                 int attrsToKeep[numLeftAttrs + numRightAttrs];
             
// // 	        int k = 0;
             
// // 		for (int i = 0; i < numLeftAttrs; i++) {
// //                         attrsToKeep[k++] = i;
// //                 }

// //                 for (int i = 0; i < numRightAttrs; i++) {
// //                         attrsToKeep[k++] = i;
// //                 }

// //                 Record mergedRecord;
// //                 int mergedRecCount = 0;

// //                 // get records from left output pipe and right output pipe
// //                 while (isLeftPipeEmpty != 0 && isRightPipeEmpty != 0) {
                        
// //                         int orderMakerAnswer = compEng.Compare(&RL, omL, RR, omR);
                        
// //                         // if left and right record are equal on join attributes
// //                         if (orderMakerAnswer == 0) {
     
// //                                 for (int i = 0; i < mergeVector.size(); i++) {
// //                                         delete mergeVector[i];
// //                                         mergeVector[i] = NULL;
// //                                 }
// //                                 mergeVector.clear();
                                
// //                                 // get all matching records (on join attr) in vector
// //                                 while (orderMakerAnswer == 0 && isRightPipeEmpty != 0) {
// //                                         mergeVector.push_back(RR);
// //                                         RR = new Record();
// //                                         isRightPipeEmpty = RO->Remove(RR);
// //                                         if (isRightPipeEmpty != 0) {
// //                                                 orderMakerAnswer = compEng.Compare(&RL, omL, RR, omR);
// //                                         }
// //                                 }

// //                                 // compare left Rec with first from vector 
// //                                 orderMakerAnswer = compEng.Compare(&RL, omL, mergeVector[0], omR);
                                
// //                                 // compare left Rec with all records from vector
// //                                 while (orderMakerAnswer == 0 && isLeftPipeEmpty != 0) {

// //                                         for (int i = 0; i < mergeVector.size(); i++) {
// //                                                 mergedRecord.MergeRecords(&RL, mergeVector[i], numLeftAttrs, numRightAttrs, attrsToKeep, numLeftAttrs + numRightAttrs, numLeftAttrs);
// //                                                 outPipe->Insert(&mergedRecord);
// //                                                 mergedRecCount++;
// //                                         }
                                        
// //                                         //Take next Record from left pipe;
// //                                         isLeftPipeEmpty = LO->Remove(&RL);
// //                                         orderMakerAnswer = compEng.Compare(&RL, omL, mergeVector[0], omR);

// //                                 }

// //                         } 
// //                         // take next from left pipe if it is smaller , else take next from right pipe
// //                         else if (orderMakerAnswer < 0) {
// //                                 isLeftPipeEmpty = LO->Remove(&RL);                                
// //                         } 
// //                         else {
// //                                 isRightPipeEmpty = RO->Remove(RR);
// //                         }
// //                 }
// //                 cout << "Total Records Merged :: " << mergedRecCount << endl;

// //         }
// //         // BLOCK NESTED LOOP JOIN - if no suitable order maker found
// //         else {
                
// //                 int mergedRecCount = 0;
// //                 char fileName[200];
// //                 int x = rand();
// //                 sprintf(fileName, "rightRelation_bnj.tmp%d", x);

		


// //                 DBFile rightRelationFIle;
// //                 Record RR;
// //                 rightRelationFIle.Create(fileName, heap, NULL);

// //                 //Add entire right relation to a temporary dbFile
// //                 while (inPipeR->Remove(&RR)) {
// //                         rightRelationFIle.Add(RR);
// //                 }
// //                 rightRelationFIle.Close();

// //                 vector<Record*> leftRelationVector;
// //                 int numPagesAllocatedForLeft = 0;
// //                 Page pageForLeftRecords;
// //                 Record leftRecord;
// //                 Record* rec = new Record();

// //                 // Remove records from left pipe in units of block ( block is n - 1 pages)                
// //                 int isLeftRecordPresent = inPipeL->Remove(&leftRecord);
// //                 bool isPipeEnded = false;
// //                 int isRecordAdded;
                
// //                 while (isLeftRecordPresent || isPipeEnded) {
                        
// //                         // Start reading left Record into a page
// //                         if (isLeftRecordPresent) {
// //                                 isRecordAdded = pageForLeftRecords.Append(&leftRecord);
// //                         }
// //                         // when page is full or pipe is empty
// //                         if (isRecordAdded == 0 || isPipeEnded) {

// //                                 // increment number of pages used
// //                                 numPagesAllocatedForLeft++;
                                
// //                                 // flush records of the page into vector
// //                                 while (pageForLeftRecords.GetFirst(rec)) {
// //                                         leftRelationVector.push_back(rec);
// //                                         rec = new Record();
// //                                 }
                                
// //                                 //For block nested loop join, n-1 pages of left relation are joined with each record of right relation
                                
// //                                 // start reading right relation from file when n - 1 buffer pages are full OR pipe is empty
// //                                 if (numPagesAllocatedForLeft == this->nPages - 1 || isPipeEnded) {

// //                                         rightRelationFIle.Open(fileName);
// //                                         rightRelationFIle.MoveFirst();
// //                                         Record rightRec;
// //                                         int isRightRecPresent = rightRelationFIle.GetNext(rightRec);
                                        
// //                                         Record mergedRecord;
// //                                         int numLeftAttrs = leftRelationVector[0]->GetNumAtts();
// //                                         int numRightAttrs = rightRec.GetNumAtts();
// //                                         int attrsToKeep[numLeftAttrs + numRightAttrs];
// //                                         int k = 0;
// //                                         for (int i = 0; i < numLeftAttrs; i++) {
// //                                                 attrsToKeep[k++] = i;
// //                                         }
// //                                         for (int i = 0; i < numRightAttrs; i++) {
// //                                                 attrsToKeep[k++] = i;
// //                                         }

// //                                         // while right relation file has next record
// //                                         while (isRightRecPresent != 0) {
// //                                                 for (int i = 0; i < leftRelationVector.size(); i++) {
                                                        
// //                                                         int isAccepted = compEng.Compare(leftRelationVector[i], &rightRec, literal, selOp);
                                                       
// //                                                         // merge records when the cnf is accepted
// //                                                         if (isAccepted != 0) {
// //                                                                 mergedRecord.MergeRecords(leftRelationVector[i], &rightRec, numLeftAttrs, numRightAttrs, attrsToKeep, numLeftAttrs + numRightAttrs, numLeftAttrs);
// //                                                                 outPipe->Insert(&mergedRecord);
// //                                                                 mergedRecCount++;
// //                                                         }
// //                                                 }
// //                                                 isRightRecPresent = rightRelationFIle.GetNext(rightRec);
// //                                         }
// //                                         rightRelationFIle.Close();

// //                                         // flush the vector 
// //                                         numPagesAllocatedForLeft = 0;
// //                                         for (int i = 0; i < leftRelationVector.size(); i++) {
// //                                                 delete leftRelationVector[i];
// //                                                 leftRelationVector[i] = NULL;
// //                                         }
// //                                         leftRelationVector.clear();
                                        
// //                                         // exit loop is pipe is empty
// //                                         if (isPipeEnded)
// //                                                 break;
// //                                 }

// //                         }
                        
// //                         isLeftRecordPresent = inPipeL->Remove(&leftRecord);
// //                         if (isLeftRecordPresent == 0) {
// //                                 isPipeEnded = true;
// //                         }
// //                 }
// //                 cout << "Total Records Merged :: " << mergedRecCount << endl;
// //                 remove(fileName);
// //         }
        
// //         // shut down output pipe after join is complete
// //         outPipe->ShutDown();


// // }

// // void Join::WaitUntilDone () { 

// // 	pthread_join(thread,NULL);

// // }


// // void Join::Use_n_Pages (int n) { 

// // 	this->nPages = n;

// // }

// void * Join :: thread_starter(void *context)
// {
//   clog << "starting join thread" << endl;
//   return reinterpret_cast<Join*>(context)->WorkerThread();
// }

// // void Join::Use_n_Pages (int runlen) {
// // 	// this->runLength = runlen;
// // 	// return;
// // }


// void * Join :: WorkerThread(void) {
//   Pipe& inPipeL = *inL;
//   Pipe& inPipeR = *inR;
//   Pipe& outPipe = *out;
//   CNF& selOp = *cnf;
//   unsigned int counterL = 0;
//   unsigned int counterR = 0;
//   unsigned int counterOut = 0;
//   OrderMaker sortOrderL;
//   OrderMaker sortOrderR;
//   bool validOrderMaker = (0 < selOp.GetSortOrders(sortOrderL, sortOrderR));
//   if(validOrderMaker)
//     {
//       clog << "ordermakers are valid, sort-merge join" << endl;
//       { // sort merge join
//         clog << runLength << endl;
//         Pipe outPipeL(runLength);
//         Pipe outPipeR(runLength);
//         BigQ Left(inPipeL,outPipeL,sortOrderL,runLength);
//         BigQ Right(inPipeR,outPipeR,sortOrderR,runLength);
//         clog << "BigQs initialized" << endl;
//         Record LeftRecord;
//         Record RightRecord;
//         unsigned int counter = 0;
//         clog << "getting first records" << endl;
//         outPipeL.Remove(&LeftRecord); // TODO check return value
//         clog << "left record" << endl;
//         outPipeR.Remove(&RightRecord); // TODO check return value
//         clog << "right record" << endl;

//         const int LeftNumAtts = LeftRecord.GetNumAtts();
//         const int RightNumAtts = RightRecord.GetNumAtts();
//         const int NumAttsTotal = LeftNumAtts + RightNumAtts;

//         int * attsToKeep = (int *)alloca(sizeof(int) * NumAttsTotal);
//         clog << "setup atts" << endl;
//         // consider factoring this into a MergeRecords that takes just two records.
//         { // setup AttsToKeep for MergeRecords
//           int curEl = 0;
//           for (int i = 0; i < LeftNumAtts; i++)
//             attsToKeep[curEl++] = i;
//           for (int i = 0; i < RightNumAtts; i++)
//             attsToKeep[curEl++] = i;
//         }
//         clog << "atts set up" << endl;
//         ComparisonEngine ceng;

//         vector<Record> LeftBuffer;
//         LeftBuffer.reserve(1000);
//         vector<Record> RightBuffer;
//         RightBuffer.reserve(1000);
//         Record MergedRecord;

//         do {
//           do { // burn records
//             if(0 < ceng.Compare(&LeftRecord,&sortOrderL,&RightRecord,&sortOrderR))
//               { // pos, left is greater than right.
//                 // left is greater, right is lesser
//                 // advance right until 0.
//                 do {
//                   counter++; counterR++;
//                   if(0 == outPipeR.Remove(&RightRecord)) // pipe is empty
//                     { RightRecord.SetNull(); break; } // set record to null
//                 }
//                 while (0 < ceng.Compare(&LeftRecord,&sortOrderL,&RightRecord,&sortOrderR));
//               }
//             if(0 < ceng.Compare(&LeftRecord,&sortOrderL,&RightRecord,&sortOrderR))
//               {
//                 // right is greater, left is lesser.
//                 // advance left until equal
//                 do {
//                   counter++; counterL++;
//                   if(0 == outPipeL.Remove(&LeftRecord))
//                     { LeftRecord.SetNull(); break; }
//                 }
//                 while (0 < ceng.Compare(&LeftRecord,&sortOrderL,&RightRecord,&sortOrderR));
//               }
//           } while (0 != ceng.Compare(&LeftRecord,&sortOrderL,&RightRecord,&sortOrderR)); // discard left and right until equal.
//           { // do join
//             // records are the same
//             // fill both left/right buffers until they change.
//             // consider using std::async in the future. or omp, but need to make sure to have enough records to join
//             //#pragma omp sections
//             { // The buffer filling could be done in parallel, as long as we have lots of things from each buffer.
//               //#pragma omp section
//               FillBuffer( LeftRecord,  LeftBuffer, outPipeL, sortOrderL);
//               //#pragma omp section
//               FillBuffer(RightRecord, RightBuffer, outPipeR, sortOrderR);
//             }
//             unsigned int lSize = LeftBuffer.size();
//             unsigned int rSize = RightBuffer.size();
//             counterL += lSize;
//             counterR += rSize;
//             counterOut += (lSize * rSize);
//             // merge buffers of records. This could also be done in parallel, as long as we had lots of records. probably approx 50k
//             // #pragma omp parallel for
//             for (unsigned int i = 0; i < lSize; ++i)
//               {
//                 for (unsigned int j = 0; j < rSize; ++j)
//                   {
//                     MergedRecord.MergeRecords(&LeftBuffer[i],&RightBuffer[j],LeftNumAtts,RightNumAtts,attsToKeep,NumAttsTotal,LeftNumAtts);
//                     outPipe.Insert(&MergedRecord);
//                   }
//               }
//             LeftBuffer.clear();
//             RightBuffer.clear();
//           }
//         } while (!LeftRecord.isNull() || !RightRecord.isNull()); // there is something in either pipes
//       }
//     }
//   else
//     {clog << "ordermakers are not valid, block nested loop join" << endl;}

//   clog << "Join" << endl
//        << "read " << counterL << " records from the left"  << endl
//        << "read " << counterR << " records from the right" << endl
//        << "put  " << counterOut << " records out" << endl;
//   outPipe.ShutDown();
//   pthread_exit(NULL); // make our worker thread go away
// }

// void Join :: FillBuffer(Record & in, vector<Record> &buffer, Pipe & pipe, OrderMaker & sortOrder)
// {
//   ComparisonEngine ceng;
//   buffer.push_back(in);
//   if (0 == pipe.Remove(&in))
//     {
//       in.SetNull();
//       return;
//     }
//   while(0 == ceng.Compare(&buffer[0], &in, &sortOrder))
//     {
//       buffer.push_back(in);
//       if (0 == pipe.Remove(&in))
//         {
//           in.SetNull();
//           return;
//         }
//     }
//   return;
// }

/**************************************************************************************************
DUPLICATE REMOVAL
**************************************************************************************************/
void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
	OperatorThreadMemberHolder *params = new OperatorThreadMemberHolder(&mySchema, &inPipe, NULL, &outPipe, NULL, NULL, NULL, 0, 0, NULL, this->runLength, NULL, NULL, NULL);
	// pthread_create(&operatorThread, NULL, operate, (void*) params);
	create_joinable_thread(&operatorThread, operate, params);
}

/*void DuplicateRemoval::WaitUntilDone () {
	pthread_join (operatorThread, NULL);
}*/

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
	OperatorThreadMemberHolder *params = new OperatorThreadMemberHolder(NULL, &inPipe, NULL, &outPipe, NULL, NULL, NULL, 0, 0, NULL, 0, &computeMe, NULL, NULL);
	// pthread_create(&operatorThread, NULL, operate, (void*) params);
	create_joinable_thread(&operatorThread, operate, params);
	// cout<<"Run:end\n";
}

/*void Sum::WaitUntilDone () {
	pthread_join (operatorThread, NULL);
	// cout<<"WaitUntilDone:end\n";
}*/

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
	OperatorThreadMemberHolder *params = new OperatorThreadMemberHolder(NULL, &inPipe, NULL, &outPipe, NULL, NULL, NULL, 0, 0, NULL, 0, &computeMe, &groupAtts, NULL);
	// pthread_create(&operatorThread, NULL, operate, (void*) params);
	create_joinable_thread(&operatorThread, operate, params);
}

/*void GroupBy::WaitUntilDone () {
	pthread_join (operatorThread, NULL);
}*/

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

/**************************************************************************************************
WRITEOUT
**************************************************************************************************/
void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
	OperatorThreadMemberHolder *params = new OperatorThreadMemberHolder(&mySchema, &inPipe, NULL, NULL, outFile, NULL, NULL, 0, 0, NULL, 0, NULL, NULL, NULL);
	// pthread_create(&operatorThread, NULL, operate, (void*) params);
	create_joinable_thread(&operatorThread, operate, params);
}

/*void GroupBy::WaitUntilDone () {
	pthread_join (operatorThread, NULL);
}*/

void WriteOut::Use_n_Pages (int runlen) {
	// this->runLength = runlen;
	return;
}

void* WriteOut::operate (void *arg) {
	OperatorThreadMemberHolder *params = (OperatorThreadMemberHolder*) arg;
	Record currentRecord;
	while(params->inPipe->Remove(&currentRecord)){
		currentRecord.Write(params->outFile, params->mySchema);
	}
}






// new
// #include "RelOp.h"
// #include <iostream>
// #include <sstream>
// #include <string>
// #include <cassert>

// void SelectFile::Run (DBFile &inFile_, Pipe &outPipe_, CNF &selOp_, Record &literal_) {
//   clog << "select file starting" << endl;
//   inF = &inFile_;
//   outP = &outPipe_;
//   cnf = &selOp_;
//   lit = &literal_;
//   pthread_create (&SelectFileThread, NULL, &SelectFile::thread_starter, this);
// }

// void * SelectFile :: thread_starter(void *context)
// {
//   return reinterpret_cast<SelectFile*>(context)->WorkerThread();
// }

// void * SelectFile :: WorkerThread(void) {
//   clog << "SF worker thread started" << endl;
//   DBFile & inFile = *inF;
//   Pipe & outPipe = *outP;
//   CNF & selOp = *cnf;
//   Record & literal = *lit;
//   int counter = 0;
//   Record temp;
//   inFile.MoveFirst ();
//   while (1 == inFile.GetNext (temp, selOp, literal)) {
//     counter += 1;
//     if (counter % 10000 == 0) {
//       clog << counter/10000 << " ";
//     }
//     outPipe.Insert(&temp);
//   }
//   cout << endl << " selected " << counter << " recs \n";

//   outPipe.ShutDown();
//   clog << "select file ending, after selecting " << counter << " records" << endl;
//   pthread_exit(NULL); // make our worker thread go away
// }

// void SelectFile::WaitUntilDone () {
//   clog << "SF waiting til done" << endl;
//   pthread_join (SelectFileThread, NULL);
//   clog << "SF complete, joined" << endl;
// }





// void * SelectPipe :: thread_starter(void *context)
// {
//   return reinterpret_cast<SelectPipe*>(context)->WorkerThread();
// }

// void * SelectPipe :: WorkerThread(void) {
//   clog << "SF worker thread started" << endl;
//   Pipe & inPipe = *in;
//   Pipe & outPipe = *out;
//   CNF & selOp = *cnf;
//   Record & literal = *lit;
//   int counter = 0;
//   Record temp;

//   ComparisonEngine comp;

//   while (inPipe.Remove(&temp))
//     {
//       if (1 == comp.Compare (&temp, &literal, &selOp)) {
//         counter += 1;
//         if (counter % 10000 == 0) {
//           clog << counter/10000 << " ";
//         }
//         outPipe.Insert(&temp);
//       }
//     }
//   cout << endl << " selected " << counter << " recs \n";

//   outPipe.ShutDown();
//   clog << "select pipe ending, after selecting " << counter << " records" << endl;
//   pthread_exit(NULL); // make our worker thread go away
// }

// void SelectPipe::WaitUntilDone () {
//   clog << "SP waiting til done" << endl;
//   pthread_join (SelectPipeThread, NULL);
//   clog << "SP complete, joined" << endl;
// }




// void * Sum :: thread_starter(void *context)
// {
//   clog << "starting sum thread" << endl;
//   return reinterpret_cast<Sum*>(context)->WorkerThread();
// }

// void * Sum :: WorkerThread(void) {
//   cout << "Sum thread started" << endl;
//   Pipe &inPipe = *in;
//   Pipe &outPipe = *out;
//   Function &computeMe = *fn;
//   Record temp;
//   clog << "begin summing" << endl;
//   Type retType = Int;
//   unsigned int counter = 0;
//   int intresult = 0;
//   double doubleresult = 0.0;
//   while(1 == inPipe.Remove(&temp))
//     {
//       counter++;
//       int tr = 0;
//       double td = 0.0;
//       retType = computeMe.Apply(temp,tr,td);
//       intresult += tr;
//       doubleresult += td;
//     }
//   clog << "summing complete" << endl;
//   // sum complete, take value from function and put into outpipe.
//   {
//     Record ret;
//     stringstream ss;
//     Attribute attr;
//     attr.name = "SUM";
//     if (Int == retType)
//       {
//         attr.myType = Int;
//         ss << intresult << "|";
//       }
//     else if (Double == retType) // floating point result
//       {
//         attr.myType = Double;
//         ss << doubleresult << "|";
//       }
//     Schema retSchema ("out_schema",1,&attr);
//     ret.ComposeRecord(&retSchema, ss.str().c_str());
//     outPipe.Insert(&ret);
//   }
//   outPipe.ShutDown();
//   clog << "Sum ending, after seeing " << counter << " records." << endl;
//   pthread_exit(NULL); // make our worker thread go away
// }

// void * GroupBy :: thread_starter(void *context)
// {
//   clog << "starting GroupBy thread" << endl;
//   return reinterpret_cast<GroupBy*>(context)->WorkerThread();
// }

// void * GroupBy :: WorkerThread(void) {
//   cout << "GroupBy thread started" << endl;
//   Pipe &inPipe = *in;
//   Pipe &outPipe = *out;
//   OrderMaker & compare = *comp;
//   Function &computeMe = *fn;
//   clog << runLength << endl;
//   Pipe sortedOutput(runLength);
//   BigQ sorter(inPipe, sortedOutput, compare, runLength);
//   clog << "GB BigQ initialized" << endl;

//   // sort everything, do what dupremoval does, but sum over the group rather than deleting it.
//   Record recs[2];

//   Type retType = Int;
//   int intresult = 0;
//   double doubleresult = 0.0;
//   unsigned int counter = 0;

//   if(1 == sortedOutput.Remove(&recs[1]))
//     {
//       int tr; double td;
//       retType = computeMe.Apply(recs[1],tr,td);
//       intresult += tr; doubleresult += td;
//       counter++;
//     }
//   clog << "first removed" << endl;
//   unsigned int i = 0;
//   ComparisonEngine ceng;

//   while(1 == sortedOutput.Remove(&recs[i%2])) // i%2 is 'current' record
//     {
//       counter++;
//       if (0 == ceng.Compare(&recs[i%2],&recs[(i+1)%2],&compare)) // groups are the same
//         { // add to current sum
//           int tr; double td;
//           computeMe.Apply(recs[i%2],tr,td);
//           intresult += tr; doubleresult += td;
//         }
//       else // groups are different, thus there is a new group
//         {
//           WriteRecordOut(recs[(i+1)%2],retType, intresult, doubleresult);
//           i++; // switch slots.
//         }
//     }
//   WriteRecordOut(recs[i%2],retType, intresult, doubleresult);
//   i++;

//   outPipe.ShutDown();
//   clog << "GroupBy ending, after seeing " << counter << " records, in " << i << "groups." << endl;
//   pthread_exit(NULL); // make our worker thread go away
// }

// void GroupBy :: WriteRecordOut(Record & rec, Type const retType, int & intresult, double & doubleresult) {
//   {
//     Record ret;
//     stringstream ss;
//     Attribute attr;
//     attr.name = "SUM";
//     if (Int == retType)
//       {
//         attr.myType = Int;
//         ss << intresult << "|";
//       }
//     else if (Double == retType) // floating point result
//       {
//         attr.myType = Double;
//         ss << doubleresult << "|";
//       }
//     Schema retSchema ("out_schema",1,&attr);
//     ret.ComposeRecord(&retSchema, ss.str().c_str());
//     // sum as first attribute, group attr as rest
//     int RightNumAtts = comp->GetNumAtts();
//     int numAttsToKeep = 1 + RightNumAtts;

//     int * attsToKeep = (int *)alloca(sizeof(int) * numAttsToKeep);
//     { // setup AttsToKeep for MergeRecords
//       int curEl = 0;
//       attsToKeep[0] = 0;
//       curEl++;

//       for (int i = 0; i < RightNumAtts; i++)
//         {
//           attsToKeep[curEl] = (comp->GetAtts())[i];
//           curEl++;
//         }
//     }
//     Record newret;
//     newret.MergeRecords(&ret,&rec, 1, rec.GetNumAtts(),
//                         attsToKeep, numAttsToKeep, 1);
//     Pipe &outPipe = *out;
//     outPipe.Insert(&newret);
//   }
//   // reset group total
//   intresult = 0;
//   doubleresult = 0.0;
// }

// void * Project :: thread_starter(void *context)
// {
//   clog << "starting project thread" << endl;
//   return reinterpret_cast<Project*>(context)->WorkerThread();
// }

// void * Project :: WorkerThread(void) {
//   Pipe &inPipe = *in;
//   Pipe &outPipe = *out;
//   Record temp;
//   unsigned int counter = 0;
//   while(1 == inPipe.Remove(&temp))
//     {
//       counter++;
//       temp.Project(atts, numAttsOut,numAttsIn);
//       outPipe.Insert(&temp);
//     }
//   outPipe.ShutDown();
//   clog << "projected " << counter << " records." << endl;
//   pthread_exit(NULL); // make our worker thread go away
// }

// void * Join :: thread_starter(void *context)
// {
//   clog << "starting join thread" << endl;
//   return reinterpret_cast<Join*>(context)->WorkerThread();
// }

// void * Join :: WorkerThread(void) {
//   Pipe& inPipeL = *inL;
//   Pipe& inPipeR = *inR;
//   Pipe& outPipe = *out;
//   CNF& selOp = *cnf;
//   unsigned int counterL = 0;
//   unsigned int counterR = 0;
//   unsigned int counterOut = 0;
//   OrderMaker sortOrderL;
//   OrderMaker sortOrderR;
//   bool validOrderMaker = (0 < selOp.GetSortOrders(sortOrderL, sortOrderR));
//   if(validOrderMaker)
//     {
//       clog << "ordermakers are valid, sort-merge join" << endl;
//       { // sort merge join
//         clog << runLength << endl;
//         Pipe outPipeL(runLength);
//         Pipe outPipeR(runLength);
//         BigQ Left(inPipeL,outPipeL,sortOrderL,runLength);
//         BigQ Right(inPipeR,outPipeR,sortOrderR,runLength);
//         clog << "BigQs initialized" << endl;
//         Record LeftRecord;
//         Record RightRecord;
//         unsigned int counter = 0;
//         clog << "getting first records" << endl;
//         outPipeL.Remove(&LeftRecord); // TODO check return value
//         clog << "left record" << endl;
//         outPipeR.Remove(&RightRecord); // TODO check return value
//         clog << "right record" << endl;

//         const int LeftNumAtts = LeftRecord.GetNumAtts();
//         const int RightNumAtts = RightRecord.GetNumAtts();
//         const int NumAttsTotal = LeftNumAtts + RightNumAtts;

//         int * attsToKeep = (int *)alloca(sizeof(int) * NumAttsTotal);
//         clog << "setup atts" << endl;
//         // consider factoring this into a MergeRecords that takes just two records.
//         { // setup AttsToKeep for MergeRecords
//           int curEl = 0;
//           for (int i = 0; i < LeftNumAtts; i++)
//             attsToKeep[curEl++] = i;
//           for (int i = 0; i < RightNumAtts; i++)
//             attsToKeep[curEl++] = i;
//         }
//         clog << "atts set up" << endl;
//         ComparisonEngine ceng;

//         vector<Record> LeftBuffer;
//         LeftBuffer.reserve(1000);
//         vector<Record> RightBuffer;
//         RightBuffer.reserve(1000);
//         Record MergedRecord;

//         do {
//           do { // burn records
//             if(0 < ceng.Compare(&LeftRecord,&sortOrderL,&RightRecord,&sortOrderR))
//               { // pos, left is greater than right.
//                 // left is greater, right is lesser
//                 // advance right until 0.
//                 do {
//                   counter++; counterR++;
//                   if(1 == outPipeR.Remove(&RightRecord)) // pipe is empty
//                     { RightRecord.SetNull(); break; } // set record to null
//                 }
//                 while (0 < ceng.Compare(&LeftRecord,&sortOrderL,&RightRecord,&sortOrderR));
//               }
//             if(0 < ceng.Compare(&LeftRecord,&sortOrderL,&RightRecord,&sortOrderR))
//               {
//                 // right is greater, left is lesser.
//                 // advance left until equal
//                 do {
//                   counter++; counterL++;
//                   if(1 == outPipeL.Remove(&LeftRecord))
//                     { LeftRecord.SetNull(); break; }
//                 }
//                 while (0 < ceng.Compare(&LeftRecord,&sortOrderL,&RightRecord,&sortOrderR));
//               }
//           } while (0 != ceng.Compare(&LeftRecord,&sortOrderL,&RightRecord,&sortOrderR)); // discard left and right until equal.
//           { // do join
//             // records are the same
//             // fill both left/right buffers until they change.
//             // consider using std::async in the future. or omp, but need to make sure to have enough records to join
//             //#pragma omp sections
//             { // The buffer filling could be done in parallel, as long as we have lots of things from each buffer.
//               //#pragma omp section
//               FillBuffer( LeftRecord,  LeftBuffer, outPipeL, sortOrderL);
//               //#pragma omp section
//               FillBuffer(RightRecord, RightBuffer, outPipeR, sortOrderR);
//             }
//             unsigned int lSize = LeftBuffer.size();
//             unsigned int rSize = RightBuffer.size();
//             counterL += lSize;
//             counterR += rSize;
//             counterOut += (lSize * rSize);
//             // merge buffers of records. This could also be done in parallel, as long as we had lots of records. probably approx 50k
//             // #pragma omp parallel for
//             for (unsigned int i = 0; i < lSize; ++i)
//               {
//                 for (unsigned int j = 0; j < rSize; ++j)
//                   {
//                     MergedRecord.MergeRecords(&LeftBuffer[i],&RightBuffer[j],LeftNumAtts,RightNumAtts,attsToKeep,NumAttsTotal,LeftNumAtts);
//                     outPipe.Insert(&MergedRecord);
//                   }
//               }
//             LeftBuffer.clear();
//             RightBuffer.clear();
//           }
//         } while (!LeftRecord.isNull() || !RightRecord.isNull()); // there is something in either pipes
//       }
//     }
//   else
//     {clog << "ordermakers are not valid, block nested loop join" << endl;}

//   clog << "Join" << endl
//        << "read " << counterL << " records from the left"  << endl
//        << "read " << counterR << " records from the right" << endl
//        << "put  " << counterOut << " records out" << endl;
//   outPipe.ShutDown();
//   pthread_exit(NULL); // make our worker thread go away
// }

// void Join :: FillBuffer(Record & in, vector<Record> &buffer, Pipe & pipe, OrderMaker & sortOrder)
// {
//   ComparisonEngine ceng;
//   buffer.push_back(in);
//   if (1 == pipe.Remove(&in))
//     {
//       in.SetNull();
//       return;
//     }
//   while(0 == ceng.Compare(&buffer[0], &in, &sortOrder))
//     {
//       buffer.push_back(in);
//       if (1 == pipe.Remove(&in))
//         {
//           in.SetNull();
//           return;
//         }
//     }
//   return;
// }

// void * DuplicateRemoval :: thread_starter(void *context)
// {
//   clog << "starting DuplicateRemoval thread" << endl;
//   return reinterpret_cast<DuplicateRemoval*>(context)->WorkerThread();
// }

// void * DuplicateRemoval :: WorkerThread(void) {
//   Pipe& inPipe = *in;
//   Pipe& outPipe = *out;

//   Pipe sortedOutput(runLength);
//   BigQ sorter(inPipe, sortedOutput, compare, runLength);
//   clog << "BigQ initialized" << endl;
//   Record recs[2];
//   unsigned int counter = 0;

//   if(1 == sortedOutput.Remove(&recs[1]))
//     {
//       Record copy;
//       copy.Copy(&recs[1]);
//       outPipe.Insert(&copy);
//       counter++;
//     }

//   unsigned int i = 0;
//   ComparisonEngine ceng;
//   while(1 == sortedOutput.Remove(&recs[i%2]))
//     {
//       if (0 == ceng.Compare(&recs[i%2],&recs[(i+1)%2],&compare)) // elements are the same
//         { // do nothing, put it in the pipe earlier.
//         }
//       else // elements are different, thus there is a new element.
//         {
//           counter++;
//           Record copy;
//           copy.Copy(&recs[i%2]); // make a copy
//           outPipe.Insert(&copy); // put it in the pipe.
//           i++; // switch slots.
//         }
//     }

//   outPipe.ShutDown();
//   pthread_exit(NULL); // make our worker thread go away
// }

// void * WriteOut :: thread_starter(void *context)
// {
//   clog << "starting WriteOut thread" << endl;
//   return reinterpret_cast<WriteOut*>(context)->WorkerThread();
// }

// void * WriteOut :: WorkerThread(void) {
//   Pipe& inPipe = *in;

//   Record temp;
//   while(1 == inPipe.Remove(&temp))
//     {
//       ostringstream os;
//       temp.Print(sch,os);
//       fputs(os.str().c_str(),out);
//     }
//   pthread_exit(NULL); // make our worker thread go away
// }