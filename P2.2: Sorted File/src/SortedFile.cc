#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <fstream>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

SortedFile::SortedFile () {
	inPipe = NULL; // input pipe
	outPipe = NULL; // output pipe
	bigQ = NULL; // bigQ instance

	sortInfo = NULL; // SortInfo from startup
	queryOrder = NULL; // OrderMaker from startup
	didCNFChange = true; // did the CNF/Query change 

	ptrCurrentRecord = new Record(); // pointer to current record

	currentPageNumber = 0; // current page number
	runPageNumber = 0; // used to read records from sorted file while merging sorted file with pipe
	isPipeDirty=0; // does pipe have records?
	fileMode = READ; // set the initial mode to read
}


int SortedFile::Create (char *name, fType myType, void *startup) {	// done
	file.Open(0, name); // create a file for sorted file
	fileName = name; // store name
	isPipeDirty=0; // pipe is not dirty
	
	sortInfo = (SortInfo *) startup; // cast void* to startup
	currentPageNumber=0;
	// isThisEndOfFile=1;
	return 1;
}


int SortedFile::Open (char *name) {

	isPipeDirty=0; // pipe is not dirty
	char *metaFileName = new char[20]; // 
	sprintf(metaFileName, "%s.meta", name);

	fileName = name;

	ifstream metaFile(metaFileName, ios::binary); // open meta file in binary mode
	metaFile.seekg(sizeof(fileName)-1);
	
	// init sortInfo if null
	if(sortInfo==NULL){
		sortInfo = new SortInfo;
		sortInfo->myOrder = new OrderMaker();
	}

	metaFile.read((char*)sortInfo->myOrder, sizeof(*(sortInfo->myOrder))); //read myOrder from meta file
	metaFile.read((char*)&(sortInfo->runLength), sizeof(sortInfo->runLength)); // read runLength from meta file
	metaFile.close(); // close meta files

	fileMode = READ; // set mode to read
	file.Open(1, name); // open file for this sorted file
	currentPageNumber = 0;
	isThisEndOfFile = 0;
}


void SortedFile::Load (Schema &mySchema, char *loadMe) {
	if(fileMode != WRITE){
		fileMode = WRITE; // set mode to WRITE
		isPipeDirty=1; // pipe is dirty
		if(bigQ==NULL){ // setup bigQ if null
			bigQ = new BigQ(*inPipe,*outPipe,*(sortInfo->myOrder),sortInfo->runLength);
		}
	}

	FILE* tableFile = fopen (loadMe, "r"); // open table file
	Record temp;
	while(temp.SuckNextRecord(&mySchema, tableFile)!=0){
		inPipe->Insert(&temp); // add record to pipe
	}
	fclose(tableFile); // close table file
}


void* SortedFile::TriggerBigQThread(void* arg){
	bigQThreadParams *params;
	params = (bigQThreadParams *) arg;
	params->bigQ = new BigQ(*(params->inPipe),*(params->outPipe),*((params->sortInfo).myOrder),(params->sortInfo).runLength);
}


void SortedFile::Add (Record &addMe) {	// requires BigQ instance		done
	if(fileMode != WRITE){
		fileMode = WRITE; // set mode to WRITE
		isPipeDirty=1; // pipe is dirty
		
		inPipe= new Pipe(PIPE_SIZE);
		outPipe= new Pipe(PIPE_SIZE);

		if(bigQ==NULL){ // setup big thread
			threadParams.inPipe = inPipe;
			threadParams.outPipe = outPipe;
			threadParams.sortInfo.myOrder = sortInfo->myOrder;
			threadParams.sortInfo.runLength =  sortInfo->runLength;
			threadParams.bigQ = bigQ;
			pthread_create(&bigQThread, NULL, &SortedFile::TriggerBigQThread , (void *)&threadParams); // create bigq thread	
		}
	}
	inPipe->Insert(&addMe); // add to pipe
	didCNFChange = true; // assuming that the CNF will change
}


void SortedFile::MoveFirst () {
	currentPageNumber = 0;
	isPipeDirty=0;	

	if(fileMode != READ){ // file mode is read so we can safely fetch the first page and the first record
		fileMode = READ; // set mode to read
		MergeFromOutpipe(); // merge the pipe with the sorted file
	}
	if(file.GetLength()!=0){
		file.GetPage(&page,currentPageNumber); // fetch the first page
		page.GetFirst(ptrCurrentRecord); // fetch the first record
	}
	didCNFChange = true; // assuming that the CNF will change
}


int SortedFile::Close () {			// requires MergeFromOuputPipe()	done
	
	if(fileMode==WRITE)	
		MergeFromOutpipe();

	file.Close();
	isPipeDirty=0;
	isThisEndOfFile = 1;
	// write updated state to meta file

	char fName[30];
	sprintf(fName,"%s.meta",fileName);

	ofstream out(fName);
	out <<"sorted"<<endl;
	out.close();


	ofstream ofs(fName,ios::binary|ios::app);

	ofs.write((char*)sortInfo->myOrder,sizeof(*(sortInfo->myOrder)));	
	ofs.write((char*)&(sortInfo->runLength),sizeof(sortInfo->runLength));

	ofs.close();
}


int SortedFile::GetNext (Record &fetchme) {		// requires MergeFromOuputPipe()		done

	if(fileMode != READ){
		fileMode = READ;
		isPipeDirty=0; // pipe is dirty
		page.EmptyItOut(); // empty the page
		MergeFromOutpipe(); // merge pipe with the sorted file
		MoveFirst(); // move to the start of the file
	}

	if(isThisEndOfFile==1){
		return 0;
	}

	fetchme.Consume(ptrCurrentRecord); // put the record in fetchMe

	if(!page.GetFirst(ptrCurrentRecord)) { // if there is no next record
		if(currentPageNumber >= file.GetLength()-2){ // if all records have been read
			isThisEndOfFile = 1; // we have reached the end of the file
			return 1;	
		} else{
			currentPageNumber++;
			file.GetPage(&page, currentPageNumber); // get next page
			page.GetFirst(ptrCurrentRecord); // get first record from this page
		}
	}
	return 1;
}


int SortedFile::GetNext (Record &fetchme, CNF &applyMe, Record &literal) {

	if(fileMode!=READ){
		fileMode = READ;
		isPipeDirty=0; // pipe is dirty
		page.EmptyItOut(); // empty the page
		MergeFromOutpipe(); // merge pipe with the sorted file
		MoveFirst(); // move to the start of the file
	}

	if(didCNFChange){ // if the CNF changed
		queryOrder = applyMe.CreateQueryMaker(*(sortInfo->myOrder)); // generate new order maker				
	}
	
	ComparisonEngine comparisonEngine;
		
	if(queryOrder==NULL) { // if the order maker is not compatible the get the first record that matchses the literal
		while(GetNext(fetchme)){ // sequential scan from ptrCurrentRecord
			if(comparisonEngine.Compare(&fetchme, &literal, &applyMe)) { // if required record found
				return 1;
			}
		}
		return 0; // if required not record found
	}else{	// if the order maker is compatible then we can apply binary search on the file
		Record *result = new Record();
		result = LoadProspectivePage(literal); // load the page which might have the record we are looking for
		if(result==NULL){ // no matching record found
			return 0;
		}
		fetchme.Consume(result); // put the record into fetchMe

		if(comparisonEngine.Compare(&fetchme, &literal, &applyMe)){ // if the record matches
			return 1;
		}
		
		while(GetNext(fetchme)) { // sequential search in the prospective page
			if(comparisonEngine.Compare(&fetchme, &literal, queryOrder)!=0) { // if the record does not match
				return 0;
			} else {
				if(comparisonEngine.Compare(&fetchme, &literal, &applyMe)) { // if the record matches
					return 1;
				}
			}
		}
	
	}
	return 0;
}


Record* SortedFile::LoadProspectivePage(Record &literal) {			//returns the first record which equals to literal based on queryorder;
	
	if(didCNFChange) { //  if the CNF changed
		int low = currentPageNumber; // current page is the lower page for binary search
		int high = file.GetLength()-2; // last page is the lower page for binary search
		int prospectivePageNumber = BinarySearch(low, high, queryOrder, literal); // get the page that might have the record we want
		if(prospectivePageNumber == -1) { // such a page does not exist
			return NULL;
		}

		if(prospectivePageNumber != currentPageNumber) { // load the page if not already loaded
			page.EmptyItOut();
			file.GetPage(&page, prospectivePageNumber);
			currentPageNumber = prospectivePageNumber+1;
		}
		didCNFChange = false; // assuming that the CNF will not change unless explicitly done by MoveFirst,.. etc
	}

	//find the potential page, make reader buffer pointer to the first record
	// that equal to query order
	Record *resultRecord = new Record();
	ComparisonEngine comparisonEngine;
	while(page.GetFirst(resultRecord)) { // sequentially search the page until we find a matching record
		if(comparisonEngine.Compare(resultRecord, &literal, queryOrder) == 0) { // record found
			return resultRecord;
		}
	}

	if(currentPageNumber >= file.GetLength()-2) { // if there are no more pages
		return NULL;
	} else {
		currentPageNumber++;
		file.GetPage(&page, currentPageNumber); // load next page
		while(page.GetFirst(resultRecord)) { // sequentially search the page until we find a matching record
			if(comparisonEngine.Compare(resultRecord, &literal, queryOrder) == 0) { // record found
				return resultRecord;
			}
		}
	}
	return NULL; // not found :(
}


int SortedFile::BinarySearch(int low, int high, OrderMaker *queryOrderMaker, Record &literal) {

	if(low > high) return -1;
	if(low == high) return low;
	int mid = (high+low)/2;

	Page tempPage;
	Record tempRecord;
	
	file.GetPage(&tempPage, mid);
	tempPage.GetFirst(&tempRecord); // get 1st record from the page

	int compareStatus;
	ComparisonEngine comparisonEngine;
	compareStatus = comparisonEngine.Compare(&tempRecord, sortInfo->myOrder, &literal, queryOrderMaker);

	if(compareStatus == -1){
		if(low==mid){
			return mid;
		}
		return BinarySearch(low, mid-1, queryOrderMaker, literal); // search before the mid page
	} else if(compareStatus == 0) { // prospective page found
		return mid;
	}else{ 
		return BinarySearch(mid+1, high,queryOrderMaker, literal); // search past the mid page
	}
}


void SortedFile:: MergeFromOutpipe(){		// requires both read and write modes

	inPipe->ShutDown();
	// get sorted records from output pipe
	ComparisonEngine *ce;

	// following four lines get the first record from those already present (not done)
	Record *rFromFile = new Record();

	Record *rtemp = new Record();		
	Page *ptowrite = new Page();			// new page that would be added
	File *newFile = new File();				// new file after merging
	newFile->Open(0,"mergedFile");				

	bool nomore = false;
    int fileNotEmpty = GetNew(rFromFile);
    // int fileNotEmpty = !file.IsEmpty();

	int currentPageNumber = 0;

	int counter = 0;
	if(fileNotEmpty){
		/*counter++;
		while(GetNew(rFromFile) != 0){
			counter++;
		}*/
		// runPageNumber = 0; // duplicates
		// file.GetPage(&runPage, runPageNumber);
		// fileNotEmpty = GetNew(rFromFile);
	}
	cout<<"nRecordsInFile: "<<counter<<endl;


	int pipeNotEmpty = outPipe->Remove(rtemp);	

	while (fileNotEmpty || pipeNotEmpty){
	    if (!fileNotEmpty || (pipeNotEmpty && ce->Compare(rFromFile, rtemp, sortInfo->myOrder) > 0)) {
      		
	    	if(ptowrite->Append(rtemp)!=1){				// copy record from pipe
				// write this page to file
				newFile->AddPage(ptowrite,currentPageNumber++);
				// empty this out
				ptowrite->EmptyItOut();
				// append the ptrCurrentRecord record ?
				ptowrite->Append(rtemp);		// does this consume the record ?
			}

      		// fromPipe.Print(&mySchema);
      		pipeNotEmpty = outPipe->Remove(rtemp);
    	} else if (!pipeNotEmpty || (fileNotEmpty && ce->Compare(rFromFile, rtemp, sortInfo->myOrder) <= 0)) {
    		if(ptowrite->Append(rFromFile)!=1){				// copy record from pipe
				// write this page to file
				newFile->AddPage(ptowrite,currentPageNumber++);
				// empty this out
				ptowrite->EmptyItOut();
				// append the ptrCurrentRecord record ?
				ptowrite->Append(rFromFile);		// does this consume the record ?
			}
      		// fromPipe.Print(&mySchema);
      		fileNotEmpty = GetNew(rFromFile);
    	}
    }
    if(!ptowrite->IsEmpty()){
		newFile->AddPage(ptowrite,currentPageNumber++);
	}	

	Schema nu("catalog","nation");
	
	newFile->Close();
	file.Close();

	// delete resources that are not required
	if(rename(fileName,"mergefile.tmp")!=0) {				// making merged file the new file
		cerr <<"rename file error!"<<endl;
		return;
	}
	
	remove("mergefile.tmp");

	if(rename("mergedFile",fileName)!=0) {				// making merged file the new file
		cerr <<"rename file error!"<<endl;
		return;
	}

	page.EmptyItOut();
	file.Open(1, this->fileName);
	file.GetPage(&page, 0);
}


int SortedFile:: GetNew(Record *r1){
	while(!this->runPage.GetFirst(r1)) {
		if(runPageNumber >= file.GetLength()-1)
			return 0;
		else {
			file.GetPage(&runPage, runPageNumber);
			runPageNumber++;
		}
	}
	return 1;
}	


SortedFile::~SortedFile() {
	delete sortInfo;
	delete queryOrder;
	delete ptrCurrentRecord;
	delete inPipe;
	delete outPipe;
	delete bigQ;
	delete fileName;
}