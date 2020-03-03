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

void* SortedFile::TriggerBigQThread(void* arg){
	bigQThreadParams *params;
	params = (bigQThreadParams *) arg;
	params->bigQ = new BigQ(*(params->inPipe),*(params->outPipe),*((params->sortInfo).myOrder),(params->sortInfo).runLength);
}


void SortedFile::Load (Schema &f_schema, char *loadpath) {
	if(fileMode!=WRITE){
		fileMode = WRITE;
		isPipeDirty=1;
		if(bigQ==NULL)bigQ = new BigQ(*inPipe,*outPipe,*(sortInfo->myOrder),sortInfo->runLength);
	}

	FILE* tableFile = fopen (loadpath,"r");
	Record temp;
	while(temp.SuckNextRecord(&f_schema,tableFile)!=0){
		inPipe->Insert(&temp);
	}
	fclose(tableFile);	
}

void SortedFile::MoveFirst () {			// requires MergeFromOuputPipe()

	isPipeDirty=0;	
	currentPageNumber = 0;

	if(fileMode==READ){
		// In read mode, so direct movefirst is possortInfoble
		if(file.GetLength()!=0){
			file.GetPage(&page,currentPageNumber); //TODO: check off_t type,  void GetPage (Page *putItHere, off_t whichPage)
			int result = page.GetFirst(ptrCurrentRecord);
		}
	}
	else{
		// change mode to read
		fileMode = READ;
		// Merge contents if any from BigQ
		MergeFromOutpipe();
		file.GetPage(&page,currentPageNumber); //TODO: check off_t type,  void GetPage (Page *putItHere, off_t whichPage)
		page.GetFirst(ptrCurrentRecord);
		// bring the first page into page
		// Set curr Record to first record
		// 
	}
	didCNFChange = true;
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

void SortedFile::Add (Record &rec) {	// requires BigQ instance		done
	if(fileMode!=WRITE){
		isPipeDirty=1;
		fileMode = WRITE;

		inPipe= new Pipe(100);
		outPipe= new Pipe(100);

		if(bigQ==NULL){
			threadParams.inPipe = inPipe;
			threadParams.outPipe = outPipe;
			threadParams.sortInfo.myOrder = sortInfo->myOrder;
			threadParams.sortInfo.runLength =  sortInfo->runLength;
			threadParams.bigQ = bigQ;
			pthread_create(&bigQThread, NULL, &SortedFile::TriggerBigQThread , (void *)&threadParams);		
		}
	}
	inPipe->Insert(&rec);
	didCNFChange = true;
}

int SortedFile::GetNext (Record &fetchme) {		// requires MergeFromOuputPipe()		done

	if(fileMode!=READ){
		isPipeDirty=0;
		fileMode = READ;
		page.EmptyItOut();		// requires flush
		MergeFromOutpipe();		// 
		MoveFirst();	// always start from first record

	}

	if(isThisEndOfFile==1) return 0;

	fetchme.Copy(ptrCurrentRecord);

	if(!page.GetFirst(ptrCurrentRecord)) {

		if(currentPageNumber>=this->file.GetLength()-2){
				isThisEndOfFile = 1;
				return 1;	
		}
		else {
			currentPageNumber++;
			file.GetPage(&page,currentPageNumber);
			page.GetFirst(ptrCurrentRecord);

		}
	}

	return 1;

	/*if(isThisEndOfFile){ // if the end of the file has been reached
		return 0;
	}
	fetchme.Copy(ptrCurrentRecord); // Consume?

	if(page.GetFirst(ptrCurrentRecord) == 1){ // is there a record to fetch?
		// readPageRecordNumber++;
		return 1;
	}

	currentPageNumber+=1; // page has been consumed. Increment page number
	if(currentPageNumber == file.GetLength()-1){ // if there is no next page return 0
		isThisEndOfFile = 1;
		return 1;
	}

	file.GetPage(&page, currentPageNumber); // get the next page
	// readPageNumber+=1;
	if(page.GetFirst(ptrCurrentRecord) == 1){ // record found
		// readPageRecordNumber++;
		return 1;
	}*/
}

int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {		// requires binary search // requires MergeFromOuputPipe()

	if(fileMode!=READ){
		isPipeDirty=0;
		fileMode = READ;
		page.EmptyItOut();
		MergeFromOutpipe();
		MoveFirst();
	}

	if(didCNFChange){
		queryOrder = cnf.CreateQueryMaker(*(sortInfo->myOrder));				
	}
	
	ComparisonEngine *comp;
		
	if(queryOrder==NULL) {		// no compatible order maker; return first record that matches the Record literal
		while(GetNext(fetchme)){			// linear scan from ptrCurrentRecord record
			if(comp->Compare(&fetchme, &literal, &cnf)) {		//record found, return 1
				return 1;
			}
		}
		return 0;					// record not found
	}
	else{							 // some order maker compatible to given CNF is constructed
		Record *r1 = new Record();			
		r1 = GetMatchPage(literal);
		if(r1==NULL) return 0;
		fetchme.Consume(r1);

		if(comp->Compare(&fetchme,&literal,&cnf)){
			 return 1;
		}
		
		while(GetNext(fetchme)) {
			if(comp->Compare(&fetchme, &literal, queryOrder)!=0) {
				//not match to query order
				return 0;
			} else {
				if(comp->Compare(&fetchme, &literal, &cnf)) {
					//find the right record
					return 1;
				}
			}
		}
	
	}
	return 0;
}


Record* SortedFile::GetMatchPage(Record &literal) {			//returns the first record which equals to literal based on queryorder;
	
	if(didCNFChange) {
		int low = currentPageNumber;
		int high = file.GetLength()-2;
		int matchPage = BinarySearch(low, high, queryOrder, literal);
		if(matchPage == -1) {
			//not found
			return NULL;
		}
		if(matchPage != currentPageNumber) {
			page.EmptyItOut();
			file.GetPage(&page, matchPage);
			currentPageNumber = matchPage+1;
		}
		didCNFChange = false;
	}

	//find the potential page, make reader buffer pointer to the first record
	// that equal to query order
	Record *returnRcd = new Record;
	ComparisonEngine cmp1;
	while(page.GetFirst(returnRcd)) {
		if(cmp1.Compare(returnRcd, &literal, queryOrder) == 0) {
			//find the first one
			return returnRcd;
		}
	}
	if(currentPageNumber >= file.GetLength()-2) {
		return NULL;
	} else {
		//sortInfonce the first record may exist on the next page
		currentPageNumber++;
		file.GetPage(&page, currentPageNumber);
		while(page.GetFirst(returnRcd)) {
			if(cmp1.Compare(returnRcd, &literal, queryOrder) == 0) {
				//find the first one
				return returnRcd;
			}
		}
	}
	return NULL;
		

}

int SortedFile::BinarySearch(int low, int high, OrderMaker *queryOrderMaker, Record &literal) {
	
	cout<<"serach OM "<<endl;
	queryOrderMaker->Print();
	cout<<endl<<"file om"<<endl;
	sortInfo->myOrder->Print();

	if(high < low) return -1;
	if(high == low) return low;
	//high > low
	
	ComparisonEngine *comp;
	Page *tmpPage = new Page;
	Record *tmpRcd = new Record;
	int mid = (int) (high+low)/2;
	file.GetPage(tmpPage, mid);
	
	int res;

	Schema nu("catalog","lineitem");

	tmpPage->GetFirst(tmpRcd);

	tmpRcd->Print(&nu);
	res = comp->Compare(tmpRcd,sortInfo->myOrder, &literal,queryOrderMaker );
	delete tmpPage;
	delete tmpRcd;

	if( res == -1) {
		if(low==mid)
			return mid;
		return BinarySearch(low, mid-1, queryOrderMaker, literal);
	}
	else if(res == 0) {
		return mid;//BinarySearch(low, mid-1, queryOrderMaker, literal);
	}
	else
		return BinarySearch(mid+1, high,queryOrderMaker, literal);
}

/*void SortedFile:: MergeFromOutpipe(){		// requires both read and write modes

	inPipe->ShutDown();
	// get sorted records from output pipe
	ComparisonEngine *ce;

	// following four lines get the first record from those already present (not done)
	Record *rFromFile = new Record();
	GetNew(rFromFile);						// loads the first record from existing records

	Record *rtemp = new Record();		
	Page *ptowrite = new Page();			// new page that would be added
	File *newFile = new File();				// new file after merging
	newFile->Open(0,"mergedFile");				

	bool nomore = false;
    int result =GetNew(rFromFile);
	int currentPageNumber = 0;


	Schema nu("catalog","nation");


	if(result==0){
		nomore = true;
	}

	while(isPipeDirty!=0&&!nomore){
		if(outPipe->Remove(rtemp)==1){		// got the record from out pipe
			while(ce->Compare(rFromFile,rtemp,sortInfo->myOrder)<0){ 		// merging this record with others
				if(ptowrite->Append(rFromFile)==0){		// merge already existing record
						// page full
						// write this page to file
						newFile->AddPage(ptowrite,currentPageNumber++);
						//currentPageNumber++;
						// empty this out
						ptowrite->EmptyItOut();
						// append the ptrCurrentRecord record ?
						ptowrite->Append(rFromFile);		// does this consume the record ?
				}
				if(!GetNew(rFromFile)){ nomore = true; break; }	// bring next rFromFile record ?// check if records already present are exhausted
			}
			if(ptowrite->Append(rtemp)!=1){				// copy record from pipe
						// page full
						// write this page to file
						newFile->AddPage(ptowrite,currentPageNumber++);
						// empty this out
						ptowrite->EmptyItOut();
						// append the ptrCurrentRecord record ?
						ptowrite->Append(rtemp);		// does this consume the record ?
			}
		}
		else{
			// pipe is empty now, copy rest of records to new file
			do{				
				if(ptowrite->Append(rFromFile)!=1){
					newFile->AddPage(ptowrite,currentPageNumber++);
					// empty this out
					ptowrite->EmptyItOut();
					// append the ptrCurrentRecord record ?
					ptowrite->Append(rFromFile);		// does this consume the record ?
				}
			}while(GetNew(rFromFile)!=0);
			break;
		}
	}
	outPipe->Remove(rtemp);//1 missing record

	if(nomore==true){									// file is empty
		do{
			if(ptowrite->Append(rtemp)!=1){				// copy record from pipe
						// write this page to file
						newFile->AddPage(ptowrite,currentPageNumber++);
						// empty this out
						ptowrite->EmptyItOut();
						// append the ptrCurrentRecord record ?
						ptowrite->Append(rtemp);		// does this consume the record ?
			}
		}while(outPipe->Remove(rtemp)!=0);
	}
	newFile->AddPage(ptowrite,currentPageNumber);
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
}*/

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


	// if file has no records then write out all the records from pipe to the new file
	/*if(result == 0){
		while(outPipe->Remove(rtemp)){
			if(ptowrite->Append(rtemp)!=1){				// copy record from pipe
				// write this page to file
				newFile->AddPage(ptowrite,currentPageNumber++);
				// empty this out
				ptowrite->EmptyItOut();
				// append the ptrCurrentRecord record ?
				ptowrite->Append(rtemp);		// does this consume the record ?
			}
		}
		if(!ptowrite->IsEmpty()){
			newFile->AddPage(ptowrite,currentPageNumber++);
		}	
	} else{

	}*/

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