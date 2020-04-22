#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "HeapFile.h"

HeapFile::HeapFile () {
	ptrCurrentRecord = new Record();
	isThisEndOfFile = false;

	currentPageNumber = 0;
	fileMode = WRITE;

	readPageNumber = 0;
	readPageRecordNumber = 0;
}

HeapFile::~HeapFile(){
	delete ptrCurrentRecord;
}

int HeapFile::Create (char *f_path, fType f_type, void *startup) {

	filePath = f_path;
	if(f_type == heap){ // if f_type is heap
    	file.Open(0, (char *)filePath); // first parameter <0> will always create a new file
    	return 1;
	}
	return 0;
}

void HeapFile::Load (Schema &f_schema, char *loadpath) {
	SwitchToWriteMode(); // change to write mode
	FILE *tableFile = fopen (loadpath, "r"); // open the table file
	Record newRecord; // holder for new record
	while(newRecord.SuckNextRecord(&f_schema, tableFile) == 1){
		Add(newRecord); // add the new record to the file
	}
	SwitchToReadMode(); // Write out the last page to the file
}

int HeapFile::Open (char *f_path) {
	filePath = f_path;
	file.Open(1, (char *)filePath); // first parameter <1> (any non zero value) will open an existing file
	MoveFirst();
	return 1;
}

void HeapFile::MoveFirst () {
	isThisEndOfFile = false;
	readPageNumber = 0;
	readPageRecordNumber = 0;
	SwitchToReadMode(); // change to read mode
	currentPageNumber = 0; // read from start
	if(file.GetLength()-2 < 0){
		return;
	}
	file.GetPage(&page, currentPageNumber); // get the first page
	page.GetFirst(ptrCurrentRecord); // initialise the current pointer to the first record
}

int HeapFile::Close () {
	MoveFirst();
	if(fileMode == WRITE && !page.IsEmpty()){
		file.AddPage(&page, currentPageNumber); // add the dirty page to the file
	}
	cout<<filePath;
	char metaFilePath[20];
	ofstream metaFile;
	sprintf (metaFilePath, "%s.meta", filePath);
	metaFile.open (metaFilePath);
	metaFile<<"heap";
	metaFile.close();
	file.Close(); // close the file
	return 1;
}

void HeapFile::Add (Record &rec) {
	SwitchToWriteMode(); // switch to write mode
	int addStatus = page.Append(&rec);
	if(addStatus == 0){ // is page full?
		file.AddPage(&page, currentPageNumber); // add new page to the file
		currentPageNumber+=1; // increase the current page number by 1
		page.EmptyItOut(); // empty out the newly added page because the content is same as the previous page
		page.Append(&rec); // add the record to the page
	}
	cout<<"Added 1 record"<<endl;
}

int HeapFile::GetNext (Record &fetchme) {

	if(isThisEndOfFile){ // if the end of the file has been reached
		return 0;
	}
	fetchme.Consume(ptrCurrentRecord);

	if(page.GetFirst(ptrCurrentRecord) == 1){ // is there a record to fetch?
		readPageRecordNumber++;
		return 1;
	}

	currentPageNumber+=1; // page has been consumed. Increment page number
	if(currentPageNumber == file.GetLength()-1){ // if there is no next page return 0
		isThisEndOfFile = true;
		return 1;
	}

	file.GetPage(&page, currentPageNumber); // get the next page
	readPageNumber+=1;
	if(page.GetFirst(ptrCurrentRecord) == 1){ // record found
		readPageRecordNumber++;
		return 1;
	}
}

int HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

	ComparisonEngine comparisonEngine;
	while(GetNext(fetchme)){ // while there are records
		cout<<"Asked for next record macthing CNF"<<endl;
		if(comparisonEngine.Compare(&fetchme, &literal, &cnf) == 1){ // when the fetched records matches the CNF
			cout<<"Fetched 1 record"<<endl;
			return 1;
		}
	}
	return 0; // No match found
}

// Utils

void HeapFile::SwitchToWriteMode(){
	if(fileMode == WRITE){ // if file mode is already write
		return;
	}
	fileMode = WRITE; // change fileMode to write
	/*
	* if file.GetLength()-2<0, then there are no pages except the first blank page so currentPageNumber=0
	* else currentPageNumber=file.GetLength()-2 because the first page is blank
	*/
	currentPageNumber = file.GetLength()-2<0?0:file.GetLength()-2; 
	file.GetPage(&page, currentPageNumber); // get the required page
}

void HeapFile::SwitchToReadMode(){
	if(fileMode==READ){ // if file mode is already read
		return;
	}
	if(file.GetLength()-2 < 0){
		return;
	}
	fileMode = READ; // change fileMode to read
	if(page.GetNumRecs() != 0){ // if the current page has records
		file.AddPage(&page, currentPageNumber); // add the dirty page to the file
		page.EmptyItOut();
	}

	file.GetPage(&page, readPageNumber); // get the page that was being read earlier(before the write started)
	Record temp;
	for(int i=0; i<readPageRecordNumber; i++){ // move to the record that was being pointed to earlier (before the write started)
		if(GetNext(temp)){
			ptrCurrentRecord = &temp;
		}
	}
}