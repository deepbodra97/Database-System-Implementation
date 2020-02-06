#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
	ptrCurrentRecord = new Record();
	pageNumber = 0;
	fileMode = WRITE;
}


int DBFile::Create (const char *f_path, fType f_type, void *startup) {
	
	if(f_type == heap){ // if f_type is heap
    	file.Open(0, (char *)f_path); // first parameter <0> will always create a new file
    	return 1;
	} else if(f_type == sorted){
		cout<<"ImplementationError: SortedFile"<<endl;
	} else if(f_type == tree){
		cout<<"ImplementationError: TreeFile"<<endl;
	} else{
		cout<<"UndefinedError: No such implementation"<<endl;
	}
	return 0;
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
	SwitchToWriteMode(); // change to write mode
	FILE *tableFile = fopen (loadpath, "r"); // open the table file
	Record newRecord; // holder for new record
	while(newRecord.SuckNextRecord(&f_schema, tableFile) == 1){
		Add(newRecord); // add the new record to the file
	}
	SwitchToReadMode(); // Write out the last page to the file
}

int DBFile::Open (const char *f_path) {
	file.Open(1, (char *)f_path); // first parameter <1> (any non zero value) will open an existing file
	return 1;
}

void DBFile::MoveFirst () {
	SwitchToReadMode(); // change to read mode
	pageNumber = 0; // read from start
	file.GetPage(&page, pageNumber); // get the first page
	page.GetFirst(ptrCurrentRecord); // initialise the current pointer to the first record
}

int DBFile::Close () {
	file.Close(); // close the file
	return 1;
}

void DBFile::Add (Record &rec) {
	SwitchToWriteMode(); // switch to write mode
	int addStatus = page.Append(&rec);
	if(addStatus == 0){ // is page full?
		file.AddPage(&page, pageNumber); // add new page to the file
		pageNumber+=1; // increase the current page number by 1
		page.EmptyItOut(); // empty out the newly added page because the content is same as the previous page
		page.Append(&rec); // add the record to the page
	}
}

int DBFile::GetNext (Record &fetchme) {
	if(page.GetFirst(&fetchme) == 1){ // is there a record to fetch?
		ptrCurrentRecord = &fetchme; // update ptrCurrentRecord to the newly fetched record
		return 1;
	}
	pageNumber+=1; // page has been consumed. Increment page number
	if(pageNumber == file.GetLength()-1){ // if there is no next page return 0
		return 0;
	}
	file.GetPage(&page, pageNumber); // get the next page
	if(page.GetFirst(&fetchme) == 1){ // record found
		ptrCurrentRecord = &fetchme; // update ptrCurrentRecord to the newly fetched record
		return 1;
	}
	return 0; // record not found in the next page return 0
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine comparisonEngine;
	while(GetNext(fetchme)){ // while there are records
		if(comparisonEngine.Compare(&fetchme, &literal, &cnf) == 1){ // when the fetched records matches the CNF
			return 1;
		}
	}
	return 0; // No match found
}

// Utils

void DBFile::SwitchToWriteMode(){
	if(fileMode == WRITE){ // if file mode is already write
		return;
	}
	fileMode = WRITE; // change fileMode to write
	/*
	* if file.GetLength()-2<0, then there are no pages except the first blank page so pageNumber=0
	* else pageNumber=file.GetLength()-2 because the first page is blank
	*/
	pageNumber = file.GetLength()-2<0?0:file.GetLength()-2; 
	file.GetPage(&page, pageNumber); // get the required page
}

void DBFile::SwitchToReadMode(){
	if(fileMode==READ){ // if file mode is already read
		return;
	}
	fileMode = READ; // change fileMode to read
	if(page.GetNumRecs() != 0){ // if the current page has records
		file.AddPage(&page, pageNumber); // add the dirty page to the file
	}
}
