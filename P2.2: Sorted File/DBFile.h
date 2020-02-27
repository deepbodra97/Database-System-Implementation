#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

#include <iostream>

using namespace std;

typedef enum {heap, sorted, tree} fType;
typedef enum {READ, WRITE} fMode;

// stub DBFile header..replace it with your own DBFile.h 

class DBFile {

private:
	File file; // an instance of a File class that represents a page in secondary memory
	Page page; // an instance of a Page class that represents a page in main memory (frame)

public:

	// extra state variables required
	Record *ptrCurrentRecord; // pointer to the current record
	bool readAllRecords;
	int currentPageNumber; // current page number that is in the main memory
	fMode fileMode; // to track if the file is currently being written to or read from

	int readPageNumber;
	int readPageRecordNumber;

	DBFile (); 

	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

	void SwitchToWriteMode();
	void SwitchToReadMode();

};
#endif
