
#ifndef HEAPFILE_H
#define HEAPFILE_H
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"

#include <fstream>


class HeapFile : public GenericDBFile{

private:
	File file; // an instance of a File class that represents a page in secondary memory
	Page page; // an instance of a Page class that represents a page in main memory (frame)

public:
	char *filePath;
	// extra state variables required
	Record *ptrCurrentRecord; // pointer to the current record
	bool isThisEndOfFile;
	int currentPageNumber; // current page number that is in the main memory
	fMode fileMode; // to track if the file is currently being written to or read from

	int readPageNumber;
	int readPageRecordNumber;

	HeapFile (); 
	~HeapFile();

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

	// Utils
	void SwitchToWriteMode();
	void SwitchToReadMode();
};
#endif
