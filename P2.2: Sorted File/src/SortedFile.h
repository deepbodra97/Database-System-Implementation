#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

struct SortInfo { 
	OrderMaker *myOrder; 
	int runLength; 
}; 

class SortedFile: public GenericDBFile{

private:
	Record* ptrCurrentRecord; // current record
	File file; // current file
	Page page; // current page
	int currentPageNumber; // current page number
	int isThisEndOfFile; // indicates if all the records are read from the file

	int runPageNumber; // run page number
	Page runPage; // for merging pipe and sorted file, the page will be fetched from 'file' into this pages 

	fMode fileMode; // to track current file mode

	ComparisonEngine comparisonEngine; // for comparison
	SortInfo *sortInfo; // sort info from startup
	OrderMaker *queryOrder; // store order maker
	bool didCNFChange; // to track if CNF/OrderMaker changed

	char *fileName; // store file name

	int isPipeDirty; // track if the pipe has records or not
	Pipe *inPipe; // input pipe
	Pipe *outPipe; // output pipe
	BigQ *bigQ; // bigQ
	
	pthread_t bigQThread; // bigQ thread

	struct bigQThreadParams{
		Pipe *inPipe;
		Pipe *outPipe;
		SortInfo sortInfo; // sort info from startups
		BigQ *bigQ;
	}threadParams;

	typedef struct bigQThreadParams bigQThreadParams;

public:
	SortedFile (); 

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

	// Utils
	void MergeFromOutpipe();
	int GetNextRecordFromRunPage(Record *fetchMe);	

	int BinarySearch(int low, int high, OrderMaker *queryOrderMaker, Record &literal);
	Record* LoadProspectivePage(Record &literal);
	// OrderMaker* CheckIfMatches(CNF &cnf, OrderMaker &o);

	static void *TriggerBigQThread(void*);

	~SortedFile();
};
