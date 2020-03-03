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
	File file;
	Page page;
	int currentPageNumber;
	int isThisEndOfFile;

	int mergePageNumber;
	Page mergePage;

	fMode fileMode;

	SortInfo *sortInfo;

	char *fileName;

	int isPipeDirty;
	// change

	
	Pipe *inPipe;
	Pipe *outPipe;
	BigQ *bigQ;
	
	
	
	
	
	
	Record* ptrCurrentRecord;
		
	int recordIndex;
	bool queryChange;
	OrderMaker *queryOrder;
	pthread_t bigQ_t;


	struct bigQThreadParams{
	
		Pipe *inPipe;
		Pipe *outPipe;
		SortInfo sortInfo;
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
	int GetNew(Record *r1);	

	int binarySearch(int low, int high, OrderMaker *queryOM, Record &literal);
	Record* GetMatchPage(Record &literal);
	OrderMaker* checkIfMatches(CNF &c, OrderMaker &o);

	static void *triggerBigQThread(void*);

	~SortedFile();
};
