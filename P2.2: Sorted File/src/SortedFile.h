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
	Record* ptrCurrentRecord;
	File file;
	Page page;
	int currentPageNumber;
	int isThisEndOfFile;

	int runPageNumber;
	Page runPage;

	fMode fileMode;

	SortInfo *sortInfo;
	OrderMaker *queryOrder;
	bool didCNFChange;

	char *fileName;

	int isPipeDirty;
	Pipe *inPipe;
	Pipe *outPipe;
	BigQ *bigQ;
	
	pthread_t bigQThread;

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

	int BinarySearch(int low, int high, OrderMaker *queryOrderMaker, Record &literal);
	Record* LoadProspectivePage(Record &literal);
	// OrderMaker* CheckIfMatches(CNF &cnf, OrderMaker &o);

	static void *TriggerBigQThread(void*);

	~SortedFile();
};
