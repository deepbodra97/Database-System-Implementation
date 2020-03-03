#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
//#include "DBFile.h"


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
	Page *mergePage;

	fMode fileMode;

	SortInfo *sortInfo;

	// change

	char *fileName;
	Pipe *inPipe;
	Pipe *outPipe;
	BigQ *bq;
	
	
	
	
	
	
	Record* current;
	
	off_t writeIndex;
	
	int recordIndex;
	bool queryChange;
	OrderMaker *queryOrder;
	pthread_t bigQ_t;
	int isDirty;


	struct thread_arguments{
	
		Pipe *in;
		Pipe *out;
		SortInfo s;
		BigQ *b; 

	}thread_args;

	typedef struct thread_arguments thread_arguments; 

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
	void CreateBigQ();	
	void DeleteBigQ();	

	void SwitchToWriteMode();
	void SwitchToReadMode();

	void MergeFromOutpipe();
	int GetNew(Record *r1);	

	int bsearch(int low, int high, OrderMaker *queryOM, Record &literal);
	Record* GetMatchPage(Record &literal);
	OrderMaker* checkIfMatches(CNF &c, OrderMaker &o);

	static void *instantiate_BigQ(void*);

	~SortedFile();
};
