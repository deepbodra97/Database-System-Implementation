#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <sstream>

class RelationalOpThreadMemberHolder{
public:
	Schema *mySchema;
	Pipe *inPipe;
	Pipe *outPipe;
	CNF *selOp;
	DBFile *inFile;
	Record *literal;
	int numAttsInput;
	int numAttsOutput;
	int *keepMe;
	int runLength;
	Function *function;
	OrderMaker *groupAtts;
	Pipe *inPipeR;
	FILE *outFile;
	std::vector<Record*> outputCache;
	Schema *outputSchema;

	RelationalOpThreadMemberHolder(Schema *mySchema, Pipe *inPipe, DBFile *inFile, Pipe *outPipe, FILE *outFile, CNF *selOp, Record *literal, int numAttsInput, int numAttsOutput, int *keepMe, int runLength, Function *function, OrderMaker *groupAtts, Pipe *inPipeR){
		this->mySchema = mySchema;
		this->inPipe = inPipe;
		this->inFile = inFile;
		this->outPipe = outPipe;
		this->outFile = outFile;
		this->selOp = selOp;
		this->literal = literal;
		this->numAttsInput = numAttsInput;
		this->numAttsOutput = numAttsOutput;
		this->keepMe = keepMe;
		this->runLength = runLength;
		this->function = function;
		this->groupAtts = groupAtts;
		this->inPipeR = inPipeR;
	}
};

class RelationalOp {

public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone ();

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n=100);
	int runLength = 100; //default runlength
	int GetRunLength (void) {return runLength;}

	// debug
	Schema* outputSchema;
	
	void PrintCache(std::vector<Record*> cache, Schema *mySchema){
		Record record;
		for(vector<Record*>::iterator it=cache.begin();it!=cache.end();++it){
        	(*it)->Print(mySchema);
    	}
	};
	// debug end

protected:
	pthread_t operatorThread;
};

/**************************************************************************************************
SELECT FILE
**************************************************************************************************/
class SelectFile : public RelationalOp { 
public:
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	static void* Operate(void* arg);
};

/**************************************************************************************************
SELECT PIPE
**************************************************************************************************/
class SelectPipe : public RelationalOp {
public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	static void* Operate(void* arg);
};

/**************************************************************************************************
PROJECT
**************************************************************************************************/
class Project : public RelationalOp { 
public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	static void* Operate(void* arg);
};


/**************************************************************************************************
JOIN
**************************************************************************************************/
class FixedSizeRecordBuffer;
class Join : public RelationalOp { 
public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
private:
	static void* Operate(void* param);
	static void SortMergeJoin(Pipe* leftPipe, OrderMaker* leftOrderMaker, Pipe* rightPipe, OrderMaker* rightOrderMaker, Pipe* pout, CNF* selOp, Record* literal, int runLength, RelationalOpThreadMemberHolder* params);
	static void NestedLoopJoin(Pipe* leftPipe, Pipe* rightPipe, Pipe* pout, CNF* selOp, Record* literal, int runLength);
	static void JoinBufferWithFile(FixedSizeRecordBuffer& buffer, DBFile& file, Pipe& out, Record& literal, CNF& selOp);
	static void PipeToFile(Pipe& inPipe, DBFile& outFile);

};

class FixedSizeRecordBuffer {
friend class Join;
public:	
	FixedSizeRecordBuffer(int runLength);
	~FixedSizeRecordBuffer();
private:
  	Record* buffer;
  	int numRecords;
  	int size;
  	int capacity;

	bool Add (Record& addme);
  	void Clear ();
};

/**************************************************************************************************
DUPLICATE REMOVAL
**************************************************************************************************/
class DuplicateRemoval : public RelationalOp {
public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	static void* Operate(void* arg);	
};

/**************************************************************************************************
SUM
**************************************************************************************************/
class Sum : public RelationalOp {
public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	static void* Operate(void* arg);
	template <class T> static void CalculateSum(Pipe* in, Pipe* out, Function* function);
};

/**************************************************************************************************
GROUPBY
**************************************************************************************************/
class GroupBy : public RelationalOp {
private:

public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	static void* Operate(void* arg);

	template <class T>
	static void MakeGroups(Pipe* inPipe, Pipe* outPipe, OrderMaker* orderMaker, Function* function, int runLength);

  	template <class T>
  	static void AddGroup(Record& record, const T& sum, Pipe* outPipe, OrderMaker* orderMaker, Function* function);
};

/**************************************************************************************************
WRITEOUT
**************************************************************************************************/
class WriteOut : public RelationalOp {
public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	static void* Operate(void* arg);
};
#endif