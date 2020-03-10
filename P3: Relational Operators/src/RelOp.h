#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class OperatorThreadMemberHolder{
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

	OperatorThreadMemberHolder(Schema *mySchema, Pipe *inPipe, DBFile *inFile, Pipe *outPipe, CNF *selOp, Record *literal, int numAttsInput, int numAttsOutput, int *keepMe, int runLength){
		this->mySchema = mySchema;
		this->inPipe = inPipe;
		this->inFile = inFile;
		this->outPipe = outPipe;
		this->selOp = selOp;
		this->literal = literal;
		this->numAttsInput = numAttsInput;
		this->numAttsOutput = numAttsOutput;
		this->keepMe = keepMe;
		this->runLength = runLength;
	}
};

class RelationalOp {

public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;

protected:
	pthread_t operatorThread;
};

class SelectFile : public RelationalOp { 
public:
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void* operate(void* arg);
};

class SelectPipe : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void* operate(void* arg);
};

class Project : public RelationalOp { 
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
class Join : public RelationalOp { 
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};

class DuplicateRemoval : public RelationalOp {
private:
	int runLength;
public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void* operate(void* arg);	
};

class Sum : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
class GroupBy : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
class WriteOut : public RelationalOp {
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
#endif
