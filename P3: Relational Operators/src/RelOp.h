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
	Function *function;
	OrderMaker *groupAtts;
	Pipe *inPipeR;
	FILE *outFile;

	OperatorThreadMemberHolder(Schema *mySchema, Pipe *inPipe, DBFile *inFile, Pipe *outPipe, FILE *outFile, CNF *selOp, Record *literal, int numAttsInput, int numAttsOutput, int *keepMe, int runLength, Function *function, OrderMaker *groupAtts, Pipe *inPipeR){
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
	virtual void Use_n_Pages (int n) = 0;

protected:
	pthread_t operatorThread;
	static int create_joinable_thread(pthread_t *thread, void *(*start_routine) (void *), void *arg);
};

class SelectFile : public RelationalOp { 
public:
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	// void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void* operate(void* arg);
};

class SelectPipe : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	// void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void* operate(void* arg);
};

class Project : public RelationalOp { 
public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	// void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void* operate(void* arg);
};


class JoinBuffer;
class Join : public RelationalOp { 
public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	// void WaitUntilDone ();
	void Use_n_Pages (int n);
private:
	int runLength;
	
	static void* operate(void* param);
	static void sortMergeJoin(Pipe* pleft, OrderMaker* orderLeft, Pipe* pright, OrderMaker* orderRight, Pipe* pout,
                             CNF* sel, Record* literal, size_t runLen);
	static void nestedLoopJoin(Pipe* pleft, Pipe* pright, Pipe* pout, CNF* sel, Record* literal, size_t runLen);
	static void joinBuf(JoinBuffer& buffer, DBFile& file, Pipe& out, Record& literal, CNF& sleOp);
	static void dumpFile(Pipe& in, DBFile& out);
};

class JoinBuffer {
	friend class Join;
	JoinBuffer(size_t npages);
	~JoinBuffer();
 
	bool add (Record& addme);
  	void clear () { size=nrecords=0; }

  	size_t size, capacity;   // in bytes
  	size_t nrecords;
  	Record* buffer;
};


class DuplicateRemoval : public RelationalOp {
private:
	int runLength;
public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	// void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void* operate(void* arg);	
};

class Sum : public RelationalOp {
public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	// void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void* operate(void* arg);
	template <class T> static void calculateSum(Pipe* in, Pipe* out, Function* function);
};

class GroupBy : public RelationalOp {
private:
	int runLength;

public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	// void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void* operate(void* arg);

	template <class T>
	static void doGroup(Pipe* in, Pipe* out, OrderMaker* order, Function* func, size_t runLen);

	template <class T>
  	static void putGroup(Record& cur, const T& sum, Pipe* out, OrderMaker* order) {
	    cur.Project(order->GetAtts(), order->GetNumAtts(), cur.GetNumAtts());
	    cur.prepend(sum);
	    out->Insert(&cur);
  	}
};

class WriteOut : public RelationalOp {
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	// void WaitUntilDone () { }
	void Use_n_Pages (int n);
	static void* operate(void* arg);
};
#endif
