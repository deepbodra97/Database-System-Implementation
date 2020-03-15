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
	int runLength;
private:
	
	
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

// class Join : public RelationalOp { 

// 	private:
// 	pthread_t thread;
// 	Pipe *inPipeL;
// 	Pipe *inPipeR;
// 	Pipe *outPipe;
// 	CNF *selOp;
// 	Record *literal;
// 	int nPages;

// 	public:
// 	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
// 	static void* jswpn(void *);
// 	void join();
// 	void WaitUntilDone () ;
// 	void Use_n_Pages (int n);
// };


// class Join : public RelationalOp {
// 	int runLength;
//   Pipe * inL;
//   Pipe * inR;
//   Pipe * out;
//   CNF * cnf;

//   pthread_t JoinThread;
//   static void *thread_starter(void *context);
//   void * WorkerThread(void);

//   void FillBuffer(Record &, vector<Record> &buffer, Pipe & pipe, OrderMaker & sortOrder);
//  public:
//   Join () : inL(0), inR(0), out(0), cnf(0), JoinThread() {}
//   void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal __attribute__ ((unused))) {
//     inL = &inPipeL;
//     inR = &inPipeR;
//     out = &outPipe;
//     cnf = & selOp;
//     clog << "Join pthread create" << endl;
//     pthread_create (&JoinThread, NULL, &Join::thread_starter, this);
//   }
//   void WaitUntilDone () {
//     clog << "J waiting til done" << endl;
//     pthread_join (JoinThread, NULL);
//     clog << "J complete, joined" << endl;
//   }

//   void Use_n_Pages (int runlen) {
// 		this->runLength = runlen;
// 		return;
// 	}

// 	int GetRunLength (void) {return runLength;}

// };

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


// // new
// #ifndef REL_OP_H
// #define REL_OP_H

// #include "Pipe.h"
// #include "DBFile.h"
// #include "Record.h"
// #include "Function.h"
// #include <string>
// #include <vector>

// class RelationalOp {
//  protected:
//   int runLength;
//  public:
//  RelationalOp() : runLength(100) {}
//   // blocks the caller until the particular relational operator
//   // has run to completion
//   virtual void WaitUntilDone () = 0;

//   // tell us how much internal memory the operation can use
//   void Use_n_Pages (int n) {runLength = n; clog << "runLength is now " << runLength << endl; return;}
//   int GetRunLength (void) {return runLength;}
// };

// class SelectFile : public RelationalOp {
//  private:
//   DBFile * inF;
//   Pipe * outP;
//   CNF * cnf;
//   Record * lit;

//   pthread_t SelectFileThread;
//   static void *thread_starter(void *context);
//   void * WorkerThread(void);
//   SelectFile operator=(const SelectFile&);
//   // SelectFile & SelectFile(const SelectFile &);
//  public:
//  SelectFile() : inF(0),outP(0),cnf(0),lit(0), SelectFileThread() {}
//   void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
//   void WaitUntilDone ();
// };

// class SelectPipe : public RelationalOp {
//   Pipe * in;
//   Pipe * out;
//   CNF * cnf;
//   Record * lit;

//   pthread_t SelectPipeThread;
//   static void *thread_starter(void *context);
//   void * WorkerThread(void);
//  public:
//  SelectPipe() : in(0),out(0),cnf(0),lit(0) {}
//   void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
//     in = &inPipe;
//     out = &outPipe;
//     cnf = &selOp;
//     lit = &literal;
//     pthread_create (&SelectPipeThread, NULL, &SelectPipe::thread_starter, this);
//   }
//   void WaitUntilDone ();
// };

// class Project : public RelationalOp {
//   Pipe * in;
//   Pipe * out;
//   int * atts;
//   int numAttsIn;
//   int numAttsOut;

//   pthread_t ProjectThread;
//   static void *thread_starter(void *context);
//   void * WorkerThread(void);
//  public:
//   Project () : in(0), out(0), atts(0), numAttsIn(0), numAttsOut(0), ProjectThread() {}
//   void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
//     in = &inPipe;
//     out = &outPipe;
//     atts = keepMe;
//     numAttsIn = numAttsInput;
//     numAttsOut = numAttsOutput;
//     clog << "P pthread create" << endl;
//     pthread_create (&ProjectThread, 0, &Project::thread_starter, this);
//   }
//   void WaitUntilDone () {
//     clog << "P waiting til done" << endl;
//     pthread_join (ProjectThread, 0);
//     clog << "P complete, joined" << endl;
//   }
// };

// class Join : public RelationalOp {
//   Pipe * inL;
//   Pipe * inR;
//   Pipe * out;
//   CNF * cnf;

//   pthread_t JoinThread;
//   static void *thread_starter(void *context);
//   void * WorkerThread(void);

//   void FillBuffer(Record &, vector<Record> &buffer, Pipe & pipe, OrderMaker & sortOrder);
//  public:
//   Join () : inL(0), inR(0), out(0), cnf(0), JoinThread() {}
//   void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal __attribute__ ((unused))) {
//     inL = &inPipeL;
//     inR = &inPipeR;
//     out = &outPipe;
//     cnf = & selOp;
//     clog << "Join pthread create" << endl;
//     pthread_create (&JoinThread, NULL, &Join::thread_starter, this);
//   }
//   void WaitUntilDone () {
//     clog << "J waiting til done" << endl;
//     pthread_join (JoinThread, NULL);
//     clog << "J complete, joined" << endl;
//   }
// };

// class DuplicateRemoval : public RelationalOp {
//   Pipe * in;
//   Pipe * out;
//   OrderMaker compare;

//   pthread_t DuplicateRemovalThread;
//   static void *thread_starter(void *context);
//   void * WorkerThread(void);
//  public:
//   DuplicateRemoval () : in(0), out(0), DuplicateRemovalThread() {}
//   void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
//     in = &inPipe;
//     out = &outPipe;
//     compare = OrderMaker(&mySchema);
//     pthread_create (&DuplicateRemovalThread, NULL, &DuplicateRemoval::thread_starter, this);
//   }
//   void WaitUntilDone () {
//     clog << "DR waiting til done" << endl;
//     pthread_join (DuplicateRemovalThread, NULL);
//     clog << "DR complete, joined" << endl;
//   }
// };

// class Sum : public RelationalOp {
//   int integerResult;
//   double FPResult;

//   Pipe * in;
//   Pipe * out;
//   Function * fn;
//   pthread_t SumThread;
//   static void *thread_starter(void *context);
//   void * WorkerThread(void);
//  public:
//  Sum() : integerResult(0),FPResult(0),in(0),out(0),fn(0), SumThread() {}
//   void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
//     in = &inPipe;
//     out = &outPipe;
//     fn = &computeMe;
//     clog << "SUM pthread create" << endl;
//     pthread_create (&SumThread, NULL, &Sum::thread_starter, this);
//   }
//   void WaitUntilDone () {
//     clog << "Sum waiting" << endl;
//     pthread_join (SumThread, NULL);
//     clog << "Sum complete, joined" << endl;
//   }
// };

// class GroupBy : public RelationalOp {
//   Pipe * in;
//   Pipe * out;
//   OrderMaker * comp;
//   Function * fn;

//   pthread_t GroupByThread;
//   static void *thread_starter(void *context);
//   void * WorkerThread(void);
//   GroupBy & operator=(const GroupBy&);
//  public:
//  GroupBy() :in(0),out(0),comp(0),fn(0), GroupByThread() {}
//   void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
//     in = &inPipe;
//     out = &outPipe;
//     comp = &groupAtts;
//     fn = &computeMe;
//     clog << "GB pthread create" << endl;
//     pthread_create (&GroupByThread, NULL, &GroupBy::thread_starter, this);
//   }
//   void WaitUntilDone () {
//     clog << "GB waiting" << endl;
//     pthread_join (GroupByThread, NULL);
//     clog << "GB complete, joined" << endl;
//   }
//   void WriteRecordOut(Record &, Type, int & intresult, double & doubleresult);
// };

// class WriteOut : public RelationalOp {
//   Pipe * in;
//   FILE * out;
//   Schema * sch;
//   pthread_t WriteOutThread;
//   static void *thread_starter(void *context);
//   void * WorkerThread(void);
//  public:
//  WriteOut() : in(0), out(0), sch(0), WriteOutThread() {}
//   void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
//     in = &inPipe;
//     out = outFile;
//     sch = &mySchema;
//     clog << "W pthread create" << endl;
//     pthread_create (&WriteOutThread, NULL, &WriteOut::thread_starter, this);
//   }
//   void WaitUntilDone () {
//     clog << "W waiting til done" << endl;
//     pthread_join (WriteOutThread, NULL);
//     clog << "W complete, joined" << endl;
//   }
// };
// #endif