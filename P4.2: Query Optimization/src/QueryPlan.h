#ifndef QUERY_PLAN_H_
#define QUERY_PLAN_H_

#include <iostream>
#include <string>
#include <vector>

#include "DBFile.h"
#include "Schema.h"
#include "Function.h"
#include "ParseTree.h"
#include "Statistics.h"
#include "Comparison.h"

#define MAX_RELS 12
#define MAX_RELNAME 50
#define MAX_ATTS 100

class QueryNode;
class QueryPlan {
public:
	QueryPlan(Statistics* st);
	~QueryPlan();

	void Plan();
	void Print(std::ostream& os = std::cout) const;
	void SetOutput(char* out);
	void Execute();

private:
	QueryNode* root;
	std::vector<QueryNode*> nodes;
	std::string outName;
	FILE* outputFile;

	Statistics* statistics;
	AndList* used;  // reconstruct AndList so that it can be used next time. should be assigned to boolean after each round

	void CreateLeafQueryNodes();
	void CreateJoinQueryNodes();
	void LoadMinCostJoin();
	void CreateSumQueryNodes();
	void CreateProjectQueryNodes();
	void CreateDistinctQueryNodes();
	void CreateWriteOutQueryNodes();
  	int EstimateJoinPermutationCost(std::vector<QueryNode*> operands, Statistics st, int bestFound); // intentional copy

	/*void recycleList(AndList* alist) {
		ConcatenateAndList(used, alist);
	}*/
	
	static void ConcatenateAndList(AndList*& left, AndList*& right);

	QueryPlan(const QueryPlan&);
	QueryPlan& operator=(const QueryPlan&);
};

class Pipe; class RelationalOp;
class QueryNode {
	friend class QueryPlan;
	friend class OnePipeQueryNode;
	friend class TwoPipeQueryNode;   // passed as argument to binary node
	friend class ProjectQueryNode;
	friend class DistinctQueryNode;
	friend class JoinQueryNode;
	friend class SumQueryNode;
	friend class GroupByQueryNode;
	friend class WriteOutQueryNode;

public:
	virtual ~QueryNode();

protected:
	std::string opName;
	Schema* outputSchema;
	char* relNames[MAX_RELS];
	int numRels;
	int estimatedCost, actualCost;  // estimatedCostd number of tuples and total actualCost
	Statistics* statistics;
	int outputPipeId;  // output pipe
	static int pipeId;

	QueryNode(const std::string& op, Schema* out, Statistics* st);
	QueryNode(const std::string& op, Schema* out, char* rName, Statistics* st);
	QueryNode(const std::string& op, Schema* out, char* rNames[], int num, Statistics* st);

	virtual void Print(std::ostream& os = std::cout, int level = 0) const;
	virtual void PrintOperator(std::ostream& os = std::cout, int level = 0) const;
	virtual void PrintSchema(std::ostream& os = std::cout, int level = 0) const;
	virtual void PrintOperatorInfo(std::ostream& os = std::cout, int level = 0) const = 0; // operator specific
	virtual void PrintPipe(std::ostream& os, int level = 0) const = 0;
	virtual void PrintChildren(std::ostream& os, int level = 0) const = 0;

	virtual void Execute(Pipe** pipes, RelationalOp** relops) = 0;

	static AndList* MoveSelectionDown(AndList*& alist, Schema* target);
	static bool DoesSchemaContainOr(OrList* ors, Schema* target);
	static bool DoesSchemaContainComparisonOp(ComparisonOp* cmp, Schema* target);


	virtual void PrintLeftChild(std::ostream& os, int level = 0) const = 0;
	virtual void PrintRightChild(std::ostream& os, int level = 0) const = 0;
};

class LeafQueryNode: private QueryNode {  // read from file
	friend class QueryPlan;

	DBFile dbfile;
	bool isDBfileOpen;
	CNF selOp;
	Record literal;

	LeafQueryNode (AndList*& boolean, AndList*& pushed, char* relName, char* alias, Statistics* st);
	~LeafQueryNode() {
		if (isDBfileOpen) dbfile.Close();
	}

	void PrintOperator(std::ostream& os = std::cout, int level = 0) const;
	void PrintOperatorInfo(std::ostream& os = std::cout, int level = 0) const;
	void PrintPipe(std::ostream& os, int level) const;
	void PrintChildren(std::ostream& os, int level) const {}

	void Execute(Pipe** pipes, RelationalOp** relops);


	void PrintLeftChild(std::ostream& os, int level) const {}
	void PrintRightChild(std::ostream& os, int level) const {}
};

class OnePipeQueryNode: protected QueryNode {
	friend class QueryPlan;

protected:
	QueryNode* child;
	int inputPipeId;  // input pipe

	OnePipeQueryNode(const std::string& opName, Schema* out, QueryNode* node, Statistics* st);
	virtual ~OnePipeQueryNode() { delete child; }
	void PrintPipe(std::ostream& os, int level) const;
	void PrintChildren(std::ostream& os, int level) const { child->Print(os, level+1); }

	void PrintLeftChild(std::ostream& os, int level) const{
		child->Print(os, level+1);
	}

	void PrintRightChild(std::ostream& os, int level) const{
		// rightChild->Print(os, level+1);
	}
};

class TwoPipeQueryNode: protected QueryNode {  // not including set operations.
	friend class QueryPlan;

protected:
	QueryNode* leftChild;
	QueryNode* rightChild;
  	int leftInputPipeId, rightInputPipeId; // input pipes

	TwoPipeQueryNode(const std::string& opName, QueryNode* l, QueryNode* r, Statistics* st);
	virtual ~TwoPipeQueryNode() {
		delete leftChild; delete rightChild;
	}
	
	void PrintPipe(std::ostream& os, int level) const;
	void PrintChildren(std::ostream& os, int level) const{
		leftChild->Print(os, level+1);
		rightChild->Print(os, level+1);
	}

	void PrintLeftChild(std::ostream& os, int level) const{
		leftChild->Print(os, level+1);
	}

	void PrintRightChild(std::ostream& os, int level) const{
		rightChild->Print(os, level+1);
	}
};

class ProjectQueryNode: private OnePipeQueryNode {
	friend class QueryPlan;

	int keepMe[MAX_ATTS];
	int numInputAttributes, numOutputAttributes;

	ProjectQueryNode(NameList* atts, QueryNode* node);
	void PrintOperatorInfo(std::ostream& os = std::cout, int level = 0) const;
	void Execute(Pipe** pipes, RelationalOp** relops);
};

class DistinctQueryNode: private OnePipeQueryNode {
	friend class QueryPlan;

	OrderMaker orderMakerDistinct;

	DistinctQueryNode(QueryNode* node);
	void PrintOperatorInfo(std::ostream& os = std::cout, int level = 0) const {}
	void Execute(Pipe** pipes, RelationalOp** relops);
};

class SumQueryNode: private OnePipeQueryNode {
	friend class QueryPlan;

	Function function;

	SumQueryNode(FuncOperator* parseTree, QueryNode* node);
	Schema* resultSchema(FuncOperator* parseTree, QueryNode* node);
	void PrintOperatorInfo(std::ostream& os = std::cout, int level = 0) const;
	void Execute(Pipe** pipes, RelationalOp** relops);
};

class GroupByQueryNode: private OnePipeQueryNode {
	friend class QueryPlan;

	OrderMaker orderMakerGroupBy;
	Function function;

	GroupByQueryNode(NameList* groupingAttributes, FuncOperator* parseTree, QueryNode* node);
	Schema* resultSchema(NameList* groupingAttributes, FuncOperator* parseTree, QueryNode* node);
	void PrintOperatorInfo(std::ostream& os = std::cout, int level = 0) const;
	void Execute(Pipe** pipes, RelationalOp** relops);
};

class JoinQueryNode: private TwoPipeQueryNode {
	friend class QueryPlan;

	CNF selOp;
	Record literal;

	JoinQueryNode(AndList*& boolean, AndList*& pushed, QueryNode* l, QueryNode* r, Statistics* st);
	void PrintOperatorInfo(std::ostream& os = std::cout, int level = 0) const;
	void Execute(Pipe** pipes, RelationalOp** relops);
};

class WriteOutQueryNode: private OnePipeQueryNode {
	friend class QueryPlan;

	FILE*& outputFile;

	WriteOutQueryNode(FILE*& out, QueryNode* node);
	void PrintOperatorInfo(std::ostream& os = std::cout, int level = 0) const;
	void Execute(Pipe** pipes, RelationalOp** relops);
};

#endif