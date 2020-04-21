#include <cstring>
#include <climits>
#include <string>
#include <algorithm>

#include "Defs.h"
// #include "Errors.h"
#include "QueryPlan.h"
#include "Pipe.h"
#include "RelOp.h"

// #define _OUTPUT_SCHEMA__


using std::endl;
using std::string;

extern char* catalog_path;
extern char* dbfileile_dir;
extern char* tpch_dir;

// from parser
extern FuncOperator* finalFunction;
extern TableList* tables;
extern AndList* boolean;
extern NameList* groupingAtts;
extern NameList* attsToSelect;
extern int distinctAtts;
extern int distinctFunc;


// DEBUG
	int PrintTable(){
		TableList *t = tables;
		while(t!=0){
			cout<<t->tableName<<endl;
			t = t->next;
		}
	}

	void PrintParseTree(struct AndList *pAnd)
	{
		cout << "(";
		while (pAnd)
		{
			struct OrList *pOr = pAnd->left;
			while (pOr)
			{
				struct ComparisonOp *pCom = pOr->left;
				if (pCom!=NULL)
				{
					{
						struct Operand *pOperand = pCom->left;
						if(pOperand!=NULL)
						{
							cout<<pOperand->value<<"";
						}
					}
					switch(pCom->code)
					{
						case LESS_THAN:
						cout<<" < "; break;
						case GREATER_THAN:
						cout<<" > "; break;
						case EQUALS:
						cout<<" = "; break;
						default:
						cout << " unknown code " << pCom->code;
					}
					{
						struct Operand *pOperand = pCom->right;
						if(pOperand!=NULL)
						{
							cout<<pOperand->value<<"";
						}
					}
				}
				if(pOr->rightOr)
				{
					cout<<" OR ";
				}
				pOr = pOr->rightOr;
			}
			if(pAnd->rightAnd)
			{
				cout<<") AND (";
			}
			pAnd = pAnd->rightAnd;
		}
		cout << ")" << endl;
	}

	void PrintAttributes(Attribute* ptrAtt, int n){
		for(int i=0; i<n; i++){
			cout<<ptrAtt->name<<endl;
			ptrAtt++;
		}
	}

/**********************************************************************
 * API                                                                *
 **********************************************************************/
	QueryPlan::QueryPlan(Statistics* st): root(NULL), outName("STDOUT"), statistics(st), used(NULL) {}

	void QueryPlan::Plan() {
  CreateLeafQueryNodes();  // these nodes read from file
  CreateJoinQueryNodes();
  CreateSumQueryNodes();
  CreateProjectQueryNodes();
  CreateDistinctQueryNodes();
  CreateWriteOutQueryNodes();

  // clean up
  swap(boolean, used);
  // FATALIF(used, "WHERE clause syntax error.");
}

void QueryPlan::Print(std::ostream& os) const {
	root->Print(os);
}

void QueryPlan::SetOutput(char* out) {
	outName = out;
}

void QueryPlan::Execute() {
	cout<<"outName:"<<outName<<"stdout:"<<stdout<<endl;
	outputFile = (outName == "STDOUT" ? stdout
		: outName == "NONE" ? NULL
	: fopen(outName.c_str(), "a"));   // closed by query executor
	if (outputFile) {
		cout<<"outputFile:"<<outputFile<<endl;
		int numNodes = root->pipeId;
		Pipe** pipes = new Pipe*[numNodes];
		RelationalOp** relops = new RelationalOp*[numNodes];
		root->Execute(pipes, relops);
		for (int i=0; i<numNodes; ++i)
			relops[i] -> WaitUntilDone();
		for (int i=0; i<numNodes; ++i) {
			delete pipes[i]; delete relops[i];
		}
		delete[] pipes; delete[] relops;
		if (outputFile!=stdout) fclose(outputFile);
	}
	root->pipeId = 0;
	delete root; root = NULL;
	nodes.clear();
}


/**********************************************************************
 * Query optimization                                                 *
 **********************************************************************/
void QueryPlan::CreateLeafQueryNodes() {
	for (TableList* table = tables; table; table = table->next) {
		cout<<"CreateLeafQueryNodes:"<<table->tableName<<endl;
	// PrintTable();
		statistics->CopyRel(table->tableName, table->aliasAs);
		// makeNode(pushed, used, LeafQueryNode, newLeaf, (boolean, pushed, table->tableName, table->aliasAs, statistics));
		// makeNode(pushed, recycler, nodeType, newNode, params)
		AndList* pushed;
		LeafQueryNode* newLeaf = new LeafQueryNode(boolean, pushed, table->tableName, table->aliasAs, statistics);
		ConcatenateAndList(used, pushed);


		nodes.push_back(newLeaf);
		cout<<"here"<<endl;
	}
}

void QueryPlan::CreateJoinQueryNodes() {
	LoadMinCostJoin();
	while (nodes.size()>1) {
		// popVector(nodes, left, right);

		QueryNode* left = nodes.back();
		nodes.pop_back();
		QueryNode* right = nodes.back();
		nodes.pop_back();

		// makeNode(pushed, used, JoinQueryNode, newJoinQueryNode, (boolean, pushed, left, right, statistics));

		AndList* pushed;
		JoinQueryNode* newJoinQueryNode = new JoinQueryNode(boolean, pushed, left, right, statistics);
		ConcatenateAndList(used, pushed);

		nodes.push_back(newJoinQueryNode);
	}
	root = nodes.front();
}

void QueryPlan::CreateSumQueryNodes() {
	if (groupingAtts) {
	// FATALIF (!finalFunction, "Grouping without aggregation functions!");
	// FATALIF (distinctAtts, "No dedup after aggregate!");
		if (distinctFunc) root = new DistinctQueryNode(root);
		root = new GroupByQueryNode(groupingAtts, finalFunction, root);
	} else if (finalFunction) {
		root = new SumQueryNode(finalFunction, root);
	}
}

void QueryPlan::CreateProjectQueryNodes() {
	if (attsToSelect && !finalFunction && !groupingAtts) {
		root = new ProjectQueryNode(attsToSelect, root);
	}
}

void QueryPlan::CreateDistinctQueryNodes() {
	if (distinctAtts) root = new DistinctQueryNode(root);
}

void QueryPlan::CreateWriteOutQueryNodes() {
	root = new WriteOutQueryNode(outputFile, root);
}

void QueryPlan::LoadMinCostJoin() {
	std::vector<QueryNode*> operands(nodes);
	sort(operands.begin(), operands.end());
	int minCost = INT_MAX, actualCost;
  do {           // traverse all possible permutations
  	if ((actualCost=EstimateJoinPermutationCost(operands, *statistics, minCost))<minCost && actualCost>0) {
  		minCost = actualCost; nodes = operands; 
  	}
  } while (next_permutation(operands.begin(), operands.end()));
}

int QueryPlan::EstimateJoinPermutationCost(std::vector<QueryNode*> operands, Statistics st, int bestFound) {  // intentional copy
  	std::vector<JoinQueryNode*> freeList;  // all new nodes made in this simulation; need to be freed
  	AndList* recycler = NULL;         // AndList needs recycling
  	while (operands.size()>1) {       // simulate join
	  	// popVector(operands, left, right);
	  	QueryNode* left = operands.back();
		operands.pop_back();
		QueryNode* right = operands.back();
		operands.pop_back();

	  	// makeNode(pushed, recycler, JoinQueryNode, newJoinQueryNode, (boolean, pushed, left, right, &st));

	  	AndList* pushed;
		JoinQueryNode* newJoinQueryNode = new JoinQueryNode(boolean, pushed, left, right, &st);
		ConcatenateAndList(recycler, pushed);

	  	operands.push_back(newJoinQueryNode);
	  	freeList.push_back(newJoinQueryNode);
		if (newJoinQueryNode->estimatedCost<=0 || newJoinQueryNode->actualCost>bestFound) break;  // branch and bound
	}
	int actualCost = operands.back()->actualCost;
	// freeAll(freeList);
	for (int i = 0; i < freeList.size(); ++i) {
		--freeList[i]->pipeId;
		free(freeList[i]);  // recycler pipeIds but do not free children
	}
	ConcatenateAndList(boolean, recycler);   // put the AndLists back for future use
  	return operands.back()->estimatedCost<0 ? -1 : actualCost;
}

void QueryPlan::ConcatenateAndList(AndList*& left, AndList*& right) {
	if (!left) { swap(left, right); return; }
	AndList *pre = left, *cur = left->rightAnd;
	for (; cur; pre = cur, cur = cur->rightAnd);
		pre->rightAnd = right;
	right = NULL;
}

/**********************************************************************
 * Node construction                                                  *
 **********************************************************************/
int QueryNode::pipeId = 0;

QueryNode::QueryNode(const std::string& op, Schema* out, Statistics* st):
opName(op), outputSchema(out), numRels(0), estimatedCost(0), actualCost(0), statistics(st), outputPipeId(pipeId++) {}

QueryNode::QueryNode(const std::string& op, Schema* out, char* rName, Statistics* st):
opName(op), outputSchema(out), numRels(0), estimatedCost(0), actualCost(0), statistics(st), outputPipeId(pipeId++) {
	if (rName) relNames[numRels++] = strdup(rName);
}

QueryNode::QueryNode(const std::string& op, Schema* out, char* rNames[], int num, Statistics* st):
opName(op), outputSchema(out), numRels(0), estimatedCost(0), actualCost(0), statistics(st), outputPipeId(pipeId++) {
	for (; numRels<num; ++numRels)
		relNames[numRels] = strdup(rNames[numRels]);
}

QueryNode::~QueryNode() {
	delete outputSchema;
	for (int i=0; i<numRels; ++i)
		delete[] relNames[i];
}

AndList* QueryNode::MoveSelectionDown(AndList*& alist, Schema* target) {
	AndList header; header.rightAnd = alist;  // make a list header to
	// avoid handling special cases deleting the first list element
	// cout<<"alist:"<<alist<<endl;
	AndList *cur = alist, *pre = &header, *result = NULL;
	for (; cur; cur = pre->rightAnd)
	if (DoesSchemaContainOr(cur->left, target)) {   // should push
		pre->rightAnd = cur->rightAnd;
		cur->rightAnd = result;        // *move* the node to the result list
		result = cur;        // prepend the new node to result list
	} else pre = cur;
	alist = header.rightAnd;  // special case: first element moved
	// cout<<"result:"<<result<<endl;
	return result;
}

bool QueryNode::DoesSchemaContainOr(OrList* ors, Schema* target) {
	cout<<"containedIn:ors"<<endl;
	for (; ors; ors=ors->rightOr)
		if (!DoesSchemaContainComparisonOp(ors->left, target)) return false;
	return true;
}

bool QueryNode::DoesSchemaContainComparisonOp(ComparisonOp* cmp, Schema* target) {
	cout<<"containedIn:cmp"<<endl;
	PrintAttributes(target->GetAtts(), target->GetNumAtts());
	Operand *left = cmp->left, *right = cmp->right;
	cout<<"left:"<<left->value<<","<<left->code<<"|"<<" right->value:"<<right->value<<","<<right->code<<endl;
  // bool result = (left->code!=NAME || target->Find(left->value)!=-1)&&(right->code!=NAME || target->Find(right->value)!=-1);
	bool result = target->Find(right->value);
	cout<<"return="<<result<<endl;
	return (left->code!=NAME || target->Find(left->value)!=-1) &&
	(right->code!=NAME || target->Find(right->value)!=-1);
}

LeafQueryNode::LeafQueryNode(AndList*& boolean, AndList*& pushed, char* relName, char* alias, Statistics* st):
QueryNode("Select File", new Schema(catalog_path, relName, alias), relName, st), isDBfileOpen(false) {
	pushed = MoveSelectionDown(boolean, outputSchema);
	cout<<"pushed:"<<pushed<<endl;
	estimatedCost = statistics->Estimate(pushed, relNames, numRels);
	statistics->Apply(pushed, relNames, numRels);
	selOp.GrowFromParseTree(pushed, outputSchema, literal);
}

OnePipeQueryNode::OnePipeQueryNode(const std::string& opName, Schema* out, QueryNode* node, Statistics* st): QueryNode (opName, out, node->relNames, node->numRels, st), child(node), inputPipeId(node->outputPipeId) {}

TwoPipeQueryNode::TwoPipeQueryNode(const std::string& opName, QueryNode* l, QueryNode* r, Statistics* st): QueryNode (opName, new Schema(*l->outputSchema, *r->outputSchema), st),
leftChild(l), rightChild(r), leftInputPipeId(leftChild->outputPipeId), rightInputPipeId(rightChild->outputPipeId) {
	cout<<"TwoPipeQueryNode Schema:"<<endl;
	outputSchema->Print();
	for (int i=0; i<l->numRels;)
		relNames[numRels++] = strdup(l->relNames[i++]);
	for (int j=0; j<r->numRels;)
		relNames[numRels++] = strdup(r->relNames[j++]);
  // statistics->numRels = l->numRels+r->numRels;
}

ProjectQueryNode::ProjectQueryNode(NameList* atts, QueryNode* node):
OnePipeQueryNode("Project", NULL, node, NULL), numInputAttributes(node->outputSchema->GetNumAtts()), numOutputAttributes(0) {
	Schema* cSchema = node->outputSchema;
	Attribute resultAtts[MAX_ATTS];
  // FATALIF (cSchema->GetNumAtts()>MAX_ATTS, "Too many attributes.");
	if(cSchema->GetNumAtts()>MAX_ATTS){
		cout<<"Error: Too many attributes"<<endl; 
		exit(EXIT_FAILURE);   
	}
	for (; atts; atts=atts->next, numOutputAttributes++) {
		cout<<"ProjectQueryNode: atts->name="<<atts->name<<","<<cSchema->Find(atts->name)<<endl;
		if((keepMe[numOutputAttributes]=cSchema->Find(atts->name)) == -1){
			cout<<"Error: Attribute not found"<<endl;
			exit(EXIT_FAILURE);
		}
		cout<<"keepMe verify: "<<keepMe[numOutputAttributes]<<endl;
	// FATALIF ((keepMe[numOutputAttributes]=cSchema->Find(atts->name))==-1,
			 // "Projecting non-existing attribute.");
		resultAtts[numOutputAttributes].name = atts->name;
		resultAtts[numOutputAttributes].myType = cSchema->FindType(atts->name);
	}
	outputSchema = new Schema ("", numOutputAttributes, resultAtts);
}

DistinctQueryNode::DistinctQueryNode(QueryNode* node):
OnePipeQueryNode("Deduplication", new Schema(*node->outputSchema), node, NULL), orderMakerDistinct(node->outputSchema) {}

JoinQueryNode::JoinQueryNode(AndList*& boolean, AndList*& pushed, QueryNode* l, QueryNode* r, Statistics* st):
TwoPipeQueryNode("Join", l, r, st) {
	pushed = MoveSelectionDown(boolean, outputSchema);
	estimatedCost = statistics->Estimate(pushed, relNames, numRels);
	statistics->Apply(pushed, relNames, numRels);
	actualCost = l->actualCost + estimatedCost + r->actualCost;
	cout<<"JoinQueryNode:actualCost="<<actualCost<<endl;
	selOp.GrowFromParseTree(pushed, l->outputSchema, r->outputSchema, literal);
}

SumQueryNode::SumQueryNode(FuncOperator* parseTree, QueryNode* node):
OnePipeQueryNode("Sum", resultSchema(parseTree, node), node, NULL) {
	function.GrowFromParseTree (parseTree, *node->outputSchema);
}

Schema* SumQueryNode::resultSchema(FuncOperator* parseTree, QueryNode* node) {
	Function fun;
	Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
	fun.GrowFromParseTree (parseTree, *node->outputSchema);
	return new Schema ("", 1, atts[fun.GetReturnsIntType()]);
}

GroupByQueryNode::GroupByQueryNode(NameList* groupingAttributes, FuncOperator* parseTree, QueryNode* node):
OnePipeQueryNode("Group by", resultSchema(groupingAttributes, parseTree, node), node, NULL) {
	orderMakerGroupBy.growFromParseTree(groupingAttributes, node->outputSchema);
	function.GrowFromParseTree (parseTree, *node->outputSchema);
}

Schema* GroupByQueryNode::resultSchema(NameList* groupingAttributes, FuncOperator* parseTree, QueryNode* node) {
	Function fun;
	Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
	Schema* cSchema = node->outputSchema;
	fun.GrowFromParseTree (parseTree, *cSchema);
	Attribute resultAtts[MAX_ATTS];
  // FATALIF (1+cSchema->GetNumAtts()>MAX_ATTS, "Too many attributes.");
	resultAtts[0].name = "sum";
	resultAtts[0].myType = fun.GetReturnsIntType();
	int numOutputAttributes = 1;
	for (; groupingAttributes; groupingAttributes=groupingAttributes->next, numOutputAttributes++) {
	// FATALIF (cSchema->Find(groupingAttributes->name)==-1, "Grouping by non-existing attribute.");
		resultAtts[numOutputAttributes].name = groupingAttributes->name;
		resultAtts[numOutputAttributes].myType = cSchema->FindType(groupingAttributes->name);
	}
	return new Schema ("", numOutputAttributes, resultAtts);
}

WriteOutQueryNode::WriteOutQueryNode(FILE*& out, QueryNode* node):
OnePipeQueryNode("WriteOut", new Schema(*node->outputSchema), node, NULL), outputFile(out) {
	// cout<<"WriteOutQueryNodeSchema:"<<endl;
	// node->outputSchema->Print();
}


/**********************************************************************
 * Query execution                                                    *
 **********************************************************************/
void LeafQueryNode::Execute(Pipe** pipes, RelationalOp** relops) {
	std::string dbName = std::string(relNames[0]) + ".bin";
	cout<<"dbName="<<dbName<<" relNames[0]="<<relNames[0]<<endl;
	dbfile.Open((char*)dbName.c_str()); isDBfileOpen = true;
	SelectFile* sf = new SelectFile();

  sf->outputSchema = outputSchema; // debug

  pipes[outputPipeId] = new Pipe(PIPE_SIZE);
  relops[outputPipeId] = sf;
  sf -> Run(dbfile, *pipes[outputPipeId], selOp, literal);
}

void ProjectQueryNode::Execute(Pipe** pipes, RelationalOp** relops) {
	child -> Execute(pipes, relops);
	Project* p = new Project();

  p->outputSchema = outputSchema; // debug
  cout<<"ProjectQueryNode Execute: keepMe="<<*keepMe<<endl;

  pipes[outputPipeId] = new Pipe(PIPE_SIZE);
  relops[outputPipeId] = p;
  p -> Run(*pipes[inputPipeId], *pipes[outputPipeId], keepMe, numInputAttributes, numOutputAttributes);
}

void DistinctQueryNode::Execute(Pipe** pipes, RelationalOp** relops) {
	child -> Execute(pipes, relops);
	DuplicateRemoval* dedup = new DuplicateRemoval();
	pipes[outputPipeId] = new Pipe(PIPE_SIZE);
	relops[outputPipeId] = dedup;
	dedup -> Run(*pipes[inputPipeId], *pipes[outputPipeId], *outputSchema);
}

void SumQueryNode::Execute(Pipe** pipes, RelationalOp** relops) {
	child -> Execute(pipes, relops);
	Sum* s = new Sum();
	pipes[outputPipeId] = new Pipe(PIPE_SIZE);
	relops[outputPipeId] = s;
	s -> Run(*pipes[inputPipeId], *pipes[outputPipeId], function);
}

void GroupByQueryNode::Execute(Pipe** pipes, RelationalOp** relops) {
	child -> Execute(pipes, relops);
	GroupBy* grp = new GroupBy();
	pipes[outputPipeId] = new Pipe(PIPE_SIZE);
	relops[outputPipeId] = grp;
	grp -> Run(*pipes[inputPipeId], *pipes[outputPipeId], orderMakerGroupBy, function);
}

void JoinQueryNode::Execute(Pipe** pipes, RelationalOp** relops) {
  // cout<<"JoinQueryNode Schema:"<<endl;
  // outputSchema->Print();
	leftChild -> Execute(pipes, relops); rightChild -> Execute(pipes, relops);
	Join* j = new Join();

	j->outputSchema = outputSchema;

	pipes[outputPipeId] = new Pipe(PIPE_SIZE);
	relops[outputPipeId] = j;
	j -> Run(*pipes[leftInputPipeId], *pipes[rightInputPipeId], *pipes[outputPipeId], selOp, literal);
}

void WriteOutQueryNode::Execute(Pipe** pipes, RelationalOp** relops) {
	child -> Execute(pipes, relops);
	WriteOut* w = new WriteOut();
	pipes[outputPipeId] = new Pipe(PIPE_SIZE);
	relops[outputPipeId] = w;
	w -> Run(*pipes[inputPipeId], outputFile, *outputSchema);

}

/**********************************************************************
 * Print utilities                                                    *
 **********************************************************************/
// Indent at a given level and using a given string
string Indent(int level, string s){
	return string(3*(level+1), ' ') + s;
}

// Indentation for Operator
string IndentOperator(int level){
	return Indent(level, "---> ");
}

// Indentation for Operator Info
string IndentOperatorInfo(int level){
	return Indent(level, "+ ");
}

void QueryNode::Print(std::ostream& os, int level) const {
	PrintOperator(os, level);
	PrintOperatorInfo(os, level);
	PrintSchema(os, level);
	PrintPipe(os, level);
	PrintChildren(os, level);
}

void QueryNode::PrintOperator(std::ostream& os, int level) const {
	os << IndentOperator(level) << opName << ": ";
}

void QueryNode::PrintSchema(std::ostream& os, int level) const {
// #ifdef _OUTPUT_SCHEMA__
	os << IndentOperatorInfo(level) << "Output schema:" << endl;
	outputSchema->Print(os);
// #endif
}

void LeafQueryNode::PrintPipe(std::ostream& os, int level) const {
	os << IndentOperatorInfo(level) << "Output pipe: " << outputPipeId << endl;
}

void OnePipeQueryNode::PrintPipe(std::ostream& os, int level) const {
	os << IndentOperatorInfo(level) << "Output pipe: " << outputPipeId << endl;
	os << IndentOperatorInfo(level) << "Input pipe: " << inputPipeId << endl;
}

void TwoPipeQueryNode::PrintPipe(std::ostream& os, int level) const {
	os << IndentOperatorInfo(level) << "Output pipe: " << outputPipeId << endl;
	os << IndentOperatorInfo(level) << "Input pipe: " << leftInputPipeId << ", " << rightInputPipeId << endl;
}

void LeafQueryNode::PrintOperator(std::ostream& os, int level) const {
	os << IndentOperator(level) << "Select from " << relNames[0] << ": ";
}

void LeafQueryNode::PrintOperatorInfo(std::ostream& os, int level) const {
	selOp.Print();
}

void ProjectQueryNode::PrintOperatorInfo(std::ostream& os, int level) const {
	os << keepMe[0];
	for (int i=1; i<numOutputAttributes; ++i) os << ',' << keepMe[i];
		os << endl;
	os << IndentOperatorInfo(level) << numInputAttributes << " input attributes; " << numOutputAttributes << " output attributes" << endl;
}

void JoinQueryNode::PrintOperatorInfo(std::ostream& os, int level) const {
	selOp.Print();
	os << IndentOperatorInfo(level) << "Estimate = " << estimatedCost << ", Cost = " << actualCost << endl;
}

void SumQueryNode::PrintOperatorInfo(std::ostream& os, int level) const {
	os << IndentOperatorInfo(level) << "Function: "; (const_cast<Function*>(&function))->Print();
}

void GroupByQueryNode::PrintOperatorInfo(std::ostream& os, int level) const {
	os << IndentOperatorInfo(level) << "OrderMaker: "; (const_cast<OrderMaker*>(&orderMakerGroupBy))->Print();
	os << IndentOperatorInfo(level) << "Function: "; (const_cast<Function*>(&function))->Print();
}

void WriteOutQueryNode::PrintOperatorInfo(std::ostream& os, int level) const {
	os << IndentOperatorInfo(level) << "Output to " << outputFile << endl;
}