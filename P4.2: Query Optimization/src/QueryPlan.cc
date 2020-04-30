#include <cstring>
#include <climits>
#include <string>
#include <algorithm>

#include "Defs.h"
#include "QueryPlan.h"
#include "Pipe.h"
#include "RelOp.h"


using std::endl;
using std::string;

extern char* catalog_path;
extern char* dbfileile_dir;
extern char* tpch_dir;

// will be loaded using parser
extern FuncOperator* finalFunction;
extern TableList* tables;
extern AndList* boolean;
extern NameList* groupingAtts;
extern NameList* attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

/**************************************************************************************************
DEBUGGIN FUNCTIONS
**************************************************************************************************/
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

/**************************************************************************************************
QUERY PLAN INTERFACE
**************************************************************************************************/
QueryPlan::QueryPlan(Statistics* st): root(NULL), outName("STDOUT"), statistics(st), used(NULL) {}

QueryPlan::~QueryPlan() { if (root) delete root; }

void QueryPlan::Plan() {
	CreateLeafQueryNodes();
	CreateJoinQueryNodes();
	CreateSumQueryNodes();
	CreateProjectQueryNodes();
	CreateDistinctQueryNodes();
	CreateWriteOutQueryNodes();

	swap(boolean, used);
	if(used){
		cout<<"Syntax error: WHERE CNF"<<endl;
		exit(EXIT_FAILURE);
	}
}

void QueryPlan::Print(std::ostream& os) const {
	root->Print(os);
}

void QueryPlan::SetOutput(char* out) {
	outName = out;
}

void QueryPlan::Execute() {
	outputFile = (outName == "STDOUT" ? stdout : outName == "NONE" ? NULL : fopen(outName.c_str(), "a"));   // closed by query executor
	if (outputFile) {
		int numNodes = root->pipeId;
		Pipe** pipes = new Pipe*[numNodes];
		RelationalOp** relops = new RelationalOp*[numNodes];
		root->Execute(pipes, relops);
		for (int i=0; i<numNodes; ++i){
			relops[i] -> WaitUntilDone();
		}
		for (int i=0; i<numNodes; ++i) {
			delete pipes[i];
			delete relops[i];
		}
		delete[] pipes;
		delete[] relops;
		if (outputFile!=stdout){
			fclose(outputFile);
		}
	}
	root->pipeId = 0;
	nodes.clear();
}


/**************************************************************************************************
QUERY PLAN CREATE NODES
**************************************************************************************************/

//*************************************************************************************************
// Leaf
void QueryPlan::CreateLeafQueryNodes() {
	for (TableList* table = tables; table; table = table->next) {
		statistics->CopyRel(table->tableName, table->aliasAs); // copy relation with new alias

		AndList* pushed;
		LeafQueryNode* newLeaf = new LeafQueryNode(boolean, pushed, table->tableName, table->aliasAs, statistics); // create leaf node
		ConcatenateAndList(used, pushed);

		nodes.push_back(newLeaf); // add the node
	}
}

//*************************************************************************************************
// Project
void QueryPlan::CreateProjectQueryNodes() {
	if (attsToSelect && !finalFunction && !groupingAtts) {
		root = new ProjectQueryNode(attsToSelect, root); // add project node
	}
}

//*************************************************************************************************
// Join
void QueryPlan::CreateJoinQueryNodes() {
	LoadMinCostJoin(); // get the join for which the cost is minimum
	while (nodes.size()>1) {

		QueryNode* left = nodes.back(); // get left node
		nodes.pop_back();
		QueryNode* right = nodes.back(); // get right node
		nodes.pop_back();


		AndList* pushed;
		JoinQueryNode* newJoinQueryNode = new JoinQueryNode(boolean, pushed, left, right, statistics); // create join node
		ConcatenateAndList(used, pushed);

		nodes.push_back(newJoinQueryNode);
	}
	root = nodes.front(); // update root
}

void QueryPlan::LoadMinCostJoin() {
	std::vector<QueryNode*> operands(nodes); // init operands
	sort(operands.begin(), operands.end()); // sort them
	int minCost = INT_MAX, actualCost;
	do {           
	  	if ((actualCost=EstimateJoinPermutationCost(operands, *statistics, minCost))<minCost && actualCost>0) { // enumerate all permutations and find min cost join
	  		minCost = actualCost; nodes = operands; 
	  	}
  	} while (next_permutation(operands.begin(), operands.end())); // get next permutation
}

int QueryPlan::EstimateJoinPermutationCost(std::vector<QueryNode*> operands, Statistics st, int bestFound) {  // intentional copy
  	std::vector<JoinQueryNode*> copyOfJoinNodes;  // all new nodes made in this simulation; need to be freed
  	AndList* recycler = NULL;         // AndList needs recycling
  	while (operands.size()>1) {       // simulate join
	  	
	  	QueryNode* left = operands.back(); // get left node
		operands.pop_back();
		QueryNode* right = operands.back(); // get right node
		operands.pop_back();

	  	AndList* pushed;
		JoinQueryNode* newJoinQueryNode = new JoinQueryNode(boolean, pushed, left, right, &st); // create join node
		ConcatenateAndList(recycler, pushed);

	  	operands.push_back(newJoinQueryNode); // add this join node
	  	copyOfJoinNodes.push_back(newJoinQueryNode); // duplicate this join node

		if (newJoinQueryNode->estimatedCost<=0 || newJoinQueryNode->actualCost > bestFound) {
			break; // take the new join if its better than the previous or skip this join if cost<0
		}
	}
	int actualCost = operands.back()->actualCost;

	for (int i = 0; i < copyOfJoinNodes.size(); ++i) { // free the copy
		--copyOfJoinNodes[i]->pipeId;
		free(copyOfJoinNodes[i]);  // free pipeIds
	}

	ConcatenateAndList(boolean, recycler);   // add the AND list so that it can be used later
  	return operands.back()->estimatedCost<0 ? -1 : actualCost; // return actual cost
}

//*************************************************************************************************
// SUM
void QueryPlan::CreateSumQueryNodes() {
	if (groupingAtts) {
		if(!finalFunction){
			cout<<"Error: GroupBy without aggregate function"<<endl;
			exit(EXIT_FAILURE);
		}

		if(distinctAtts){
			cout<<"Error: No Distinct after aggregation"<<endl;
			exit(EXIT_FAILURE);
		}

		if (distinctFunc) {
			root = new DistinctQueryNode(root); // update root
		}
		root = new GroupByQueryNode(groupingAtts, finalFunction, root);
	} else if (finalFunction) {
		root = new SumQueryNode(finalFunction, root); // update root
	}
}

//*************************************************************************************************
// Distinct
void QueryPlan::CreateDistinctQueryNodes() {
	if (distinctAtts) root = new DistinctQueryNode(root);
}

//*************************************************************************************************
// Writeout
void QueryPlan::CreateWriteOutQueryNodes() {
	root = new WriteOutQueryNode(outputFile, root);
}

// Utils
void QueryPlan::ConcatenateAndList(AndList*& left, AndList*& right) {
	if (!left) { swap(left, right); return; }
	AndList *pre = left, *cur = left->rightAnd;
	for (; cur; pre = cur, cur = cur->rightAnd);
		pre->rightAnd = right;
	right = NULL;
}

/**************************************************************************************************
QUERY PLAN NODE INITIALISATION
**************************************************************************************************/
//*************************************************************************************************
// QueryNode
int QueryNode::pipeId = 0; // default pipe id

QueryNode::QueryNode(const std::string& op, Schema* out, Statistics* st): opName(op), outputSchema(out), numRels(0), estimatedCost(0), actualCost(0), statistics(st), outputPipeId(pipeId++) {}

QueryNode::QueryNode(const std::string& op, Schema* out, char* rName, Statistics* st): opName(op), outputSchema(out), numRels(0), estimatedCost(0), actualCost(0), statistics(st), outputPipeId(pipeId++) {
	if (rName) {
		relNames[numRels++] = strdup(rName);
	}
}

QueryNode::QueryNode(const std::string& op, Schema* out, char* rNames[], int num, Statistics* st): opName(op), outputSchema(out), numRels(0), estimatedCost(0), actualCost(0), statistics(st), outputPipeId(pipeId++) {
	for (; numRels<num; ++numRels){
		relNames[numRels] = strdup(rNames[numRels]);
	}
}

QueryNode::~QueryNode() {
	// delete outputSchema;
	for (int i=0; i<numRels; ++i){
		delete[] relNames[i];
	}
}

// Move selection down if possible
AndList* QueryNode::MoveSelectionDown(AndList*& alist, Schema* target) {
	AndList head; head.rightAnd = alist;  // make a list header to

	AndList *cur = alist, *pre = &head, *result = NULL;
	for (; cur; cur = pre->rightAnd)
	if (DoesSchemaContainOr(cur->left, target)) {   // should push
		pre->rightAnd = cur->rightAnd;
		cur->rightAnd = result;        // set right child as the result
		result = cur;        // prepend new node to result
	} else {
		pre = cur;
	}
	alist = head.rightAnd; // why? head is just a dummy node
	return result;
}

// does the schema contain Or?
bool QueryNode::DoesSchemaContainOr(OrList* orlist, Schema* target) {
	for (; orlist; orlist=orlist->rightOr){
		if (!DoesSchemaContainComparisonOp(orlist->left, target)) {
			return false;
		}
	}
	return true;
}

// does the schema contain ComparisonOp?
bool QueryNode::DoesSchemaContainComparisonOp(ComparisonOp* comparisonOp, Schema* target) {
	Operand *left = comparisonOp->left, *right = comparisonOp->right;
	bool result = target->Find(right->value);
	return (left->code!=NAME || target->Find(left->value)!=-1) &&
	(right->code!=NAME || target->Find(right->value)!=-1);
}

//*************************************************************************************************
// LeafQueryNode
LeafQueryNode::LeafQueryNode(AndList*& boolean, AndList*& pushed, char* relName, char* alias, Statistics* st): QueryNode("SELECT FILE", new Schema(catalog_path, relName, alias), relName, st), isDBfileOpen(false) {
	pushed = MoveSelectionDown(boolean, outputSchema);
	estimatedCost = statistics->Estimate(pushed, relNames, numRels);
	statistics->Apply(pushed, relNames, numRels);
	selOp.GrowFromParseTree(pushed, outputSchema, literal);
}

//*************************************************************************************************
// OnePipeQueryNode
OnePipeQueryNode::OnePipeQueryNode(const std::string& opName, Schema* out, QueryNode* node, Statistics* st): QueryNode (opName, out, node->relNames, node->numRels, st), child(node), inputPipeId(node->outputPipeId) {}

//*************************************************************************************************
// TwoPipeQueryNode
TwoPipeQueryNode::TwoPipeQueryNode(const std::string& opName, QueryNode* l, QueryNode* r, Statistics* st): QueryNode (opName, new Schema(*l->outputSchema, *r->outputSchema), st),
leftChild(l), rightChild(r), leftInputPipeId(leftChild->outputPipeId), rightInputPipeId(rightChild->outputPipeId) {
	for (int i=0; i<l->numRels;){
		relNames[numRels++] = strdup(l->relNames[i++]);
	}
	for (int j=0; j<r->numRels;){
		relNames[numRels++] = strdup(r->relNames[j++]);
	}
}

//*************************************************************************************************
// Project
ProjectQueryNode::ProjectQueryNode(NameList* atts, QueryNode* node): OnePipeQueryNode("PROJECT", NULL, node, NULL), numInputAttributes(node->outputSchema->GetNumAtts()), numOutputAttributes(0) {
	Schema* cSchema = node->outputSchema;
	Attribute resultAtts[MAX_ATTS];

	if(cSchema->GetNumAtts()>MAX_ATTS){
		cout<<"Error: Too many attributes"<<endl; 
		exit(EXIT_FAILURE);   
	}
	for (; atts; atts=atts->next, numOutputAttributes++) {
		if((keepMe[numOutputAttributes]=cSchema->Find(atts->name)) == -1){
			cout<<"Error: Project Attribute not found"<<endl;
			exit(EXIT_FAILURE);
		}

		resultAtts[numOutputAttributes].name = atts->name; // set attribute name
		resultAtts[numOutputAttributes].myType = cSchema->FindType(atts->name); // set attribute type
	}
	outputSchema = new Schema ("", numOutputAttributes, resultAtts); // create output schema
}

//*************************************************************************************************
// Distinct
DistinctQueryNode::DistinctQueryNode(QueryNode* node): OnePipeQueryNode("DISTINCT", new Schema(*node->outputSchema), node, NULL), orderMakerDistinct(node->outputSchema) {}

//*************************************************************************************************
// Join
JoinQueryNode::JoinQueryNode(AndList*& boolean, AndList*& pushed, QueryNode* l, QueryNode* r, Statistics* st): TwoPipeQueryNode("JOIN", l, r, st) {
	pushed = MoveSelectionDown(boolean, outputSchema);
	estimatedCost = statistics->Estimate(pushed, relNames, numRels);
	statistics->Apply(pushed, relNames, numRels);
	actualCost = l->actualCost + estimatedCost + r->actualCost; // add up the costs
	selOp.GrowFromParseTree(pushed, l->outputSchema, r->outputSchema, literal);
}

//*************************************************************************************************
// Sum
SumQueryNode::SumQueryNode(FuncOperator* parseTree, QueryNode* node): OnePipeQueryNode("SUM", resultSchema(parseTree, node), node, NULL) {
	function.GrowFromParseTree (parseTree, *node->outputSchema);
}

Schema* SumQueryNode::resultSchema(FuncOperator* parseTree, QueryNode* node) {
	Function fun;
	Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
	fun.GrowFromParseTree (parseTree, *node->outputSchema);
	return new Schema ("", 1, atts[fun.GetReturnsIntType()]); // return new schema
}
//*************************************************************************************************
// GroupBy
GroupByQueryNode::GroupByQueryNode(NameList* groupingAttributes, FuncOperator* parseTree, QueryNode* node): OnePipeQueryNode("GROUPBY", resultSchema(groupingAttributes, parseTree, node), node, NULL) {
	orderMakerGroupBy.growFromParseTree(groupingAttributes, node->outputSchema);
	function.GrowFromParseTree (parseTree, *node->outputSchema);
}

Schema* GroupByQueryNode::resultSchema(NameList* groupingAttributes, FuncOperator* parseTree, QueryNode* node) {
	Function fun;
	Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
	Schema* cSchema = node->outputSchema;
	fun.GrowFromParseTree (parseTree, *cSchema);
	Attribute resultAtts[MAX_ATTS];

	resultAtts[0].name = "sum";
	resultAtts[0].myType = fun.GetReturnsIntType();
	int numOutputAttributes = 1;
	for (; groupingAttributes; groupingAttributes=groupingAttributes->next, numOutputAttributes++) {
		resultAtts[numOutputAttributes].name = groupingAttributes->name; // set attribute name
		resultAtts[numOutputAttributes].myType = cSchema->FindType(groupingAttributes->name); // set attribute type
	}
	return new Schema ("", numOutputAttributes, resultAtts);
}

//*************************************************************************************************
// Writeout
WriteOutQueryNode::WriteOutQueryNode(FILE*& out, QueryNode* node): OnePipeQueryNode("WRITEOUT(PROJECT)", new Schema(*node->outputSchema), node, NULL), outputFile(out) {}


/**************************************************************************************************
QUERY EXECUTION
**************************************************************************************************/
//*************************************************************************************************
// Leaf
void LeafQueryNode::Execute(Pipe** pipes, RelationalOp** relops) {
	std::string dbName = std::string(relNames[0]) + ".bin";
	dbfile.Open((char*)dbName.c_str()); isDBfileOpen = true;
	SelectFile* sf = new SelectFile();

	pipes[outputPipeId] = new Pipe(PIPE_SIZE);
	relops[outputPipeId] = sf;
	sf -> Run(dbfile, *pipes[outputPipeId], selOp, literal);
}

//*************************************************************************************************
// Project
void ProjectQueryNode::Execute(Pipe** pipes, RelationalOp** relops) {
	child -> Execute(pipes, relops);
	Project* p = new Project();

	pipes[outputPipeId] = new Pipe(PIPE_SIZE);
	relops[outputPipeId] = p;
	p -> Run(*pipes[inputPipeId], *pipes[outputPipeId], keepMe, numInputAttributes, numOutputAttributes);
}

//*************************************************************************************************
// Distinct
void DistinctQueryNode::Execute(Pipe** pipes, RelationalOp** relops) {
	child -> Execute(pipes, relops);
	DuplicateRemoval* dedup = new DuplicateRemoval();
	pipes[outputPipeId] = new Pipe(PIPE_SIZE);
	relops[outputPipeId] = dedup;
	dedup -> Run(*pipes[inputPipeId], *pipes[outputPipeId], *outputSchema);
}

//*************************************************************************************************
// Sum
void SumQueryNode::Execute(Pipe** pipes, RelationalOp** relops) {
	child -> Execute(pipes, relops);
	Sum* s = new Sum();
	pipes[outputPipeId] = new Pipe(PIPE_SIZE);
	relops[outputPipeId] = s;
	s -> Run(*pipes[inputPipeId], *pipes[outputPipeId], function);
}

//*************************************************************************************************
// GroupBy
void GroupByQueryNode::Execute(Pipe** pipes, RelationalOp** relops) {
	child -> Execute(pipes, relops);
	GroupBy* grp = new GroupBy();
	pipes[outputPipeId] = new Pipe(PIPE_SIZE);
	relops[outputPipeId] = grp;
	grp -> Run(*pipes[inputPipeId], *pipes[outputPipeId], orderMakerGroupBy, function);
}

//*************************************************************************************************
// Join
void JoinQueryNode::Execute(Pipe** pipes, RelationalOp** relops) {
	leftChild -> Execute(pipes, relops); rightChild -> Execute(pipes, relops);
	Join* j = new Join();
	pipes[outputPipeId] = new Pipe(PIPE_SIZE);
	relops[outputPipeId] = j;
	j -> Run(*pipes[leftInputPipeId], *pipes[rightInputPipeId], *pipes[outputPipeId], selOp, literal);
}

//*************************************************************************************************
// Writeout
void WriteOutQueryNode::Execute(Pipe** pipes, RelationalOp** relops) {
	child -> Execute(pipes, relops);
	WriteOut* w = new WriteOut();
	pipes[outputPipeId] = new Pipe(PIPE_SIZE);
	relops[outputPipeId] = w;
	w -> Run(*pipes[inputPipeId], outputFile, *outputSchema);

}

/**************************************************************************************************
PRINT
**************************************************************************************************/
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

// Order in which the operator related information should  be printed
void QueryNode::Print(std::ostream& os, int level) const {
	PrintOperator(os, level);
	PrintOperatorInfo(os, level);
	PrintSchema(os, level);
	PrintPipe(os, level);
	PrintChildren(os, level);
}

// Print Operator
void QueryNode::PrintOperator(std::ostream& os, int level) const {
	os << IndentOperator(level) << opName << ": ";
}

void QueryNode::PrintSchema(std::ostream& os, int level) const {
	os << IndentOperatorInfo(level) << "Output schema:" << endl;
	outputSchema->Print(os);
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

// Print Operator Info
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