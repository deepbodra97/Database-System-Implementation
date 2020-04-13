#include <cstring>
#include <climits>
#include <string>
#include <algorithm>

#include "Defs.h"
// #include "Errors.h"
#include "Stl.h"
#include "QueryPlan.h"
#include "Pipe.h"
#include "RelOp.h"

// #define _OUTPUT_SCHEMA__

#define popVector(vel, el1, el2)                \
  QueryNode* el1 = vel.back();                  \
  vel.pop_back();                               \
  QueryNode* el2 = vel.back();                  \
  vel.pop_back();

#define makeNode(pushed, recycler, nodeType, newNode, params)           \
  AndList* pushed;                                                      \
  nodeType* newNode = new nodeType params;                              \
  concatList(recycler, pushed);

#define freeAll(freeList)                                        \
  for (size_t __ii = 0; __ii < freeList.size(); ++__ii) {        \
    --freeList[__ii]->pipeId; free(freeList[__ii]);  } // recycler pipeIds but do not free children

#define makeAttr(newAttr, name1, type1)                 \
  newAttr.name = name1; newAttr.myType = type1;

#define indent(level) (string(3*(level), ' ') + "-> ")
#define annot(level) (string(3*(level+1), ' ') + "* ")

using std::endl;
using std::string;

extern char* catalog_path;
extern char* dbfile_dir;
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
QueryPlan::QueryPlan(Statistics* st): root(NULL), outName("STDOUT"), stat(st), used(NULL) {}

void QueryPlan::plan() {
  makeLeafs();  // these nodes read from file
  makeJoins();
  makeSums();
  makeProjects();
  makeDistinct();
  makeWrite();

  // clean up
  swap(boolean, used);
  // FATALIF(used, "WHERE clause syntax error.");
}

void QueryPlan::print(std::ostream& os) const {
  root->print(os);
}

void QueryPlan::setOutput(char* out) {
  outName = out;
}

void QueryPlan::execute() {
  cout<<"outName:"<<outName<<"stdout:"<<stdout<<endl;
  outFile = (outName == "STDOUT" ? stdout
    : outName == "NONE" ? NULL
    : fopen(outName.c_str(), "a"));   // closed by query executor
  if (outFile) {
    cout<<"outFile:"<<outFile<<endl;
    int numNodes = root->pipeId;
    Pipe** pipes = new Pipe*[numNodes];
    RelationalOp** relops = new RelationalOp*[numNodes];
    root->execute(pipes, relops);
    for (int i=0; i<numNodes; ++i)
      relops[i] -> WaitUntilDone();
    for (int i=0; i<numNodes; ++i) {
      delete pipes[i]; delete relops[i];
    }
    delete[] pipes; delete[] relops;
    if (outFile!=stdout) fclose(outFile);
  }
  root->pipeId = 0;
  delete root; root = NULL;
  nodes.clear();
}


/**********************************************************************
 * Query optimization                                                 *
 **********************************************************************/
void QueryPlan::makeLeafs() {
  for (TableList* table = tables; table; table = table->next) {
    cout<<"makeLeafs:"<<table->tableName<<endl;
    // PrintTable();
    stat->CopyRel(table->tableName, table->aliasAs);
    makeNode(pushed, used, LeafNode, newLeaf, (boolean, pushed, table->tableName, table->aliasAs, stat));
    nodes.push_back(newLeaf);
    cout<<"here"<<endl;
  }
}

void QueryPlan::makeJoins() {
  orderJoins();
  while (nodes.size()>1) {
    popVector(nodes, left, right);
    makeNode(pushed, used, JoinNode, newJoinNode, (boolean, pushed, left, right, stat));
    nodes.push_back(newJoinNode);
  }
  root = nodes.front();
}

void QueryPlan::makeSums() {
  if (groupingAtts) {
    // FATALIF (!finalFunction, "Grouping without aggregation functions!");
    // FATALIF (distinctAtts, "No dedup after aggregate!");
    if (distinctFunc) root = new DedupNode(root);
    root = new GroupByNode(groupingAtts, finalFunction, root);
  } else if (finalFunction) {
    root = new SumNode(finalFunction, root);
  }
}

void QueryPlan::makeProjects() {
  if (attsToSelect && !finalFunction && !groupingAtts) {
    root = new ProjectNode(attsToSelect, root);
  }
}

void QueryPlan::makeDistinct() {
  if (distinctAtts) root = new DedupNode(root);
}

void QueryPlan::makeWrite() {
  root = new WriteNode(outFile, root);
}

void QueryPlan::orderJoins() {
  std::vector<QueryNode*> operands(nodes);
  sort(operands.begin(), operands.end());
  int minCost = INT_MAX, cost;
  do {           // traverse all possible permutations
    if ((cost=evalOrder(operands, *stat, minCost))<minCost && cost>0) {
      minCost = cost; nodes = operands; 
    }
  } while (next_permutation(operands.begin(), operands.end()));
}

int QueryPlan::evalOrder(std::vector<QueryNode*> operands, Statistics st, int bestFound) {  // intentional copy
  std::vector<JoinNode*> freeList;  // all new nodes made in this simulation; need to be freed
  AndList* recycler = NULL;         // AndList needs recycling
  while (operands.size()>1) {       // simulate join
    popVector(operands, left, right);
    makeNode(pushed, recycler, JoinNode, newJoinNode, (boolean, pushed, left, right, &st));
    operands.push_back(newJoinNode);
    freeList.push_back(newJoinNode);
    if (newJoinNode->estimate<=0 || newJoinNode->cost>bestFound) break;  // branch and bound
  }
  int cost = operands.back()->cost;
  freeAll(freeList);
  concatList(boolean, recycler);   // put the AndLists back for future use
  return operands.back()->estimate<0 ? -1 : cost;
}

void QueryPlan::concatList(AndList*& left, AndList*& right) {
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
  opName(op), outSchema(out), numRels(0), estimate(0), cost(0), stat(st), pout(pipeId++) {}

QueryNode::QueryNode(const std::string& op, Schema* out, char* rName, Statistics* st):
  opName(op), outSchema(out), numRels(0), estimate(0), cost(0), stat(st), pout(pipeId++) {
  if (rName) relNames[numRels++] = strdup(rName);
}

QueryNode::QueryNode(const std::string& op, Schema* out, char* rNames[], size_t num, Statistics* st):
  opName(op), outSchema(out), numRels(0), estimate(0), cost(0), stat(st), pout(pipeId++) {
  for (; numRels<num; ++numRels)
    relNames[numRels] = strdup(rNames[numRels]);
}

QueryNode::~QueryNode() {
  delete outSchema;
  for (size_t i=0; i<numRels; ++i)
    delete[] relNames[i];
}

AndList* QueryNode::pushSelection(AndList*& alist, Schema* target) {
  AndList header; header.rightAnd = alist;  // make a list header to
  // avoid handling special cases deleting the first list element
  // cout<<"alist:"<<alist<<endl;
  AndList *cur = alist, *pre = &header, *result = NULL;
  for (; cur; cur = pre->rightAnd)
    if (containedIn(cur->left, target)) {   // should push
      pre->rightAnd = cur->rightAnd;
      cur->rightAnd = result;        // *move* the node to the result list
      result = cur;        // prepend the new node to result list
    } else pre = cur;
  alist = header.rightAnd;  // special case: first element moved
  // cout<<"result:"<<result<<endl;
  return result;
}

bool QueryNode::containedIn(OrList* ors, Schema* target) {
  cout<<"containedIn:ors"<<endl;
  for (; ors; ors=ors->rightOr)
    if (!containedIn(ors->left, target)) return false;
  return true;
}

bool QueryNode::containedIn(ComparisonOp* cmp, Schema* target) {
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

LeafNode::LeafNode(AndList*& boolean, AndList*& pushed, char* relName, char* alias, Statistics* st):
  QueryNode("Select File", new Schema(catalog_path, relName, alias), relName, st), opened(false) {
  pushed = pushSelection(boolean, outSchema);
  cout<<"pushed:"<<pushed<<endl;
  estimate = stat->Estimate(pushed, relNames, numRels);
  stat->Apply(pushed, relNames, numRels);
  selOp.GrowFromParseTree(pushed, outSchema, literal);
}

UnaryNode::UnaryNode(const std::string& opName, Schema* out, QueryNode* c, Statistics* st):
  QueryNode (opName, out, c->relNames, c->numRels, st), child(c), pin(c->pout) {}

BinaryNode::BinaryNode(const std::string& opName, QueryNode* l, QueryNode* r, Statistics* st):
  QueryNode (opName, new Schema(*l->outSchema, *r->outSchema), st),
  left(l), right(r), pleft(left->pout), pright(right->pout) {
  cout<<"BinaryNode Schema:"<<endl;
  outSchema->print();
  for (size_t i=0; i<l->numRels;)
    relNames[numRels++] = strdup(l->relNames[i++]);
  for (size_t j=0; j<r->numRels;)
    relNames[numRels++] = strdup(r->relNames[j++]);
  // stat->numRels = l->numRels+r->numRels;
}

ProjectNode::ProjectNode(NameList* atts, QueryNode* c):
  UnaryNode("Project", NULL, c, NULL), numAttsIn(c->outSchema->GetNumAtts()), numAttsOut(0) {
  Schema* cSchema = c->outSchema;
  Attribute resultAtts[MAX_ATTS];
  // FATALIF (cSchema->GetNumAtts()>MAX_ATTS, "Too many attributes.");
  if(cSchema->GetNumAtts()>MAX_ATTS){
    cout<<"Error: Too many attributes"<<endl; 
     exit(EXIT_FAILURE);   
  }
  for (; atts; atts=atts->next, numAttsOut++) {
    cout<<"ProjectNode: atts->name="<<atts->name<<","<<cSchema->Find(atts->name)<<endl;
    if((keepMe[numAttsOut]=cSchema->Find(atts->name)) == -1){
      cout<<"Error: Attribute not found"<<endl;
      exit(EXIT_FAILURE);
    }
    cout<<"keepMe verify: "<<keepMe[numAttsOut]<<endl;
    // FATALIF ((keepMe[numAttsOut]=cSchema->Find(atts->name))==-1,
             // "Projecting non-existing attribute.");
    makeAttr(resultAtts[numAttsOut], atts->name, cSchema->FindType(atts->name));
  }
  outSchema = new Schema ("", numAttsOut, resultAtts);
}

DedupNode::DedupNode(QueryNode* c):
  UnaryNode("Deduplication", new Schema(*c->outSchema), c, NULL), dedupOrder(c->outSchema) {}

JoinNode::JoinNode(AndList*& boolean, AndList*& pushed, QueryNode* l, QueryNode* r, Statistics* st):
  BinaryNode("Join", l, r, st) {
  pushed = pushSelection(boolean, outSchema);
  estimate = stat->Estimate(pushed, relNames, numRels);
  stat->Apply(pushed, relNames, numRels);
  cost = l->cost + estimate + r->cost;
  cout<<"JoinNode:cost="<<cost<<endl;
  selOp.GrowFromParseTree(pushed, l->outSchema, r->outSchema, literal);
}

SumNode::SumNode(FuncOperator* parseTree, QueryNode* c):
  UnaryNode("Sum", resultSchema(parseTree, c), c, NULL) {
  f.GrowFromParseTree (parseTree, *c->outSchema);
}

Schema* SumNode::resultSchema(FuncOperator* parseTree, QueryNode* c) {
  Function fun;
  Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
  fun.GrowFromParseTree (parseTree, *c->outSchema);
  return new Schema ("", 1, atts[fun.GetReturnsIntType()]);
}

GroupByNode::GroupByNode(NameList* gAtts, FuncOperator* parseTree, QueryNode* c):
  UnaryNode("Group by", resultSchema(gAtts, parseTree, c), c, NULL) {
  grpOrder.growFromParseTree(gAtts, c->outSchema);
  f.GrowFromParseTree (parseTree, *c->outSchema);
}

Schema* GroupByNode::resultSchema(NameList* gAtts, FuncOperator* parseTree, QueryNode* c) {
  Function fun;
  Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
  Schema* cSchema = c->outSchema;
  fun.GrowFromParseTree (parseTree, *cSchema);
  Attribute resultAtts[MAX_ATTS];
  // FATALIF (1+cSchema->GetNumAtts()>MAX_ATTS, "Too many attributes.");
  makeAttr(resultAtts[0], "sum", fun.GetReturnsIntType());
  int numAtts = 1;
  for (; gAtts; gAtts=gAtts->next, numAtts++) {
    // FATALIF (cSchema->Find(gAtts->name)==-1, "Grouping by non-existing attribute.");
    makeAttr(resultAtts[numAtts], gAtts->name, cSchema->FindType(gAtts->name));
  }
  return new Schema ("", numAtts, resultAtts);
}

WriteNode::WriteNode(FILE*& out, QueryNode* c):
  UnaryNode("WriteOut", new Schema(*c->outSchema), c, NULL), outFile(out) {
    // cout<<"WriteNodeSchema:"<<endl;
    // c->outSchema->print();
  }


/**********************************************************************
 * Query execution                                                    *
 **********************************************************************/
void LeafNode::execute(Pipe** pipes, RelationalOp** relops) {
  std::string dbName = std::string(relNames[0]) + ".bin";
  cout<<"dbName="<<dbName<<" relNames[0]="<<relNames[0]<<endl;
  dbf.Open((char*)dbName.c_str()); opened = true;
  SelectFile* sf = new SelectFile();

  sf->outputSchema = outSchema; // debug

  pipes[pout] = new Pipe(PIPE_SIZE);
  relops[pout] = sf;
  sf -> Run(dbf, *pipes[pout], selOp, literal);
}

void ProjectNode::execute(Pipe** pipes, RelationalOp** relops) {
  child -> execute(pipes, relops);
  Project* p = new Project();

  p->outputSchema = outSchema; // debug
  cout<<"ProjectNode execute: keepMe="<<*keepMe<<endl;

  pipes[pout] = new Pipe(PIPE_SIZE);
  relops[pout] = p;
  p -> Run(*pipes[pin], *pipes[pout], keepMe, numAttsIn, numAttsOut);
}

void DedupNode::execute(Pipe** pipes, RelationalOp** relops) {
  child -> execute(pipes, relops);
  DuplicateRemoval* dedup = new DuplicateRemoval();
  pipes[pout] = new Pipe(PIPE_SIZE);
  relops[pout] = dedup;
  dedup -> Run(*pipes[pin], *pipes[pout], *outSchema);
}

void SumNode::execute(Pipe** pipes, RelationalOp** relops) {
  child -> execute(pipes, relops);
  Sum* s = new Sum();
  pipes[pout] = new Pipe(PIPE_SIZE);
  relops[pout] = s;
  s -> Run(*pipes[pin], *pipes[pout], f);
}

void GroupByNode::execute(Pipe** pipes, RelationalOp** relops) {
  child -> execute(pipes, relops);
  GroupBy* grp = new GroupBy();
  pipes[pout] = new Pipe(PIPE_SIZE);
  relops[pout] = grp;
  grp -> Run(*pipes[pin], *pipes[pout], grpOrder, f);
}

void JoinNode::execute(Pipe** pipes, RelationalOp** relops) {
  // cout<<"JoinNode Schema:"<<endl;
  // outSchema->print();
  left -> execute(pipes, relops); right -> execute(pipes, relops);
  Join* j = new Join();

  j->outputSchema = outSchema;

  pipes[pout] = new Pipe(PIPE_SIZE);
  relops[pout] = j;
  j -> Run(*pipes[pleft], *pipes[pright], *pipes[pout], selOp, literal);
}

void WriteNode::execute(Pipe** pipes, RelationalOp** relops) {
  child -> execute(pipes, relops);
  WriteOut* w = new WriteOut();
  pipes[pout] = new Pipe(PIPE_SIZE);
  relops[pout] = w;
  w -> Run(*pipes[pin], outFile, *outSchema);

}

/**********************************************************************
 * Print utilities                                                    *
 **********************************************************************/
void QueryNode::print(std::ostream& os, size_t level) const {
  printOperator(os, level);
  printAnnot(os, level);
  printSchema(os, level);
  printPipe(os, level);
  printChildren(os, level);
}

void QueryNode::printOperator(std::ostream& os, size_t level) const {
  os << indent(level) << opName << ": ";
}

void QueryNode::printSchema(std::ostream& os, size_t level) const {
// #ifdef _OUTPUT_SCHEMA__
  os << annot(level) << "Output schema:" << endl;
  outSchema->print(os);
// #endif
}

void LeafNode::printPipe(std::ostream& os, size_t level) const {
  os << annot(level) << "Output pipe: " << pout << endl;
}

void UnaryNode::printPipe(std::ostream& os, size_t level) const {
  os << annot(level) << "Output pipe: " << pout << endl;
  os << annot(level) << "Input pipe: " << pin << endl;
}

void BinaryNode::printPipe(std::ostream& os, size_t level) const {
  os << annot(level) << "Output pipe: " << pout << endl;
  os << annot(level) << "Input pipe: " << pleft << ", " << pright << endl;
}

void LeafNode::printOperator(std::ostream& os, size_t level) const {
  os << indent(level) << "Select from " << relNames[0] << ": ";
}

void LeafNode::printAnnot(std::ostream& os, size_t level) const {
  selOp.Print();
}

void ProjectNode::printAnnot(std::ostream& os, size_t level) const {
  os << keepMe[0];
  for (size_t i=1; i<numAttsOut; ++i) os << ',' << keepMe[i];
  os << endl;
  os << annot(level) << numAttsIn << " input attributes; " << numAttsOut << " output attributes" << endl;
}

void JoinNode::printAnnot(std::ostream& os, size_t level) const {
  selOp.Print();
  os << annot(level) << "Estimate = " << estimate << ", Cost = " << cost << endl;
}

void SumNode::printAnnot(std::ostream& os, size_t level) const {
  os << annot(level) << "Function: "; (const_cast<Function*>(&f))->Print();
}

void GroupByNode::printAnnot(std::ostream& os, size_t level) const {
  os << annot(level) << "OrderMaker: "; (const_cast<OrderMaker*>(&grpOrder))->Print();
  os << annot(level) << "Function: "; (const_cast<Function*>(&f))->Print();
}

void WriteNode::printAnnot(std::ostream& os, size_t level) const {
  os << annot(level) << "Output to " << outFile << endl;
}