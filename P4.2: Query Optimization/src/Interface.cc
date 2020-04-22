#include <iostream>
#include "ParseTree.h"
#include "Interface.h"
#include "Statistics.h"
#include "QueryPlan.h"
#include "DataDefinitionLanguage.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct FuncOperator *finalFunction; // aggregate function
extern struct TableList *tables; // tables and aliases in query
extern struct AndList *boolean; // condition in WHERE in the query
extern struct NameList *groupingAtts; // grouping attributes
extern struct NameList *attsToSelect; // attributes to select
extern int distinctAtts; // 1=DISTINCT for aggregation-less query 
extern int distinctFunc; // 1=DISTINCT for aggregation query


extern char* newtable;
extern char* oldtable;
extern char* newfile;
extern char* outputName;
extern struct AttrList *newattrs; //to create Attribute array and record schema

// Debug only
int PrintTables (){
	TableList *t = tables;
	while(t!=0){
		cout<<t->tableName<<endl;
		t = t->next;
	}
}

void Interface::Run() {
	char *fileName = "Statistics.txt"; // statistics file name
	Statistics s;
	DataDefinitionLanguage d;
	QueryPlan plan(&s);
	while(true) {
		s.Read(fileName);
		cout << "DeepDB> ";
		if (yyparse() != 0) {
			cout << "Can't parse your CNF.\n";
			continue;
		}

		if (newtable) { // create table query
			if (d.Create()) {
				cout << "Table Creation Success: table " << newtable << std::endl;
			}
			else{ 
				cout << "CreateError: Table " << newtable << " already exists" << std::endl;
			}
		} else if (oldtable && newfile) { // insert query
			if (d.Insert()) {
				cout << "Insertion Success: table:" << oldtable << std::endl;
			}
			else{
				cout << "InsertionError" << std::endl;
			}
		} else if (oldtable && !newfile) { // drop table query
			if (d.Drop()) {
				cout << "Drop Table Success " << oldtable << std::endl;
			}
			else cout << "DropError: Table" << oldtable << " does not exist." << std::endl;
		} else if (outputName) { // set output query
			plan.SetOutput(outputName);
		} else if (attsToSelect || finalFunction) {
			plan.Plan();
			plan.Print();
			plan.Execute();
		}
		Clear();
	}
}

void Interface::Clear() {
	newattrs = NULL;
	finalFunction = NULL;
	tables = NULL;
	boolean = NULL;
	groupingAtts = NULL;
	attsToSelect = NULL;
	newtable = oldtable = newfile = outputName = NULL;
	distinctAtts = distinctFunc = 0;
	!remove ("*.tmp");
}