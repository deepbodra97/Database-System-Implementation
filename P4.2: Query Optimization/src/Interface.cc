#include <iostream>
#include "ParseTree.h"
#include "Interface.h"
#include "Statistics.h"
#include "QueryPlan.h"
// #include "Ddl.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query


extern char* newtable;
extern char* oldtable;
extern char* newfile;
extern char* deoutput;
extern struct AttrList *newattrs; //Use this to build Attribute* array and record schema


int PrintTables (){
	TableList *t = tables;
	while(t!=0){
		cout<<t->tableName<<endl;
		t = t->next;
	}
}

void Interface::Run() {
	/*char *statFileName = "Statistics.txt";
    Statistics s;
	while(true) {
	    cout << "DeepDB> ";
	    if (yyparse() != 0) {
	    	cout << "Error parsing CNF"<<endl;
	    	continue;
	    }

	    s.Read(statFileName);
	    cout<<"Statistics.txt has been loaded"<<endl;
	    
	    cout<<"-----Tables in the Query-----"<<endl;
	    PrintTables();



  	}*/
  	char *fileName = "Statistics.txt";
  /*  Statistics s;
  Ddl d; 
  QueryPlan plan(&s); */
  Statistics s;
  // Ddl d; 
  QueryPlan plan(&s);
  cout<<"plan(): Done"<<endl;
  // while(true) {
    cout << "DeepDB> ";
    if (yyparse() != 0) {
      cout << "Can't parse your CNF.\n";
      // continue;
    }
    cout<<"CNF is correct"<<endl;
    s.Read(fileName);
    cout<<"Statistics.txt has been loaded"<<endl;
    // PrintTables();
    /*if (newtable) {
      if (d.createTable()) cout << "Create table " << newtable << std::endl;
      else cout << "Table " << newtable << " already exists." << std::endl;
    } else if (oldtable && newfile) {
      if (d.insertInto()) cout << "Insert into " << oldtable << std::endl;
      else cout << "Insert failed." << std::endl;
    } else if (oldtable && !newfile) {
      if (d.dropTable()) cout << "Drop table " << oldtable << std::endl;
      else cout << "Table " << oldtable << " does not exist." << std::endl;
    } else if (deoutput) {
      plan.setOutput(deoutput);
    } else if (attsToSelect || finalFunction) {*/
      plan.SetOutput("output.txt");
      plan.Plan();
      plan.Print();
      // plan.Execute();
    // }
    // Clear();
  // }
}

void Interface::Clear() {
  newattrs = NULL;
  finalFunction = NULL;
  tables = NULL;
  boolean = NULL;
  groupingAtts = NULL;
  attsToSelect = NULL;
  newtable = oldtable = newfile = deoutput = NULL;
  distinctAtts = distinctFunc = 0;
  !remove ("*.tmp");
}