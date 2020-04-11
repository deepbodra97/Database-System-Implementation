#include <iostream>
#include "ParseTree.h"
#include "Interface.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern TableList* tables;

int PrintTables (){

	TableList *t = tables;
	while(t!=0){
		cout<<t->tableName<<endl;
		t = t->next;
	}
}

void Interface::Run() {
	char *statFileName = "Statistics.txt";
    Statistics s;
	while(true) {
	    cout << "DeepDB> ";
	    if (yyparse() != 0) {
	    	cout << "Error parsing CNF"<<endl;
	    	continue;
	    }

	    s.Read(statFileName);
	    cout<<"Statistics.txt has been loaded"<<endl;
	    PrintTables();
  	}
}