#include <iostream>
#include "ParseTree.h"
#include "Interface.h"
#include "QueryPlan.h"
#include "DataDefinitionLanguage.h"
#include <gtest/gtest.h>

// using namespace std;

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
extern char* deoutput;
extern struct AttrList *newattrs; //Use this to build Attribute* array and record schema
extern struct NameList *sortattrs;

char* catalog_path = "catalog";
char* dbfile_dir = "";
char* tpch_dir = "../../data/tpch-1gb";

TEST(QueryExecutionTest, CreateTableTest){
	DataDefinitionLanguage d;

	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
	}

	if (d.Create()) {
		ifstream dbfile(string(newtable)+".bin");
		ifstream metafile(string(newtable)+".bin.meta");
		ASSERT_TRUE(dbfile.good() && metafile.good()) << "Either dbfile or metafile wasn't created";
	} else{ 
		cout << "CreateError: Table " << newtable << " already exists" << std::endl;
	}
}

TEST(QueryExecutionTest, DropTableTest){
	DataDefinitionLanguage d;

	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
	}

	if (d.Drop()) {

		ifstream dbfile(string(oldtable)+".bin");
		ifstream metafile(string(oldtable)+".bin.meta");
		ASSERT_FALSE(dbfile.good() || metafile.good()) << "Either dbfile or metafile wasn't deleted";
	} else{ 
		cout << "DropError: Table" << oldtable << " does not exist." << std::endl;
	}	
}




int main(int argc, char **argv){
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}