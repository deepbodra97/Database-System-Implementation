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


char* catalog_path = "catalog";
char* dbfile_dir = "";
char* tpch_dir = "../../data/tpch-1gb";

TEST(QueryPlanTest, OutputSchemaNumAttributeTest){
	char *fileName = "Statistics.txt"; // statistics file name
	Statistics s;
	DataDefinitionLanguage d;
	QueryPlan plan(&s);
	s.Read(fileName);

	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
	}

	plan.Plan();
	ASSERT_EQ(plan.GetRoot()->GetOutputSchema()->GetNumAtts(), 2);
}

TEST(QueryPlanTest, NumberOfTablesTest){
	char *fileName = "Statistics.txt"; // statistics file name
	Statistics s;
	DataDefinitionLanguage d;
	QueryPlan plan(&s);
	s.Read(fileName);

	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
	}
	plan.CreateLeaves();

	int numTables = 0;
	for(TableList *t = tables; t; t=t->next){
		numTables++;
	}

	ASSERT_EQ(plan.GetNodes().size(), numTables);
}


int main(int argc, char **argv){
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}