#include <iostream>
#include <stdlib.h>
#include "Statistics.h"
#include "ParseTree.h"
#include <math.h>
#include <gtest/gtest.h>

extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct AndList *final;

using namespace std;

TEST(AddRelationTest, InsertionTest){
	Statistics s;
	s.AddRel("nation", 25);

	ASSERT_NE(s.statMap.find("nation"), s.statMap.end()); // check if key="nation" exists
	ASSERT_EQ(s.statMap["nation"]->numTuples, 25); // check if value=25 is saved correctly
}

TEST(AddAttributeTest, InsertionTest){
	Statistics s;
	s.AddRel("nation", 25);
	s.AddAtt("nation", "n_nationkey", -1);

	ASSERT_NE(s.statMap["nation"]->attributes.find("n_nationkey"), s.statMap["nation"]->attributes.end()); // check if key="n_nationkey" exists
	ASSERT_EQ(s.statMap["nation"]->attributes["n_nationkey"], 25); // check if value=25 is saved correctly
}

TEST(GetRelationTest, ReturnValueTest){
	Statistics s;
	s.AddRel("nation", 25);
	s.AddAtt("nation", "n_nationkey", -1);

	s.AddRel("region", 10);
	s.AddAtt("region", "r_regionkey", -1);

	ASSERT_EQ(s.GetRelation("n_nationkey"), "nation"); // check if attribute=n_nationkey is under the relation nation
	ASSERT_EQ(s.GetRelation("r_regionkey"), "region"); // check if attribute=r_regionkey is under the relation region
}

TEST(Query4Test, EstimationTest){
	Statistics s;
    char *relName[] = { "part", "partsupp", "supplier", "nation", "region", "p", "ps", "s", "n", "r"};

	s.AddRel(relName[0], 200000);
	s.AddAtt(relName[0], "p_partkey",200000);
	s.AddAtt(relName[0], "p_size",50);

	s.AddRel(relName[1], 800000);
	s.AddAtt(relName[1], "ps_suppkey",10000);
	s.AddAtt(relName[1], "ps_partkey", 200000);
	
	s.AddRel(relName[2], 10000);
	s.AddAtt(relName[2], "s_suppkey",10000);
	s.AddAtt(relName[2], "s_nationkey",25);
	
	s.AddRel(relName[3], 25);
	s.AddAtt(relName[3], "n_nationkey",25);
	s.AddAtt(relName[3], "n_regionkey",5);

	s.AddRel(relName[4],5);
	s.AddAtt(relName[4], "r_regionkey",5);
	s.AddAtt(relName[4], "r_name",5);

	s.CopyRel("part","p");
	s.CopyRel("partsupp","ps");
	s.CopyRel("supplier","s");
	s.CopyRel("nation","n");
	s.CopyRel("region","r");
	char *cnf = "(p.p_partkey=ps.ps_partkey) AND (p.p_size = 2)";
	yy_scan_string(cnf);
	yyparse();
	s.Apply(final, relName, 2);

	cnf ="(s.s_suppkey = ps.ps_suppkey)";
	yy_scan_string(cnf);
	yyparse();
	s.Apply(final, relName, 3);

	cnf =" (s.s_nationkey = n.n_nationkey)";
	yy_scan_string(cnf);
	yyparse();
	s.Apply(final, relName, 4);

	cnf ="(n.n_regionkey = r.r_regionkey) AND (r.r_name = 'AMERICA') ";
	yy_scan_string(cnf);
	yyparse();

	double result = s.Estimate(final, relName, 5);
	ASSERT_LT(fabs(result-3200), 0.1);
}

int main(int argc, char **argv){
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}