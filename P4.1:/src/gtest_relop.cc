#include "DBFile.h"
#include "BigQ.h"
#include "RelOp.h"
#include "test.h"
#include <fstream>
#include <gtest/gtest.h>
#include <pthread.h>

Attribute IA = {"int", Int};
Attribute SA = {"string", String};
Attribute DA = {"double", Double};

int clear_pipe (Pipe &in_pipe, Schema *schema, bool print) {
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove (&rec)) {
		if (print) {
			rec.Print (schema);
		}
		cnt++;
	}
	return cnt;
}

int clear_pipe (Pipe &in_pipe, Schema *schema, Function &func, bool print) {
	Record rec;
	int cnt = 0;
	double sum = 0;
	while (in_pipe.Remove (&rec)) {
		if (print) {
			rec.Print (schema);
		}
		int ival = 0; double dval = 0;
		func.Apply (rec, ival, dval);
		sum += (ival + dval);
		cnt++;
	}
	cout << " Sum: " << sum << endl;
	return cnt;
}

// int pAtts = 9;
int psAtts = 5;
// int liAtts = 16;
// int oAtts = 9;
int sAtts = 7;
// int cAtts = 8;
int nAtts = 4;
// int rAtts = 3;

void printNHyphen(int n){
	for(int i=0; i<n; i++){
		cout<<"-";
	}
	cout<<endl;
}


// SELECT * FROM nation WHERE n_nationkey<10
// expected number of records in output = 10
TEST(SelectFileTest, NumberOfRecordsTest){
	setup();

	SelectFile SF_n;
	Pipe _n(100);
	CNF cnf_n;
	Record lit_n;
	DBFile dbf_n;
	
	char *pred_n = "(n_nationkey < 10)";
	dbf_n.Open (n->path());
	get_cnf (pred_n, n->schema (), cnf_n, lit_n);
	SF_n.Use_n_Pages (100);

	SF_n.Run (dbf_n, _n, cnf_n, lit_n);
	SF_n.WaitUntilDone ();

	int cnt = clear_pipe (_n, n->schema (), false);
	dbf_n.Close ();
	cleanup();

	ASSERT_EQ(cnt, 10);
}

// SELECT n_nationkey, n_name FROM nation
TEST(ProjectTest, NumberOfAttributesTest){
	setup();

	SelectFile SF_n;
	Pipe _n(100);
	CNF cnf_n;
	Record lit_n;
	DBFile dbf_n;

	char *pred_n = "(n_nationkey = 0)";
	dbf_n.Open (n->path());
	get_cnf (pred_n, n->schema (), cnf_n, lit_n);
	SF_n.Use_n_Pages (100);

	Project P_n;
	Pipe _out (100);
	int keepMe[] = {0,1};
	int numAttsIn = nAtts;
	int numAttsOut = 2;
	P_n.Use_n_Pages (100);

	SF_n.Run (dbf_n, _n, cnf_n, lit_n);
	P_n.Run (_n, _out, keepMe, numAttsIn, numAttsOut);

	SF_n.WaitUntilDone ();
	P_n.WaitUntilDone ();

	Attribute att2[] = {IA, SA};
	Schema out_sch ("out_sch", numAttsOut, att2);

	Record rec;	
	_out.Remove(&rec);
	cout<<"First projected record is"<<endl;
	printNHyphen(200);
	rec.Print(&out_sch);
	printNHyphen(200);

	dbf_n.Close ();
	cleanup();
	ASSERT_EQ(2, rec.GetNumAtts()); // rec must have 2 attributes only
}


// SELECT SUM(n_nationkey) FROM nation
// expected sum = 300 (Formula: n*(n+1)/2 here n=24)
TEST(SumTest, SumCalculationTest){
	setup();

	SelectFile SF_n;
	Pipe _n(100);
	CNF cnf_n;
	Record lit_n;
	DBFile dbf_n;
	char *pred_n = "(n_nationkey = n_nationkey)";
	
	dbf_n.Open (n->path());
	get_cnf (pred_n, n->schema (), cnf_n, lit_n);
	SF_n.Use_n_Pages (100);

	Sum T;
	Pipe _out (1);
	Function func;
	char *str_sum = "(n_nationkey)";
	get_cnf (str_sum, n->schema (), func);
	func.Print ();
	T.Use_n_Pages (1);
	SF_n.Run (dbf_n, _n, cnf_n, lit_n);
	T.Run (_n, _out, func);

	SF_n.WaitUntilDone ();
	T.WaitUntilDone ();

	Schema out_sch ("out_sch", 1, &IA);

	Record rec;
	_out.Remove (&rec);
	cout<<"Sum record is"<<endl;
	printNHyphen(200);
	rec.Print(&out_sch);
	printNHyphen(200);
	int pointer = ((int *) rec.bits)[1];
	int *outputSum = (int *) &(rec.bits[pointer]);


	dbf_n.Close ();

	ASSERT_EQ(*outputSum, 300); // check if outputsum is equal to the expected sum of 300
}

// join supplier and partsupp
// expected attributes in each record=7+5=12
TEST(JoinTest, NumberOfAttributesTest){
	setup();

	SelectFile SF_s;
	Pipe _s(100);
	CNF cnf_s;
	Record lit_s;
	DBFile dbf_s;
	char *pred_s = "(s_suppkey = s_suppkey)";

	dbf_s.Open (s->path());
	get_cnf (pred_s, s->schema (), cnf_s, lit_s);
	SF_s.Use_n_Pages (100);
	SF_s.Run (dbf_s, _s, cnf_s, lit_s); // 10k recs qualified


	SelectFile SF_ps;
	Pipe _ps(100);
	CNF cnf_ps;
	Record lit_ps;
	DBFile dbf_ps;
	char *pred_ps = "(ps_suppkey = ps_suppkey)";
	dbf_ps.Open (ps->path());
	get_cnf (pred_ps, ps->schema (), cnf_ps, lit_ps);
	SF_ps.Use_n_Pages (100);
	
	Join J;
	Pipe _s_ps (100);
	CNF cnf_p_ps;
	Record lit_p_ps;
	get_cnf ("(s_suppkey = ps_suppkey)", s->schema(), ps->schema(), cnf_p_ps, lit_p_ps);

	int outAtts = sAtts + psAtts;
	Attribute ps_supplycost = {"ps_supplycost", Double};
	Attribute joinatt[] = {IA,SA,SA,IA,SA,DA,SA, IA,IA,IA,ps_supplycost,SA};
	Schema join_sch ("join_sch", outAtts, joinatt);

	SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps); // 161 recs qualified
	J.Run (_s, _ps, _s_ps, cnf_p_ps, lit_p_ps);

	SF_ps.WaitUntilDone ();

	Record rec;
	_s_ps.Remove(&rec);
	cout<<"First joined record is"<<endl;
	printNHyphen(200);
	rec.Print(&join_sch);
	printNHyphen(200);
	ASSERT_EQ(sAtts+psAtts, rec.GetNumAtts()); // number of attributes in a joined record should be equal to sAtts+psAtts=12
}

int main(int argc, char **argv){
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}