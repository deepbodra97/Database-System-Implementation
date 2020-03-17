// #include "test.h"
// #include "BigQ.h"
// #include "RelOp.h"
// #include <pthread.h>

// Attribute IA = {"int", Int};
// Attribute SA = {"string", String};
// Attribute DA = {"double", Double};

// int clear_pipe (Pipe &in_pipe, Schema *schema, bool print) {
// 	Record rec;
// 	int cnt = 0;
// 	while (in_pipe.Remove (&rec)) {
// 		if (print) {
// 			rec.Print (schema);
// 		}
// 		cnt++;
// 	}
// 	return cnt;
// }

// int clear_pipe (Pipe &in_pipe, Schema *schema, Function &func, bool print) {
// 	Record rec;
// 	int cnt = 0;
// 	double sum = 0;
// 	while (in_pipe.Remove (&rec)) {
// 		if (print) {
// 			rec.Print (schema);
// 		}
// 		int ival = 0; double dval = 0;
// 		func.Apply (rec, ival, dval);
// 		sum += (ival + dval);
// 		cnt++;
// 	}
// 	cout << " Sum: " << sum << endl;
// 	return cnt;
// }
// int pipesz = 100; // buffer sz allowed for each pipe
// int buffsz = 100; // pages of memory allowed for operations

// SelectFile SF_ps, SF_p, SF_s, SF_o, SF_li, SF_c, SF_n;
// DBFile dbf_ps, dbf_p, dbf_s, dbf_o, dbf_li, dbf_c, dbf_n;
// Pipe _ps (pipesz), _p (pipesz), _s (pipesz), _o (pipesz), _li (pipesz), _c (pipesz), _n(pipesz);
// CNF cnf_ps, cnf_p, cnf_s, cnf_o, cnf_li, cnf_c, cnf_n;
// Record lit_ps, lit_p, lit_s, lit_o, lit_li, lit_c, lit_n;
// Function func_ps, func_p, func_s, func_o, func_li, func_c, func_n;

// int pAtts = 9;
// int psAtts = 5;
// int liAtts = 16;
// int oAtts = 9;
// int sAtts = 7;
// int cAtts = 8;
// int nAtts = 4;
// int rAtts = 3;

// void init_SF_n (char *pred_str, int numpgs) {
// 	dbf_n.Open (n->path());
// 	get_cnf (pred_str, n->schema (), cnf_n, lit_n);
// 	SF_n.Use_n_Pages (numpgs);
// }

// void init_SF_ps (char *pred_str, int numpgs) {
// 	dbf_ps.Open (ps->path());
// 	get_cnf (pred_str, ps->schema (), cnf_ps, lit_ps);
// 	SF_ps.Use_n_Pages (numpgs);
// }

// void init_SF_p (char *pred_str, int numpgs) {
// 	dbf_p.Open (p->path());
// 	get_cnf (pred_str, p->schema (), cnf_p, lit_p);
// 	SF_p.Use_n_Pages (numpgs);
// }

// void init_SF_s (char *pred_str, int numpgs) {
// 	dbf_s.Open (s->path());
// 	get_cnf (pred_str, s->schema (), cnf_s, lit_s);
// 	SF_s.Use_n_Pages (numpgs);
// }

// void init_SF_o (char *pred_str, int numpgs) {
// 	dbf_o.Open (o->path());
// 	get_cnf (pred_str, o->schema (), cnf_o, lit_o);
// 	SF_o.Use_n_Pages (numpgs);
// }

// void init_SF_li (char *pred_str, int numpgs) {
// 	dbf_li.Open (li->path());
// 	get_cnf (pred_str, li->schema (), cnf_li, lit_li);
// 	SF_li.Use_n_Pages (numpgs);
// }

// void init_SF_c (char *pred_str, int numpgs) {
// 	dbf_c.Open (c->path());
// 	get_cnf (pred_str, c->schema (), cnf_c, lit_c);
// 	SF_c.Use_n_Pages (numpgs);
// }

// // select * from partsupp where ps_supplycost <1.03 
// // expected output: 31 records
// void q1 () {

// 	char *pred_ps = "(ps_supplycost < 1.03)";
// 	init_SF_ps (pred_ps, 100);

// 	SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps);
// 	SF_ps.WaitUntilDone ();

// 	int cnt = clear_pipe (_ps, ps->schema (), true);
// 	cout << "\n\n query1 returned " << cnt << " records \n";

// 	dbf_ps.Close ();
// }

// void q1_n () {

// 	char *pred_n = "(n_nationkey > 5)";
// 	init_SF_n (pred_n, 100);

// 	SF_n.Run (dbf_n, _n, cnf_n, lit_n);
// 	SF_n.WaitUntilDone ();

// 	int cnt = clear_pipe (_n, n->schema (), true);
// 	cout << "\n\n query1 returned " << cnt << " records \n";

// 	dbf_n.Close ();
// }


// // select p_partkey(0), p_name(1), p_retailprice(7) from part where (p_retailprice > 931.01) AND (p_retailprice < 931.3);
// // expected output: 22 records
// void q2 () {

// 	char *pred_p = "(p_retailprice > 931.01) AND (p_retailprice < 931.3)";
// 	init_SF_p (pred_p, 100);

// 	Project P_p;
// 		Pipe _out (pipesz);
// 		int keepMe[] = {0,1,7};
// 		int numAttsIn = pAtts;
// 		int numAttsOut = 3;
// 	P_p.Use_n_Pages (buffsz);

// 	SF_p.Run (dbf_p, _p, cnf_p, lit_p);
// 	P_p.Run (_p, _out, keepMe, numAttsIn, numAttsOut);

// 	SF_p.WaitUntilDone ();
// 	P_p.WaitUntilDone ();

// 	Attribute att3[] = {IA, SA, DA};
// 	Schema out_sch ("out_sch", numAttsOut, att3);
// 	int cnt = clear_pipe (_p, p->schema (), true);

// 	cout << "\n\n query2 returned " << cnt << " records \n";

// 	dbf_p.Close ();
// }

// void q2_n () {

// 	char *pred_n = "(n_nationkey > 5)";
// 	init_SF_n (pred_n, 100);

// 	Project P_n;
// 		Pipe _out (pipesz);
// 		int keepMe[] = {0,1};
// 		int numAttsIn = nAtts;
// 		int numAttsOut = 2;
// 	P_n.Use_n_Pages (buffsz);

// 	SF_n.Run (dbf_n, _n, cnf_n, lit_n);
// 	P_n.Run (_n, _out, keepMe, numAttsIn, numAttsOut);

// 	SF_n.WaitUntilDone ();
// 	P_n.WaitUntilDone ();

// 	Attribute att3[] = {IA, SA};
// 	Schema out_sch ("out_sch", numAttsOut, att3);
// 	int cnt = clear_pipe (_n, n->schema (), true);

// 	cout << "\n\n query2 returned " << cnt << " records \n";

// 	dbf_n.Close ();
// }

// // select sum (s_acctbal + (s_acctbal * 1.05)) from supplier;
// // expected output: 9.24623e+07
// void q3 () {

// 	char *pred_s = "(s_suppkey = s_suppkey)";
// 	init_SF_s (pred_s, 100);

// 	Sum T;
// 		// _s (input pipe)
// 		Pipe _out (1);
// 		Function func;
// 			char *str_sum = "(s_acctbal + (s_acctbal * 1.05))";
// 			get_cnf (str_sum, s->schema (), func);
// 			func.Print ();
// 	T.Use_n_Pages (1);
// 	SF_s.Run (dbf_s, _s, cnf_s, lit_s);
// 	T.Run (_s, _out, func);

// 	SF_s.WaitUntilDone ();
// 	T.WaitUntilDone ();

// 	Schema out_sch ("out_sch", 1, &DA);
// 	int cnt = clear_pipe (_out, &out_sch, true);

// 	cout << "\n\n query3 returned " << cnt << " records \n";

// 	dbf_s.Close ();
// }


// // select sum (ps_supplycost) from supplier, partsupp 
// // where s_suppkey = ps_suppkey;
// // expected output: 4.00406e+08
// // void q4 () {

// // 	cout << " query4 \n";
// // 	char *pred_s = "(s_suppkey = s_suppkey)";
// // 	init_SF_s (pred_s, 100);
// // 	SF_s.Run (dbf_s, _s, cnf_s, lit_s); // 10k recs qualified

// // 	char *pred_ps = "(ps_suppkey = ps_suppkey)";
// // 	init_SF_ps (pred_ps, 100);

// // 	Join J;
// // 		// left _s
// // 		// right _ps
// // 		Pipe _s_ps (pipesz);
// // 		CNF cnf_p_ps;
// // 		Record lit_p_ps;
// // 		get_cnf ("(s_suppkey = ps_suppkey)", s->schema(), ps->schema(), cnf_p_ps, lit_p_ps);

// // 	int outAtts = sAtts + psAtts;
// // 	Attribute ps_supplycost = {"ps_supplycost", Double};
// // 	Attribute joinatt[] = {IA,SA,SA,IA,SA,DA,SA, IA,IA,IA,ps_supplycost,SA};
// // 	Schema join_sch ("join_sch", outAtts, joinatt);

// // 	Sum T;
// // 		// _s (input pipe)
// // 		Pipe _out (1);
// // 		Function func;
// // 			char *str_sum = "(ps_supplycost)";
// // 			get_cnf (str_sum, &join_sch, func);
// // 			func.Print ();
// // 	T.Use_n_Pages (1);

// // 	SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps); // 161 recs qualified
// // 	J.Run (_s, _ps, _s_ps, cnf_p_ps, lit_p_ps);
// // 	T.Run (_s_ps, _out, func);

// // 	SF_ps.WaitUntilDone ();
// // 	J.WaitUntilDone ();
// // 	T.WaitUntilDone ();

// // 	Schema sum_sch ("sum_sch", 1, &DA);
// // 	int cnt = clear_pipe (_out, &sum_sch, true);
// // 	cout << " query4 returned " << cnt << " recs \n";
// // }

// void q4 () {

//   cout << " query4 \n";
//   char *pred_s = "(s_suppkey = s_suppkey)";
//   init_SF_s (pred_s, 100);
//   SF_s.Run (dbf_s, _s, cnf_s, lit_s); // 10k recs qualified

//   char *pred_ps = "(ps_suppkey = ps_suppkey)";
//   init_SF_ps (pred_ps, 100);
//   SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps); // 161 recs qualified

//   Join J;
//   // left _s
//   // right _ps
//   Pipe _s_ps (pipesz);
//   CNF cnf_p_ps;
//   Record lit_p_ps;
//   get_cnf ("(s_suppkey = ps_suppkey)", s->schema(), ps->schema(), cnf_p_ps, lit_p_ps);
//   clog << "join default runlength is " << J.GetRunLength() << endl;
//   J.Use_n_Pages(pipesz);

//   int outAtts = sAtts + psAtts;
//   Attribute ps_supplycost = {"ps_supplycost", Double};
//   Attribute joinatt[] = {IA,SA,SA,IA,SA,DA,SA, IA,IA,IA,ps_supplycost,SA};
//   Schema join_sch ("join_sch", outAtts, joinatt);

//   J.Run (_s, _ps, _s_ps, cnf_p_ps, lit_p_ps);

//   Sum T;
//   // _s (input pipe)
//   Pipe _out (1);
//   Function func;
//   char *str_sum = "(ps_supplycost)";
//   get_cnf (str_sum, &join_sch, func);
//   func.Print ();
//   T.Use_n_Pages (1);
//   T.Run (_s_ps, _out, func);

//   SF_s.WaitUntilDone();
//   SF_ps.WaitUntilDone ();
//   J.WaitUntilDone ();
//   T.WaitUntilDone ();

//   Schema sum_sch ("sum_sch", 1, &DA);
//   int cnt = clear_pipe (_out, &sum_sch, true);
//   cout << " query4 returned " << cnt << " recs \n";
// }
// // select distinct ps_suppkey from partsupp where ps_supplycost < 100.11;
// // expected output: 9996 rows
// void q5 () {

// 	char *pred_ps = "(ps_supplycost < 100.11)";
// 	init_SF_ps (pred_ps, 100);

// 	Project P_ps;
// 		Pipe __ps (pipesz);
// 		int keepMe[] = {1};
// 		int numAttsIn = psAtts;
// 		int numAttsOut = 1;
// 	P_ps.Use_n_Pages (buffsz);

// 	DuplicateRemoval D;
// 		// inpipe = __ps
// 		Pipe ___ps (pipesz);
// 		Schema __ps_sch ("__ps", 1, &IA);
		
// 	WriteOut W;
// 		// inpipe = ___ps
// 		char *fwpath = "ps.w.tmp";
// 		FILE *writefile = fopen (fwpath, "w");

// 	SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps);
// 	P_ps.Run (_ps, __ps, keepMe, numAttsIn, numAttsOut);
// 	D.Run (__ps, ___ps,__ps_sch);
// 	W.Run (___ps, writefile, __ps_sch);

// 	SF_ps.WaitUntilDone ();
// 	P_ps.WaitUntilDone ();
// 	D.WaitUntilDone ();
// 	W.WaitUntilDone ();

// 	cout << " query5 finished..output written to file " << fwpath << "\n";
// }

// // select sum (ps_supplycost) from supplier, partsupp 
// // where s_suppkey = ps_suppkey groupby s_nationkey;
// // expected output: 25 rows
// void q6 () {

// 	cout << " query6 \n";
// 	char *pred_s = "(s_suppkey = s_suppkey)";
// 	init_SF_s (pred_s, 100);
// 	SF_s.Run (dbf_s, _s, cnf_s, lit_s); // 10k recs qualified

// 	char *pred_ps = "(ps_suppkey = ps_suppkey)";
// 	init_SF_ps (pred_ps, 100);

// 	Join J;
// 		// left _s
// 		// right _ps
// 		Pipe _s_ps (pipesz);
// 		CNF cnf_p_ps;
// 		Record lit_p_ps;
// 		get_cnf ("(s_suppkey = ps_suppkey)", s->schema(), ps->schema(), cnf_p_ps, lit_p_ps);

// 	int outAtts = sAtts + psAtts;
// 	Attribute s_nationkey = {"s_nationkey", Int};
// 	Attribute ps_supplycost = {"ps_supplycost", Double};
// 	Attribute joinatt[] = {IA,SA,SA,s_nationkey,SA,DA,SA,IA,IA,IA,ps_supplycost,SA};
// 	Schema join_sch ("join_sch", outAtts, joinatt);

// 	GroupBy G;
// 		// _s (input pipe)
// 		Pipe _out (1);
// 		Function func;
// 			char *str_sum = "(ps_supplycost)";
// 			get_cnf (str_sum, &join_sch, func);
// 			func.Print ();
// 			OrderMaker grp_order (&join_sch);
// 	G.Use_n_Pages (1);

// 	SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps); // 161 recs qualified
// 	J.Run (_s, _ps, _s_ps, cnf_p_ps, lit_p_ps);
// 	G.Run (_s_ps, _out, grp_order, func);

// 	SF_ps.WaitUntilDone ();
// 	J.WaitUntilDone ();
// 	G.WaitUntilDone ();

// 	Schema sum_sch ("sum_sch", 1, &DA);
// 	int cnt = clear_pipe (_out, &sum_sch, true);
// 	cout << " query6 returned sum for " << cnt << " groups (expected 25 groups)\n"; 
// }

// void q7 () { 
// /*
// select sum(ps_supplycost)
// from part, supplier, partsupp
// where p_partkey = ps_partkey and
// s_suppkey = ps_suppkey and
// s_acctbal > 2500;

// ANSWER: 274251601.96 (5.91 sec)

// possible plan:
// 	SF(s_acctbal > 2500) => _s
// 	SF(p_partkey = p_partkey) => _p 
// 	SF(ps_partkey = ps_partkey) => _ps  
// 	On records from pipes _p and _ps: 
// 		J(p_partkey = ps_partkey) => _p_ps
// 	On _s and _p_ps: 
// 		J(s_suppkey = ps_suppkey) => _s_p_ps
// 	On _s_p_ps:
// 		S(s_supplycost) => __s_p_ps
// 	On __s_p_ps:
// 		W(__s_p_ps)

// Legend:
// SF : select all records that satisfy some simple cnf expr over recs from in_file 
// SP: same as SF but recs come from in_pipe
// J: select all records (from left_pipe x right_pipe) that satisfy a cnf expression
// P: project some atts from in-pipe
// T: apply some aggregate function
// G: same as T but do it over each group identified by ordermaker
// D: stuff only distinct records into the out_pipe discarding duplicates
// W: write out records from in_pipe to a file using out_schema
// */
// 	cout << " TBA\n";
// }

// void q8 () { 
// /*
// select l_orderkey, l_partkey, l_suppkey
// from lineitem
// where l_returnflag = 'R' and l_discount < 0.04 or 
// l_returnflag = 'R' and l_shipmode = 'MAIL';

// ANSWER: 671392 rows in set (29.45 sec)


// possible plan:
// 	SF (l_returnflag = 'R' and ...) => _l
// 	On _l:
// 		P (l_orderkey,l_partkey,l_suppkey) => __l
// 	On __l:
// 		W (__l)
// */
// 	cout << " TBA\n";
// }

// int main (int argc, char *argv[]) {

// 	if (argc != 2) {
// 		cerr << " Usage: ./test.out [1-8] \n";
// 		exit (0);
// 	}

// 	void (*query_ptr[]) () = {&q1, &q2, &q3, &q4, &q5, &q6, &q7, &q8};  
// 	void (*query) ();
// 	int qindx = atoi (argv[1]);

// 	if (qindx > 0 && qindx < 9) {
// 		setup ();
// 		query = query_ptr [qindx - 1];
// 		query ();
// 		cleanup ();
// 		cout << "\n\n";
// 	}
// 	else {
// 		cout << " ERROR!!!!\n";
// 	}
// }

#include "test.h"
#include "BigQ.h"
#include "RelOp.h"
#include <pthread.h>
#include <omp.h>
// #include "HiResTimer.h"



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
int pipesz = 100; // buffer sz allowed for each pipe
int buffsz = 100; // pages of memory allowed for operations

SelectFile SF_ps, SF_p, SF_s, SF_o, SF_li, SF_c;
DBFile dbf_ps, dbf_p, dbf_s, dbf_o, dbf_li, dbf_c;
Pipe _ps (pipesz), _p (pipesz), _s (pipesz), _o (pipesz), _li (pipesz), _c (pipesz);
CNF cnf_ps, cnf_p, cnf_s, cnf_o, cnf_li, cnf_c;
Record lit_ps, lit_p, lit_s, lit_o, lit_li, lit_c;
Function func_ps, func_p, func_s, func_o, func_li, func_c;

int pAtts = 9;
int psAtts = 5;
int liAtts = 16;
int oAtts = 9;
int sAtts = 7;
int cAtts = 8;
int nAtts = 4;
int rAtts = 3;

// initializers
// open the database
// parse the cnf from the string
// set the internal runlength/number of pages to use variable
void init_SF_ps (char *pred_str, int numpgs) {
  dbf_ps.Open (ps->path());
  get_cnf (pred_str, ps->schema (), cnf_ps, lit_ps);
  SF_ps.Use_n_Pages (numpgs);
}

void init_SF_p (char *pred_str, int numpgs) {
  dbf_p.Open (p->path());
  get_cnf (pred_str, p->schema (), cnf_p, lit_p);
  SF_p.Use_n_Pages (numpgs);
}

void init_SF_s (char *pred_str, int numpgs) {
  dbf_s.Open (s->path());
  get_cnf (pred_str, s->schema (), cnf_s, lit_s);
  SF_s.Use_n_Pages (numpgs);
}

void init_SF_o (char *pred_str, int numpgs) {
  dbf_o.Open (o->path());
  get_cnf (pred_str, o->schema (), cnf_o, lit_o);
  SF_o.Use_n_Pages (numpgs);
}

void init_SF_li (char *pred_str, int numpgs) {
  dbf_li.Open (li->path());
  get_cnf (pred_str, li->schema (), cnf_li, lit_li);
  SF_li.Use_n_Pages (numpgs);
}

void init_SF_c (char *pred_str, int numpgs) {
  dbf_c.Open (c->path());
  get_cnf (pred_str, c->schema (), cnf_c, lit_c);
  SF_c.Use_n_Pages (numpgs);
}

// select * from partsupp where ps_supplycost <1.03
// expected output: 31 records
// 21 records?
void q1 () {

  char *pred_ps = "(ps_supplycost < 1.03)";
  init_SF_ps (pred_ps, 100);

  SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps);
  SF_ps.WaitUntilDone ();

  int cnt = clear_pipe (_ps, ps->schema (), true);
  cout << "\n\n query1 returned " << cnt << " records \n";

  dbf_ps.Close ();
}


// select p_partkey(0), p_name(1), p_retailprice(7) from part where (p_retailprice > 931.01) AND (p_retailprice < 931.3);
// expected output: 22 records
// also a typo? 12 records?
void q2 () {

  char *pred_p = "(p_retailprice > 931.01) AND (p_retailprice < 931.3)";
  init_SF_p (pred_p, 100);

  Project P_p;
  Pipe _out (pipesz);
  int keepMe[] = {0,1,7};
  int numAttsIn = pAtts;
  int numAttsOut = 3;
  P_p.Use_n_Pages (buffsz);

  SF_p.Run (dbf_p, _p, cnf_p, lit_p);
  P_p.Run (_p, _out, keepMe, numAttsIn, numAttsOut);

  SF_p.WaitUntilDone ();
  P_p.WaitUntilDone ();

  Attribute att3[] = {IA, SA, DA};
  Schema out_sch ("out_sch", numAttsOut, att3);
  int cnt = clear_pipe (_out, &out_sch, true);

  cout << "\n\n query2 returned " << cnt << " records \n";

  dbf_p.Close ();
}

// select sum (s_acctbal + (s_acctbal * 1.05)) from supplier;
// expected output: 9.24623e+07
void q3 () {
  clog << "enter q3" << endl;
  char *pred_s = "(s_suppkey = s_suppkey)";
  init_SF_s (pred_s, 100);

  Sum T;
  // _s (input pipe)
  Pipe _out (1); // only need one record for final output.
  Function func;
  char *str_sum = "(s_acctbal + (s_acctbal * 1.05))";
  get_cnf (str_sum, s->schema (), func);
  func.Print ();
  clog << "printed function" << endl;
  T.Use_n_Pages (1);
  SF_s.Run (dbf_s, _s, cnf_s, lit_s);
  clog << "return from starting SF worker" << endl;
  T.Run (_s, _out, func);
  clog << "return from starting sum worker" << endl;

  SF_s.WaitUntilDone ();
  T.WaitUntilDone ();

  Schema out_sch ("out_sch", 1, &DA);
  int cnt = clear_pipe (_out, &out_sch, true);
  cout << "output:  9.24623e+07 expected" << endl;
  cout << "\n\n query3 returned " << cnt << " records \n";

  dbf_s.Close ();
}

// selectfile & join
// select sum (ps_supplycost) from supplier, partsupp
// where s_suppkey = ps_suppkey;
// expected output: 4.00406e+08
void q4 () {

  cout << " query4 \n";
  char *pred_s = "(s_suppkey = s_suppkey)";
  init_SF_s (pred_s, 100);
  SF_s.Run (dbf_s, _s, cnf_s, lit_s); // 10k recs qualified

  char *pred_ps = "(ps_suppkey = ps_suppkey)";
  init_SF_ps (pred_ps, 100);
  SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps); // 161 recs qualified

  Join J;
  // left _s
  // right _ps
  Pipe _s_ps (pipesz);
  CNF cnf_p_ps;
  Record lit_p_ps;
  get_cnf ("(s_suppkey = ps_suppkey)", s->schema(), ps->schema(), cnf_p_ps, lit_p_ps);
  clog << "join default runlength is " << J.GetRunLength() << endl;
  J.Use_n_Pages(pipesz);

  int outAtts = sAtts + psAtts;
  Attribute ps_supplycost = {"ps_supplycost", Double};
  Attribute joinatt[] = {IA,SA,SA,IA,SA,DA,SA, IA,IA,IA,ps_supplycost,SA};
  Schema join_sch ("join_sch", outAtts, joinatt);

  J.Run (_s, _ps, _s_ps, cnf_p_ps, lit_p_ps);

  Sum T;
  // _s (input pipe)
  Pipe _out (1);
  Function func;
  char *str_sum = "(ps_supplycost)";
  get_cnf (str_sum, &join_sch, func);
  func.Print ();
  T.Use_n_Pages (1);
  T.Run (_s_ps, _out, func);

  SF_s.WaitUntilDone();
  SF_ps.WaitUntilDone ();
  J.WaitUntilDone ();
  T.WaitUntilDone ();
  cout<<"calling clear_pipe\n";
  Schema sum_sch ("sum_sch", 1, &DA);
  int cnt = clear_pipe (_out, &sum_sch, true);
  cout << " query4 returned " << cnt << " recs \n";
}

// selectfile & join
// select sum (ps_supplycost) from supplier, partsupp
// where s_suppkey = ps_suppkey;
// expected output: 4.00406e+08
void q4opt () {

  cout << " query4 \n";

  char *pred_ps = "(ps_suppkey = ps_suppkey)";
  init_SF_ps (pred_ps, 100);
  SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps); // 161 recs qualified

  Sum T;
  // _s (input pipe)
  Pipe _out (1);
  Function func;
  char *str_sum = "(ps_supplycost)";
  get_cnf (str_sum, ps->schema(), func);
  func.Print ();
  T.Use_n_Pages (1);
  T.Run (_ps, _out, func);

  SF_ps.WaitUntilDone ();
  T.WaitUntilDone ();

  Schema sum_sch ("sum_sch", 1, &DA);
  int cnt = clear_pipe (_out, &sum_sch, true);
  cout << " query4 returned " << cnt << " recs \n";
}

// select distinct ps_suppkey from partsupp where ps_supplycost < 100.11;
// duplicate removal
// expected output: 9996 rows
void q5 () {

  char *pred_ps = "(ps_supplycost < 100.11)";
  init_SF_ps (pred_ps, 100);

  Project P_ps;
  Pipe __ps (pipesz);
  int keepMe[] = {1};
  int numAttsIn = psAtts;
  int numAttsOut = 1;
  // clog << "project default runlength is " << P_ps.runLength << endl;
  P_ps.Use_n_Pages (buffsz);

  DuplicateRemoval D;
  // inpipe = __ps
  Pipe ___ps (pipesz);
  Schema __ps_sch ("__ps", 1, &IA);

  WriteOut W;
  // inpipe = ___ps
  char *fwpath = "ps.w.tmp";
  FILE *writefile = fopen (fwpath, "w");

  SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps);
  P_ps.Run (_ps, __ps, keepMe, numAttsIn, numAttsOut);
  D.Run (__ps, ___ps,__ps_sch);
  W.Run (___ps, writefile, __ps_sch);

  SF_ps.WaitUntilDone ();
  P_ps.WaitUntilDone ();
  D.WaitUntilDone ();
  W.WaitUntilDone ();

  cout << " query5 finished..output written to file " << fwpath << "\n";
}

// select sum (ps_supplycost) from supplier, partsupp
// where s_suppkey = ps_suppkey groupby s_nationkey;
// expected output: 25 rows
void q6 () {

  cout << " query6 \n";
  char *pred_s = "(s_suppkey = s_suppkey)";
  init_SF_s (pred_s, 100);

  char *pred_ps = "(ps_suppkey = ps_suppkey)";
  init_SF_ps (pred_ps, 100);

  Join J;
  Pipe _s_ps (pipesz);
  CNF cnf_p_ps;
  Record lit_p_ps;
  get_cnf ("(s_suppkey = ps_suppkey)", s->schema(), ps->schema(), cnf_p_ps, lit_p_ps);

  int outAtts = sAtts + psAtts;
  Attribute s_nationkey = {"s_nationkey", Int};
  Attribute ps_supplycost = {"ps_supplycost", Double};
  Attribute joinatt[] = {IA,SA,SA,s_nationkey,SA,DA,SA,IA,IA,IA,ps_supplycost,SA};
  Schema join_sch ("join_sch", outAtts, joinatt);

  GroupBy G;
  Pipe _out (100);
  Function func;
  char *str_sum = "(ps_supplycost)";
  get_cnf (str_sum, &join_sch, func);
  func.Print ();
  OrderMaker grp_order;
  s->get_sort_order("(s_nationkey)",grp_order);
  grp_order.Print();
  clog << endl << endl << " and";

  G.Use_n_Pages (100);

  SF_s.Run (dbf_s, _s, cnf_s, lit_s); // 10k recs qualified
  SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps); // 161 recs qualified
  J.Run (_s, _ps, _s_ps, cnf_p_ps, lit_p_ps);
  G.Run (_s_ps, _out, grp_order, func);

  SF_ps.WaitUntilDone ();
  J.WaitUntilDone ();
  G.WaitUntilDone ();

  Attribute sumGroupByAttrs[] = {DA, IA};
  Schema sum_sch ("sum_sch", 2, sumGroupByAttrs);
  int cnt = clear_pipe (_out, &sum_sch, true);
  cout << " query6 returned sum for " << cnt << " groups (expected 25 groups)\n";
}

void q7 () {
  /*
    select sum(ps_supplycost)
    from part, supplier, partsupp
    where p_partkey = ps_partkey and
    s_suppkey = ps_suppkey and
    s_acctbal > 2500;
    ANSWER: 274251601.96 (5.91 sec)
    possible plan:
    SF(s_acctbal > 2500) => _s
    SF(p_partkey = p_partkey) => _p
    SF(ps_partkey = ps_partkey) => _ps
    On records from pipes _p and _ps:
    J(p_partkey = ps_partkey) => _p_ps
    On _s and _p_ps:
    J(s_suppkey = ps_suppkey) => _s_p_ps
    On _s_p_ps:
    S(s_supplycost) => __s_p_ps
    On __s_p_ps:
    W(__s_p_ps)
    Legend:
    SF : select all records that satisfy some simple cnf expr over recs from in_file
    SP: same as SF but recs come from in_pipe
    J: select all records (from left_pipe x right_pipe) that satisfy a cnf expression
    P: project some atts from in-pipe
    T: apply some aggregate function
    G: same as T but do it over each group identified by ordermaker
    D: stuff only distinct records into the out_pipe discarding duplicates
    W: write out records from in_pipe to a file using out_schema
    supplier
    s_suppkey Int  s_name String  s_address String  s_nationkey Int  s_phone String  s_acctbal Double  s_comment String
    0               1              2                 3                 4                5                 6
    part
    p_partkey Int  p_name String  p_mfgr String  p_brand String  p_type String  p_size Int  p_container String  p_retailprice Double  p_comment String
    0             1              2                 3            4                5            6                   7                    8
    partsupp
    ps_partkey Int  ps_suppkey Int  ps_availqty Int  ps_supplycost Double  ps_comment String
    0                1             2                  3                     4
  */
  // select from three tables
  // selection predicates
  char *pred_s = "(s_acctbal > 2500.0)";
  char *pred_p = "(p_partkey = p_partkey)";
  char *pred_ps = "(ps_partkey = ps_partkey)";
  Attribute ps_suppkey = {"ps_suppkey", Int};
  Attribute ps_supplycost = {"ps_supplycost", Double};
  // initialize input pipes
  init_SF_s (pred_s, 100);
  init_SF_p (pred_p, 100);
  init_SF_ps (pred_ps, 100);
  clog << "set up first join" << endl;
  // join p and ps // p_partkey = ps_partkey // want to match 0 with 0, need to keep 3 from ps
  Join Jpps;
  Attribute ppsjoinatt[] = {IA,SA,SA,SA,SA,IA,SA,DA,SA,  IA,ps_suppkey,IA,ps_supplycost,SA};
  int ppsOutAtts = pAtts + psAtts;
  Jpps.Use_n_Pages(pipesz);
  Schema pps_join_sch ("pps_join_sch", ppsOutAtts, ppsjoinatt);
  CNF cnf_p_ps;
  Record lit_p_ps;
  get_cnf ("(p_partkey = ps_partkey)", p->schema(), ps->schema(), cnf_p_ps, lit_p_ps);
  Pipe JppsP(pipesz);

  // join s and pps // (s_suppkey = ps_suppkey) // want to match 0 with, wherever ps_suppkey is
  clog << "set up second join" << endl;
  Join Jspps;
  Attribute sppsjoinatt[] = {IA,SA,SA,IA,SA,DA,SA,  IA,SA,SA,SA,SA,IA,SA,DA,SA,  IA,ps_suppkey,IA,ps_supplycost,SA};
  int sppsOutAtts = sAtts + pAtts + psAtts;
  Jspps.Use_n_Pages(pipesz);
  Schema spps_join_sch ("spps_join_sch", sppsOutAtts, sppsjoinatt);
  CNF cnf_s_p_ps;
  Record lit_s_p_ps;
  get_cnf ("(s_suppkey = ps_suppkey)", s->schema(), &pps_join_sch, cnf_s_p_ps, lit_s_p_ps);
  Pipe JsppsP(pipesz);

  clog << "set up SUM" << endl;
  // sum
  Sum S;
  S.Use_n_Pages(pipesz);
  Pipe SP(pipesz);
  // function for sum to run
  Function func;
  char *str_sum = "(ps_supplycost)";
  get_cnf (str_sum, &spps_join_sch, func);
  func.Print ();

  // write out

  clog << "run all" << endl;
  // run all
  // s = dbf_s
  // p = dbf_p
  // ps = dbf_ps
  SF_s.Run (dbf_s, _s, cnf_s, lit_s);
  SF_p.Run (dbf_p, _p, cnf_p, lit_p);
  SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps);

  Jpps.Run (_p,_ps,JppsP,cnf_p_ps,lit_p_ps);
  Jspps.Run (_s,JppsP,JsppsP,cnf_s_p_ps,lit_s_p_ps);
  S.Run (JsppsP, SP, func);
  // W
  WriteOut W;
  string path(dbfile_dir);
  path += "q7.tmp";
  const char *fwpath = path.c_str();
  FILE *writefile = fopen (fwpath, "w");
  Schema sum_sch ("sum_sch", 1, &DA);
  W.Run (SP, writefile, sum_sch);


  SF_s.WaitUntilDone();
  SF_p.WaitUntilDone();
  SF_ps.WaitUntilDone ();
  Jpps.WaitUntilDone ();
  Jspps.WaitUntilDone ();
  S.WaitUntilDone ();
  W.WaitUntilDone();
  //                 double: [2.74252e+08]  274252000.
  cout << " check file, expecting   ANSWER: 274251601.96 (5.91 sec)";
}

void q8 () {
  /*
    select l_orderkey, l_partkey, l_suppkey
    from lineitem
    where l_returnflag = 'R' and l_discount < 0.04 or
    l_returnflag = 'R' and l_shipmode = 'MAIL';
    ANSWER: 671392 rows in set (29.45 sec)
  */

  /*
    possible plan:
    SF (l_returnflag = 'R' and ...) => _l
    On _l:
    P (l_orderkey,l_partkey,l_suppkey) => __l
    On __l:
    W (__l)
  */
  char *pred_li = "(l_returnflag = 'R') AND (l_discount < 0.04 OR l_shipmode = 'MAIL')";
  init_SF_li (pred_li, 100);

  SF_ps.Run (dbf_li, _li, cnf_li, lit_li); // start the pipe.
  // now have _li to connect to project;

  Project P_li;
  Pipe _out (pipesz);
  // keep only: orderkey, partkey, suppkey, which are the first three, 0,1,2, and are all ints
  int keepMe[] = {0,1,2};
  int numAttsIn = liAtts;
  int numAttsOut = 3;

  P_li.Run (_li, _out, keepMe, numAttsIn, numAttsOut);
  // now have _out to connect to write out.

  WriteOut W;
  string path(dbfile_dir);
  path += "li.w.tmp";
  const char *fwpath = path.c_str();
  FILE *writefile = fopen (fwpath, "w");

  Attribute att3[] = {IA, IA, IA};
  Schema out_sch ("out_sch", numAttsOut, att3);

  W.Run (_out, writefile, out_sch);

  SF_ps.WaitUntilDone ();
  P_li.WaitUntilDone();
  W.WaitUntilDone();

  cout << "expect ANSWER: 671392 rows in set (29.45 sec)" << endl;
}

int main (int argc, char *argv[]) {

  if (argc != 2) {
    cerr << " Usage: ./test.out [1-8] \n"
         << "1. SelectFile" << endl
         << "2. SelectFile & Project" << endl
         << "3. SelectFile & Sum" << endl
         << "4. SelectFile & Join" << endl
         << "5. SelectFile & Project & DuplicateRemoval & WriteOut" << endl
         << "6. SelectFile & Join & GroupBy" << endl
         << "7. SelectFile & Join & Join & WriteOut" << endl
         << "8. SelectFile & Project & WriteOut" << endl;
    exit (-1);
  }

  void (*query_ptr[]) () = {&q1, &q2, &q3, &q4, &q5, &q6, &q7, &q8, &q4opt};
  void (*query) ();
  int qindx = atoi (argv[1]);

  if (qindx > 0 && qindx <= 9) {
    setup ();
    query = query_ptr [qindx - 1];

    double start = 0;
    double end = 0;

    // start = omp_get_wtime();

    // HiResTimer hrt;
    // hrt.start();

    query ();

    // hrt.stop();

    // end = omp_get_wtime();
    // double elapsed = end - start;
    // cout << endl << "elapsed query time is " << elapsed << "seconds" << endl ;
    // hrt.duration();

    cleanup ();
    cout << "\n\n";
  }
  else if (qindx == 10)
    {
      for(int indx = 0; indx < 8;indx++)
        {
          setup ();
          query = query_ptr [indx];
          double start = 0;
          double end = 0;
          // start = omp_get_wtime();
          // clog << "time for query " << (indx+1) << endl;
          query ();

          // end = omp_get_wtime();
          // double elapsed = end - start;
          // cout << "elapsed query time is " << elapsed << "seconds" << endl ;

          cleanup ();
          cout << "\n\n";
        }
    }
  else {
    cout << " ERROR!!!!\n";
  }
}