#include <iostream>
#include <stdlib.h>
#include "Statistics.h"
#include "ParseTree.h"
#include <math.h>
extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);

extern "C" {
	int yyparse(void);
}

extern struct AndList *final;

using namespace std;

int main(){
	// basic stat test
	Statistics s;
    char *relName[] = { "part", "partsupp", "supplier", "nation", "region"};

	s.AddRel(relName[0], 200000);
	s.AddAtt(relName[0], "p_partkey",200000);
	s.AddAtt(relName[0], "p_size",50);

	s.AddRel(relName[1], 800000);
	s.AddAtt(relName[1], "ps_suppkey",10000);
	s.AddAtt(relName[1], "ps_partkey", 200000);
	
	s.AddRel(relName[2],10000);
	s.AddAtt(relName[2], "s_suppkey",10000);
	s.AddAtt(relName[2], "s_nationkey",25);
	
	s.AddRel(relName[3], 25);
	s.AddAtt(relName[3], "n_nationkey",25);
	s.AddAtt(relName[3], "n_regionkey",5);

	s.AddRel(relName[4], 5);
	s.AddAtt(relName[4], "r_regionkey",5);
	s.AddAtt(relName[4], "r_name",5);
	s.Write("Statistics.txt");
	s.Print();

	// parse tree 
	/*char *cnf = "(p_partkey=ps_partkey) AND (p_size =3 OR p_size=6 OR p_size =19)";

	// char *cnf = "(n_nationkey=0)";
	// char *cnf = "(n_nationkey=0) AND (n_comment='nation')";
	
	// char *cnf = "(n_nationkey=0 OR n_comment='nation')";
	
	// char *cnf = "(n_nationkey=n_comment)";

	yy_scan_string(cnf);
	yyparse();

	struct AndList *currentAnd = final;
    struct OrList *currentOr;

	while(currentAnd != NULL){
		currentOr = currentAnd->left;
		cout<<"and"<<endl;
		while (currentOr != NULL) {
			cout<<"or"<<endl;
			cout<<currentOr->left->left->value<<endl;
			cout<<currentOr->left->right->value<<endl;
			currentOr = currentOr->rightOr;
		}
		currentAnd = currentAnd->rightAnd;
	}*/
}