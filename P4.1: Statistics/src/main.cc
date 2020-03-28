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
	Statistics s;
	s.Read("Statistics.txt");
	s.Print();
}