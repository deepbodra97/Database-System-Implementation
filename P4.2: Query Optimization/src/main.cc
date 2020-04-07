
#include <iostream>
#include "ParseTree.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

int main () {

	yyparse();
}


