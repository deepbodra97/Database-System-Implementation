#ifndef TEST_H
#define TEST_H
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include "DBFile.h"
#include "Record.h"
using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

class relation {

private:
	const char *rname;
	const char *prefix;
	char rpath[100]; 
	Schema *rschema;
public:
	relation (const char *_name, Schema *_schema, const char *_prefix) :
		rname (_name), rschema (_schema), prefix (_prefix) {
		sprintf (rpath, "%s%s.bin", prefix, rname);
	}
	const char* name () { return rname; }
	const char* path () { return rpath; }
	Schema* schema () { return rschema;}
	void info () {
		cout << " relation info\n";
		cout << "\t name: " << name () << endl;
		cout << "\t path: " << path () << endl;
	}

	void get_cnf (CNF &cnf_pred, Record &literal) {
		cout << " Enter CNF predicate (when done press ctrl-D):\n\t";
  		if (yyparse() != 0) {
			std::cout << "Can't parse your CNF.\n";
			exit (1);
		}
		cnf_pred.GrowFromParseTree (final, schema (), literal); // constructs CNF predicate
	}
};

const char *supplier = "supplier"; 
const char *partsupp = "partsupp"; 
const char *part = "part"; 
const char *nation = "nation"; 
const char *customer = "customer"; 
const char *orders = "orders"; 
const char *region = "region"; 
const char *lineitem = "lineitem"; 

relation *s, *p, *ps, *n, *li, *r, *o, *c;

void setup (const char *catalog_path, const char *dbfile_dir, const char *tpch_dir) {
	cout << " \n** IMPORTANT: MAKE SURE THE INFORMATION BELOW IS CORRECT **\n";
	cout << " catalog location: \t" << catalog_path << endl;
	cout << " tpch files dir: \t" << tpch_dir << endl;
	cout << " heap files dir: \t" << dbfile_dir << endl;
	cout << " \n\n";

	s = new relation (supplier, new Schema (catalog_path, supplier), dbfile_dir);
	ps = new relation (partsupp, new Schema (catalog_path, partsupp), dbfile_dir);
	p = new relation (part, new Schema (catalog_path, part), dbfile_dir);
	n = new relation (nation, new Schema (catalog_path, nation), dbfile_dir);
	li = new relation (lineitem, new Schema (catalog_path, lineitem), dbfile_dir);
	r = new relation (region, new Schema (catalog_path, region), dbfile_dir);
	o = new relation (orders, new Schema (catalog_path, orders), dbfile_dir);
	c = new relation (customer, new Schema (catalog_path, customer), dbfile_dir);
}

void cleanup () {
	delete s, p, ps, n, li, r, o, c;
}

#endif
