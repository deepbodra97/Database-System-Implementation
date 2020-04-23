#include <stdio.h>
#include <string>
#include <fstream>

#include "DBFile.h"
#include "ParseTree.h"
#include "Comparison.h"
#include "DataDefinitionLanguage.h"

using std::ifstream;
using std::string;
using std::endl;

extern struct FuncOperator *finalFunction; // aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

extern char* newtable;
extern char* oldtable;
extern char* newfile;
extern char* deoutput;
extern struct AttrList *newattrs; //Use this to build Attribute* array and record schema
extern struct NameList *sortattrs;

extern char* catalog_path;
extern char* dbfile_dir;
extern char* tpch_dir;

bool DataDefinitionLanguage::Create() {
	// Create bin files
	if (exists(newtable)){
		return false;
	}
	std::ofstream ofmeta ((std::string(newtable)+".bin.meta").c_str());
	fType t = (sortattrs ? sorted : heap);
	if (t == heap){
		ofmeta << "heap" << endl;   // 1 Filetype
	} else if (t == sorted){
		ofmeta << "sorted" << endl;
	}

  	// schema for new table
	int numAtts = 0;
	std::ofstream ofcat ("catalog", std::ios_base::app);
	ofcat << "BEGIN\n" << newtable << '\n' << newtable << ".tbl" << endl;
	const char* myTypes[3] = {"Int", "Double", "String"};
	for (AttrList* att = newattrs; att; att = att->next, ++numAtts)
		ofcat << att->name << ' ' << myTypes[att->type] << endl;
	ofcat << "END" << endl;

	Attribute* atts = new Attribute[numAtts];
	Type types[3] = {Int, Double, String};
	numAtts = 0;
	for (AttrList* att = newattrs; att; att = att->next, numAtts++) {
		atts[numAtts].name = strdup(att->name);
		atts[numAtts].myType = types[att->type];
	}
	Schema newSchema ("", numAtts, atts);

	// ordermaker for new table
	OrderMaker sortOrder;
	if (sortattrs) {
		sortOrder.growFromParseTree(sortattrs, &newSchema);
		ofmeta << sortOrder;
		ofmeta << 512 << endl;
	}

	struct SortInfo { OrderMaker* myOrder; int runLength; } info = {&sortOrder, 256};
	DBFile newTable;
  	newTable.Create((char*)(std::string(newtable)+".bin").c_str(), t, (void*)&info); // create ".bin" files
  	newTable.Close();

	delete[] atts;
	ofmeta.close(); ofcat.close();
	return true;
}

bool DataDefinitionLanguage::Insert() {
	DBFile table;
	char* fpath = new char[strlen(oldtable)+4];
	strcpy(fpath, oldtable); strcat(fpath, ".bin");
	Schema sch (catalog_path, oldtable);
	if (table.Open(fpath)) {
		table.Load(sch, newfile);
		table.Close();
		delete[] fpath; return true;
	} delete[] fpath; return false;
}

bool DataDefinitionLanguage::Drop() {
	// delete table info from catalog file
	string schString = "", line = "", relName = oldtable;
	ifstream fin (catalog_path);
	ofstream fout (".cata.tmp");
	bool found = false, exists = false;
	while (getline(fin, line)) {
		if (trim(line).empty()) continue;
		if (line == oldtable) exists = found = true;
		schString += trim(line) + '\n';
		if (line == "END") {
			if (!found) fout << schString << endl;
			found = false;
			schString.clear();
		}
	}

	rename (".cata.tmp", catalog_path);
	fin.close(); fout.close();

	// delete bin, meta
	if (exists) {
		remove ((relName+".bin").c_str());
		remove ((relName+".bin.meta").c_str());
		return true;
	}
	return false;
}

bool DataDefinitionLanguage::exists(const char* relName) {
	ifstream fin (catalog_path);
	string line;
	while (getline(fin, line))
		if (trim(line) == relName) {
			fin.close(); return true;
		}
		fin.close();  return false;
}