#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"

#include <iostream>
#include <map>
#include <string>
#include <sstream>
#include <fstream>
#include <set>
#include <math.h>
#include <string.h>

using namespace std;

class RelationInfo{
public:
	map<string, int> attributes;
	int numTuples;

	RelationInfo();
	RelationInfo(int numTuples);
};


class Statistics{
public:
	map<string, RelationInfo*> statMap;
	bool shouldApply; // if true then the estimation will be applied
	int numRels = 0;

	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
	double ApplyEstimate(struct AndList *parseTree, char **relNames, int numToJoin);

	string GetRelation(string attribute);
	void Print();
};

#endif
