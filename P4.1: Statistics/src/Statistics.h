#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"

#include <iostream>
#include <map>
#include <string>
#include <sstream>
#include <fstream>

using namespace std;

class RelationInfo{
public:
	map<string, int> attributes;
	int numTuples;
	int numRelations;

	RelationInfo(int numTuples, int numRelations);
};


class Statistics{
private:
	map<string, RelationInfo*> statMap;
	double tempResult;

public:
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

	void Print();
};

#endif
