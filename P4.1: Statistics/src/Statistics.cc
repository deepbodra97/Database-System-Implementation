#include "Statistics.h"

RelationInfo::RelationInfo(int numTuples, int numRelations){
	this->numTuples = numTuples;
	this->numRelations = numRelations;
}

Statistics::Statistics()
{
}

Statistics::Statistics(Statistics &copyMe)
{
}
Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples){
	RelationInfo *relation = new RelationInfo(numTuples, 1);
	infoMap[string(relName)] = relation;
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts){
	if (numDistincts == -1){
		numDistincts = infoMap[string(relName)]->numTuples;
	}
	infoMap[string(relName)]->attributes[string(attName)] = numDistincts;
}

void Statistics::CopyRel(char *oldName, char *newName)
{
}
	
void Statistics::Read(char *fromWhere)
{
}
void Statistics::Write(char *fromWhere)
{
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
}

void Statistics::Print(){
	for(map<string, RelationInfo*>::iterator it1 = infoMap.begin(); it1 != infoMap.end(); it1++) {
		string relationName = it1->first; 
		RelationInfo *relationInfo = it1->second;
		cout << relationName << " :: " << relationInfo->numTuples << endl;
		for(map<string, int>::iterator it2 = relationInfo->attributes.begin(); it2 != relationInfo->attributes.end(); it2++) {
			string attributeName = it2->first; 
			int numDistincts = it2->second;
			cout << attributeName << " :: " << numDistincts << endl;
		}
	}
}