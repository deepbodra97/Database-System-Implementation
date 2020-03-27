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
	RelationInfo *relationInfo = new RelationInfo(numTuples, 1);
	statMap[string(relName)] = relationInfo;
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts){
	map<string, RelationInfo*>::iterator it = statMap.find(string(relName));
	if (numDistincts == -1){
		numDistincts = it->second->numTuples;
	}
	it->second->attributes[string(attName)] = numDistincts;
}

void Statistics::CopyRel(char *oldName, char *newName){
	map<string,RelationInfo*>::iterator it1 = statMap.find(string(oldName));
	RelationInfo *newRelationInfo = new RelationInfo(it1->second->numTuples, it1->second->numRelations);
	for (map<string,int>::iterator it2 = it1->second->attributes.begin(); it2!=it1->second->attributes.end(); it2++){
		ostringstream attributeName;
		attributeName << newName << "." << it2->first;
		newRelationInfo->attributes[attributeName.str()] = it2->second;
	}
	statMap[newName] = newRelationInfo;
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
	for(map<string, RelationInfo*>::iterator it1 = statMap.begin(); it1 != statMap.end(); it1++) {
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