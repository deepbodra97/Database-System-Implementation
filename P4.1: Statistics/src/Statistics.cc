#include "Statistics.h"

RelationInfo::RelationInfo(int numTuples, int numRelations){
	this->numTuples = numTuples;
	this->numRelations = numRelations;
}

Statistics::Statistics(){
}

Statistics::Statistics(Statistics &copyMe){
	for (map<string, RelationInfo*>::iterator it1 = copyMe.statMap.begin(); it1!=copyMe.statMap.end(); it1++){
		RelationInfo *relationInfo = new RelationInfo(it1->second->numTuples, it1->second->numRelations);		

		for (map<string, int>::iterator it2 = it1->second->attributes.begin(); it2!=it1->second->attributes.end(); it2++){
			relationInfo->attributes.insert(pair<string, int>(it2->first, it2->second));
		}
		statMap[it1->first] = relationInfo;
	}
}

Statistics::~Statistics(){
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
	
void Statistics::Read(char *fromWhere){
	statMap.clear();
	tempResult = 0.0;

	ifstream statFile;
    statFile.open(fromWhere);
    if(!statFile){
    	cout<<"Statistics.txt is empty"<<endl;
    	statFile.close();
    	ofstream newStatFile;
    	newStatFile.open(fromWhere);
    	newStatFile<<"EOF";
    	newStatFile.close();
        return;
    }

    string line;
    statFile >> line;
    while(line.compare("EOF") != 0){
    	// cout<<"*"<<line<<endl;
    	if(line.compare("relation") == 0){
    		statFile >> line;
    		string relationName(line);
    		statFile >> line;
    		RelationInfo *relationInfo = new RelationInfo(stoi(line), 1);
    		statFile >> line; 
    		// statFile >> line;
    		while(statFile>>line && line.compare("relation") != 0 && line.compare("EOF") != 0){
    			string attributeName(line);
    			statFile >> line;
    			relationInfo->attributes[attributeName] = stoi(line);
       			// statFile >> line;
       		}
       		statMap[relationName] = relationInfo;
    	}
    }
    statFile.close();

    // string line;
    // string key;
    // vector<string> v_str;
    // while(statFile>>line)
    // {
    //     splitString(v_str,line,'|');
    //     for(vector<string>::iterator iter = v_str.begin();;++iter)        //First vector element is the key.
    //     {
    //         if(iter == v_str.begin())
    //         {
    //             fileMap[*iter]="Unavailable";
    //             key= *iter;
    //             continue;
    //         }
    //         fileMap[key]= *iter;
    //         break;
    //     }
    // }
}

void Statistics::Write(char *fromWhere){
	ofstream statFile;
    statFile.open(fromWhere);
    if(!statFile) {
    	cout<<"FileError: Cannot open the statfile file"<<endl;
        return;
    }

    for(map<string, RelationInfo*>::iterator it1 = statMap.begin(); it1!=statMap.end(); it1++){
        statFile << "relation" << endl << it1->first << endl << it1->second->numTuples <<endl;
        statFile << "attributes" << endl;
        for (map<string,int>::iterator it2 = it1->second->attributes.begin(); it2!=it1->second->attributes.end(); it2++){
			// char * watt = new char[it2->first.length()+1];
			// strcpy(watt, it2->first.c_str());
			// fprintf(statfile,"%s\n%d\n", watt, it2->second);
			statFile << it2->first << endl << it2->second << endl;
		}
    }
    statFile << "EOF";
    statFile.close();
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
			cout << attributeName << " : " << numDistincts << endl;
		}
	}
}