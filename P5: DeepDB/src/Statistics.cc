#include "Statistics.h"

RelationInfo::RelationInfo(){};

RelationInfo::RelationInfo(int numTuples){this->numTuples = numTuples;}

Statistics::Statistics(){}

Statistics::Statistics(Statistics &copyMe){
	for (map<string, RelationInfo*>::iterator it1 = copyMe.statMap.begin(); it1!=copyMe.statMap.end(); it1++){
		RelationInfo *relationInfo = new RelationInfo(it1->second->numTuples); // create new RelationInfo instance
		for (map<string, int>::iterator it2 = it1->second->attributes.begin(); it2!=it1->second->attributes.end(); it2++){ // copy all the attributes
			relationInfo->attributes[it2->first] = it2->second;
		}
		statMap[it1->first] = relationInfo; // set the <key, value> pair
	}
}

Statistics::~Statistics(){
	for (map<string, RelationInfo*>::iterator it1 = statMap.begin(); it1!=statMap.end(); it1++){ // loop through all the relations
		delete it1->second; // delete RelationInfo instance
		statMap.erase(it1->first); // remove the key
	}
}

void Statistics::AddRel(char *relName, int numTuples){
	RelationInfo *relationInfo = new RelationInfo(numTuples);
	statMap[string(relName)] = relationInfo;
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts){
	map<string, RelationInfo*>::iterator it = statMap.find(string(relName)); // find the relation by the name relName
	if (numDistincts == -1){
		numDistincts = it->second->numTuples;
	}
	it->second->attributes[string(attName)] = numDistincts;
}

void Statistics::CopyRel(char *oldName, char *newName){
	map<string,RelationInfo*>::iterator it1 = statMap.find(string(oldName)); // get old relation
	RelationInfo *newRelationInfo = new RelationInfo(it1->second->numTuples); // create RelationInfo for new relation
	for (map<string,int>::iterator it2 = it1->second->attributes.begin(); it2!=it1->second->attributes.end(); it2++){ // copy the attributes
		ostringstream attributeName;
		attributeName << newName << "." << it2->first; // new attribute name = newRelationName.oldAttributeName
		newRelationInfo->attributes[attributeName.str()] = it2->second;
	}
	statMap[newName] = newRelationInfo;
}
	
void Statistics::Read(char *fromWhere){
	for (map<string, RelationInfo*>::iterator it1 = statMap.begin(); it1!=statMap.end(); it1++){ // loop through all the relations
		delete it1->second; // delete RelationInfo instance
		statMap.erase(it1->first); // remove the key
	}
	statMap.clear();
	ifstream statFile;
	statFile.open(fromWhere);
	if(!statFile){ // if statFile not found then create new file and write "EOF" to it
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
		if(line.compare("relation") == 0){ // if start of new relation
			statFile >> line;
			string relationName(line);
			statFile >> line;
			RelationInfo *relationInfo = new RelationInfo(stoi(line));
			statFile >> line; 
			while(statFile>>line && line.compare("relation") != 0 && line.compare("EOF") != 0){ // while relation or EOF is not encountered
				string attributeName(line);
				statFile >> line;
				relationInfo->attributes[attributeName] = stoi(line); // read the line as an attribute for the current relation
			}
			statMap[relationName] = relationInfo; // create entry for the new relation in the statMap
		}
	}
	statFile.close();
}

void Statistics::Write(char *fromWhere){
	ofstream statFile;
	statFile.open(fromWhere);
	if(!statFile) {
		cout<<"FileError: Cannot open the statfile file"<<endl;
		return;
	}
	for(map<string, RelationInfo*>::iterator it1 = statMap.begin(); it1!=statMap.end(); it1++){ // for all relations
		statFile << "relation" << endl << it1->first << endl << it1->second->numTuples <<endl;
		statFile << "attributes" << endl;
		for (map<string,int>::iterator it2 = it1->second->attributes.begin(); it2!=it1->second->attributes.end(); it2++){ // for all the attributes
			statFile << it2->first << endl << it2->second << endl;
		}
	}
	statFile << "EOF";
	statFile.close();
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin){
	shouldApply = true;
	Estimate(parseTree, relNames, numToJoin);
	shouldApply = false;
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin){
	
	if(parseTree == NULL){
		if(numToJoin>1){
			// cout<<"Error: parseTree=NULL and numToJoin>1";
			return -1;
		}
		if(statMap.find(relNames[0]) == statMap.end()){
			cout<<"Error:"<<relNames[0]<<" not present"<<endl;
			exit(EXIT_FAILURE);
		}
		return statMap[relNames[0]]->numTuples;
	}
	double cost = 0.0, ANDMultiplier = 1.0, ORMultiplier = 1.0;
	struct AndList *ptrAND = parseTree;
	struct OrList *ptrOR;

	string leftAttribute, leftRelation, leftJoinRelation;
	string rightAttribute, rightRelation, rightJoinRelation;

	bool isJoin = false, isJoinPerformed = false;

	bool isDependentOnPreviousAttribute = false;
	string previousAttribute;

	bool isDependencyHandled = false;

	map<string, int> attributeToComparisonTypeMap;
	while (ptrAND != NULL) { // traversing the parseTree wrt AND

		ptrOR = ptrAND->left;
		ORMultiplier = 1.0;

		while (ptrOR != NULL) { // traversing the left child of ptrAND i.e. [OR child]
			isJoin = false;
			ComparisonOp *ptrComparisonOp = ptrOR->left;

			if (ptrComparisonOp->left->code != NAME) { // left child must be of type NAME
				cout<<"Error: left child is not of type NAME"<<endl;
				exit(EXIT_FAILURE);
				return -1;
			} else {
				leftAttribute = ptrComparisonOp->left->value;
				if(leftAttribute.compare(previousAttribute) == 0){ // leftAttribute is same as previous attribute
					isDependentOnPreviousAttribute=true; // i.e. leftAttribute is dependent on previous attribute
				}
				previousAttribute = leftAttribute; // cache this attribute to check for dependency with attributes encountered in the next iteration
				leftRelation = GetRelation(leftAttribute); // find the relation which has the leftAttribute
			}

			if (ptrComparisonOp->right->code == NAME) {// if right child is also of type NAME
				isJoin = true; // then it is a join
				isJoinPerformed = true; //
				rightAttribute = ptrComparisonOp->right->value;
				rightRelation = GetRelation(rightAttribute); // find the relation which has the leftAttribute
				if(leftRelation.compare(rightRelation) == 0){return -1;}
			}

			if (isJoin == true) { // if this is a join
				double leftNumDistinct = statMap[leftRelation]->attributes[leftAttribute]; //get num distinct of left attribute
				double rightNumDistinct = statMap[rightRelation]->attributes[rightAttribute]; //get num distinct of right attribute

				if (ptrComparisonOp->code == EQUALS) { // if equality
					ORMultiplier *= (1.0 - (1.0/max(leftNumDistinct, rightNumDistinct)));
				}
				leftJoinRelation = leftRelation;
				rightJoinRelation = rightRelation;
			} else { // if this is not a join
				if(isDependentOnPreviousAttribute){ // if leftAttribute depends on previous attribute
					if(!isDependencyHandled){
						ORMultiplier = 1.0-ORMultiplier;
						isDependencyHandled = true;
					}

					if (ptrComparisonOp->code == GREATER_THAN || ptrComparisonOp->code == LESS_THAN) { // if GREATER_THAN or LESS_THAN
						ORMultiplier += (1.0/3.0);
						attributeToComparisonTypeMap[leftAttribute] = ptrComparisonOp->code;
					}

					if (ptrComparisonOp->code == EQUALS) { // if EQUALS
							ORMultiplier += (1.0/(statMap[leftRelation]->attributes[leftAttribute]));
							attributeToComparisonTypeMap[leftAttribute] = ptrComparisonOp->code;
					}
				} else{
					if (ptrComparisonOp->code == GREATER_THAN || ptrComparisonOp->code == LESS_THAN) { // if GREATER_THAN or LESS_THAN
						ORMultiplier *= (2.0/3.0);
						attributeToComparisonTypeMap[leftAttribute] = ptrComparisonOp->code;
					}

					if (ptrComparisonOp->code == EQUALS) { // if EQUALS
						ORMultiplier *= (1.0-(1.0 / statMap[leftRelation]->attributes[leftAttribute]));
						attributeToComparisonTypeMap[leftAttribute] = ptrComparisonOp->code;
					}
				}
			}
			ptrOR = ptrOR->rightOr; // go to the right OR child
		}
		if(!isDependentOnPreviousAttribute){
			ORMultiplier = 1.0-ORMultiplier;
		}
		isDependentOnPreviousAttribute = false;
		isDependencyHandled = false;

		ANDMultiplier *= ORMultiplier;
		ptrAND = ptrAND->rightAnd; // go to the right AND child
	}

	double rightNumTuples;
	if(statMap.find(rightRelation) != statMap.end()){
		rightNumTuples = statMap[rightRelation]->numTuples;
	}

	if (isJoinPerformed == true) {
		double leftNumTuples = statMap[leftJoinRelation]->numTuples;
		cost = leftNumTuples*rightNumTuples*ANDMultiplier; // cost for join
	} else {
		double leftNumTuples = statMap[leftRelation]->numTuples;
		cost = leftNumTuples * ANDMultiplier; // cost if not a join
	}
	if (shouldApply) { // if the estimation is to be applied
		if(isJoinPerformed){ // if it is join
			statMap[leftJoinRelation + "_" + rightJoinRelation] = new RelationInfo(); // add new relation to statMap
		}
		map<string, int>::iterator iter_attributeToComparisonTypeMap, iter_NumDistinctAttribute;
		set<string> joinAttributesProcessedSet;
		if (isJoinPerformed){
			for (iter_attributeToComparisonTypeMap = attributeToComparisonTypeMap.begin(); iter_attributeToComparisonTypeMap != attributeToComparisonTypeMap.end(); iter_attributeToComparisonTypeMap++) {
				for (int i = 0; i < statMap.size()-1 && i<numToJoin ; i++) {
					int attributeCount = 0;
					if(relNames[i] == NULL){
						continue;
					}

					if(statMap.find(relNames[i]) != statMap.end()){ // if relNames[i] is in statMap
						attributeCount = statMap[relNames[i]]->attributes.count(iter_attributeToComparisonTypeMap->first); // find how many attributes have iter_attributeToComparisonTypeMap->first
					}

					if (attributeCount == 0){ // if none
						continue; // then skip this relation
					}
					else if (attributeCount == 1) { // if 1
						for (iter_NumDistinctAttribute = statMap[relNames[i]]->attributes.begin(); iter_NumDistinctAttribute != statMap[relNames[i]]->attributes.end(); iter_NumDistinctAttribute++) {
							if ((iter_attributeToComparisonTypeMap->second == LESS_THAN) || (iter_attributeToComparisonTypeMap->second == GREATER_THAN)) {
								statMap[leftJoinRelation + "_" + rightJoinRelation]->attributes[iter_NumDistinctAttribute->first] = (int)round((double)(iter_NumDistinctAttribute->second) / 3.0);
							} else if (iter_attributeToComparisonTypeMap->second == EQUALS) { // if equality
								if (iter_attributeToComparisonTypeMap->first == iter_NumDistinctAttribute->first) { // left and right attributes are same
									statMap[leftJoinRelation + "_" + rightJoinRelation]->attributes[iter_NumDistinctAttribute->first] = 1; // then 1 distinct
								} else{ // left and right attributes are not same
									statMap[leftJoinRelation + "_" + rightJoinRelation]->attributes[iter_NumDistinctAttribute->first] = min((int)round(cost), iter_NumDistinctAttribute->second); // min of the two
								}
							}
						}
						break;
					} else if (attributeCount > 1) {
						for (iter_NumDistinctAttribute = statMap[relNames[i]]->attributes.begin(); iter_NumDistinctAttribute != statMap[relNames[i]]->attributes.end(); iter_NumDistinctAttribute++) {
							if (iter_attributeToComparisonTypeMap->second == EQUALS) { // if equality
								if (iter_attributeToComparisonTypeMap->first == iter_NumDistinctAttribute->first) { // left and right attributes are same
									statMap[leftJoinRelation + "_" + rightJoinRelation]->attributes[iter_NumDistinctAttribute->first] = attributeCount;
								} else{
									statMap[leftJoinRelation + "_" + rightJoinRelation]->attributes[iter_NumDistinctAttribute->first] = min((int) round(cost), iter_NumDistinctAttribute->second); // then min of the two
								}
							}
						}
						break;
					}
					joinAttributesProcessedSet.insert(relNames[i]); // add the relation to the set of processed relations
				}
			}
			if (joinAttributesProcessedSet.count(leftJoinRelation) == 0) { // if leftJoinRelation is not in processed set
				for (map<string, int>::iterator entry = statMap[leftJoinRelation]->attributes.begin(); entry != statMap[leftJoinRelation]->attributes.end(); entry++) {
					statMap[leftJoinRelation + "_" + rightJoinRelation]->attributes[entry->first] = entry->second; // then add all attributes from leftJoinRelation into the joined relation
				}
			}
			if (joinAttributesProcessedSet.count(rightJoinRelation) == 0) { // if rightJoinRelation is not in processed set
				for (map<string, int>::iterator entry = statMap[rightJoinRelation]->attributes.begin(); entry != statMap[rightJoinRelation]->attributes.end(); entry++) {
					statMap[leftJoinRelation + "_" + rightJoinRelation]->attributes[entry->first] = entry->second; // then add all attributes from rightJoinRelation into the joined relation
				}
			}
			statMap[leftJoinRelation + "_" + rightJoinRelation]->numTuples = round(cost); // set the cost of the joined relation
			delete statMap[leftJoinRelation]; // free RelationInfo instance of leftJoinRelation
			delete statMap[rightJoinRelation]; // free RelationInfo instance of rightJoinRelation
			statMap.erase(leftJoinRelation); // erase entry leftJoinRelation from statMap
			statMap.erase(rightJoinRelation); // clear entry of rightJoinRelation from statMap
		}
	}
	return cost;
}

string Statistics::GetRelation(string attribute){ // get the relation from statMap which has a <attribute>
	string relation;
	for (map<string, RelationInfo*>::iterator it1 = statMap.begin(); it1 != statMap.end(); it1++) {
		map<string, int>::iterator it2 = it1->second->attributes.find(attribute);
		if(it2 != it1->second->attributes.end()){
			relation = it1->first;
			return relation;
		}
	}
}

void Statistics::Print(){ // Prints the statMap on the cmd line
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