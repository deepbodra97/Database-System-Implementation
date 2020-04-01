#include "Statistics.h"

RelationInfo::RelationInfo(){
};

RelationInfo::RelationInfo(int numTuples, int numRelations){
	this->numTuples = numTuples;
	this->numRelations = numRelations;
}

Statistics::Statistics(){
	isCalledFrmApply = false;
    isApply = false;
}

Statistics::Statistics(Statistics &copyMe){
	for (map<string, RelationInfo*>::iterator it1 = copyMe.statMap.begin(); it1!=copyMe.statMap.end(); it1++){
		RelationInfo *relationInfo = new RelationInfo(it1->second->numTuples, it1->second->numRelations);		

		for (map<string, int>::iterator it2 = it1->second->attributes.begin(); it2!=it1->second->attributes.end(); it2++){
			relationInfo->attributes[it2->first] = it2->second;
		}
		statMap[it1->first] = relationInfo;
	}
}

Statistics::~Statistics(){
	for (map<string, RelationInfo*>::iterator it1 = statMap.begin(); it1!=statMap.end(); it1++){
		delete it1->second;
		statMap.erase(it1->first);
	}
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
	// tempResult = 0.0;

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
			statFile << it2->first << endl << it2->second << endl;
		}
    }
    statFile << "EOF";
    statFile.close();
}
/*
double Statistics::ApplyEstimate(struct AndList *parseTree, char **relNames, int numToJoin) { // apply and return
	double estimate = Estimate(parseTree, relNames, numToJoin);
	Apply(parseTree, relNames, numToJoin);
	return estimate;
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin){
	struct AndList * andlist = parseTree;
	struct OrList * orlist;
	while (andlist != NULL){
		if (andlist->left != NULL){
			orlist = andlist->left;
			while(orlist != NULL){
				if (orlist->left->left->code == 3 && orlist->left->right->code == 3){//
					map<string, int>::iterator itAtt[2];
					map<string, RelationInfo*>::iterator itRel[2];
					string joinAtt1(orlist->left->left->value);
					string joinAtt2(orlist->left->right->value);
					for (map<string, RelationInfo*>::iterator iit = statMap.begin(); iit!=statMap.end(); ++iit){
						itAtt[0] = iit->second->attributes.find(joinAtt1);
						if(itAtt[0] != iit->second->attributes.end()){
							itRel[0] = iit;
							break;
						}
					}
					for (map<string, RelationInfo*>::iterator iit = statMap.begin(); iit!=statMap.end(); ++iit){
						itAtt[1] = iit->second->attributes.find(joinAtt2);
						if(itAtt[1] != iit->second->attributes.end()){
							itRel[1] = iit;
							break;
						}
					}
					RelationInfo joinedRel;
					char * joinName = new char[200];
					sprintf(joinName, "%s|%s", itRel[0]->first.c_str(), itRel[1]->first.c_str());
					string joinNamestr(joinName);
					joinedRel.numTuples = tempResult;
					joinedRel.numRelations = numToJoin;
					for(int i = 0; i < 2; i++){
						for (map<string, int>::iterator iit = itRel[i]->second->attributes.begin(); iit!=itRel[i]->second->attributes.end(); ++iit){
							joinedRel.attributes.insert(*iit);
						}
						statMap.erase(itRel[i]);
					}
					statMap.insert(pair<string, RelationInfo*>(joinNamestr, &joinedRel));
				}
				else{
					string seleAtt(orlist->left->left->value);
					map<string, int>::iterator itAtt;
					map<string, RelationInfo*>::iterator itRel;
					for (map<string, RelationInfo*>::iterator iit = statMap.begin(); iit!=statMap.end(); ++iit){
						itAtt = iit->second->attributes.find(seleAtt);
						if(itAtt != iit->second->attributes.end()){
							itRel = iit;
							break;
						}
					}
					itRel->second->numTuples = tempResult;
					
				}
				orlist = orlist->rightOr;
			}
		}
		andlist = andlist->rightAnd;
	}
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin){
	struct AndList * andlist = parseTree;
	struct OrList * orlist;
	double result = 0.0, fraction = 1.0;
	int state = 0;
    if (andlist == NULL) {
		if (numToJoin>1) return -1;
		if(statMap.find(relNames[0]) == statMap.end()){
			string("Error: Relation ")+relNames[0]+" does not exist";
			// exit(EXIT_FAILURE);
		}
      	return statMap[relNames[0]]->numTuples;
    }
	while (andlist != NULL){
		if (andlist->left != NULL){
			orlist = andlist->left;
			double fractionOr = 0.0;
			map<string, int>::iterator lastAtt;
			while(orlist != NULL){
				if (orlist->left->left->code == 3 && orlist->left->right->code == 3){
					map<string, int>::iterator itAtt[2];
					map<string, RelationInfo*>::iterator itRel[2];
					string joinAtt1(orlist->left->left->value);
					string joinAtt2(orlist->left->right->value);

					for (map<string, RelationInfo*>::iterator iit = statMap.begin(); iit!=statMap.end(); ++iit){
						itAtt[0] = iit->second->attributes.find(joinAtt1);
						if(itAtt[0] != iit->second->attributes.end()){
							itRel[0] = iit;
							break;
						}
					}
					for (map<string, RelationInfo*>::iterator iit = statMap.begin(); iit!=statMap.end(); ++iit){
						itAtt[1] = iit->second->attributes.find(joinAtt2);
						if(itAtt[1] != iit->second->attributes.end()){
							itRel[1] = iit;
							break;
						}
					}
					
					double max;
					if (itAtt[0]->second >= itAtt[1]->second)		max = (double)itAtt[0]->second;
					else		max = (double)itAtt[1]->second;
					if (state == 0)
						result = (double)itRel[0]->second->numTuples*(double)itRel[1]->second->numTuples/max;
					else
						result = result*(double)itRel[1]->second->numTuples/max;
					
					//cout << "max " << max << endl;
					//cout << "join result: " << result << endl;
					state = 1;
				}
				else{
					string seleAtt(orlist->left->left->value);
					map<string, int>::iterator itAtt;
					map<string, RelationInfo*>::iterator itRel;
					for (map<string, RelationInfo*>::iterator iit = statMap.begin(); iit!=statMap.end(); ++iit){
						itAtt = iit->second->attributes.find(seleAtt);
						if(itAtt != iit->second->attributes.end()){
							itRel = iit;
							break;
						}
					}
					if (result == 0.0)
						result = ((double)itRel->second->numTuples);
					double tempFrac;
					if(orlist->left->code == 7)
						tempFrac = 1.0 / itAtt->second;
					else
						tempFrac = 1.0 / 3.0;
					if(lastAtt != itAtt)
						fractionOr = tempFrac+fractionOr-(tempFrac*fractionOr);
					else
						fractionOr += tempFrac;
					//cout << "fracOr: " << fractionOr << endl;
					lastAtt = itAtt;//
				}
				orlist = orlist->rightOr;
			}
			if (fractionOr != 0.0)
				fraction = fraction*fractionOr;
			//cout << "frac: " << fraction << endl;
		}
		andlist = andlist->rightAnd;
	}
	result = result * fraction;
	//cout << "result " << result << endl;
	tempResult = result;
	return result;
}*/

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{

		isCalledFrmApply = true;
        isApply = true;
        Estimate(parseTree, relNames, numToJoin);
        isApply = false;
        isCalledFrmApply = false;

}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{


    double resultEstimate = 0.0;
    // TODO error checking
    struct AndList *currentAnd;
    struct OrList *currentOr;

    currentAnd = parseTree;

    string leftRelation;
    string rightRelation;

    string leftAttr;
    string rightAttr;

    string joinLeftRelation, joinRightRelation;

    bool isJoin = false;
    bool isJoinPerformed = false;

    bool isdep = false;
    bool done = false;
    string prev;

    double resultANDFactor = 1.0;
    double resultORFactor = 1.0;

    map<string, int> relOpMap;

	//And list is structured as a root, a orlist the left and andlist to the right.
	//Or list is structured as a root, a comparison the left and orlist to the right.
	//a comparison is structed as a root and operands to the left and right.
	//operands consists of a code and value.
    while (currentAnd != NULL) {
        currentOr = currentAnd->left;
        resultORFactor = 1.0;

       	while (currentOr != NULL) {
	        isJoin = false;
	        ComparisonOp *currentCompOp = currentOr->left;


	        // find relation of left attribute
			//first attribute has to be a name
	        if (currentCompOp->left->code != NAME) {
	                cout << "LEFT should be attribute name" << endl;
	                return 0;
	        } else {

				//find the relation where the attribute lies.
                leftAttr = currentCompOp->left->value;

				#ifdef _DEP
				if(strcmp(leftAttr.c_str(),prev.c_str())==0){
					isdep=true;
				}
				prev = leftAttr;
				#endif
                for (map<string, RelationInfo*>::iterator it1 = statMap.begin(); it1 != statMap.end(); it1++) {
                    cout<<isCalledFrmApply<<":"<<it1->second->attributes["l_returnflag"]<<endl;
                    map<string, int>::iterator it2 = it1->second->attributes.find(leftAttr);
					if(it2 != it1->second->attributes.end()){
						leftRelation = it1->first;
						break;
					}
                }
            }

            // find relation of right attribute
            if (currentCompOp->right->code == NAME) {//the right operand is a name too hence it is a join
                isJoin = true;
                isJoinPerformed = true;
                rightAttr = currentCompOp->right->value;
				//find right relation
                for (map<string, RelationInfo*>::iterator it1 = statMap.begin(); it1 != statMap.end(); it1++) {
                    map<string, int>::iterator it2 = it1->second->attributes.find(rightAttr);
					if(it2 != it1->second->attributes.end()){
						rightRelation = it1->first;
						break;
					}
                }
            }

            if (isJoin == true) {
				//find distinct counts of both attributes for the relations.
                double leftDistinctCount = statMap[leftRelation]->attributes[currentCompOp->left->value];
                double rightDistinctCount = statMap[rightRelation]->attributes[currentCompOp->right->value];

                if (currentCompOp->code == EQUALS) {
                        resultORFactor *=(1.0 - (1.0 / max(leftDistinctCount, rightDistinctCount)));//ORFACTOR??
                }

                joinLeftRelation = leftRelation;
                joinRightRelation = rightRelation;
            } else {
		
				#ifdef _DEP
				if(isdep){
					if(!done){
						resultORFactor =1.0 -resultORFactor;
						done = true;
					}

				    if (currentCompOp->code == GREATER_THAN || currentCompOp->code == LESS_THAN) {
	                    resultORFactor += (1.0 / 3.0);
	                    relOpMap[currentCompOp->left->value] = currentCompOp->code;
	                }

		            if (currentCompOp->code == EQUALS) {
		                    resultORFactor +=(1.0 / ((*attrData)[leftRelation][currentCompOp->left->value]));
		                    relOpMap[currentCompOp->left->value] = currentCompOp->code;
		            }


					#ifdef _DEBUG
		            cout<<"or "<<std::setprecision (15) <<resultORFactor<<endl;
				    cout<<"ikr "<< statMap[leftRelation]->attributes[currentCompOp->left->value]<<endl;
		            #endif
				} else{
                    if (currentCompOp->code == GREATER_THAN || currentCompOp->code == LESS_THAN) {
	                    resultORFactor *= (2.0 / 3.0);
	                    relOpMap[currentCompOp->left->value] = currentCompOp->code;
                    }
                    if (currentCompOp->code == EQUALS) {
                        resultORFactor *=(1.0- (1.0 / (*attrData)[leftRelation][currentCompOp->left->value]));
                        relOpMap[currentCompOp->left->value] = currentCompOp->code;
	                }
					#ifdef _DEBUG
		            cout<<"or"<<resultORFactor<<endl;
					cout<<"ikr "<< statMap[leftRelation]->attributes[currentCompOp->left->value]<<endl;
		            #endif
				}

				#else

				if (currentCompOp->code == GREATER_THAN || currentCompOp->code == LESS_THAN) {
	                resultORFactor *= (2.0 / 3.0);
	                relOpMap[currentCompOp->left->value] = currentCompOp->code;
	            }
	            
	            if (currentCompOp->code == EQUALS) {
                    resultORFactor *=(1.0- (1.0 / statMap[leftRelation]->attributes[currentCompOp->left->value]));
                    relOpMap[currentCompOp->left->value] = currentCompOp->code;
	            }
				#endif
            }
            currentOr = currentOr->rightOr;
        }

	    #ifdef _DEP
	    if(!isdep)
			resultORFactor =1.0 -resultORFactor;	
        #else
	    	resultORFactor =1.0 -resultORFactor;
	    #endif

		#ifdef _DEBUG
			cout<<"prev and"<<resultANDFactor <<" or "<< resultORFactor<<"curr and "<< resultANDFactor*resultORFactor<<endl;
		#endif

	    isdep=false;
	    done =false;

        resultANDFactor *= resultORFactor;
        currentAnd = currentAnd->rightAnd;
    }


    double rightTupleCount = statMap[rightRelation]->numTuples;

    if (isJoinPerformed == true) {
        double leftTupleCount = statMap[joinLeftRelation]->numTuples;
        resultEstimate = leftTupleCount * rightTupleCount * resultANDFactor;
    } else {
        double leftTupleCount = statMap[leftRelation]->numTuples;
        resultEstimate = leftTupleCount * resultANDFactor;
    }

    if (isApply) {
		statMap[joinLeftRelation + "_" + joinRightRelation] = new RelationInfo();
	    map<string, int>::iterator relOpMapITR, distinctCountMapITR;
	    set<string> addedJoinAttrSet;
	    if (isJoinPerformed){
            for (relOpMapITR = relOpMap.begin(); relOpMapITR != relOpMap.end(); relOpMapITR++) {
                for (int i = 0; i < statMap.size()-1; i++) {
                    if (relNames[i] == NULL){
                        continue;
                    }
                    int cnt = statMap[relNames[i]]->attributes.count(relOpMapITR->first);
                    if (cnt == 0){
                        continue;
                    }
                    else if (cnt == 1) {
                        for (distinctCountMapITR = statMap[relNames[i]]->attributes.begin(); distinctCountMapITR != statMap[relNames[i]]->attributes.end(); distinctCountMapITR++) {
                            if ((relOpMapITR->second == LESS_THAN) || (relOpMapITR->second == GREATER_THAN)) {
                            	statMap[joinLeftRelation + "_" + joinRightRelation]->attributes[distinctCountMapITR->first] = (int)round((double)(distinctCountMapITR->second) / 3.0);
                            } else if (relOpMapITR->second == EQUALS) {
                                if (relOpMapITR->first == distinctCountMapITR->first) { //same attribute on which condition is imposed
                                	statMap[joinLeftRelation + "_" + joinRightRelation]->attributes[distinctCountMapITR->first] = 1;
                                } else{
                                	statMap[joinLeftRelation + "_" + joinRightRelation]->attributes[distinctCountMapITR->first] = min((int)round(resultEstimate), distinctCountMapITR->second);
                                }
                            }
                        }
                        break;
                    } else if (cnt > 1) {
	                    for (distinctCountMapITR = statMap[relNames[i]]->attributes.begin(); distinctCountMapITR != statMap[relNames[i]]->attributes.end(); distinctCountMapITR++) {
                            if (relOpMapITR->second == EQUALS) {
                                if (relOpMapITR->first == distinctCountMapITR->first) {
                                	statMap[joinLeftRelation + "_" + joinRightRelation]->attributes[distinctCountMapITR->first] = cnt;
                                } else{
                                	statMap[joinLeftRelation + "_" + joinRightRelation]->attributes[distinctCountMapITR->first] = min((int) round(resultEstimate), distinctCountMapITR->second);
                                }
                            }
	                    }
	                    break;
                    }
                    addedJoinAttrSet.insert(relNames[i]);
                }
	        }
            if (addedJoinAttrSet.count(joinLeftRelation) == 0) {
                for (map<string, int>::iterator entry = statMap[joinLeftRelation]->attributes.begin(); entry != statMap[joinLeftRelation]->attributes.end(); entry++) {
                	statMap[joinLeftRelation + "_" + joinRightRelation]->attributes[entry->first] = entry->second;
                }
            }
            if (addedJoinAttrSet.count(joinRightRelation) == 0) {
                for (map<string, int>::iterator entry = statMap[joinRightRelation]->attributes.begin(); entry != statMap[joinRightRelation]->attributes.end(); entry++) {
                	statMap[joinLeftRelation + "_" + joinRightRelation]->attributes[entry->first] = entry->second;
                }
            }
            statMap[joinLeftRelation + "_" + joinRightRelation]->numTuples = round(resultEstimate);
            delete statMap[joinLeftRelation];
            delete statMap[joinRightRelation];
            statMap.erase(joinLeftRelation);
            statMap.erase(joinRightRelation);

            // attrData->erase(joinLeftRelation);
            // attrData->erase(joinRightRelation);
        }
    }
    return resultEstimate;
}