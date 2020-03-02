#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <fstream>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

SortedFile::SortedFile () {
	inPipe = new Pipe(100);	
	outPipe = new Pipe(100);	
	bq = NULL;
	// file = new File();
	isDirty=0;
	sortInfo = NULL;
	current = new Record();	
	// page = new Page();		// store contents of current page from where program is reading
	mergePage = new Page();			// used for merging existing records with those from BigQ
	fileMode = READ;								// Mode set to read
	OrderMaker *queryOrder = NULL;		// queryOrder used in GetNext(3 parameters)
	bool queryChange = true;			// maintains if query is changed
}

void* SortedFile::instantiate_BigQ(void* arg){

	thread_arguments *args;
	args = (thread_arguments *) arg;

	//cout<<"check   "<<(args.s).runLength;
	//cout<<"t check "<<args->in<<"\n";

	args->b = new BigQ(*(args->in),*(args->out),*((args->s).myOrder),(args->s).runLength);

}

int SortedFile::Create (char *f_path, fType f_type, void *startup) {	// done
	file.Open(0,f_path);	

	fileName = (char *)malloc(sizeof(f_path)+1);
	strcpy(fileName,f_path);
	isDirty=0;
	
	// use startup to get runlength and ordermaker
	sortInfo = (SortInfo *) startup;
	currentPageNumber=1;
	recordIndex = 0;
	isThisEndOfFile=1;
	return 1;
}

void SortedFile::Load (Schema &f_schema, char *loadpath) {		// requires BigQ instance done

	if(fileMode!=WRITE){
		fileMode = WRITE;
		isDirty=1;
		// create input, output pipe and BigQ instance
		//if(inPipe==NULL)inPipe = new Pipe(100);	// requires sortInfoze ?
		//if(outPipe==NULL)outPipe = new Pipe(100);
		if(bq==NULL)bq = new BigQ(*inPipe,*outPipe,*(sortInfo->myOrder),sortInfo->runLength);
	}

	FILE* tableFile = fopen (loadpath,"r");
	Record temp;//need reference see below, make a record

	while(temp.SuckNextRecord(&f_schema,tableFile)!=0)
		inPipe->Insert(&temp);	// pipe blocks and record is consumed or is buffering required ?

	fclose(tableFile);	
}

int SortedFile::Open (char *f_path) {


	isDirty=0;
	char *fName = new char[20];
	sprintf(fName, "%s.meta", f_path);
//	FILE *f = fopen(fName,"r");

	fileName = (char *)malloc(sizeof(f_path)+1);
	strcpy(fileName,f_path);

	//cout<<"reading metadata"<<endl;
	// to decide what to store in meta file
	// and parse and get sort order and run length
	// requires some kind of de serialization
	// initialize it

	ifstream ifs(fName,ios::binary);

	ifs.seekg(sizeof(fileName)-1);//,ifs.beg);
	
	if(sortInfo==NULL){
		sortInfo = new SortInfo;
		sortInfo->myOrder = new OrderMaker();
	}


	ifs.read((char*)sortInfo->myOrder, sizeof(*(sortInfo->myOrder)));


	//cout<<"read ordermaker"<<endl;
	//sortInfo->myOrder->Print();

/*	if (ifs)
      		cout << "all characters read successfully.";
    	else
    		cout << "error: only " << ifs.gcount() << " could be read";
*/

	//ifs.seekg(sortInfo(*(sortInfo->myOrder))-1);

	ifs.read((char*)&(sortInfo->runLength), sizeof(sortInfo->runLength));


//	cout<<"read run length "<<sortInfo->runLength<<endl;


	//ofs.write((char*)sortInfo->myOrder,sortInfo(*(sortInfo->myOrder)));	
	//ofs.write((char*)&(sortInfo->runLength),sortInfo(sortInfo->runLength));



	//ifs.sync();

	/*if (ifs)
      		cout << "all characters read successfully.";
    	else
    		cout << "error: only " << ifs.gcount() << " could be read";
*/


	ifs.close();

	fileMode = READ;


	//cout<<"sortInfoze of f name "<<sortInfo(fileName)-1<<endl;

	//cout<<"Dereferencing sortInfo\n";
	//cout<<"sortInfo "<<sortInfo<<endl;

	//sleep(2);

	//cout<<"sortInfo runlength\n";
	//cout<<"sortInfo "<<sortInfo->runLength<<endl;

	//cout<<"Dereferencing sortInfo ordermaker \n";
	//cout<<"sortInfo OM "<<sortInfo->myOrder<<endl;

	//MoveFirst();

	// set to read mode
	// bring first page into read buffer and initialize first record

	//fclose(f);

	file.Open(1, f_path);	// open the corresponding file
	currentPageNumber = 1;
	recordIndex = 0;
	isThisEndOfFile = 0;
}

void SortedFile::MoveFirst () {			// requires MergeFromOuputPipe()

	isDirty=0;	
	currentPageNumber = 1;
	recordIndex = 0;

	if(fileMode==READ){
		// In read mode, so direct movefirst is possortInfoble

	
		if(file.GetLength()!=0){
			file.GetPage(&page,currentPageNumber); //TODO: check off_t type,  void GetPage (Page *putItHere, off_t whichPage)
	
			int result = page.GetFirst(current);
			//cout<<result<<endl;
			
		//	currentPageNumber = 1;
			
		}
		else{
		//	currentPageNumber = 1;

		}

	}
	else{
		// change mode to read
		fileMode = READ;
		// Merge contents if any from BigQ
		MergeFromOutpipe();
		file.GetPage(&page,currentPageNumber); //TODO: check off_t type,  void GetPage (Page *putItHere, off_t whichPage)
		page.GetFirst(current);

		
		// bring the first page into page
		// Set curr Record to first record
		// 
	}
	queryChange = true;
}

int SortedFile::Close () {			// requires MergeFromOuputPipe()	done
	
	if(fileMode==WRITE)	
		MergeFromOutpipe();

	file.Close();
	isDirty=0;
	isThisEndOfFile = 1;
	// write updated state to meta file

	char fName[30];
	sprintf(fName,"%s.meta",fileName);

	ofstream out(fName);
    	out <<"sorted"<<endl;
    	out.close();


	ofstream ofs(fName,ios::binary|ios::app);

	ofs.write((char*)sortInfo->myOrder,sizeof(*(sortInfo->myOrder)));	
	ofs.write((char*)&(sortInfo->runLength),sizeof(sortInfo->runLength));

	ofs.close();
}

void SortedFile::Add (Record &rec) {	// requires BigQ instance		done

	//cout<<"check "<<inPipe<<"\n";
	

	
	//inPipe->ShutDown();
	//cout<<fileMode<<"\n";

	if(fileMode!=WRITE){
		isDirty=1;
		fileMode = WRITE;
		//cout<<fileMode<<"\n";

		// create input, output pipe and BigQ instance
		/*if(inPipe==NULL){
			inPipe = new Pipe(100);	// requires sortInfoze ?
			cout<<"Setting up input pipe\n";		
		}
		if(outPipe==NULL){
			outPipe = new Pipe(100);
			cout<<"Setting up output pipe\n";
		}*/

		inPipe= new Pipe(100);
		outPipe= new Pipe(100);

		if(bq==NULL){

			//cout<<"run length "<<sortInfo->runLength<<"\n";
			thread_args.in = inPipe;
			thread_args.out = outPipe;
			thread_args.s.myOrder = sortInfo->myOrder;
			thread_args.s.runLength =  sortInfo->runLength;
			thread_args.b = bq;

			

			pthread_create(&bigQ_t, NULL, &SortedFile::instantiate_BigQ , (void *)&thread_args);


			//cout<<"Setting up BigQ"<<endl;
		}
	}
	inPipe->Insert(&rec);
	queryChange = true;

	//cout<<"record adr"<<&rec<<"\n";
	

	//cout << inPipe<<"\n";
	//inPipe= new Pipe(100);
	
		// pipe blocks and record is consumed or is buffering required ?

	//cout <<"Added\n";
}

int SortedFile::GetNext (Record &fetchme) {		// requires MergeFromOuputPipe()		done

	if(fileMode!=READ){
		isDirty=0;
		fileMode = READ;
		page.EmptyItOut();		// requires flush
		MergeFromOutpipe();		// 
		MoveFirst();	// always start from first record

	}

	/*if(isThisEndOfFile==1) return 0;

	fetchme.Copy(current);

	if(!page.GetFirst(current)) {

		if(currentPageNumber>=this->file.GetLength()-2){
				isThisEndOfFile = 1;
				return 0;	
		}
		else {
			currentPageNumber++;
			file.GetPage(&page,currentPageNumber);
			page.GetFirst(current);

		}
	}

	return 1;*/


	if(isThisEndOfFile){ // if the end of the file has been reached
		return 0;
	}
	fetchme.Copy(current); // Consume?

	if(page.GetFirst(current) == 1){ // is there a record to fetch?
		// readPageRecordNumber++;
		return 1;
	}

	currentPageNumber+=1; // page has been consumed. Increment page number
	if(currentPageNumber == file.GetLength()-1){ // if there is no next page return 0
		isThisEndOfFile = 1;
		return 1;
	}

	file.GetPage(&page, currentPageNumber); // get the next page
	// readPageNumber+=1;
	if(page.GetFirst(current) == 1){ // record found
		// readPageRecordNumber++;
		return 1;
	}
}

int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {		// requires binary search // requires MergeFromOuputPipe()

	//cout<<"1. here\n";


	//Schema nu("catalog","lineitem");
	//literal.Print(&nu);

	if(fileMode!=READ){
	
		isDirty=0;
		fileMode = READ;
		page.EmptyItOut();		// requires flush
		MergeFromOutpipe();		// 
		MoveFirst();	// always start from first record
		
		//cout<<"2. here\n";

	
	}	

	//cout<<"3. here\n";
		
	// TODO: update queryChange
	
	if(queryChange != true){		// current query same last CNF query
	
	}
	else{				// query changed ; need to construct new queryOrder
	
		//cout<<"4. here\n"<<sortInfo<<" addr \n";
		queryOrder = cnf.CreateQueryMaker(*(sortInfo->myOrder));		
		//queryOrder = checkIfMatches(cnf, *(sortInfo->myOrder));
		
		
	}
	
	ComparisonEngine *comp;
		
	if(queryOrder==NULL) {		// no compatible order maker; return first record that matches the Record literal
			
		//cout<<"Empty Query OM\n";		

		while(GetNext(fetchme)){			// linear scan from current record

			
			if(comp->Compare(&fetchme, &literal, &cnf)) {		//record found, return 1
				return 1;
			}
		}
		return 0;					// record not found
	}
	else{							 // some order maker compatible to given CNF is constructed
	
		Record *r1 = new Record();

		cout<<"matchpage called usortInfong "<< queryOrder<<endl;
			
		r1 = GetMatchPage(literal);
		
		if(r1==NULL) return 0;
		
		fetchme.Consume(r1);
		
		if(comp->Compare(&fetchme,&literal,&cnf)){
			 return 1;
		}
		
		while(GetNext(fetchme)) {
			if(comp->Compare(&fetchme, &literal, queryOrder)!=0) {
				//not match to query order
				return 0;
			} else {
				if(comp->Compare(&fetchme, &literal, &cnf)) {
					//find the right record
					return 1;
				}
			}
		}
	
	}
	return 0;
}


OrderMaker *SortedFile::checkIfMatches(CNF &cnf, OrderMaker &sortOrder) {
	

	OrderMaker *matchOrder= new OrderMaker;	// create new order make
	//cout<<"ordermaker";
	//sortOrder.Print();
	//cout<<"\n";
	//cout<<"cnf";
	//cnf.Print();

	
	for(int i = 0; i<sortOrder.numAtts; i++) {	// over every attribute of sortorder

		//cout <<"now checking att: "<<sortOrder.whichAtts[i]<<endl;
		//cout<<sortOrder.numAtts<<"\n";
		//cout<<sortOrder.whichTypes[i]<<"\n";

		bool match = false;
		
		for(int j = 0; j<cnf.numAnds; j++) {			// 

			if(!match) {
				
				for(int k=0; k<cnf.orLens[j]; k++) {	//
					
					//cout <<"now try matching cnf att: "<<cnf.orList[j][k].whichAtt1<< " Op2: "<<cnf.orList[j][k].whichAtt2<<endl;

					if(cnf.orList[j][k].op == Equals) {
		
						if(cnf.orList[j][k].operand1 == Literal) {
	
							//cout<<sortOrder.whichAtts[i]<<"\t"<<cnf.orList[j][k].whichAtt1;
							//cout<<"\n"<<sortOrder.whichTypes[i]<<"\t"<<cnf.orList[j][k].attType;
	
							if((sortOrder.whichAtts[i] == cnf.orList[j][k].whichAtt1)
									&& (sortOrder.whichTypes[i] == cnf.orList[j][k].attType)){
								
								matchOrder->whichAtts[matchOrder->numAtts] = sortOrder.whichAtts[i];
								matchOrder->whichTypes[matchOrder->numAtts++] = sortOrder.whichTypes[i];
								match = true;
								
								//cout<<"matchs!!";

								//matchOrder->Print();
								break;
							}
	
						} else if(cnf.orList[j][k].operand2 == Literal) {
						
							if((sortOrder.whichAtts[i] == cnf.orList[j][k].whichAtt2)
									&& (sortOrder.whichTypes[i] == cnf.orList[j][k].attType)){
						
								matchOrder->whichAtts[matchOrder->numAtts] = sortOrder.whichAtts[i];
								matchOrder->whichTypes[matchOrder->numAtts++] = sortOrder.whichTypes[i];
								match = true;
								//cout<<"match1";
								//matchOrder->Print();
								break;
							}
						}
					}
				}
			}
		}
		if(!match) break;
	}
	if(matchOrder->numAtts == 0)
	{
		cout <<"No query OrderMaker can be constructed!"<<endl;
		delete matchOrder;
		return NULL;
	}

	//cout<<"BS OM constructed";
	return matchOrder;
}



Record* SortedFile::GetMatchPage(Record &literal) {			//returns the first record which equals to literal based on queryorder;
	
	//cout<<"pi3"<<currentPageNumber;
	if(queryChange) {
		int low = currentPageNumber;
		int high = file.GetLength()-2;
		int matchPage = bsearch(low, high, queryOrder, literal);
		//cout <<"matchpage: "<<matchPage<<endl;
		if(matchPage == -1) {
			//not found
			return NULL;
		}
		if(matchPage != currentPageNumber) {
			page.EmptyItOut();
			file.GetPage(&page, matchPage);
			currentPageNumber = matchPage+1;
		}
		queryChange = false;
	}

	//find the potential page, make reader buffer pointer to the first record
	// that equal to query order
	Record *returnRcd = new Record;
	ComparisonEngine cmp1;
	while(page.GetFirst(returnRcd)) {
		if(cmp1.Compare(returnRcd, &literal, queryOrder) == 0) {
			//find the first one
			return returnRcd;
		}
	}
	if(currentPageNumber >= file.GetLength()-2) {
		return NULL;
	} else {
		//sortInfonce the first record may exist on the next page
		currentPageNumber++;
		file.GetPage(&page, currentPageNumber);
		while(page.GetFirst(returnRcd)) {
			if(cmp1.Compare(returnRcd, &literal, queryOrder) == 0) {
				//find the first one
				return returnRcd;
			}
		}
	}
	return NULL;
		

}

int SortedFile::bsearch(int low, int high, OrderMaker *queryOM, Record &literal) {
	
	cout<<"serach OM "<<endl;
	queryOM->Print();
	cout<<endl<<"file om"<<endl;
	sortInfo->myOrder->Print();

	if(high < low) return -1;
	if(high == low) return low;
	//high > low
	
	ComparisonEngine *comp;
	Page *tmpPage = new Page;
	Record *tmpRcd = new Record;
	int mid = (int) (high+low)/2;
	file.GetPage(tmpPage, mid);
	
	int res;

	//cout<<"l "<<low<<" fileMode "<<mid<<" h "<<high<<endl;
	Schema nu("catalog","lineitem");

	//tmpPage->GetFirst(tmpRcd) == 1;

	tmpPage->GetFirst(tmpRcd);
	
	///cout<<"TeMP"<<endl<<endl<<endl;
	//tmpRcd->Print(&nu);
	//cout<<"literal"<<endl<<endl<<endl;
	//cout<<&literal;//.Print(&nu);

	tmpRcd->Print(&nu);

		res = comp->Compare(tmpRcd,sortInfo->myOrder, &literal,queryOM );

		//if(res==0){
			//cout<<"FOUND!!!"<<endl;
	//		break;
//		}
	

	delete tmpPage;
	delete tmpRcd;

	//cout<<"compare result"<<res<<"\n";


	if( res == -1) {
		if(low==mid)
			return mid;
		return bsearch(low, mid-1, queryOM, literal);
	}
	else if(res == 0) {
		return mid;//bsearch(low, mid-1, queryOM, literal);
	}
	else
		return bsearch(mid+1, high,queryOM, literal);
}




/*
OrderMaker* SortedFile::checkIfMatches(CNF &c, OrderMaker &o) {
	OrderMaker *query = new OrderMaker();	// ordermaker that we try to build
	bool matches = false;
	cout<<"o addr " <<&o;
	
	for(int i=0;i<o.numAtts;i++)	// Over every attribute of file's ordermaker
	{
		matches = false;
		
		for(int j = 0; j<c.numAnds; j++) {	// Over list of all disjunctions
			if(!matches) {
			
				for(int k=0; k<c.orLens[j]; k++) {	// over every comparison
				
					if(c.orList[j][k].op == Equals){	// check is operator is 'Equals'
						
						if(c.orList[j][k].operand1 == Literal) {		// check if operand is 'Literal'
							
							if((o.whichAtts[i] == c.orList[j][k].whichAtt1)		// 		attribute and type matches 
										&& (o.whichTypes[i] == c.orList[j][k].attType)){
										
									query->whichAtts[query->numAtts] = o.whichAtts[i];
									query->whichTypes[query->numAtts++] = o.whichTypes[i];
									matches = true;
									break;
							}
						} else if(c.orList[j][k].operand2 == Literal) {
							
							if((o.whichAtts[i] == c.orList[j][k].whichAtt2)
									&& (o.whichTypes[i] == c.orList[j][k].attType)){
									
								query->whichAtts[query->numAtts] = o.whichAtts[i];
								query->whichTypes[query->numAtts++] = o.whichTypes[i];
								matches = true;
								break;
							}
						}
					}				
				}
			}
		if(!matches) break; 	// this happens 
		}
	}
	if(query->numAtts == 0)
	{
		delete query;
		return NULL;
	}
	return query;
	
	
}
*/
/*
Record* SortedFile::GetMatchPage(Record &literal) {			//returns the first record which equals to literal based on queryorder;
		
	int low = currentPageNumber;
	int high = file.GetLength()-2;
	
	int matchPage = bsearch(low, high, queryOrder, literal);
	if(matchPage == -1) {	//not found
		cout<<"No page found"<<endl;
		return NULL;		
	}
	if(matchPage != currentPageNumber-1) {
		page.EmptyItOut();
		file.GetPage(&page, matchPage);
		currentPageNumber = matchPage+1;
	}
	queryChange = false;
	//find the potential page, make reader buffer pointer to the first record
	// that equal to query order
	ComparisonEngine *comp;
	Record *returnRcd = new Record;
	while(page.GetFirst(returnRcd)) {
		if(comp->Compare(returnRcd, &literal, queryOrder) == 0) {
			//find the first one
			return returnRcd;
		}
	}
	if(currentPageNumber >= file.GetLength()-2) {
		cout<<"Reached EOF"<<endl;
		return NULL;
	} else {
		//sortInfonce the first record may exist on the next page
		currentPageNumber++;
		file.GetPage(&page, currentPageNumber);
		while(page.GetFirst(returnRcd)) {
			if(comp->Compare(returnRcd, &literal, queryOrder) == 0) {
				//find the first one
				return returnRcd;
			}
		}
	}
	return NULL;
}
int SortedFile::bsearch(int low, int high, OrderMaker *queryOM, Record &literal) {
	
	ComparisonEngine *comp;
	if(high < low) return -1;
	if(high == low) return low;
	//high > low
	Page *tmpPage = new Page;
	Record *tmpRcd = new Record;
	int mid = (int) (high+low)/2;
	file.GetPage(tmpPage, mid);
	tmpPage->GetFirst(tmpRcd);
	int res = comp->Compare(tmpRcd, &literal, queryOM);
	if( res == -1) {
		if(low==mid)
			return mid;
		return bsearch(mid, high, queryOM, literal);
	}
	else if(0 == res) {
		return bsearch(low, mid-1, queryOM, literal);
	}
	else
		return bsearch(low, mid-1, queryOM, literal);
}
*/

void SortedFile:: MergeFromOutpipe(){		// requires both read and write modes

	// close input pipe
	//cout<<inPipe<<endl;

	inPipe->ShutDown();
	// get sorted records from output pipe
	ComparisonEngine *ce;

	// following four lines get the first record from those already present (not done)
	if(!mergePage){ mergePage = new Page(); }
	mergePageNumber = 0; 
	Record *rFromFile = new Record();
	GetNew(rFromFile);						// loads the first record from existing records

	Record *rtemp = new Record();		
	Page *ptowrite = new Page();			// new page that would be added
	File *newFile = new File();				// new file after merging
	newFile->Open(0,"mergedFile");				

	bool nomore = false;
        int result =GetNew(rFromFile);
	int currentPageNumber = 1;


	Schema nu("catalog","nation");


	if(result==0){
		nomore = true;
		cout<<"nomore: "<<nomore<<endl;
	}

	//cout<<"nomore is "<<nomore<<endl;

	while(isDirty!=0&&!nomore){

		//cout<<" rtemp "<<rtemp<<" rfile "<<rFromFile<<"\n";
		

		if(outPipe->Remove(rtemp)==1){		// got the record from out pipe

			//rtemp->Print(&nu);

			while(ce->Compare(rFromFile,rtemp,sortInfo->myOrder)<0){ 		// merging this record with others
				cout<<"rFromFile<rtemp"<<endl;

				if(ptowrite->Append(rFromFile)==0){		// merge already existing record
						// page full
						
						//int currentPageNumber = newFile->GetLength()==0? 0:newFile->GetLength()-1;

						//*
						// write this page to file


						//cout<<"1. write at index "<<currentPageNumber<<endl;



						newFile->AddPage(ptowrite,currentPageNumber++);
						//currentPageNumber++;
						// empty this out
						ptowrite->EmptyItOut();
						// append the current record ?
						ptowrite->Append(rFromFile);		// does this consume the record ?
						
				}

				//cout<<"1. rfile "<<rFromFile<<"\n";

				if(!GetNew(rFromFile)){ nomore = true; break; }	// bring next rFromFile record ?// check if records already present are exhausted

			}


			if(ptowrite->Append(rtemp)!=1){				// copy record from pipe
						// page full
						
						//int currentPageNumber = newFile->GetLength()==0? 0:newFile->GetLength()-1;


						//*
						// write this page to file


						//cout<<"2. write at index "<<currentPageNumber<<endl;


						newFile->AddPage(ptowrite,currentPageNumber++);
						// empty this out
						ptowrite->EmptyItOut();
						// append the current record ?
						ptowrite->Append(rtemp);		// does this consume the record ?
			}

		}
		else{
			// pipe is empty now, copy rest of records to new file
			do{

				//cout<<"2. rfile "<<rFromFile<<"\n";	
				
				if(ptowrite->Append(rFromFile)!=1){			

					//int currentPageNumber = newFile->GetLength()==0? 0:newFile->GetLength()-1;	// page full
					//*
					// write this page to file


					//cout<<"3. write at index "<<currentPageNumber<<endl;


					newFile->AddPage(ptowrite,currentPageNumber++);
					// empty this out
					ptowrite->EmptyItOut();
					// append the current record ?
					ptowrite->Append(rFromFile);		// does this consume the record ?
					
				}
			}while(GetNew(rFromFile)!=0);
			break;
		}
	}

	outPipe->Remove(rtemp);//1 missing record

	

	if(nomore==true){									// file is empty
		do{

			// rtemp->Print(&nu);	
			cout<<"at the last\n";
		
			if(ptowrite->Append(rtemp)!=1){				// copy record from pipe
						//int currentPageNumber = newFile->GetLength()==0? 0:newFile->GetLength()-1;		// page full
						// write this page to file
						//cout<<"write at index "<<currentPageNumber<<endl;
						newFile->AddPage(ptowrite,currentPageNumber++);
						// empty this out
						ptowrite->EmptyItOut();
						// append the current record ?
						ptowrite->Append(rtemp);		// does this consume the record ?
						
	
			}
		}while(outPipe->Remove(rtemp)!=0);
	}
	// ptowrite->Append(rtemp);

	newFile->AddPage(ptowrite,currentPageNumber);//newFile->GetLength()-1);
	//cout<<"last write at index "<<currentPageNumber<<endl;


	newFile->Close();

	// Record temp;
	// int counter = 0;	
	/*while(GetNext(temp)!=0){
		counter++;
	}*/

	/*while(outPipe->Remove(&temp) != 0){
		counter++;
	}*/

	// cout<<"Merged "<<counter<<" records"<<endl;


	file.Close();

	// delete resources that are not required

	if(rename(fileName,"mergefile.tmp")!=0) {				// making merged file the new file
		cerr <<"rename file error!"<<endl;
		return;
	}
	
	remove("mergefile.tmp");

	if(rename("mergedFile",fileName)!=0) {				// making merged file the new file
		cerr <<"rename file error!"<<endl;
		return;
	}

	page.EmptyItOut();




	file.Open(1, this->fileName);

}


int SortedFile:: GetNew(Record *r1){

	while(!this->mergePage->GetFirst(r1)) {
		if(mergePageNumber >= file.GetLength()-1)
			return 0;
		else {
			file.GetPage(mergePage, mergePageNumber);
			mergePageNumber++;
		}
	}

	return 1;
}	


SortedFile::~SortedFile() {
	delete inPipe;
	delete outPipe;
}



// void SortedFile:: MergeFromOutpipe(){		// requires both read and write modes
// 	file.Open(1, this->fileName);
	
// 	Schema mySchema ("catalog", "nation");
	

// 	inPipe->ShutDown();
// 	Record fromFile, fromPipe;
//   	bool fileNotEmpty = !file.IsEmpty(), pipeNotEmpty = outPipe->Remove(&fromPipe);
//   	DBFile tmp;
// 	tmp.Create("./mergedFile.tmp.bin", heap, NULL);  // temporary file for the merge result; will be renamed in the end
 
//   	ComparisonEngine ce;

//   	// initializes
//   	if (fileNotEmpty) {
//   		currentPageNumber=0;
//     	file.GetPage(&page, currentPageNumber);           // move first
//     	fileNotEmpty = GetNext(fromFile);
//   	}

//   	// two-way merge
//   	while (fileNotEmpty || pipeNotEmpty){
// 	    if (!fileNotEmpty || (pipeNotEmpty && ce.Compare(&fromFile, &fromPipe, sortInfo->myOrder) > 0)) {
//       		tmp.Add(fromPipe);
//       		// fromPipe.Print(&mySchema);
//       		pipeNotEmpty = outPipe->Remove(&fromPipe);
//     	} else if (!pipeNotEmpty || (fileNotEmpty && ce.Compare(&fromFile, &fromPipe, sortInfo->myOrder) <= 0)) {
//       		tmp.Add(fromFile);
//       		// fromPipe.Print(&mySchema);
//       		fileNotEmpty = GetNext(fromFile);
//     	} //else FATAL("Two-way merge failed.");
// 	}
//   	// write back
//   	tmp.Close();
//   	int renameStatus = rename("mergedFile.tmp.bin", fileName);
//   	cout<<"fileName:"<<fileName<<" renameStatus:"<<renameStatus<<endl;
//   	DeleteBigQ();
// }

// void SortedFile::CreateBigQ() {
//     inPipe = new Pipe(PIPE_SIZE), outPipe = new Pipe(PIPE_SIZE);
//     bq = new BigQ(*inPipe, *outPipe, *sortInfo->myOrder, sortInfo->runLength);
// }

// void SortedFile::DeleteBigQ() {
//     delete inPipe; delete outPipe; delete bq;
//   	inPipe = outPipe = NULL; bq = NULL;
// }

// void SortedFile::SwitchToWriteMode(){
// 	if(fileMode == WRITE){ // if file mode is already write
// 		return;
// 	}
// 	fileMode = WRITE; // change fileMode to write
// 	CreateBigQ();
// }

// void SortedFile::SwitchToReadMode(){
// 	if(fileMode==READ){ // if file mode is already read
// 		return;
// 	}
// 	fileMode = READ; // change fileMode to read
// 	MergeFromOutpipe();
// }


// SortedFile::~SortedFile() {
// 	// delete current;
// }
