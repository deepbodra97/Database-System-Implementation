#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "TwoWayList.h"
#include "Record.h"
#include "DBFile.h"
#include <string.h>

DBFile::DBFile () {

}

int DBFile::Create (char *name, fType fileType, void *startup) {
	switch(fileType){
		case heap:
			this->file= new HeapFile();
			break;
		case sorted:
			this->file= new SortedFile();
			break;
		default:
			cout<<"Implementation Error: No Implementation found for "<<fileType<<endl;
			break;
	}

	if(this->file!=NULL){
		this->file->Create(name, fileType, startup);
	}	

}


int DBFile::Open (char *f_path) {
	// char meta_path[20];
	// sprintf(meta_path,"%s.meta",f_path);

	// FILE *meta =  fopen(meta_path, "r");;
	// char f_type[10];

	// fscanf(meta,"%s",f_type);
	// if(strcmp(f_type, "heap")==0){
	// 	HeapFile *heapFile = new HeapFile();
	// 	this->file = heapFile;
	// }
	// else if(strcmp(f_type, "sorted")==0){
	// 	SortedFile *sortedFile = new SortedFile();
	// 	this->file= sortedFile;	

	// }
	// file->Open(f_path);

	string meta_path("./"+string(f_path) + ".meta");

	ifstream meta;
	meta.open(meta_path);
	/*if(!meta){
		cout<<".meta file does not exist"<<endl;
		exit(EXIT_FAILURE);
	}*/
	string f_type;
	meta >> f_type;
	cout<<"f_type="<<f_type<<endl;
	if(f_type.compare("heap") == 0){
		cout<<"This is Heap File"<<endl;
		HeapFile *heapFile = new HeapFile();
		this->file = heapFile;
	}
	else if(f_type.compare("sorted") == 0){
		SortedFile *sortedFile = new SortedFile();
		this->file= sortedFile;	

	}
	file->Open(f_path);
}


void DBFile::Load (Schema &f_schema, char *loadpath) {
	file->Load(f_schema,loadpath);
}


void DBFile::MoveFirst () {
	file->MoveFirst();
}


void DBFile::Add (Record &rec) {
	return file->Add(rec);
}


int DBFile::GetNext (Record &fetchme) {
	return file->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	return file->GetNext(fetchme,cnf,literal);
}


int DBFile::Close () {
	return file->Close();
}