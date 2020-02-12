
#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include <stdlib.h>
using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

int main () {
	// Professor
	/*
	// try to parse the CNF
	cout << "Enter in your CNF: ";
  	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
		exit (1);
	}

	// suck up the schema from the file
	Schema lineitem ("catalog", "lineitem");

	// grow the CNF expression from the parse tree 
	CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree (final, &lineitem, literal);
	
	// print out the comparison to the screen
	myComparison.Print ();

	// now open up the text file and start procesing it
    FILE *tableFile = fopen ("../files/tpch-10mb/lineitem.tbl", "r");

    Record temp;
    Schema mySchema ("catalog", "lineitem");

	//char *bits = literal.GetBits ();
	//cout << " numbytes in rec " << ((int *) bits)[0] << endl;
	//literal.Print (&supplier);

        // read in all of the records from the text file and see if they match
	// the CNF expression that was typed in
	int counter = 0;
	ComparisonEngine comp;
        while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
		counter++;
		if (counter % 10000 == 0) {
			cerr << counter << "\n";
		}

		if (comp.Compare (&temp, &literal, &myComparison))
        	temp.Print (&mySchema);
        }
	*/





	// Getting Familiar
	
	// suck up the schema from the file
	/*Schema mySchema ("catalog", "lineitem");

	// now open up the text file and start procesing it
    FILE *tableFile = fopen ("../files/tpch-10mb/lineitem.tbl", "r");

    Record temp;
    Page pageOfAllRecords;
	
	int counter = 0;
    while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
		counter++;
    	pageOfAllRecords.Append(&temp);
    }
    cout<<"nRead:"<<counter<<endl;
    File fileOfAllRecords;
    fileOfAllRecords.Open(0, (char *)"./temp_output_files/allrecords.txt");
    fileOfAllRecords.AddPage(&pageOfAllRecords, 0);
    cout<<"FileClosed:"<<fileOfAllRecords.Close()<<" pages"<<endl;

	fileOfAllRecords.Open(1, (char *)"./temp_output_files/allrecords.txt");
    pageOfAllRecords.EmptyItOut();
    cout<<"Page Emptied"<<endl;
    fileOfAllRecords.GetPage(&pageOfAllRecords, 0);
	while(pageOfAllRecords.GetFirst(&temp)){	
		// temp.Print(&mySchema);
    }*/
    
	// Test DBFile






	// Test: Sequence of reads and writes
	Schema mySchema ("catalog", "nation");
	FILE *tableFile = fopen ("../../data/tpch-10mb/nation.tbl", "r");
	Record temp1;
	Record temp2;

	DBFile dbfile;
	dbfile.Create((char *)"./dbfile.bin", heap, NULL);

	// read from lineitem.tbl and add to dbfile
	int counter=0;
	while(temp1.SuckNextRecord(&mySchema, tableFile) == 1){
		dbfile.Add(temp1);	
		counter++;
		if(counter==5000){
			break;
		}
	}
	cout<<"nWrite:"<<counter<<endl;
	
	// read dbfile from start
	dbfile.MoveFirst();
	temp2 = *(dbfile.ptrCurrentRecord);
	counter=0;
	while(1){
		// temp.Print(&mySchema);
		counter++;
		if(!dbfile.GetNext(temp2) || counter == 5000){
			break;
		}
	}
	cout<<"nRead:"<<counter<<" PageNumber:"<<dbfile.currentPageNumber<<endl;
	
	// read from lineitem.tbl and append to dbfile
	counter=0;
	while(temp1.SuckNextRecord(&mySchema, tableFile) == 1){
		dbfile.Add(temp1);	
		counter++;
		if(counter==5000){
			break;
		}
	}
	cout<<"nWrite:"<<counter<<" PageNumber:"<<dbfile.currentPageNumber<<endl;

	// read dbfile from start
	dbfile.MoveFirst();
	temp2 = *(dbfile.ptrCurrentRecord);
	counter=0;
	while(1){
		// temp.Print(&mySchema);
		counter++;
		if(!dbfile.GetNext(temp2)){
			break;
		}
	}
	cout<<"nRead:"<<counter<<" PageNumber:"<<dbfile.currentPageNumber<<endl;

	dbfile.Close();
	



	// Test: comparison based GetNext
    /*cout << "Enter in your CNF: ";
  	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
		exit (1);
	}
		
	Schema lineitem ("catalog", "lineitem");
    CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree (final, &lineitem, literal);


    Schema mySchema ("catalog", "lineitem");
	FILE *tableFile = fopen ("../files/tpch-10mb/lineitem.tbl", "r");

	DBFile dbfile;
	dbfile.Create((char *)"./temp_output_files/comparison_based_next", heap, NULL);
	Record temp1;
	while(temp1.SuckNextRecord(&mySchema, tableFile) == 1){
		dbfile.Add(temp1);	
	}
	dbfile.MoveFirst(); //make sure that all records have been written out

	temp1 = *(dbfile.ptrCurrentRecord);
	while(1){
		// temp.Print(&mySchema);
		if(dbfile.GetNext(temp1, myComparison, literal) == 0){
			break;
		} else{
			temp1.Print(&mySchema);
		}
	}*/




	// Test: Load
	/*Schema mySchema ("catalog", "lineitem");
	DBFile dbfile;
	dbfile.Create((char *)"./temp_output_files/test_load", heap, NULL);
	dbfile.Load(mySchema, "../files/tpch-10mb/lineitem.tbl");
	dbfile.MoveFirst();

	Record temp1;
	temp1 = *(dbfile.ptrCurrentRecord);
	int counter=0;
	while(1){
		counter++;
		if(!dbfile.GetNext(temp1)){
			break;
		}
	}
	cout<<"nRead:"<<counter<<" PageNumber:"<<dbfile.pageNumber<<endl;*/
    // return 0;
}


