#include "DBFile.h"
#include "BigQ.h"
#include "test.h"
#include <fstream>
#include <gtest/gtest.h>


TEST(CreateTest, ReturnValueTest){
	DBFile dbfile;
	ASSERT_EQ(1, dbfile.Create((char *)"./sortedfile.bin", sorted, NULL)); // check if the return value of Create() is 1 or not
}

TEST(MoveFirstTest, CompareRecord){
	setup ();

	relation *rel_ptr[] = {n, r, c, p, ps, s, o, li};

	int tindx = 1;
	int findx = 0;
	while (findx < 1 || findx > 8) {
		cout << "\n select table: \n";
		cout << "\t 1. nation \n";
		cout << "\t 2. region \n";
		cout << "\t 3. customer \n";
		cout << "\t 4. part \n";
		cout << "\t 5. partsupp \n";
		cout << "\t 6. supplier \n";
		cout << "\t 7. orders \n";
		cout << "\t 8. lineitem \n \t ";
		cin >> findx;
	}
	rel = rel_ptr [findx - 1];

	OrderMaker o;
	rel->get_sort_order (o);

	int runlen = 0;
	while (runlen < 1) {
		cout << "\t\n specify runlength:\n\t ";
		cin >> runlen;
	}
	struct {OrderMaker *o; int l;} startup = {&o, runlen};

	DBFile dbfile;
	cout << "\n output to dbfile : " << rel->path () << endl;
	dbfile.Create (rel->path(), sorted, &startup);
	dbfile.Close ();

	char tbl_path[100];
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name()); 
	cout << " input from file : " << tbl_path << endl;

    FILE *tblfile = fopen (tbl_path, "r");


	dbfile.Open (rel->path ());
	Record temp;

	Record firstRecordInserted; // 1st record that was inserted in the file
	Record firstRecordFetched; // 1st record after MoveFirst

	int counter=0;
	while (temp.SuckNextRecord (rel->schema (), tblfile)) {
		if(counter == 0){ // if counter=0 then temp is the 1st record that is going into the file
			firstRecordInserted.Copy(&temp); // Copy this record
		}
		dbfile.Add (temp);
		counter++;
	}
	cout << "\n create finished.. " << counter << " recs inserted\n";
	dbfile.MoveFirst(); // move first
	dbfile.GetNext(firstRecordFetched); // copy the record from ptrCurrentRecord into firstRecordFetched

	ComparisonEngine comparisonEngine;
	int comparisonResult = comparisonEngine.Compare(&firstRecordInserted, &firstRecordFetched, &o); // compare the 2 records

	ASSERT_EQ(0, comparisonResult); // the result should be '0' as the records are both the same

	dbfile.Close ();
	fclose (tblfile);
}


// After the *.bin.bigq file has been generated, this function checks if the intermediate run file was generated or not
// The run file has the name "yolo.runfile"
TEST(BigQTest, RunFileExistenceTest){		
	ifstream runFile("./yolo.runfile");
	ASSERT_TRUE(runFile.good())<<"The intermediate runfile wasn't created"; // assert: does the intermediate run file exist?
	cleanup ();
}

TEST(BigQTest, RunFileSizeTest){
	setup ();

	relation *rel_ptr[] = {n, r, c, p, ps, o, li};

	int findx = 0;
	while (findx < 1 || findx > 7) {
		cout << "\n select bigq file to test: (*.bin.bigq)\n";
		cout << "\t 1. nation \n";
		cout << "\t 2. region \n";
		cout << "\t 3. customer \n";
		cout << "\t 4. part \n";
		cout << "\t 5. partsupp \n";
		cout << "\t 6. orders \n";
		cout << "\t 7. lineitem \n \t ";
		cin >> findx;
	}
	rel = rel_ptr [findx - 1];	
	ifstream binFile(rel->path());
	ifstream runFile("./yolo.runfile");

	ASSERT_TRUE(runFile.tellg() >= binFile.tellg())<<"The size of the runFile is not as atleast as that of the binFile"; // assert: does the intermediate run file exist?
	cleanup ();
}


// After the *.bin.bigq file has been generated, this function checks if the number of records in the *.bin file
// are the same as that in "yolo.runfile"
TEST(BigQTest, NumberOfRecordsTest){
	setup ();

	relation *rel_ptr[] = {n, r, c, p, ps, o, li};

	int findx = 0;
	while (findx < 1 || findx > 7) {
		cout << "\n select bigq file to test: (*.bin.bigq)\n";
		cout << "\t 1. nation \n";
		cout << "\t 2. region \n";
		cout << "\t 3. customer \n";
		cout << "\t 4. part \n";
		cout << "\t 5. partsupp \n";
		cout << "\t 6. orders \n";
		cout << "\t 7. lineitem \n \t ";
		cin >> findx;
	}
	rel = rel_ptr [findx - 1];

	Record temp;

	DBFile dbfile;
	dbfile.Open(rel->path());
	dbfile.MoveFirst();
	int nBinFile = 0;
	while(dbfile.GetNext(temp)){
		nBinFile++;
	}
	dbfile.Close();

	char bigQFileName[100];
	sprintf (bigQFileName, "%s", rel->path ());
	dbfile.Open(bigQFileName);
	dbfile.MoveFirst();
	int nBigQFile = 0;
	while(dbfile.GetNext(temp)){
		nBigQFile++;
	}
	dbfile.Close();

	ASSERT_EQ(nBinFile, nBigQFile)<<"Number of input records is not equal to that of output records"; // assert: the number of errors should be 0
	cleanup ();
}

// After the *.bin.bigq file has been generated, this function checks if the number of records that are
// out of order is 0 or not
TEST(BigQTest, SortOrderTest){
	setup ();

	relation *rel_ptr[] = {n, r, c, p, ps, o, li};

	int findx = 0;
	while (findx < 1 || findx > 7) {
		cout << "\n select bigq file to test: (*.bin.bigq)\n";
		cout << "\t 1. nation \n";
		cout << "\t 2. region \n";
		cout << "\t 3. customer \n";
		cout << "\t 4. part \n";
		cout << "\t 5. partsupp \n";
		cout << "\t 6. orders \n";
		cout << "\t 7. lineitem \n \t ";
		cin >> findx;
	}
	rel = rel_ptr [findx - 1];

	OrderMaker sortorder;
	rel->get_sort_order (sortorder);

	DBFile dbfile;
	char bigFileName[100];
	sprintf (bigFileName, "%s", rel->path ());
	dbfile.Open(bigFileName);

	ComparisonEngine comparisonEngine;
	Record rec[2];
	Record *last = NULL, *prev = NULL;
	int i = 0;
	int nErrors = 0;
	dbfile.MoveFirst();
	while (dbfile.GetNext(rec[i%2]) == 1) {
		prev = last;
		last = &rec[i%2];

		if (prev && last) {
			if (comparisonEngine.Compare (prev, last, &sortorder) == 1) { // are prev and last out of order?
				nErrors++;
			}
		}
		i++;
	}

	ASSERT_EQ(nErrors, 0)<<nErrors<<" recs not in sorted order"; // assert: the number of errors should be 0
	cleanup ();
}


int main(int argc, char **argv){
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}