#include "DBFile.h"
#include "BigQ.h"
#include "test.h"
#include <fstream>
#include <gtest/gtest.h>


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
	sprintf (bigQFileName, "%s.bigq", rel->path ());
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
	sprintf (bigFileName, "%s.bigq", rel->path ());
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