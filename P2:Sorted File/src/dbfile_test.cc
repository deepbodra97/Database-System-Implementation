#include "DBFile.h"
#include <gtest/gtest.h>

TEST(CreateTest, ReturnValueTest){
	DBFile dbfile;
	ASSERT_EQ(1, dbfile.Create((char *)"./gtest_dbfile", heap, NULL)); // check if the return value of Create() is 1 or not
}

TEST(OpenTest, ReturnValueTest){
	DBFile dbfile;
	ASSERT_EQ(1, dbfile.Open((char *)"./gtest_dbfile")); // check if the return value of Open() is 1 or not
}

TEST(CloseTest, ReturnValueTest){
	DBFile dbfile;
	dbfile.Create((char *)"./gtest_dbfile", heap, NULL); // create a new file
	ASSERT_EQ(1, dbfile.Close()); // check if the return value for Close() is 1 or not
}

int main(int argc, char **argv){
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}