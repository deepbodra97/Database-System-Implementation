#include "File.h"
#include "Record.h"

#include <iostream>

using namespace std;

int main(){
	File runFile;
	runFile.Open(1, "./runFile.bin");

	Schema mySchema("catalog", "orders");
	Page printPage;
	int printPageNumber=0;
	Record printRecord;
	runFile.GetPage(&printPage, printPageNumber);
	int runLength=2;
	int nPages=runFile.GetLength()-1;
	int i=1;
	while(true){
		
		if(printPage.GetFirst(&printRecord) == 1){
			printRecord.Print(&mySchema);
		}else{
			cout<<"--------------------End of Page--------------------"<<endl;
			printPageNumber+=1;
			if(printPageNumber<=nPages-1){
				runFile.GetPage(&printPage, printPageNumber);
			} else{
				break;
			}
		}
	}
	return 1;
}