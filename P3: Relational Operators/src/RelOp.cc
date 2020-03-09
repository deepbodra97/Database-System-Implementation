#include "RelOp.h"

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
}

void SelectPipe::WaitUntilDone () {
	// pthread_join (thread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {
	
}

/*
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
}

void SelectFile::WaitUntilDone () {
	// pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {

}
*/