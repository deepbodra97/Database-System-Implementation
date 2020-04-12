
CC = g++ -O2 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif

gtest_statistics.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapFile.o SortedFile.o Pipe.o Statistics.o y.tab.o lex.yy.o gtest_statistics.o
	$(CC) -o gtest_statistics.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapFile.o SortedFile.o Pipe.o Statistics.o y.tab.o lex.yy.o gtest_statistics.o -lfl -lpthread -lgtest

gtest_relop.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapFile.o SortedFile.o Pipe.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o gtest_relop.o
	$(CC) -o gtest_relop.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapFile.o SortedFile.o Pipe.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o gtest_relop.o -lfl -lpthread -lgtest

main: Interface.o QueryPlan.o Ddl.o Statistics.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapFile.o SortedFile.o Pipe.o RelOp.o Function.o y.tab.o lex.yy.o main.o
	$(CC) -o main Interface.o QueryPlan.o Ddl.o Statistics.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapFile.o SortedFile.o Pipe.o RelOp.o Function.o y.tab.o lex.yy.o main.o -lfl -lpthread

# main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapFile.o SortedFile.o Pipe.o Statistics.o y.tab.o lex.yy.o main.o
# 	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapFile.o SortedFile.o Pipe.o Statistics.o y.tab.o lex.yy.o main.o -lfl -lpthread

a4-1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapFile.o SortedFile.o Pipe.o Statistics.o y.tab.o lex.yy.o test.o
	$(CC) -o a4-1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapFile.o SortedFile.o Pipe.o Statistics.o y.tab.o lex.yy.o test.o -lfl -lpthread

# a3test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapFile.o SortedFile.o Pipe.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o
# 	$(CC) -o a3test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapFile.o SortedFile.o Pipe.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a3test.o -lfl -lpthread
	
a2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapFile.o SortedFile.o Pipe.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a2test.o
	$(CC) -o a2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapFile.o SortedFile.o Pipe.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a2test.o -lfl -lpthread	

a1test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o
	$(CC) -o a1test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o -lfl

gtest_relop.o: gtest_relop.cc
	$(CC) -g -c gtest_relop.cc

main.o: main.cc
	$(CC) -g -c main.cc
	
test.o: test.cc
	$(CC) -g -c test.cc

# a3test.o: a3test.cc
# 	$(CC) -g -c a3test.cc

a2test.o: a2test.cc
	$(CC) -g -c a2test.cc

a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc

Interface.o: Interface.cc
	$(CC) -g -c Interface.cc

QueryPlan.o: QueryPlan.cc
	$(CC) -g -c QueryPlan.cc

Ddl.o: Ddl.cc
	$(CC) -g -c Ddl.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc 

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*