#ifndef DEFS_H
#define DEFS_H


#define MAX_ANDS 20
#define MAX_ORS 20

#define PAGE_SIZE 131072
#define PIPE_SIZE 100


enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int, Double, String};


unsigned int Random_Generate();

#define FOREACH(el, array, n)   \
	for (typeof(array) p=array; p!=array+(n); ++p) {      \
	typeof(*p) & el = *p;

#ifndef END_FOREACH
#define END_FOREACH }
#endif // END_FOREACH

#endif

