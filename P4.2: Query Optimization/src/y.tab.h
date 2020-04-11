/* A Bison parser, made by GNU Bison 3.0.4.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015 Free Software Foundation, Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

#ifndef YY_YY_Y_TAB_H_INCLUDED
# define YY_YY_Y_TAB_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    FilePath = 258,
    Name = 259,
    Attribute = 260,
    Float = 261,
    Int = 262,
    String = 263,
    CREATE = 264,
    DROP = 265,
    TABLE = 266,
    SELECT = 267,
    GROUP = 268,
    DISTINCT = 269,
    BY = 270,
    FROM = 271,
    WHERE = 272,
    SUM = 273,
    AS = 274,
    AND = 275,
    OR = 276,
    INTEGER_ATTR = 277,
    FLOAT_ATTR = 278,
    STRING_ATTR = 279,
    HEAP = 280,
    SORTED = 281,
    ON = 282,
    SET = 283,
    INSERT = 284,
    INTO = 285,
    OUTPUT = 286,
    NONE = 287,
    STDOUT = 288,
    UPDATE = 289,
    STATISTICS = 290,
    SHUTDOWN = 291
  };
#endif
/* Tokens.  */
#define FilePath 258
#define Name 259
#define Attribute 260
#define Float 261
#define Int 262
#define String 263
#define CREATE 264
#define DROP 265
#define TABLE 266
#define SELECT 267
#define GROUP 268
#define DISTINCT 269
#define BY 270
#define FROM 271
#define WHERE 272
#define SUM 273
#define AS 274
#define AND 275
#define OR 276
#define INTEGER_ATTR 277
#define FLOAT_ATTR 278
#define STRING_ATTR 279
#define HEAP 280
#define SORTED 281
#define ON 282
#define SET 283
#define INSERT 284
#define INTO 285
#define OUTPUT 286
#define NONE 287
#define STDOUT 288
#define UPDATE 289
#define STATISTICS 290
#define SHUTDOWN 291

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED

union YYSTYPE
{
#line 49 "Parser.y" /* yacc.c:1909  */

  struct FuncOperand *myOperand;
  struct FuncOperator *myOperator;
  struct TableList *myTables;
  struct ComparisonOp *myComparison;
  struct Operand *myBoolOperand;
  struct OrList *myOrList;
  struct AndList *myAndList;
  struct NameList *myNames;
  struct AttrList *myAttrs;
  char *actualChars;
  char whichOne;
  int attrType;

#line 141 "y.tab.h" /* yacc.c:1909  */
};

typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif


extern YYSTYPE yylval;

int yyparse (void);

#endif /* !YY_YY_Y_TAB_H_INCLUDED  */
