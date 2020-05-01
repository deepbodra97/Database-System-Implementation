# DeepDB


![DeepDB](https://drive.google.com/uc?export=view&id=1oK3rZqOpOBucAB2IsjFmd2z0cVr_KXMP)

DeepDB is a relational database that is implemented in C++. Feel free to scroll down to the features section.

It is a project for the course COP6726: Database System Implementation at the University of Florida

# Demo
[![Demo](https://drive.google.com/uc?export=view&id=18DTROUYBPlCaDLLz8X9jVhVTMIc3Za4V)](https://www.youtube.com/watch?v=x9PuqaW_HBE)

# Features

- Project 1: Heap File (stores records in a binary file)
    1. Create a file
    2. Open a file
    3. Add record to a file
    4. Get next record from a file
    5. Get next record from a file matching a given CNF (condition)
    6. Move to the start of the file
    7. Load records from a text/table file into heap file
    8. Close a file
- Project 2
    - Project 2.1: BigQ
        1. External merge sort using threads
    - Project 2.2: Sorted File
        - All the features of Project 1 except that the file will now be sorted on one or more attributes of a table (for all operations performed)
        - Binary search is used to for efficient retrieval and storage
- Project 3: Relational Operator
    1. Select
    2. Project
    3. Duplicate Removal
    4. Join
    5. GroupBy
    6. Sum
    7. WriteOut (write the output to a file)
- Project 4
    - Project 4.1: Statistics
        1. Statistics.txt keeps a track of the number of tuples in a table, all the tables in the database, their attributes, etc
        2. Given a query plan, estimate the cost of the query using the information from Statistics.txt
    - Project 4.2: Query Plan
        1. Given a SELECT query, permute all the possible query plans and find the one which has the minimum cost
- Project 5: Complete DB
        1. Given a SELECT query, run the query using the minimum cost query plan, on the database
        2. Support for CREATE, INSERT, SELECT and DROP on user defined tables

# Want to try it out?

Each project subdirectory has a Report with a brief explanation of the implementation details and steps to run the project.

# Frameworks/Libraries
- Flex and Bison for parsing
- Gtest for unit testing
- tpch (dataset) for query run time 

# Want to contribute?
- Drop a star if it helped you in your project

# Todos
 - Bonus (B+ tree for indexing)
 - Front end

**Free Project, Hell Yeah!**