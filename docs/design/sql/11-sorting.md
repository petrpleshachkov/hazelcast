+ [Background](#background)
  - [Description](#description)
      * [Goals](#goals)
      * [Non-goals](#non-goals)
+ [User Interaction](#user-interaction)
  - [API design and/or Prototypes](#api-design-andor-prototypes)
      * [Java marker interface](#ORDER BY clause example)
+ [Technical Design](#technical-design)
+ [Testing Criteria](#testing-criteria)

### Background
#### Description

The SQL sorting (ORDER BY) feature is one of the most demanding features by our customers. 
Normally an end user'd like to see final results being ordered in ascending or descending order.
Without this feature a usefulness of SQL is very limited.

##### Goals

In 4.2 we will support for sorting feature implementing ORDER BY, LIMIT and OFFSET constructions. That is,
the Mustang SQL engine should be able to parse the grammatical constructions and build a proper query plan 
for them.

However, we assume that we always have indexes supporting query ordering. Therefore, a sorting on the local member
comes for free from the indexes. If there is no supporting index, an engine throws an exception.

To support sorting by multiple fields, an engine requires a matching (on the same fields) composite index.  

The sorting feature will work on both on-heap and off-heap IMap data structures. 

##### Non-goals

Currently, the sorting will only work if there is/are index(es) supporting the query ordering. Otherwise, an exception 
will be thrown. It means we don't perform sorting on the local member and don't need a memory management to 
control memory consumption by memory-intensive sorting operation. However, sorting pre-sorted sub-results coming
from cluster members and returning a user globally ordered results is a goal. 

There are also some limitations:
 - No expressions in the ORDER BY clause: SELECT ... FROM ... ORDER BY field [ ASC | DESC ] (, field [ ASC | DESC ])*
 - Stable ordering is not supported. If the order of rows is not uniquely set by the field in the ORDER BY clause,
   we don't have to guarantee returning the rows in the same order between multiple query executions.
   This applies even for one member cluster.
 - We may omit some optimizations like pushing LIMIT clause down in the query plan to minimize the query engine's 
   runtime data set as early as possible.
   

### User Interaction
#### API design and/or Prototypes
The user will be able to use standard SQL clauses like ORDER BY, LIMIT and OFFSET to sort and limit the result set.

##### ORDER BY clause example

An example of sorting query is listed below. The statement gets the top 5 employees ordered by `first_name` field
skipping the first 3 ones.

```
SELECT 
    employee_id, first_name, last_name
FROM
    employees
ORDER BY first_name
LIMIT 5 OFFSET 3;
```

You can use the LIMIT clause to get the top N rows with the highest or lowest value.
For example, the following statement gets the top five employees with the highest salaries.

```
 SELECT 
    employee_id, first_name, last_name, salary
FROM
    employees
ORDER BY salary DESC
LIMIT 5;
```

### Technical Design

#### Sorted indexes
As we mentioned in our goals section, we plan to execute sorting statements only if there is an index supporting it.
An on-heap indexes are based on the `ConcurrentSkipListMap` that supports both ascending and descending ordering 
out of the box. The `ConcurrentSkipListMap#descendingMap` returns a reverse order view of the index entries so that 
sorting of the results in the descending comes directly from the index. 

That is not the case for HD (off-heap) index that doesn't support descending navigation over the leaf B+tree nodes. 
The reason why the descending ordering was not supported in 4.1 is a complex navigation rules in the B+tree
that require top to down and left to right navigational directions. Navigating from the right leaf  B+tree nodes
to the right ones requires extra care to avoid potential deadlock.

The B+tree leaf nodes are connected as a bi-directional list. The main idea how to avoid a deadlock navigating 
from the right leaf node to the left one is to introduce a `tryReadLock` method in the `LockManager`. 
Always use the `tryReadLock` following the `back` link and if it doesn't succeed, relase all locks, 
instantly lock the required left B+tree node (to avoid busy wait loop),
and restart the navigation operation from the B+tree root node.   

#### Sorting rules
The Calcite rules set should be extended to accomodate ORDER BY clause. The rule'll define how the sorting 
should be done depending on the pre-sorting conditions of the descendant operators. 

The rule will support a general case when the descendant operators not necessarily produce a pre-sorted result. 
However, if a sorting operator produced that requires local sorting on the member, the execution of such operator 
will throw an exception. 

In other words, the parsing and optimization framework will support a generic case, while in runtime some
operators may throw NOT_SUPPORTED exception.    

### Testing Criteria

The unit tests should cover an extra functionality introduced in every SQL engine module. That is: 
 - a LockManager;
 - a B+tree lookup operation;
 - Calcite rules sub-system;
 - A query plan generation;
 - A merge sorting operator.
 
 It is also important to quantify how much of a performance impact this feature brings. We still have to perform 
 sorting of the pre-sorted results coming from the cluster members.
 
 In addition to this, all existing SQL statements should not have any regressions/performance degradation
 if the sorting feature is not used.