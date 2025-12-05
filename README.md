# General information

"Partial Pre-Post Code Tree: A Memory-Efficient Tree Structure for Conjunctive Rule Mining" is a work published in KDD2025. P3Ctree (Partial Pre-Post Code Tree) significantly saves memory compared with PPC-tree, by factors of some to some tens.
Both tree structures generate the same N-list structure for a given dataset.
However P3Ctree can practically run faster than PPCtree for generating the basic N-lists (N-lists of single items) thanks to less cost of heap memory management.
N-list of an arbitrary k-itemset (k>1) can be generated efficiently from the basic N-lists. N-list of an itemset conveys the support count of the itemset. 
Besides the original application for Frequent Itemsets Mining, N-list structure can be leveraged for any other purposes in which the support count of sets of feature values is queried with a very high frequency, e.g. Rule learning.

# Source code organization

- src/core/discretizer
    - Contain an implementation for two discretization methods, MDLP and Fusinter, for discretizing continous data. 
    - The discretization is intergrated in the preprocessing step if the input file is ARFF file. 
    - If the input file is CSV file, no discretization is performed, all features are treated as categorical features. 

- src/core/prepr
  - Contain implementations for preprocessing raw data (in tabular form with features/values) to items/selectors
  - Items/selectors are actually conditions of feature = value. e.g. Feature F with distinct values f1, f2, f2, F=f1 is an item or a selector.

- src/core/structure
  - Contain the core implementation for PPC-tree and P3C-tree structures

- src/nlistbase
  - Contain the implementation for generating "InfoBase" from the raw tabular dataset by itergrating the above functionalities.
  - "InfoBase" (Information Base) contains the basic N-lists and the functionality of calculating the support count of arbitrary itemsets/selector-sets.

- src/tidsetbase
  - Contain the implementation of tidset and diffset structures and used these structures for the information base for calculationg the support count of arbitrary itemsets/selector-sets.

- src/zbenchmark:
  - Contain execution files for benchmarks, compare the memory and runtime between P3C-tree and PPC-tree, and between N-list vs tidset/diffset

# Data

- data/input: directory for input raw tabular data (csv or arff)
- data/output: the directory for the result of each benchmark written down to a file
