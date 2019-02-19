# levelHandler 
level handler is responsile fot holding all the tables in the level. we interact with level handler for all the table releated operation.

but retriving policy is somewhat difference because in level 0 we have overlapping key across table so we'll use tree for indexing the possible table. In level 1 we just pick one table from tree and look for the value. level handler is gaurder by mutex for concurrent get and insertion and removal of table on compaction

