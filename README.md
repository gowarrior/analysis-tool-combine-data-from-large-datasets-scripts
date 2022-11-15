# scripts to combine data from large datasets scripts
* FilterAndMergeData used chunk and Dataframe filter to load only data needed to minimize memory usage, and pandas merge to get the wanted result; 
* FilterAndMergeDataWithDatabas added database support based on the first one to better store data unto hard disk instead of memory, for this script you need to install mysql database and set your password and change the password in the code to match yours
