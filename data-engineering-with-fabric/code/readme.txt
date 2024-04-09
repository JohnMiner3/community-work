Please follow the steps to build out each demo.

~
~ Demo 1 ~ Lake House w/ data factory pipelines
~

1 - Create a lake house named lh_adv_works.

2 - Import all notebooks.

3 - Run nb-create-meta-data notebook.

4 - Create \Files\raw\saleslt directory.

5 - Copy saleslt files to adls storage.

6 - Create pipeline called pl-delimited-full-load from Json.

7 - Update connection string for ADLS storage.

8 - Test out a single run.

9 - Create pipeline called pl-refresh-advwrks from Json.

10 - Test out a full run.


~
~ Demo 2 ~ Lake House - local storage vs shortcuts
~

1 - Create file short cut to storage for stocks data.

2 - Run nb-data-mesh-via-shortcuts noteboook


~
~ Demo 3 ~ Data warehouse - SQL inserts versus copy into.
~

1 - Create warehouse named dw_pubs

2 - Run script named fabric-dw-install-pubs-database.sql

3 - Validate creation of data warehouse.

4 - Copy CSV stock data to adls gen 2 storage.

5 - Get updated account key for stock data.

6 - Run fabric-stocks-schema-copy-into.sql

7 - Validate records in stocks.snp500 table


~
~ Demo 4 ~ Kusto database - event hub vs adls storage
~

1 - Still working on the finer details.  This area will change in the future.
