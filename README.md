# hello-world
hello world
Hello Vikash


Excel File Path/Volumes/.../data_portal/input/roaddata.xlsxUse the Copy path button on the uploaded file (visible in your Catalog screenshot) to get the exact /Volumes/... path
2Output Config Path/Volumes/.../data_portal/input/roaddata_config.jsonAnywhere writable; a configs/ folder is tidier
3Sheet Names(leave blank)Blank = all sheets
4Config NameROADDATAMust type it; blank would give roaddata
5Index KeywordYearThe repeated key column that signals side-by-side
6Section Column NameIndustry_sectorMust change from the Section default


1Excel File Path/Volumes/.../data_portal/input/roaddata.xlsx
2Config File Path/Volumes/.../data_portal/input/roaddata_config.json (the file you just generated)
3Output Formatparquet
4Output Volume Path/Volumes/.../data_portal/output/roaddata/
5Unpivot Modeas_configured (or force_unpivot for tidy long output)
6Sheet Names(leave blank)
7Index KeywordYear (only used if config is blank; harmless here)
8On Errorcontinue
