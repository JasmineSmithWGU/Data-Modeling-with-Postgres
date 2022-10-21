# Data-Modeling-with-Postgres
<H1>Project: Data Modeling with Postgres</H1>

<p> 1. The purpose of this project is to use Fact and Dimension tables to insert data into a new tables. The data is derived from two files that are given by a startup company called Sparkify. The two data sets, Song Data and Log Data are both used for the creation of a star schema and database.  This database is for Sparkify to analyze the data collected on their new music streaming app that includes songs and user actvity to understand what songs their users are actually listening to. The database is used to optimize queries on song play analysis. </p>
<p>  2. The data for the star schema design that transfers data from both data sets into tables that were created using Python and SQL. The star schema is created as the Songplays table being the Fact and four dimension table of songs, users, artists, and time. These tables are able to measure the log analytics for a user. The ETL pipeline is able to query data and compare the results with expected results. The artists and song tables are extracted from the song_data data set and the users and time tables are extracted from the log_data data set. The log_data data set is not included as the file is to large for Github.
</p>

<H2> Steps to run script </H2>
<p> <li>Run <i>python create_tables.py</i> to create the sparkify database.</li>
    <li>Run <i>python etl.py</i> to process the song_data and log_data files and load the data into each table.
    *create_tables.py has to run before etl.py to reset the tables</li>
    <li>etl.ipynb is the notebook used for the etl pipeliness process for each table. This notebook is where the steps are broken down to load data into the tables.</li> 
    <li>sql_queries.py contains the created tables for the star schema. The Drop and Insert actions are included for dropping duplicates or unnecessary tables and inserting data within each table.</li>
    <li> test.ipynb will test each table for code and data insertion accuracy.</li>
