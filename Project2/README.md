__1. Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.__
This database is created for a music startup called Sparkify who wish to move their processes and data into the cloud as their user and song database has been growing with a steady pace.


__2. State and justify your database schema design and ETL pipeline.__
My ETL pipeline follow simple logical steps:
1. We store the data in S3 buckets.
2. We extract the data from S3 and stage them in a Redshift cluster.
3. We transform the data into a set of dimensional tables so they are ready to be ingested by their analytics team.


__3.[Optional] Provide example queries and results for song play analysis. We do not provide you any of these. You, as part of the Data Engineering team were tasked to build this ETL. Thorough study has gone into the star schema, tables, and columns required. The ETL will be effective and provide the data and in the format required. However, as an exercise, it seems almost silly to NOT show SOME examples of potential queries that could be ran by the users. PLEASE use your imagination here. For example, what is the most played song? When is the highest usage time of day by hour for songs? It would not take much to imagine what types of questions that corporate users of the system would find interesting. Including those queries and the answers makes your project far more compelling when using it as an example of your work to people / companies that would be interested. You could simply have a section of sql_queries.py that is executed after the load is done that prints a question and then the answer.__