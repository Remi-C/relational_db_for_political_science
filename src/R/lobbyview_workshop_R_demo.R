
library(DBI) # necessary to connect to db
library(RPostgreSQL) # postgres specific drivers
library(dplyr) # data manipulation package
library(dbplyr) # apply dplyr syntax to working with database

#password can only be used locally (after a ssh), so it is safe to share the password.
driver <- dbDriver("PostgreSQL")

##now we connect to the server  
con <- DBI::dbConnect(drv = driver, 
                      host = "xvii.mit.edu",
                      dbname = "lobby_refactored",
                      port = "5432",
                      user = "@FIX_ME",
                      password = "@FIX_ME"
)

# Print full list of tables in the database (output is long)
dbListTables(con)

# very simple query, no grouping, just a simple select statement using straight sql
# get lobbyists who have names starting with A
res <- dbSendQuery(con, "SELECT _lobbyist_uuid, lobbyist_consolidated_name FROM consolidated_layer_reports.lobbyists WHERE lobbyist_consolidated_name ILIKE 'A%'")
# actually get the results and store them in a data frame
lobbyist.table <- dbFetch(res)	
lobbyist.table
# clear the results (the data frame does not get deleted)
dbClearResult(res)


# slightly more complicated query, but also just using straight sql
# query to get the 10 biggest lobbying clients, by amount of money spent
res <- dbSendQuery(con, "SELECT _client_uuid, sum(amount) as dollars_spent
		FROM consolidated_layer_reports.reports as s
		GROUP BY _client_uuid
		ORDER BY dollars_spent DESC
		LIMIT 10")
top.lobbying.clients <- dbFetch(res)	
top.lobbying.clients
dbClearResult(res)


# can also use the dbplyr package to make this same query, but this abstracts away having to write any SQL. This is good and bad. It uses the dplyr syntax.
# It only works in R, so try not to rely on this
# This package also focuses on writing select statements -- not every SQL feature is supported or implemented. Still, 
# if you are very familiar with dplyr this could save time for simple queries.
# for in_schema: specifying that we want the "reports" table in the "consolidated_layer_reports" schema
dbplyr.results <- tbl(con, in_schema("consolidated_layer_reports", "reports")) %>% 
  select(., `_client_uuid`, "amount") %>% 
  group_by(., `_client_uuid`) %>% 
  summarise(., dollars_spent = sum(amount)) %>% 
  arrange(., desc(dollars_spent)) %>% 
  head(., n = 10)

dbplyr.results
## or convert to tibble/df for easy use in later code with tibble::as_tibble()


# often times, you need to write a query that depends on user input. Don't just concatenate strings! Unsafe.
# What are the industry codes and reference name of a company?
# sqlInterpolate function is part of DBI package

# note the ?name ---> this gets safely replaced with the desired text
# any variable name can follow the ?, but it must be consistent with the argument you provide after
param_query <- sqlInterpolate(con, "SELECT r.*, c.company_reference_name, c.primary_naics, c.primary_sic4
	FROM api.top_client_industry_by_fuzzy_name( ?name ) as r 
	LEFT OUTER JOIN lobbying_clients__industry_codes.clients__industry_codes as c  USING (_client_uuid);", name = "microsoft")

# check the generated query
param_query


query <- dbSendQuery(con, param_query)
#actually fetch the results, create a data frame
res = dbFetch(query)
#clear the results
dbClearResult(query)

######################################################################
######################################################################
dbDisconnect(con) # close connection when done using the database! This is important
######################################################################
######################################################################