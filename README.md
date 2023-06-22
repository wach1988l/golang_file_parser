# golang_file_parser

file parser is working independently as a separate application.
i used batches for saving data to redis for parsing big files efficiently.
values stored in reddis as key val.
Keys has versions e.g. version_id (1_2 where one is version and 2 is id of csv row)
it is done for deleting old values when you file arrives. when file parsed successfully old version will be changed
and we will remove old values.
App can run using unix crontabs(configures every 30 minutes)
