# CSVfilter

It reads given csv file and return rows matching the given filter criteria (Similar to SQL WHERE condition) in pages of size 50.


### Constraints:

CSV may contain million rows.

Filter expression can be a complex nested condition.

Ex: (((column_name = “practo”) and (column_name != “dogreat” )) or (column_name <= 100))

