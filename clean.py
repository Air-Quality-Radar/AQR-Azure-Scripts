import AQR

## functions to clean the tables

def remove_empty_pollution():
    def field_empty(field):
        return field == '0' or not field
    def row_empty(row):
        return field_empty(row["PM10"]) and field_empty(row["PM25"]) and field_empty(row["NOx"])
    return AQR.remove_rows(AQR.pollution_table, row_empty)

if __name__ == "__main__":
    print "Start cleaning pollution table"
    print str(remove_empty_pollution()) + " rows cleaned."
