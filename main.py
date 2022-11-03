import os
import pandas as pd
import petl as etl
from petl import dateparser

file_path=os.path.join(os.getcwd(), "wip", "TS202211.CSV")

header = (
    "_id",
    "vendor",
    "division",
    "customer_id",
    "customer",
    "address",
    "city",
    "state",
    "postal_code",
    "vendor_item",
    "buyer_item",
    "quantity",
    "uom",
    "sale",
    "po",
    "po_date"
)

def read_file(filename):
    df = pd.read_csv(filename, header=0, names=header, parse_dates=["po_date"])
    if(df.empty):
        print ('CSV file is empty')
    else:
        print ('CSV file is not empty')
        return df


def postal_code_is_ge_5_digits(row):
    if len(row["postal_code"]) < 4:
        raise ValueError("Postal code must be at least 5 digits")
    

contraints = [
    dict(name='quantity_is_float', field='quantity', test=float),
    dict(name='po_date_is_date', field='po_date', assertion=dateparser('%Y%m%d', True)),    
    dict(name='postal_code_is_ge_5_digits', field='postal_code', assertion=lambda x: len(x) >= 5),
    # field 'po' is unique
    dict(name='po_is_unique', field='po', unique=True),
]


data = etl.fromcsv(file_path)

problems = etl.validate(data, header=header, constraints=contraints)

if __name__ == "__main__":
    df = read_file(filename=file_path)
    for dtype in df.dtypes.iteritems():
        print(dtype)
    
    validation = df
    validation['chk'] = validation['po'].apply(lambda x: True if x in df else False)
    validation = validation[validation['chk'] == True].reset_index()
    
    
    print(validation)

    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = pd.to_datetime(df[col])
            except ValueError:
                pass
    
    print(df.dtypes)

    for col in df.columns:
        miss = df[col].isnull().sum()
    
        if miss>0:
            print("{} has {} missing value(s)".format(col,miss))
        else:
            print("{} has NO missing value!".format(col))

    print(problems)