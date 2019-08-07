"""
Simple table column transforms can be expressed as a lambda.
For a given column value passed in, a specific transform
is applied.
"""
column_1_simple_transform = lambda column_value: column_value.upper()

"""
More complex transforms, including calculations, table lookups etc,
can be expressed as a function using the full weight of the 
Python language.
"""
def column_2_complex_transform(column_value):
    if len(column_value) == 0: 
        return column_value 
    else: 
        return column_2_complex_transform(column_value[1:]) + column_value[0] 

"""
Once the transforms have been expressed, the delegate functions are collected
in a dictionary, keyed on the column name of the column which is to be 
transformed, with the delegate function responsible for the transform as the value
"""
transforms = {
    "col1": column_1_simple_transform,
    "col2": column_2_complex_transform
}
