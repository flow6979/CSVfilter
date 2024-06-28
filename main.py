import csv
import ast
import operator
import os

def parse_filter_expression(expression):
    try:
        return ast.parse(expression, mode='eval').body
    except SyntaxError as e:
        raise ValueError(f"Invalid filter expression: {expression}") from e

def eval_expression(expr, row):
    ops = {
        ast.Eq: operator.eq,
        ast.NotEq: operator.ne,
        ast.Lt: operator.lt,
        ast.LtE: operator.le,
        ast.Gt: operator.gt,
        ast.GtE: operator.ge,
        ast.And: lambda *args: all(args),
        ast.Or: lambda *args: any(args)
    }
    
    if isinstance(expr, ast.BoolOp):
        op = ops[type(expr.op)]
        return op(*(eval_expression(value, row) for value in expr.values))
    elif isinstance(expr, ast.Compare):
        left = eval_expression(expr.left, row)
        comparisons = (ops[type(op)](left, eval_expression(right, row)) for op, right in zip(expr.ops, expr.comparators))
        return all(comparisons)
    elif isinstance(expr, ast.Name):
        return convert_value(row.get(expr.id))
    elif isinstance(expr, ast.Constant):
        return expr.value
    elif isinstance(expr, ast.Call) and expr.func.id == 'len':
        arg_value = eval_expression(expr.args[0], row)
        return len(arg_value)
    else:
        raise ValueError(f"Unsupported expression type: {type(expr)}")

def convert_value(value):
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value

def filter_rows(reader, expression_tree):
    for row in reader:
        if eval_expression(expression_tree, row):
            yield row

def read_csv_with_filter(file_path, filter_criteria, page_number=1, page_size=50):
    filtered_data = []
    with open(file_path, mode='r', newline='') as file:
        reader = csv.DictReader(file)
        expression_tree = parse_filter_expression(filter_criteria)
        
        start_index = (page_number - 1) * page_size
        end_index = start_index + page_size
        
        for _ in range(start_index):
            next(reader, None)
        
        for row_index, row in enumerate(reader):
            if row_index >= end_index:
                break
            if eval_expression(expression_tree, row):
                filtered_data.append(row)
    
    return filtered_data

def save_filtered_data(filtered_data, output_file):
    if not filtered_data:
        print(f"No data matched the filter criteria for {output_file}.")
        return
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=filtered_data[0].keys())
        writer.writeheader()
        writer.writerows(filtered_data)
    
    print(f'Data stored for {output_file}.')

# Testing

filters = [
    {'criteria': '(specialty == "Neurology") and (age >= 35) and (age <= 45)', 'output_file': 'specialty_age_filtered.csv'},
    {'criteria': '(years_of_experience > 20) and (specialty != "Cardiology")', 'output_file': 'experience_specialty_filtered.csv'},
    {'criteria': '((specialty == "Dermatology") or (specialty == "Pediatrics")) and (age < 40)', 'output_file': 'multiple_specialties_age_filtered.csv'},
    {'criteria': '(len(name) > 10) and (salary >= 130000) and (salary <= 150000)', 'output_file': 'name_length_salary_filtered.csv'},
    {'criteria': '((specialty == "General Surgery") and (age >= 25) and (age <= 35)) or ((salary < 140000) and (specialty != "Dermatology"))', 'output_file': 'complex_nested_filtered.csv'}
]

file_path = 'doctors.csv'
output_directory = 'data'

for filter_data in filters:
    filter_criteria = filter_data['criteria']
    output_file = os.path.join(output_directory, filter_data['output_file'])
    filtered_data = read_csv_with_filter(file_path, filter_criteria, page_number=1, page_size=50)
    save_filtered_data(filtered_data, output_file)