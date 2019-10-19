"""
Name: Andy Lu
Time To Completion: 
Comments: Edited to perform more SQL functions

Sources: Professor's Project 2
"""
import string
from operator import itemgetter

_ALL_DATABASES = {}


class Connection(object):
    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        if filename in _ALL_DATABASES:
            self.database = _ALL_DATABASES[filename]
        else:
            self.database = Database(filename)
            _ALL_DATABASES[filename] = self.database

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        def create_table(tokens):
            """
            Determines the name and column information from tokens add
            has the database create a new table within itself.
            """
            pop_and_check(tokens, "CREATE")
            pop_and_check(tokens, "TABLE")
            table_name = tokens.pop(0)
            pop_and_check(tokens, "(")
            column_name_type_pairs = []
            while True:
                column_name = tokens.pop(0)
                column_type = tokens.pop(0)
                assert column_type in {"TEXT", "INTEGER", "REAL"}
                column_name_type_pairs.append((column_name, column_type))
                comma_or_close = tokens.pop(0)
                if comma_or_close == ")":
                    break
                assert comma_or_close == ','
            self.database.create_new_table(table_name, column_name_type_pairs)

        def insert(tokens):
            """
            Determines the table name and row values to add.
            """
            pop_and_check(tokens, "INSERT")
            pop_and_check(tokens, "INTO")
            table_name = tokens.pop(0)
            if tokens[0] == "(":
                pop_and_check(tokens, "(")
                row_order = []
                while True:
                    item = tokens.pop(0)
                    if item != ',' and item != ')':
                        row_order.append(item)
                    if item == ")":
                        break
                pop_and_check(tokens, "VALUES")
                pop_and_check(tokens, "(")
                row_contents = []
                while True:
                    item = tokens.pop(0)
                    if item != ',' and item != ')' and item != '(':
                        row_contents.append(item)
                    if item == ")" and len(tokens)==0:
                        break
                self.database.insert_ordered(table_name, row_contents, row_order)
            else:
                pop_and_check(tokens, "VALUES")
                pop_and_check(tokens, "(")
                row_contents = []
                while True:
                    item = tokens.pop(0)
                    if item != ',' and item != ')' and item != '(':
                        row_contents.append(item)
                    if item == ")" and len(tokens)==0:
                        break
                self.database.insert_into(table_name, row_contents)

        def select(tokens):
            """
            Determines the table name, output_columns, and order_by_columns.
            """
            pop_and_check(tokens, "SELECT")
            output_columns = []
            distinct_bool = False
            left_outer_join_bool = False
            if tokens[0] == "DISTINCT":
                pop_and_check(tokens, "DISTINCT")
                distinct_bool = True
            while True:
                item = tokens.pop(0)
                if item == "FROM":
                    break
                elif item != ",":
                    output_columns.append(item)
            output_columns = self.database.table_star(output_columns)
            table_name = tokens.pop(0)
            if tokens[0] == "LEFT":
                left_outer_join_bool = True
                pop_and_check(tokens, "LEFT")
                pop_and_check(tokens, "OUTER")
                pop_and_check(tokens, "JOIN")
                table_join_name = tokens.pop(0)
                pop_and_check(tokens, "ON")
                column_name_a = tokens.pop(0)
                pop_and_check(tokens, "=")
                column_name_b = tokens.pop(0)
            if distinct_bool == True:
                self.database.distinct(table_name, output_columns)
            if tokens[0] == "WHERE":
                pop_and_check(tokens, "WHERE")
                index = tokens.index("ORDER")
                operand = []
                value = 0
                column = 0
                for i in range(0, index):
                    temp = tokens.pop(0)
                    if i==(index-1):
                        value = temp
                    elif i == 0:
                        column = temp
                    else:
                        operand.append(temp)
                self.database.where(table_name, column, value, operand)
            pop_and_check(tokens, "ORDER")
            pop_and_check(tokens, "BY")
            order_by_columns = []
            while True:
                col = tokens.pop(0)
                order_by_columns.append(col)
                if not tokens:
                    break
                pop_and_check(tokens, ",")
            if left_outer_join_bool == True:
                return self.database.left_outer_join(output_columns, table_name, table_join_name, column_name_a, column_name_b, order_by_columns)
            else:
                return self.database.select(output_columns, table_name, order_by_columns)
            
        def delete(tokens):
            pop_and_check(tokens, "DELETE")
            pop_and_check(tokens, "FROM")
            table_name = tokens.pop(0)
            if len(tokens) != 0 and tokens[0] == "WHERE":
                pop_and_check(tokens, "WHERE")
                index = len(tokens)
                operand = []
                value = 0
                column = 0
                for i in range(0, index):
                    temp = tokens.pop(0)
                    if i==(index-1):
                        value = temp
                    elif i == 0:
                        column = temp
                    else:
                        operand.append(temp)
                self.database.where(table_name, column, value, operand)
            self.database.delete(table_name) 
            
        def update(tokens):
            pop_and_check(tokens, "UPDATE")
            table_name = tokens.pop(0)
            pop_and_check(tokens, "SET")
            column_names = []
            values = []
            before_equal = True
            after_equal = False
            while True:
                item = tokens.pop(0)
                if after_equal == True:
                    values.append(item)
                    after_equal = False
                if item == "=":
                    before_equal = False
                    after_equal = True
                if item == ",":
                    before_equal = True
                if before_equal == True and item != ",":
                    column_names.append(item)
                    before_equal = False
                if len(tokens) == 0 or item == "WHERE":
                    break;
            if len(tokens) != 0:
                index = len(tokens)
                operand = []
                value = 0
                column = 0
                for i in range(0, index):
                    temp = tokens.pop(0)
                    if i==(index-1):
                        value = temp
                    elif i == 0:
                        column = temp
                    else:
                        operand.append(temp)
                self.database.where(table_name, column, value, operand)
                column_names = self.database.update_star(table_name, column_names)
            self.database.update(table_name, column_names, values)
        tokens = tokenize(statement)
        assert tokens[0] in {"CREATE", "INSERT", "SELECT", "DELETE", "UPDATE"}
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        if tokens[0] == "CREATE":
            create_table(tokens)
            return []
        elif tokens[0] == "INSERT":
            insert(tokens)
            return []
        elif tokens[0] == "SELECT":
            return select(tokens)
        elif tokens[0] == "DELETE":
            delete(tokens)
            return []
        else: #tokens[0] == "UPDATE"
            update(tokens)
            return []
        assert not tokens

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass


def connect(filename):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)


class Database:
    def __init__(self, filename):
        self.filename = filename
        self.tables = {}

    def create_new_table(self, table_name, column_name_type_pairs):
        assert table_name not in self.tables
        self.tables[table_name] = Table(table_name, column_name_type_pairs)
        return []

    def insert_into(self, table_name, row_contents):
        assert table_name in self.tables
        table = self.tables[table_name]
        row_order = table.get_column_names()
        table.insert_new_ordered(row_contents, row_order)
        return []
        
    def insert_ordered(self, table_name, row_contents, row_order):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.insert_new_ordered(row_contents, row_order)
        return []
        
    def left_outer_join(self, output_columns, left_name, right_name, column_l, column_r, order_by_columns):
        def sort_rows(rows, order_by_columns):
            return sorted(rows, key=itemgetter(*order_by_columns))
            
        def generate_tuples(rows, output_columns):
            """
            for row in rows:
                yield tuple(row[col] for col in output_columns)
            """
            result = []
            for row in rows:
                temp_tuple = ()
                for i in range(0, len(output_columns)):
                    col = output_columns[i]
                    if col in row:
                        temp_tuple = temp_tuple + (row[col],)
                    else:
                        temp_tuple = temp_tuple + (None,)
                result.append(temp_tuple)
            return result
            
        def clean_columns(columns):
            new_columns = []
            for itr in columns:
                period_pos = itr.find('.')
                if period_pos != -1:
                    itr = itr[period_pos+1:]
                    new_columns.append(itr)
                else:
                    new_columns.append(itr)
            return new_columns
            
        assert left_name in self.tables
        assert right_name in self.tables
        left_table = self.tables[left_name]
        right_table = self.tables[right_name]
        left_rows = left_table.get_rows()
        right_rows = right_table.get_rows()
        period_pos = column_l.find(".")
        column_l = column_l[period_pos+1:]
        period_pos = column_r.find(".")
        column_r = column_r[period_pos+1:]
        new_output_columns = clean_columns(output_columns)
        new_order = clean_columns(order_by_columns)    
        new_rows = []
        
        for left_row in left_rows:
            temp_dic = {}
            on_clause_value = None
            for itr in left_row:
                if itr == column_l:
                    on_clause_value = left_row[itr]
                if itr in new_output_columns:
                    temp_dic[itr] = left_row[itr]
            for right_row in right_rows:
                if right_row[column_r] == on_clause_value:
                    for itr in right_row:
                        if itr in new_output_columns:
                            if right_row[itr] != None and on_clause_value != None:
                                temp_dic[itr] = right_row[itr]
                            else:
                                temp_dic[itr] = None
            new_rows.append(temp_dic)
            
        new_rows = sort_rows(new_rows, new_order)

        return generate_tuples(new_rows, new_output_columns)
        
    def update(self, table_name, column_names, values):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.update(column_names, dict(zip(column_names, values)))
        return []
        
    def delete(self, table_name):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.delete()
        return []
        
    def distinct(self, table_name, output_columns):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.distinct(output_columns)
        return []
        
    def select(self, output_columns, table_name, order_by_columns):
        assert table_name in self.tables
        table = self.tables[table_name]
        return table.select_rows(output_columns, order_by_columns)
    
    def table_star(self, output_columns):
        result = []
        for output_column in output_columns:
            period_pos = output_column.find('.')
            star_pos = output_column.find('*')
            if period_pos != -1 and star_pos != -1:
                table_name = output_column[:period_pos]
                assert table_name in self.tables
                table = self.tables[table_name]
                column_names = table.get_column_names()
                for itr in column_names:
                    column = table_name + "." + itr
                    result.append(column)
            else:
                result.append(output_column)
        return result
        
    def update_star(self, table_name, output_columns):
        assert table_name in self.tables
        result = []
        table = self.tables[table_name]
        if "*" in output_columns:
            column_names = table.get_column_names()
            for itr in column_names:
                result.append(itr)
            return result
        else:
            return output_columns
    def clean_columns(self, columns):
        new_columns = []
        for itr in columns:
            period_pos = itr.find('.')
            if period_pos != -1:
                itr = itr[period_pos+1:]
                new_columns.append(itr)
            else:
                new_columns.append(itr)
        return new_columns
            
    def where(self, table_name, column, value, operand):
        period_pos = column.find('.')
        if period_pos != -1:
            table_name = column[:period_pos]
            column = column[period_pos+1:]
            
        assert table_name in self.tables
        table = self.tables[table_name]
        
        if len(operand) > 1:
            if value is None:
                operand_code = -1 # Is not NULL
            else:
                operand_code = 0 # Does not equal
        else:
            if operand[0] == "IS":
                if value is None:
                    operand_code = -2 # Is NULL
                else:
                    operand_code = 1 # Does equal
            elif operand[0] == "=":
                operand_code = 1 # Does equal
            elif operand[0] == "<":
                operand_code = 2 # Less than
            else:
                operand_code = 3 # Greater than
        table.where(column, value, operand_code)
        return []


class Table:
    def __init__(self, name, column_name_type_pairs):
        self.name = name
        self.column_names, self.column_types = zip(*column_name_type_pairs)
        self.rows = []
        self.where_statement = False
        self.where_code = 4
        self.where_value = None
        self.where_column = None
        self.distinct_statement = False
        self.distinct_column = None
        
    def get_rows(self):
        result = []
        for itr in self.rows:
            result.append(itr)
        if self.where_statement == True:
            result = self.where_process(result)
        return result
        
    def where(self, column, value, operand_code):
        self.where_column = column
        self.where_value = value
        self.where_code = operand_code
        self.where_statement = True
        
    def distinct(self, output_columns):
        self.distinct_statement = True
        self.distinct_column = output_columns[0]
        
    def insert_new_row(self, row_contents):
        assert len(self.column_names) == len(row_contents)
        row = dict(zip(self.column_names, row_contents))
        self.rows.append(row)
        
    def insert_new_ordered(self, row_contents, row_order):
        """
        row = {}
        for i in self.column_names:
            if i in row_order:
                row_index = row_order.index(i)
                row[i] = row_contents[row_index]
            else:
                row[i] = None
        self.rows.append(row)
        """
        null_column = True
        counter = 0
        for i in range(0, (len(row_contents)//len(row_order))):
            temp_list = []
            insert_list = []
            #stores values according to column name in a tuple
            #counter iterates through row contents
            for itr in row_order:
                temp_tuple = (itr, row_contents[counter])
                temp_list.append(temp_tuple)
                counter = counter+1

            for c_name in self.column_names:
                for tup in temp_list:
                    if c_name == tup[0]:
                        insert_list.append(tup[1])
                        null_column = False
                if null_column == True:
                    insert_list.append(None)
                else:
                    null_column = True
            self.insert_new_row(insert_list)
            
    def select_rows(self, output_columns, order_by_columns):
        def expand_star_column(output_columns):
            new_output_columns = []
            for col in output_columns:
                if col == "*":
                    new_output_columns.extend(self.column_names)
                else:
                    new_output_columns.append(col)
            return new_output_columns

        def check_columns_exist(columns):
            assert all(col in self.column_names for col in columns)

        def sort_rows(order_by_columns):
            return sorted(self.rows, key=itemgetter(*order_by_columns))

        def generate_tuples(rows, output_columns):
            for row in rows:
                yield tuple(row[col] for col in output_columns)
        def clean_columns(columns):
            new_columns = []
            for itr in columns:
                period_pos = itr.find('.')
                if period_pos != -1:
                    itr = itr[period_pos+1:]
                    new_columns.append(itr)
                else:
                    new_columns.append(itr)
            return new_columns
        expanded_output_columns = expand_star_column(output_columns)
        expanded_output_columns = clean_columns(expanded_output_columns)
        order_by_columns = clean_columns(order_by_columns)
        check_columns_exist(expanded_output_columns)
        check_columns_exist(order_by_columns)
        sorted_rows = sort_rows(order_by_columns)
        
        if self.where_statement == True:
            sorted_rows = self.where_process(sorted_rows)
        if self.distinct_statement == True:
            return self.distinct_process(sorted_rows)
        else:    
            return generate_tuples(sorted_rows, expanded_output_columns)
        
    def delete(self):
        rows_to_delete = []
        for itr in self.rows:
           rows_to_delete.append(itr)
        if self.where_statement == True:
            rows_to_delete = self.where_process(rows_to_delete)
        for i in range(len(self.rows)-1, -1, -1):
            item = self.rows[i]
            if item in rows_to_delete:
                self.rows.pop(i)
                
    def update(self, column_names, update_dic):
        rows_to_update = []
        for itr in self.rows:
            rows_to_update.append(itr)
        if self.where_statement == True:
            rows_to_update = self.where_process(rows_to_update)
            
        for i in range(0, len(self.rows)):
            item = self.rows[i]
            if item in rows_to_update:
                for col in item:
                    if col in column_names:
                        if col in update_dic:
                            item[col] = update_dic[col]
                        else:
                            item[col] = None
        self.where_statement = False
        """else:
            for row in self.rows:
                for col in row:
                    if col in column_names:
                        row[col] = update_dic[col]
        """                
    def distinct_process(self, sorted_rows):
        temp = []
        result = []
        for itr in sorted_rows:
            temp.append(itr[self.distinct_column])
        temp = set(temp)
        for itr in temp:
            tup = (itr,)
            result.append(tup)
        result.sort()
        self.distinct_statement = False
        return result
        
    def where_process(self, sorted_rows):
        operand_code = self.where_code
        column = self.where_column
        value = self.where_value
        limit = len(sorted_rows)-1
        
        if operand_code == 0: # Where x does not equal
            for i in range(limit, -1, -1):
                temp = sorted_rows[i][column]
                if temp is None:
                    sorted_rows.pop(i)
                elif temp == value:
                    sorted_rows.pop(i)
                    
        elif operand_code == 1: # Where x equals
            for i in range(limit, -1, -1):
                temp = sorted_rows[i][column]
                if temp is None:
                    sorted_rows.pop(i)
                elif temp != value:
                    sorted_rows.pop(i)
                    
        elif operand_code == 2: # Where x is less than
            for i in range(limit, -1, -1):
                temp = sorted_rows[i][column]
                if temp is None:
                    sorted_rows.pop(i)
                elif temp >= value:
                    sorted_rows.pop(i)
                    
        elif operand_code == 3: # Where x is greater than
            for i in range(limit, -1, -1):
                temp = sorted_rows[i][column]
                if temp is None:
                    sorted_rows.pop(i)
                elif temp <= value:
                    sorted_rows.pop(i)
                    
        elif operand_code == -1: #IS NOT NULL:
            for i in range(limit, -1, -1):
                temp = sorted_rows[i][column]
                if temp is None:
                    sorted_rows.pop(i)
                    
        elif operand_code == -2: #IS NULL:
            for i in range(limit, -1, -1):
                temp = sorted_rows[i][column]
                if temp is not None:
                    sorted_rows.pop(i)
        else:
            pass
        self.where_statement = False
        return sorted_rows
        
    def get_column_names(self):
        return self.column_names
        
def pop_and_check(tokens, same_as):
    item = tokens.pop(0)
    assert item == same_as, "{} != {}".format(item, same_as)


def collect_characters(query, allowed_characters):
    letters = []
    for letter in query:
        if letter not in allowed_characters:
            break
        letters.append(letter)
    return "".join(letters)


def remove_leading_whitespace(query, tokens):
    whitespace = collect_characters(query, string.whitespace)
    return query[len(whitespace):]


def remove_word(query, tokens):
    word = collect_characters(query,
                              string.ascii_letters + "_" + "." + "*" +string.digits)
    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]


def remove_text(query, tokens):
    assert query[0] == "'"
    last_quotepos = 0
    quote_count = 0
    for i in range(0, len(query)):
        if query[i] == "'":
            last_quotepos = i
            quote_count = quote_count + 1
        if query[i] == ')':
            break
        if query[i] == ',':
            break
    if quote_count > 2:
        new_query = query[1:last_quotepos]
        new_query = new_query.replace("''", "'")
        tokens.append(new_query)
        query = query[last_quotepos+1:]
    else:
        assert query[0] == "'"
        query = query[1:]
        end_quote_index = query.find("'")
        text = query[:end_quote_index]
        tokens.append(text)
        query = query[end_quote_index + 1:]
    return query


def remove_integer(query, tokens):
    int_str = collect_characters(query, string.digits)
    tokens.append(int_str)
    return query[len(int_str):]


def remove_number(query, tokens):
    query = remove_integer(query, tokens)
    if query[0] == ".":
        whole_str = tokens.pop()
        query = query[1:]
        query = remove_integer(query, tokens)
        frac_str = tokens.pop()
        float_str = whole_str + "." + frac_str
        tokens.append(float(float_str))
    else:
        int_str = tokens.pop()
        tokens.append(int(int_str))
    return query


def tokenize(query):
    tokens = []
    while query:
        # print("Query:{}".format(query))
        # print("Tokens: ", tokens)
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
            continue

        if query[0] in "(),;*><!=":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] == "'":
            query = remove_text(query, tokens)
            continue

        if query[0] in string.digits:
            query = remove_number(query, tokens)
            continue

        if len(query) == len(old_query):
            raise AssertionError("Query didn't get shorter.")
    return tokens