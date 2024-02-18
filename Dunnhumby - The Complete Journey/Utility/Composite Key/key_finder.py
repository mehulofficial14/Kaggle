import Connection as C
import pandas as pd
from psycopg2.extensions import QueryCanceledError

class Key:
    def __init__(self, schema, table):
        self.s = schema
        self.t = table
        self.row_counts = None
        self.columns = None
        self.desc = None
        self.comp_key = None
        self.remove = False
        
    def remove_floats(self):
        q = f"""
        SELECT
            TABLE_CATALOG,
            TABLE_SCHEMA,
            TABLE_NAME, 
            COLUMN_NAME, 
            DATA_TYPE 
        FROM
            INFORMATION_SCHEMA.COLUMNS
        WHERE
            TABLE_SCHEMA = '{self.s}'
            AND TABLE_NAME = '{self.t}'
            AND DATA_TYPE = 'double precision'
        ;
        """
        c = C.Connection()
        c.start()
        c.run(q)
        rows = c.cursor.fetchall()
        c.clear_res()
        c.stop()
        for double_columns in rows:
            column_name = double_columns[3]
            if column_name in self.columns:
                self.columns.remove(column_name)
            del self.desc[column_name]
        print(f'Output after removing decimal type columns from {self.s}.{self.t}')
        print(self.desc)
    
    def get_row_counts(self):
        q = f'''
        SELECT
            COUNT(*)
        FROM
            {self.s}.{self.t}
        ;
        '''
        c = C.Connection()
        c.start()
        c.run(q)
        rows = c.cursor.fetchall()
        c.clear_res()
        c.stop()
        self.row_counts = rows[0][0]
        print(f'Total number of rows in {self.s}.{self.t} is', self.row_counts)
    
    def get_columns(self):
        q = f'''
        SELECT
            *
        FROM
            {self.s}.{self.t}
        LIMIT(0)
        ;
        '''
        c = C.Connection()
        c.start()
        c.run(q)
        self.columns = [desc[0] for desc in c.cursor.description]
        c.clear_res()
        c.stop()
        self.desc = {}
        for cc in self.columns:
            self.desc[cc] = [0, 0]
        print(f'The columns in {self.s}.{self.t}', self.columns)
        if self.remove == False:
            self.remove_floats()
            self.remove = True
        
    def get_sub(self, names):
        sub = ', '.join(names)
        q = f'''
        SELECT
            COUNT(d.*)
        FROM
            (SELECT
                DISTINCT {sub}
            FROM
                {self.s}.{self.t}) AS d
        ;
        '''
        c = C.Connection()
        c.start()
        c.run(q)
        rows = c.cursor.fetchall()
        c.clear_res()
        c.stop()
        print(f'The total disitnct counts after taking {names} as the composite key are', rows[0][0])
        return rows[0][0]
    
    def get_counts(self):
        if self.row_counts == None:
            self.get_row_counts()
        if self.columns == None:
            self.get_columns()
        for column in self.columns:
            q = f'''
            SELECT
                COUNT(DISTINCT {column})
            FROM
                {self.s}.{self.t}
            ;
            '''
            c = C.Connection()
            c.start()
            c.run(q)
            rows = c.cursor.fetchall()
            c.clear_res()
            c.stop()
            self.desc[column][0] = rows[0][0] * 100 / self.row_counts
            q = f'''
            SELECT
                COUNT(*)
            FROM
                {self.s}.{self.t}
            WHERE
                {column} IS NULL
            ;
            '''
            c = C.Connection()
            c.start()
            c.run(q)
            rows = c.cursor.fetchall()
            c.clear_res()
            c.stop()
            self.desc[column][1] = rows[0][0] * 100 / self.row_counts
        print('The desscrition of each column is', self.desc)
        
        def custom_sort_key(item):
            return (-item[1][0], item[1][1])
        self.desc = dict(sorted(self.desc.items(), key = custom_sort_key))
        
    def get_composite_key(self):
        if self.desc == None:
            self.get_counts()
        l = 1
        r = len(self.desc)
        ans = []
        benchmark = self.row_counts
        while l <= r:
            m = l + (r - l) // 2
            top_m = list(self.desc.keys())[ : m]
            print('Taking top m =', m)
            print('Columns are', top_m)
            print('Benchmark is', benchmark)
            cur = self.get_sub(top_m)
            print('Cur is', cur, 'and the difference is', benchmark - cur)
            if cur == benchmark:
                ans = top_m
                r = m - 1
            else:
                l = m + 1
        self.comp_key = ans
        print(f'The final composite key for {self.s}.{self.t} is', self.comp_key)
    
    def run(self):
        self.get_composite_key()