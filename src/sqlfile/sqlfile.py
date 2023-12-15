import sqlite3
import logging
import csv
from pathlib import Path
from tqdm import tqdm


__version__ = "0.1.0"
status_field = 'DB_ROW_STATUS'


log = logging.getLogger('sql_storage')


def _count_lines(path, chunk_size=0x10000):
    sz = Path(path).stat().st_size
    lines = 0
    with open(path) as _f, tqdm(total=sz,
                                desc='count lines', leave=False) as bar:
        while True:
            data = _f.read(chunk_size)
            bar.update(len(data))
            if not data:
                break
            lines += data.count('\n')
    return lines


class Sq:
    """
    Useful methods:
        - writerow(table, row)
        - read_csv(table, path.csv)
        - list_all_tables()
        - list_fields(table_name)
        - iter_table(table_name)
        -

    Other:
        - create_table(...)
        - drop_table(...)
        - add_new_column(...)
        - update_field(...) - update row(s) with specific value
        - write( table_name, values)
        -
    """

    def __init__(self, path='raw_msgs.sqlite',
                 append=False,
                 silent=False,
                 bulk_limit=5000,
                 replace=False
                 ):
        self.path = path
        self.bulk_limit = bulk_limit
        self.buffer = dict()
        self.buffer_updates = dict()
        self.silent = silent
        self._append = append

        # sup
        self._field_names = dict()  # table: [fields...]
        if replace:
            log.debug(f"replace: {Path(self.path).resolve().absolute()}")
            if Path(self.path).exists():
                Path(self.path).unlink()

        log.debug(f"open: {Path(self.path).resolve().absolute()}")
        self.db = sqlite3.connect(str(self.path))

        # def _dict_factory(cursor, row):
        #     d = {}
        #     for idx, col in enumerate(cursor.description):
        #         d[col[0]] = row[idx]
        #     return d
        self.db.row_factory = sqlite3.Row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        log.debug('exit')
        self.close()
        log.debug('closed')

    def detailed_header(self, table):
        existing_tables = self.tables()
        if table not in existing_tables:
            raise sqlite3.OperationalError("No such table: {}".format(table))
        else:
            # cid, name, type, notnull, default, pk
            q = f"pragma table_info({table})"
            log.debug(q)
            res = self.db.execute(q).fetchall()
            log.debug(f"{res}")
            columns = dict()
            for row in res:
                cid, name, col_type, notnull, default, pk = row
                columns.update({name: dict()})
                columns[name].update({'cid': cid})
                columns[name].update({'col_type': col_type})
                columns[name].update({'notnull': notnull})
                columns[name].update({'default': default})
                columns[name].update({'pk': pk})
            return columns

    def table_columns(self, table):
        existing_tables = self.tables()
        if table not in existing_tables:
            raise sqlite3.OperationalError("No such table: {}".format(table))
        else:
            # cid, name, type, notnull, default, pk
            q = f"pragma table_info([{table}])"
            res = self.db.execute(q).fetchall()
            log.debug(f"{res}")
            header = list()
            for row in res:
                _, column_name, *_ = row
                header.append(column_name)
            # save header
            self._field_names[table] = header
            return header

    def tables(self):
        q = "select name from sqlite_master where type = 'table'"
        res = self.db.execute(q).fetchall()

        tables = [table_desc[0] for table_desc in res]
        log.debug(f"{tables}")
        return tables

    def create_table(self, name: str, header: list = None, dtypes=None,
                     append=False):

        assert header is not None or dtypes is not None, [header, dtypes]

        if append:
            if name in self.buffer:
                log.warning(f"{name} in buffer already")
                # self.buffer[table_name] = dict()
        else:
            self.buffer[name] = dict()
            q = f'DROP TABLE IF EXISTS [{name}]'
            # log.warning(q)
            self.execute(q)

        if header is None:
            header = list()

        if dtypes is not None:
            _header = set(dtypes.keys())
            for _field in _header:
                if _field not in header:
                    header.append(_field)

        assert header is not None, header
        # expected_dtypes = {'TEXT', 'NUMERIC', 'INTEGER', 'REAL', 'BLOB'}

        def _cvt_dt(field: str) -> str:

            default_dt = str
            if dtypes is None:
                return 'TEXT'
            dt = dtypes.get(field, default_dt)
            sql_type = {
                int:   'INTEGER',
                float: 'NUMERIC',
                str:   'TEXT',
                bytes: 'BLOB',
                'BLOB': 'BLOB',
                'INT': 'INT',
                'INTEGER': 'INTEGER',
                'NUMERIC': 'NUMERIC',
            }[dt]
            return sql_type

        q_fields_and_dt = ', '.join([
            f"'{_field}' {_cvt_dt(_field)}"
            for _field in header
        ])
        q = f"CREATE TABLE IF NOT EXISTS '{name}' ({q_fields_and_dt})"

        self.buffer[name] = list()
        log.debug(f"exec: {q}")
        try:
            self.db.execute(q)
            self.db.commit()
        except sqlite3.OperationalError as exc:
            log.error(f"create_table: `{q}`")
            raise exc

        # save header
        self._field_names[name] = self.table_columns(name)

    def close(self):
        self.flush(finalize=True)
        self.db.close()
        log.debug('closed')

    def execute(self, q):
        self.flush(True)
        try:
            self.db.execute(q)
        except sqlite3.OperationalError as e:
            log.error(f"{q}")
            raise e
        self.db.commit()

    def executescript(self, q):
        self.flush(True)
        try:
            self.db.executescript(q)
        except sqlite3.OperationalError as e:
            log.error(f"```{q}```")
            raise e
        self.db.commit()

    def flush(self, finalize=False):

        log.debug('flash')
        for table_name in self.buffer.keys():
            if len(self.buffer[table_name]) > 0:
                log.debug(f"{table_name}: {len(self.buffer[table_name])}")
                fields = self._field_names[table_name]

                n_fields = len(self._field_names[table_name])
                # TODO:
                if fields[-1] == 'DB_ROW_STATUS':
                    fields = fields[:-1]
                    n_fields -= 1

                fields_s = '`,`'.join(fields)
                values_tags = ','.join(
                    '?' for _ in range(n_fields)
                )
                q = f'INSERT INTO [{table_name}] (`{fields_s}`) ' + \
                    f'VALUES({values_tags})'
                log.debug(f"Executemany: {q}")
                try:
                    self.db.executemany(q,
                                        self.buffer[table_name])
                    self.db.commit()
                    self.buffer[table_name] = list()
                except Exception as exc:
                    log.error(f"self.buffer: {self.buffer}")
                    log.error(f"{q}")
                    log.error(f"fields: {fields}")
                    raise exc

        self._flush_updates(finalize=finalize)

    def replace_value(self, table, field, value, new_value, new_field=None):
        if new_field is None:
            new_field = field
        if value == "":
            self.execute(f"""
            UPDATE "{table}" 
            SET "{field}" = '{new_value}' 
            WHERE {field} = '{value}' """)
        else:
            self.execute(f"""
            UPDATE "{table}" 
            SET "{field}" = REPLACE("{new_field}", '{value}', '{new_value}')""")

    def update_field(self, table_name, field: str, value, keys: dict):
        """e.g. update FIELD1 with VAL1
        where key_field='row_id'
        and key_value = <row_id>"""
        keys_where = list()
        values_where = list()

        for key in keys:
            keys_where.append(key)
            values_where.append(keys[key])

        q_where_clause = ' AND '.join([f'{x} = ?' for x in keys_where])
        q_template = f"UPDATE {table_name} " \
                     f"SET [{field}] = ? " \
                     f"WHERE {q_where_clause}"

        log.debug(f"{q_template}")
        if q_template not in self.buffer_updates:
            self.buffer_updates[q_template] = list()

        column_values = (value, *values_where)
        self.buffer_updates[q_template].append(column_values)

    def _flush_updates(self, finalize=False):
        drop_templates = list()
        for q_template in self.buffer_updates:
            if (len(self.buffer_updates[q_template]) > self.bulk_limit) \
                    or finalize:
                # Trigger
                self.db.executemany(
                    q_template,
                    self.buffer_updates[q_template]
                )
                self.db.commit()
                drop_templates.append(q_template)

        for dt in drop_templates:
            del self.buffer_updates[dt]

    def add_new_column(self, table_name, column_name, sql_dt='TEXT'):
        # e.g. sql_dt = 'varchar(32)'
        # Add a new column to student table

        self.flush(finalize=True)
        q = f"ALTER TABLE {table_name} ADD COLUMN '{column_name}' {sql_dt}"
        log.debug(q)
        self.flush()
        self.db.execute(q)
        self._field_names[table_name] = list(self._field_names[table_name])
        self._field_names[table_name].append(column_name)
        self._field_names[table_name] = tuple(self._field_names[table_name])

    def rename_columns(self, table_name, column_names):
        for old, new in column_names.items():
            self.db.execute(f"""
            ALTER TABLE '{table_name}' 
            RENAME COLUMN '{old}' to '{new}'
            """)

    def change_column_type(self, table_name, column_name, dtype):
        self.execute('PRAGMA journal_mode = OFF')
        tmp_col = f"tmp_{column_name}"

        log.info('create tmp_column')
        q = f"ALTER TABLE [{table_name}] ADD COLUMN {tmp_col} {dtype}"
        self.execute(q)

        q = f"UPDATE [{table_name}] " + \
            f"SET {tmp_col} = CAST({column_name} as {dtype})"
        log.info(f"{q}")
        self.execute(q)

        log.info("delete tmp column")
        q = f"ALTER TABLE [{table_name}] DROP COLUMN {column_name}"
        self.execute(q)

        q = f"ALTER TABLE [{table_name}] " + \
            f"RENAME COLUMN {tmp_col} TO {column_name}"

        self.execute(q)

        q = "VACUUM"
        log.info(f"{q}")
        self.execute(q)

    def rename_table(self, table_name, new_table_name):
        q = f'ALTER TABLE {table_name} RENAME TO {new_table_name};'
        self.execute(q)

    def append(self, table, row):
        raise NotImplementedError("Method append")

    def writerow(self, table, row):
        # replace--> INSERT OR REPLACE INTO
        if table not in self._field_names:
            # if table not in self.list_all_tables():

            self.create_table(name=table, header=list(row.keys()),
                              append=self._append)

        new_fields = set(row.keys()) - set(self._field_names[table])
        if len(new_fields):
            for new_field in new_fields:
                log.warning("add new column")
                self.add_new_column(table_name=table, column_name=new_field)

        row_list = list()
        log.debug(f"{self._field_names[table]}")
        for field in self._field_names[table]:
            value = row.get(field, None)

            row_list.append(value)
        log.debug(f"> {row_list}")

        self.buffer[table].append(row_list)
        if len(self.buffer[table]) > self.bulk_limit:
            self.flush(True)

    def query_as_table(self, query, to_table):
        self.executescript(f"DROP TABLE IF EXISTS [{to_table}];"
                           f"CREATE TABLE [{to_table}] AS {query};")

    def read_csv(self, table, path, has_header=True, append=False,
                 converter=None, count=None,
                 csv_opts: dict = None, dtypes=None):

        if not path or not Path(path).exists():
            raise IOError(f"No such file '{path}'")

        if not csv_opts:
            csv_opts = dict()
        with open(path) as f:
            total_lines = None
            if not self.silent:
                if count is None:
                    total_lines = _count_lines(path)
                else:
                    total_lines = count
                f.seek(0)

            # c = csv.DictReader(f)
            header = csv_opts.pop('fieldnames', [])
            c = csv.reader(f, **csv_opts)
            if has_header:
                header = next(c)
                self.create_table(name=table, header=header,
                                  append=append, dtypes=dtypes)
            else:
                if not header:
                    row = [f"col{i}" for i in enumerate(list(next(c)))]
                    header = [f"col{i}" for i in enumerate(row)]
                    self.write(table, row)
                self.create_table(name=table, header=header,
                                  append=append, dtypes=dtypes)
            for row in tqdm(c, total=total_lines, desc=f'load: {path}',
                            unit='row', disable=self.silent):

                if converter:
                    converter(row)
                # self.writerow(table, row)
                self.write(table, row)
        self.flush(finalize=True)

    def write(self, table, values):
        self.buffer[table].append(values)
        if len(self.buffer[table]) > self.bulk_limit:
            self.flush(True)

    def iter_query(self, query):
        self.flush(finalize=True)
        c = self.db.execute(query)
        for row in c:
            yield dict(row)

    def iter_table(self, table_name, req_columns=None, where_clause=''):
        self.flush(finalize=True)

        if req_columns is None:
            req_columns = '*'
        else:
            req_columns = '"' + '","'.join(req_columns) + '"'
            # req_columns = ', '.join(req_columns)

        if where_clause != '':
            request = "SELECT {} from [{}] where {}".format(
                req_columns, table_name, where_clause)
        else:
            request = "SELECT {} from [{}]".format(req_columns, table_name)

        c = self.db.execute(request)
        for row in c:
            yield dict(row)

    def select_random_row(self, table, where=''):
        self.flush(finalize=True)
        row = self.db.execute(f"SELECT * FROM [{table}]"
                              f"WHERE {where} "
                              f"ORDER BY RANDOM() LIMIT 1").fetchone()
        return row

    def drop(self, table):
        self.flush(finalize=True)
        self.db.execute(f"DROP TABLE IF EXISTS {table}")
        self.db.commit()

    def counts(self, table=None):
        if table is not None:
            return self.db.execute(
                f"SELECT COUNT(1) as cnt FROM [{table}]").fetchone()['cnt']
        result = dict()
        for table in self.tables():
            c = self.db.execute(
                f"SELECT COUNT(1) as cnt FROM [{table}]").fetchone()['cnt']
            result[table] = c
        return result

    def head(self, table, n=5):
        for i, row in enumerate(self.iter_table(table), start=1):
            if i > n:
                break

    def iter_by_bit(self, tab, bit, where=''):

        q = f"select rowid, * FROM [{tab}] WHERE "
        q += f"{status_field} & (1 << {bit})"

        if where:
            q += f" AND {where}"

        return self.iter_query(q)

    def mark_with_bit(self, tab, bit, where):
        self.flush(True)
        """Detect DUPLICATES"""

        # tke = _tab_key_expression(tab, keys)
        # _tab = 'tabC'
        # tab2_and = _x_expression(tab, keys, _tab, keys)

        q = f'''
        UPDATE {tab}
        SET {status_field} = {status_field} | (1 << {bit})
        WHERE {where}
        '''
        try:
            log.debug(f"mark_duplicated_rows: `{q}`")
            self.execute(
                f"ALTER TABLE {tab} ADD COLUMN '{status_field}' INT DEFAULT 0")
        except sqlite3.OperationalError:
            pass

        # except sqlite3

        self.execute(q)


if __name__ == '__main__':
    pass
