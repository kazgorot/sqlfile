import csv
import logging
import random
import string
import tempfile
from sqlfile import Sq
import time
log = logging.getLogger('sql_storage')


db_path = 'test.sqlite'


def _random_string(max_chars=90):
    return ''.join([random.choice(string.digits + string.ascii_letters)
                    for _ in range(random.randint(1, max_chars))])


def _check_rows(row, sql_row):
    assert set(sql_row.keys()) <= set(row.keys()), (
            set(row.keys()) - set(sql_row.keys()))
    for field in sql_row.keys():
        if field not in row:
            assert sql_row[field] is None
        else:
            if isinstance(sql_row[field], type(row[field])):
                log.warning(f"{field}: Different types: "
                            f"{ type(sql_row[field])} "
                            f"is not {type(row[field])}")
            if str(sql_row[field]) == str(row[field]):
                pass
            else:
                assert sql_row[field] == row[field], \
                    f"{sql_row[field]} != {row[field]}"


def test_add_new_columns():
    sq = Sq(db_path, replace=True)
    sq.create_table(name='table1',
                    dtypes={'new_col_text': str, 'new_col_blob': bytes})
    sq.writerow('table1',
                {'new_col_text': 'value1', 'new_col_blob': b'123'})
    sq.writerow('table1',
                {'new_col_text': 'value2', 'new_col_blob': b'345'})

    sq.flush()

    # text

    val_1_text = '567'
    val_2_text = '890'
    val_1_blob = b'\x01\x02'
    val_2_blob = b'\x03\x04'

    sq.update_field(table_name='table1',
                    field='new_col_text',
                    value=val_1_text,

                    keys={'rowid': 1}
                    )

    sq.update_field(table_name='table1',
                    field='new_col_text',
                    value=val_2_text,

                    keys={'rowid': 2}
                    )

    # blob

    sq.update_field(table_name='table1',
                    field='new_col_blob',
                    value=val_1_blob,
                    keys={'rowid': 1}
                    )
    sq.flush(True)

    sq.update_field(table_name='table1',
                    field='new_col_blob',
                    value=val_2_blob,
                    keys={'rowid': 2}
                    )

    sq.flush(finalize=True)

    res_text = sq.db.execute(
        'select new_col_text from table1 where rowid = 1').fetchone()[0]
    assert res_text == val_1_text, f"{res_text} == {val_1_text}"

    res_text = sq.db.execute(
        'select new_col_text from table1 where rowid = 2').fetchone()[0]
    assert res_text == val_2_text

    res_blob = sq.db.execute(
        'select new_col_blob from table1 where rowid = 1').fetchone()[0]
    assert res_blob == val_1_blob

    res_blob, = sq.db.execute(
        'select new_col_blob from table1 where rowid = 2').fetchone()
    assert res_blob == val_2_blob


def test_context_manager():
    with Sq(db_path, replace=True) as sq1:
        sq1.create_table('table1', dtypes={'raw': bytes})
        sq1.writerow('table1',
                     {'meta': '{"key1": "value1"}', 'raw': b'123'})

    with (Sq(db_path, replace=False) as sq2):
        res = sq2.db.execute('select * from table1 where rowid = 1').fetchone()
        assert {'meta': '{"key1": "value1"}', 'raw': b'123'} == dict(res), \
            dict(res)


def test_generate_data1():
    with Sq(db_path, replace=True) as sq:
        sq.writerow('t123', dict({'field1': 'b1', 'raw': b'12345'}))


def test_random_write_and_update():
    bulk_limit = 1000
    with Sq(db_path, replace=True,
            bulk_limit=bulk_limit) as sq:
        sq.create_table('t1',
                        header=['value', 'new', 'updated'],
                        dtypes=dict(value=int, updated=int))
        for i in range(bulk_limit//2):
            # n = random.randint(0, i)
            sq.writerow(table='t1', row=dict(value=i, new=0, updated=0))
            sq.update_field(table_name='t1', field='updated', value=i*2,
                            keys=dict(value=i))

        for row in sq.iter_table(table_name='t1'):
            assert row['value']*2 == row['updated']


def test_create_table_with_dt():
    with Sq(db_path, replace=True) as sq:
        sq.create_table(name='tab1',
                        header=['a', 'b', 'c', 'd'],
                        dtypes={'a': int, 'b': bytes, 'c': float, 'd': str})
        # sq.flush()
        pragma = sq.detailed_header('tab1')

        assert pragma['a']['col_type'] == 'INTEGER'
        assert pragma['b']['col_type'] == 'BLOB'
        assert pragma['c']['col_type'] == 'NUMERIC'
        assert pragma['d']['col_type'] == 'TEXT'


def test_add_rows_with_different_fields():
    with Sq(db_path, replace=True) as sq:
        sq.create_table(
            name='tab1',
            header=['a', 'b', 'c', 'd'],
            dtypes={'a': int, 'b': int, 'c': int}
            )

        rows = [
            # row_normal
            {'a': 1, 'b': 12, 'c': 13, 'd': 14},

            # row_new_column1
            {'new1': 202, 'a': 21, 'b': 22, 'c': 23, 'd': 24},

            # row_new_column2
            {'new2': 203, 'new3': 204, 'a': 21, 'b': 22, 'c': 23, 'd': 24}
        ]
        for row in rows:
            sq.writerow('tab1', row=row)

    with Sq(db_path, replace=False) as sq:
        for sql_row, expected_row in zip(
                sq.iter_table(table_name='tab1'), rows):
            _check_rows(sql_row, expected_row)


def test_multiple_tables1():
    with Sq(db_path, replace=True) as sq:
        same_header = ['a', 'b']
        sq.create_table(name='tab1',
                        header=same_header
                        )

        sq.create_table(name='tab2',
                        header=same_header
                        )
        sq.writerow('tab1', row={'a': 1, 'b': 10})

        sq.writerow('tab2', row={'a': 1, 'b': 20})

        sq.update_field('tab1', field='a', value=11, keys={'a': 1})
        sq.update_field('tab2', field='a', value=22, keys={'a': 1})

        row1 = list(sq.iter_table('tab1'))[0]
        assert row1['a'] == '11'
        assert row1['b'] == '10'

        row2 = list(sq.iter_table('tab2'))[0]
        assert row2['a'] == '22'
        assert row2['b'] == '20'


def test_write_update():
    with Sq(db_path, replace=True) as sq1:
        sq1.create_table('tab1', dtypes={'a': int, 'b': int})
        sq1.writerow('tab1', row={'a': 1, 'b': 10})
        sq1.writerow('tab1', row={'a': 2, 'b': 20})

    with Sq(db_path, append=True) as sq2:
        sq2.writerow('tab1', row={'a': 3, 'b': 30})

    with Sq(db_path, append=True) as sq3:
        for row, exp in zip(sq3.iter_table('tab1'), [
            {'a': 1, 'b': 10},
            {'a': 2, 'b': 20},
            {'a': 3, 'b': 30},
        ]):
            assert row == exp, [row, exp]


def test_new_file_append_random():
    msgs = dict(tab1=list(), tab2=list())
    n = 10**3
    bulk_limit = random.randint(2, 100)

    def _append(tab, info=''):
        with Sq(db_path,
                append=True,
                bulk_limit=bulk_limit) as sq2:
            for i_ in range(n):
                row_ = dict({
                    'a': f"{i_}", 'b': f"{i_ + 1}", 'c': f"{i_ + 2}",
                    'd': f"{random.randint(1, n)}",
                    'info': info
                    })
                msgs[tab].append(row_)
                sq2.writerow(tab, row=row_)

    log.info('init file')
    with Sq(
            db_path,
            replace=True,
            bulk_limit=bulk_limit) as sq1:
        for i in range(n):
            row = dict({'a': f"{i}", 'b': f"{i + 1}", 'c': f"{i+2}",
                        'd': f"{random.randint(1, n)}",
                        'info': 'replace123'
                        })
            msgs['tab1'].append(row)
            sq1.writerow('tab1', row=row)

    log.info('* append batches')
    for i in range(10):
        start = time.time()
        _append('tab1', info=f"try: {i}")
        e = time.time() - start
        log.info(f'> {i}, {e:0.2f}s, {n/e:0.2f} m/s')

    _append('tab2', info="final")

    log.info('check msgs')
    with Sq(db_path) as sq3:
        for row, exp in zip(sq3.iter_table('tab1'), msgs['tab1']):
            assert row == exp, [row, exp]

    log.info('check msgs2')
    with Sq(db_path) as sq3:
        for row, exp in zip(sq3.iter_table('tab2'), msgs['tab2']):
            assert row == exp, [row, exp]


def test_read_csv():
    sql_path = db_path
    rows_n = random.randint(1, 1000)
    header = ["c_" + _random_string() for _ in range(random.randint(1, 90))]
    random.shuffle(header)

    log.debug(f'header: {header}')
    sq = Sq(sql_path, replace=True)
    rows = list()
    with tempfile.NamedTemporaryFile(mode='a+') as f:
        cw = csv.DictWriter(f, fieldnames=header)
        cw.writeheader()
        for i in range(rows_n):
            row = dict({c: _random_string() for c in header})
            rows.append(row)
            cw.writerow(row)

        f.seek(0)
        sq.read_csv('test_table', str(f.name))

        for orig, saved in zip(rows, sq.iter_table('test_table')):
            assert orig == saved, f"orig: {orig}\n == \nsaved: {saved}"


def test_iter_table():
    with Sq(db_path) as sq:
        msgs = [
            {'field1': 'value1_1', 'field2': 'value_1_2'},
            {'field1': 'value2_1', 'field2': 'value_2_2'},
            {'field1': 'value3_1', 'field2': 'value_3_2'}
        ]
        for m in msgs:
            sq.writerow('tab1', m)

        for ma, mb in zip(msgs, sq.iter_table('tab1')):
            assert ma == mb

        l1 = list(sq.iter_table(
            'tab1', where_clause="field2 = 'value_2_2'"))

        assert len(l1) == 1
        assert l1[0] == msgs[1]

        l2 = list(sq.iter_table(
            'tab1', req_columns=['field2'],
            where_clause="field1 = 'value3_1'"))

        assert len(l2) == 1
        assert l2[0] == {'field2': 'value_3_2'}, l2[0]


def test_mark_bit():
    from collections import Counter

    n = 10
    with Sq(db_path, replace=True) as sq:
        cnt_bits = Counter()
        # msgs = [
        #     {'field1': '0', 'field2': 'value_1_2'},
        #     {'field1': '1|2', 'field2': 'value_2_2'},
        #     {'field1': '0|2', 'field2': 'value_2_2'},
        #     {'field1': 'no', 'field2': 'value_3_2'}
        # ]

        msgs = list()

        field_bit_ref = 'field1'
        bits_per_message = set()
        for i in range(n):
            bits = set()
            if random.choice([True, False]):
                for _ in range(random.randint(1, 10)):
                    b = random.randint(0, 30)
                    bits.add(str(b))
            else:
                bits = ['no']

            bits_per_message.add(tuple(bits))
            msg = dict({field_bit_ref: '|'.join(bits),
                        'field2': f'value_1_{i}_bit-{bits}'})
            for b in bits:
                cnt_bits[b] += 1
            msgs.append(msg)

        random.shuffle(msgs)

        for msg in msgs:
            sq.writerow('tab1', msg)

        # mark bits by values

        for msg in msgs:
            bits_raw = msg['field1']
            if bits_raw == 'no':
                continue
            bits = bits_raw.split('|')
            for b in bits:
                # print(b)

                try:
                    bit = int(b)
                    sq.mark_with_bit('tab1', bit,
                                     where=f"field1 = '{bits_raw}'")
                except ValueError:
                    pass

        sq.mark_with_bit('tab1', 1, where="field1 = 'value2_1'")

        # for row in sq.iter_table('tab1'):
        #     log.info(f"{row}")
        # log.info(f"{cnt_bits}")
        actual, expected = sum((1 for _ in sq.iter_table('tab1'))), len(msgs)
        assert expected == actual, f"{expected} == {actual}"

        # log.info("show by bit: 1")
        # bit = 1
        # for row in sq.iter_by_bit('tab1', bit):
        #     log.info(f"{[bit, row]}")
        #
        # log.info("show by bit: 0")
        # bit = 0
        # for row in sq.iter_by_bit('tab1', bit):
        #     log.info(f"{[bit, row]}")

        bits = set()
        for bit in cnt_bits:
            if bit == 'no':
                expected, actual = cnt_bits[bit], sum(
                    (1 for _ in sq.iter_table(
                        'tab1',
                        where_clause=f"{field_bit_ref} = '{bit}'")))
            else:
                expected, actual = cnt_bits[bit], sum((
                    1 for _ in sq.iter_by_bit('tab1', bit)))
                bits.add(bit)
            assert expected == actual, f"bit:{bit}; {expected} == {actual}"

        # # many bits: TODO. Separate test


if __name__ == '__main__':
    logging.basicConfig(level='INFO')
