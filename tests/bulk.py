import csv
from sqlfile import Sq


db_path = ':memory:'


def test_1():
    def get_rows(*args):
        return [
            dict(a=f"a{_i}", b=f"b{_i}", c=f"c{_i}", d=f"d{_i}")
            for _i in range(*args)
        ]
    t = 'x'

    db_path = 'test.sqlite'
    with Sq(path=db_path, bulk_limit=5, replace=True) as sq:
        for i, row in enumerate(get_rows(10), start=1):
            sq.writerow(t, row)
            assert len(sq.buffer.get(t, [])) == i % 6, f"{len(sq.buffer.get(t, []))} == {i % 6}"
        sq.flush(finalize=True)
        for i, row in enumerate(get_rows(10, 20), start=1):
            sq.writerow(t, row)
            assert len(sq.buffer.get(t, [])) == i % 6, f"{len(sq.buffer.get(t, []))} == {i % 6}"

    with Sq(path=db_path) as sq:
        for row, row2 in zip(get_rows(20), sq.iter_table(table_name=t)):
            assert row == row2


if __name__ == '__main__':
    test_1()
