import logging
from sqlfile import Sq
from itertools import zip_longest
log = logging.getLogger('sql_storage')


db_path = 'test.sqlite'


def test_read_iter():

    def g(n):
        for _i in range(n):
            yield dict(a=f"a{_i}", b=f"b{_i}", c=f"c{_i}", d=f"d{_i}")

    with Sq(':memory:') as sq:
        sq.read_iter('x', it=g(10))

        for (row1, row2) in zip_longest(sq.iter_table('x'), g(10)):
            assert row1 == row2


if __name__ == '__main__':
    logging.basicConfig(level='INFO')
