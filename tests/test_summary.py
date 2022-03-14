from dbrecord.summary import check_db_table_ok, summary_db, summary_table_struct
from dbrecord import PList


def test_summary():
    res = summary_db('./disk.sqlite')
    assert res['exists'] == 'not exists'

    res = summary_db(__file__)
    assert res['exists'] == 'ok'
    assert res['is_database'] == False

    lis = PList('./disk.sqlite')
    for i in range(10):
        lis.append(i)

    res = summary_db('./disk.sqlite')
    assert res['exists'] == 'ok'

    res, _ = check_db_table_ok('./disk.sqlite', 'map', ['id'])
    assert not res

    res, _ = check_db_table_ok('./disk.sqlite', 'list', ['id'])
    assert res
