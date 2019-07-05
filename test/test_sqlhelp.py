from sqlhelp import insert, update, select


def test_select():
    q = select('a', 'b', 'c')
    q.table(
        'table1', 'table2'
    ).join(
        'table3 on (table3.table1 = table1.id)'
    ).join('table4 as t on (t.table2 = table2.id)', jtype='outer'
    ).where(
        'table1.a > 50',
        'table2.name = %(name)s',
        name='Babar'
    ).limit(1
    ).order('a', 'desc')

    assert str(q) == (
        "query::[select  a, b, c from table1, table2 "
        "inner join table3 on (table3.table1 = table1.id) "
        "outer join table4 as t on (t.table2 = table2.id) "
        "where table1.a > 50 and table2.name = %(name)s "
        "order by a desc "
        "limit 1 "
        "{'name': 'Babar'}]"
    )


def test_insert():
    q = insert(
        'table1'
    ).values(
        a=1
    )

    assert str(q) == (
        "query::[insert into table1 (a) "
        "values (%(a)s) "
        "returning id "
        "{'a': 1}]"
    )

    q = insert(
        'table1'
    ).values(
        a=1,
        b=2
    ).returning('b')

    assert str(q) == (
        "query::[insert into table1 (a,b) "
        "values (%(a)s,%(b)s) "
        "returning b "
        "{'a': 1, 'b': 2}]"
    )


def test_update():
    q = update(
        'table'
    ).where(
        attr=42, # constrain `attr` from python value
    ).where(
        'id in %(ids)s', ids=(3, 7, 9)
    ).values(
        age=36,
        bar='hello'
    )

    assert str(q) == (
        "query::[update table set age = %(age)s, bar = %(bar)s  "
        "where id in %(ids)s and attr = %(attr)s "
        "{'attr': 42, 'ids': (3, 7, 9), 'age': 36, 'bar': 'hello'}]"
    )
