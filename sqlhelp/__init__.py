"""A tiny helper to write plain sql the python way."""

def sqlfile(path, **kw):
    """Load sql statements from function and complete placeholders with `kw`."""
    sql = path.read_text()
    return sql.format(**kw)


class _sqlbase:
    """Abstract base for sql query builders.

    It provides standard __str__, __repr__ and a .do helper
    needs .sql and .kw attributes
    """

    def __str__(self):
        sql, kw = self._assemble()
        return f"query::[{sql} {kw}]"

    __repr__ = __str__

    def do(self, executor):
        """Execute the sql using the given `executor`."""
        sql, kw = self._assemble()
        return executor.execute(
            sql, kw
        )


class insert(_sqlbase):
    """Utility to build an insert query string.

    usage: insert('mytable').values(v1=42, v2='Babar')
    """
    __slots__ = ('_table', '_kw', '_returning')

    def __init__(self, table):
        self._table = table
        self._kw = None
        self._returning = 'id'

    def values(self, **values):
        """Declare values to insert."""
        self._kw = values
        return self

    def returning(self, name):
        self._returning = name
        return self

    def _assemble(self):
        """Return the sql query string and values suitable for executor."""
        assert self._kw, "Call values() first"
        names = ','.join(list(self._kw))
        holders = ','.join(f'%({name})s' for name in self._kw)
        return (
            f'insert into {self._table} '
            f'({names}) values ({holders}) '
            f'returning {self._returning}'
        ), self._kw.copy()


class _wherebase(_sqlbase):
    """base class to bring .table and .where facilities"""
    __slots__ = ('_tables', '_wheres',
                 '_whereskw', '_kw')

    def table(self, *tables):
        """Add table in from section."""
        self._tables.append(', '.join(tables))
        return self

    def where(self, *wheres, **kw):
        """Add filter in where section."""
        if wheres:  # arbitrary expressions
            self._whereskw.update(kw)
            for where in wheres:
                self._wheres.append(where)
        else:
            # plain x=<val> expressions
            self._kw.update(kw)
        return self

    def _build_where(self):
        kw = self._kw.copy()
        wheres = ''
        if kw or self._wheres:
            wherelist = self._wheres + [
                f'{name} = %({name})s'
                for name in kw
            ]
            wheres = f'where {" and ".join(wherelist)}'
        kw.update(self._whereskw)
        return wheres, kw


class update(_wherebase):
    """utility to build an update query string

    q = update(
        'table'
    ).where(
        attr=42, # constrain `attr` from python value
        'id in %(ids)s', ids=tuple(3, 7, 9)
    ).values(
        age=36,
        bar='hello'
    )
    """
    __slots__ = ('_table', '_tables', '_wheres',
                 '_whereskw', '_valueskw', '_kw')

    def __init__(self, table):
        self._table = table
        self._tables = []
        self._valueskw = {}
        self._wheres = []
        self._whereskw = {}
        self._kw = {}

    def values(self, **values):
        """Declare values to update."""
        self._valueskw = values
        return self

    def _assemble(self):
        """Return the sql query string and values suitable for executor."""
        setexpr = ', '.join(
            f'{name} = %({name})s'
            for name in self._valueskw
        )
        froms = 'from ' + ', '.join(self._tables) if self._tables else ''
        kw = self._kw.copy()
        wheres, wkw = self._build_where()
        kw.update(wkw)
        kw.update(self._valueskw)
        return (
            f'update {self._table} '
            f'set {setexpr} '
            f'{froms} '
            f'{wheres}'
        ), kw


class select(_wherebase):
    """utility to incrementally build an sql query string along with its
    parameters

    usage:

    q = select(
        'id', 'attr1', 't2.attr2'
    ).table(
        'table'
    ).join('table2 as t2 on (t2.id = table.t2)'
    ).where(
        attr3=42
    .where(
        'table.attr1 < t2.attr42',
        'attr3 < %(attr3)s',
        attr3=42
    ).limit(50)
    """
    __slots__ = ('_head', '_headopt',
                 '_tables', '_joins',
                 '_wheres', '_order', '_limit',
                 '_whereskw', '_kw')

    def __init__(self, *head, opt=''):
        assert opt in ('', 'distinct')
        self._head = list(head)
        self._headopt = opt
        self._tables = []
        self._joins = []
        self._wheres = []
        self._whereskw = {}
        self._order = ()
        self._limit = None
        self._kw = {}

    def select(self, *select):
        """Add query statement in select section."""
        self._head.append(', '.join(select))
        return self

    def join(self, *joins, jtype='inner'):
        """Add relation in join section."""
        assert jtype in ('inner', 'outer')
        for j in joins:
            self._joins.append(f'{jtype} join {j}')
        return self

    def order(self, by, direction='asc'):
        assert direction in ('asc', 'desc')
        self._order = (by, direction)
        return self

    def limit(self, limit):
        assert isinstance(limit, int)
        self._limit = limit
        return self

    def _assemble(self):
        """Return the sql query string and values suitable for executor."""
        selectop = self._headopt and f'{self._headopt}' or ''
        select = f'{selectop} ' + ', '.join(self._head)
        froms = 'from ' + ', '.join(self._tables)
        joins = ' '.join(self._joins)
        wheres, wkw = self._build_where()

        order = ''
        if self._order:
            order = f'order by {self._order[0]} {self._order[1]}'
        limit = ''
        if self._limit:
            limit = f'limit {self._limit}'

        kw = self._kw.copy()
        kw.update(wkw)
        return (f'select {select} '
                f'{froms} '
                f'{joins} '
                f'{wheres} '
                f'{order} '
                f'{limit}'
        ), kw
