import os.path
from tools import readURLorFile, genProtoFile

CURRENCY = (
    (1, 'BYN', 'Белорусский рубль'),
    (2, 'CHF', 'Швейцарский франк'),
    (3, 'CNY', 'юань'),
    (4, 'EUR', 'евро'),
    (5, 'GBP', 'фунт'),
    (6, 'GLD', 'Золото'),
    (7, 'HKD', 'Гонконгский доллар'),
    (8, 'JPY', 'Японская иена'),
    (9, 'KZT', 'Казахстанский тенге'),
    (10, 'SLV', 'Серебро'),
    (11, 'SUR', 'руб.'),
    (12, 'TRY', 'Турецкая лира'),
    (13, 'UAH', 'Украинская гривна'),
    (14, 'USD', 'долл. США'),
    (15, 'XAU', 'Золото локо Лондон'),
)

E_M_C = {
    'metadata': {
        'engine_id': {'type': 'int32'},
        'market_id': {'type': 'int32'},
        'block': {'type': 'string', 'bytes': 100, "max_size": 0},
        'columns_id': {'type': 'int32'},
        'engine_name': {"type": "string", "bytes": 189, "max_size": 0},
        'market_name': {"type": "string", "bytes": 189, "max_size": 0},
        'columns_name': {"type": "string", "bytes": 189, "max_size": 0},
        'columns_num': {'type': 'int32'},
    },
    'columns': ['engine_id', 'market_id', 'block', 'columns_id', 'engine_name', 'market_name', 'columns_name',
                'columns_num'],
    'data': [],
}


def toType(col_meta):
    if col_meta['type'].lower().startswith('int'):
        return 'INTEGER'
    elif col_meta['type'].lower().startswith('string'):
        return 'VARCHAR(%d)' % col_meta['bytes']

    print('Unknown type %s' % col_meta['type'])
    return 'NONE'


def toValue(row):
    srow = []
    for v in row:
        if v is None:
            srow.append('NULL')
        elif not isinstance(v, str):
            srow.append(str(v))
        else:
            srow.append(r"'%s'" % v.replace(r"'", r"''"))
    return ', '.join(srow)


def createTable(file, tbl, meta):
    file.write('DROP TABLE IF EXISTS %s;\n' % tbl)
    file.write('CREATE TABLE %s (\n' % tbl)
    scol = []
    for col in meta:
        scol.append('\t%s %s' % (col, toType(meta[col])))
    file.write('%s\n);\n' % ',\n'.join(scol))


def insertRow(file, tbl, cols, rows):
    insertPref = 'INSERT INTO %s %s VALUES (%%s);\n' % (tbl, str(tuple(cols)))
    for row in rows:
        file.write(insertPref % toValue(row))


URL_PREF = 'http://iss.moex.com/'
accum = {}
with open('iss.sql', 'w') as f_sql:
    ISS = readURLorFile(URL_PREF + 'iss.json', 'iss.json')

    for tbl in ISS:
        createTable(f_sql, tbl, ISS[tbl]['metadata'])
        insertRow(f_sql, tbl, ISS[tbl]['columns'], ISS[tbl]['data'])
        f_sql.write('\n')

    colInfo = readURLorFile(URL_PREF + 'iss/engines/stock/markets/shares/columns.json', 'columns.json',
                            params={'iss.meta': 'on', 'iss.data': 'off', 'iss.only': 'marketdata'})
    createTable(f_sql, 'columns', colInfo['marketdata']['metadata'])

    cols = {}
    for e_m in ISS['markets']['data']:
        info = readURLorFile(URL_PREF + 'iss/engines/%s/markets/%s/columns.json' % (e_m[2], e_m[4]),
                             '%s-%s.json' % (e_m[2], e_m[4]),
                             params={'iss.meta': 'off'})

        for blk in info:
            if 'is_ordered' in info[blk]['columns']:
                colNum = 1
                for dt in info[blk]['data']:
                    cols[dt[0]] = dt
                    E_M_C['data'].append([
                        e_m[1],
                        e_m[6],
                        blk,
                        dt[0],
                        e_m[2],
                        e_m[4],
                        dt[1],
                        colNum
                    ])
                    colNum += 1

        secInfo = readURLorFile(URL_PREF + 'iss/engines/%s/markets/%s/securities.json' % (e_m[2], e_m[4]),
                                '%s-%s-sec.json' % (e_m[2], e_m[4]),
                                params={'iss.data': 'off'})

        pref = '%s%s' % (e_m[2].capitalize(), e_m[4].capitalize())
        accum = genProtoFile('%s%s' % (e_m[2], e_m[4]),
                     ((pref, 'Security', 'securities'), (pref, 'Marketdata', 'marketdata'),
                      (pref, 'Yield', 'marketdata_yields')),
                     secInfo, dirName='proto.gen', genSet=True, accum=accum
                     )
    genProtoFile(
        'dataversion',
        (('', 'Dataversion', 'dataversion'),),
        secInfo, dirName='proto.gen'
    )

    for blk in accum:
        accum[blk]['metadata'] = sorted(accum[blk]['accum'].items(), key=lambda item: float(item[1]['row']) / float(item[1]['cnt']))
    genProtoFile('universal',
                 (('Universal', 'Security', 'securities'), ('Universal', 'Marketdata', 'marketdata'),
                  ('Universal', 'Yield', 'marketdata_yields')),
                 accum, dirName='proto.gen', genSet=True
                 )

    insertRow(f_sql, 'columns', colInfo['marketdata']['metadata'], list(cols.values()))

    createTable(f_sql, 'eng_mrt_blk_col', E_M_C['metadata'])
    insertRow(f_sql, 'eng_mrt_blk_col', E_M_C['columns'], E_M_C['data'])

with open(os.path.join('proto.gen', 'iss-types.proto'), 'w') as f_p:
    f_p.write('syntax = "proto3";\n')

    f_p.write('\nenum CurrencyEnum {\n')
    i = 0
    for cur in CURRENCY:
        f_p.write('\t%s = %d;\t//%s\n' % (cur[1], i, cur[2]))
        i += 1
    f_p.write('}\n')

    f_p.write('\nenum BoardsEnum {\n')
    i = 0
    for row in ISS['boards']['data']:
        f_p.write('\t%s = %d;\t//%s\n' % (row[4], i, row[5]))
        i += 1
    f_p.write('}\n')
