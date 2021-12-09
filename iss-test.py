from tools import readURLorFile
from time import time_ns
import importlib

import iss_types_pb2
import universal_pb2

settingISS = {
    'stock': {
        'ndm': ('PSAU', 'PSBB'),
        'bonds': ('TQCB', 'TQIR', 'TQOB', 'TQRD'),
        'index': ('SNDX', 'RTSI'),
        'shares': ('TQBR', 'TQPI', 'TQIF', 'TQTF'),
        'foreignshares': ('FQBR', )
    },
    'currency': {
        'selt': ('CETS', ),
    },
    'futures': {
        'forts': ('BR'),
    },
}

def issDateTime(val):
    dt = iss_types_pb2.issDataTime()
    for s in val.split(' '):
        if s.find('-') > 0:
            s = s.split('-')
            dt.date.value = int(s[0])*10000 + int(s[1])*100 + int(s[2])
        elif s.find(':') > 0:
            s = s.split(':')
            dt.time.value = int(s[0])*10000 + int(s[1])*100 + int(s[2])


def pack(name, columns, data, itemPB2, packPB2, boardidFilter=None):
    st = {
        'minT': 9999999999,
        'maxT': 0,
        'sumT': 0,
        'minL': 9999999999,
        'maxL': 0,
        'sumL': 0,
        'cnt': 0
    }
    pack = packPB2()
    item = itemPB2()
    if len(data) == 0:
        return
    i=1
    for row in data:
        item.Clear()
        bi = ''
        for col in range(len(columns)):
            colName = columns[col].upper()
            if row[col] is not None and hasattr(item, colName):
                if colName in ('FACEUNIT', 'CURRENCYID'):
                    setattr(item, colName, iss_types_pb2.CurrencyEnum.Value(row[col]))
                elif colName in ('BOARDID',):
                    bi = row[col]
                    setattr(item, colName, iss_types_pb2.BoardsEnum.Value(row[col]))
                elif getattr(item, colName).DESCRIPTOR.name == 'issDataTime':
                    setattr(item, colName, issDateTime(row[col]))
                else:
                    #print('%s -> %s' %(colName, str(row[col])))
                    #setattr(item, colName, row[col])
                    getattr(item, colName).value = row[col]
        if boardidFilter is not None and bi in boardidFilter:
            continue

        pack.pack.append(item)
        bT = time_ns()
        s = item.SerializeToString()
        eT = time_ns()
        st = {
            'minT': min(st['minT'], eT - bT),
            'maxT': max(st['maxT'], eT - bT),
            'sumT': st['sumT'] + (eT - bT),
            'minL': min(st['minL'], len(s)),
            'maxL': max(st['maxL'], len(s)),
            'sumL': st['sumL'] + len(s),
            'cnt': i
        }
        i += 1

    print('Serialize     1 {0:18s}: Len ({1:>3d}, {2:>6.2f}, {3:>3d}) b. Time ({4:8.3f}, {5:8.3f}, {6:8.3f}) ms'.format(
        name, st['minL'], st['sumL'] / st['cnt'], st['maxL'],
        st['minT'] / 1e6, (st['sumT'] / st['cnt']) / 1e6, st['maxT'] / 1e6
    ))
    bT = time_ns()
    s = pack.SerializeToString()
    eT = time_ns()

    print('Serialize {0:>5d} {1:18s}: Len {2:>10,d} bytes. Time {3:8.3f} ms'.format(i - 1, name, len(s), (eT - bT) / 1e6))

URL_PREF = 'http://iss.moex.com/'
stat = {}
for e in settingISS:
    for m in settingISS[e]:
        Info = readURLorFile(URL_PREF + 'iss/engines/%s/markets/%s/securities.json' %(e, m),
                                '%s-%s.json' %(e, m), dirName='data',
                                params={'iss.meta': 'off'}, stat=stat, useCache=False)
        print('\nGet {0:s} - {1:s}. Read {2:>10,d} bytes. Time: read {3:>8.3f} ms, toJSON {4:>8.3f} ms.'.format(e, m, stat['len'], stat['get'] / 1e6, stat['json'] / 1e6))

        moduleName = e + m + '_pb2'
        structPrefix = e.capitalize() + m.capitalize()
        moduleImp = importlib.import_module(moduleName)

        pack('securities', Info['securities']['columns'], Info['securities']['data'],
             universal_pb2.UniversalSecurity, universal_pb2.UniversalSecuritySet, boardidFilter=settingISS[e][m])
        pack('securities', Info['securities']['columns'], Info['securities']['data'],
             getattr(moduleImp, structPrefix + 'Security'), getattr(moduleImp, structPrefix + 'SecuritySet'), boardidFilter=settingISS[e][m])

        print('')
        pack('marketdata', Info['marketdata']['columns'], Info['marketdata']['data'],
             universal_pb2.UniversalMarketdata, universal_pb2.UniversalMarketdataSet, boardidFilter=settingISS[e][m])
        pack('marketdata', Info['marketdata']['columns'], Info['marketdata']['data'],
             getattr(moduleImp, structPrefix + 'Marketdata'), getattr(moduleImp, structPrefix + 'MarketdataSet'), boardidFilter=settingISS[e][m])

        print('')
        pack('marketdata_yields', Info['marketdata_yields']['columns'], Info['marketdata_yields']['data'],
             universal_pb2.UniversalYield, universal_pb2.UniversalYieldSet, boardidFilter=settingISS[e][m])
        pack('marketdata_yields', Info['marketdata_yields']['columns'], Info['marketdata_yields']['data'],
             getattr(moduleImp, structPrefix + 'Yield'), getattr(moduleImp, structPrefix + 'YieldSet'), boardidFilter=settingISS[e][m])

        print('-' * 60)

