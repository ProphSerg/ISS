from tools import readURLorFile, updateStat
from time import time_ns
import importlib

import universal_pb2

import instrument
settingISS = {
    'stock': {
        'ndm': dict.fromkeys(['PSAU', 'PSBB'], None),
        'bonds': dict.fromkeys(['TQCB', 'TQIR', 'TQOB', 'TQRD'], None),
        'index': dict.fromkeys(['SNDX', 'RTSI'], None),
        'shares': dict.fromkeys(['TQBR', 'TQPI', 'TQIF', 'TQTF'], None),
        'foreignshares': dict.fromkeys(['FQBR', ], None)
    },
    'currency': {
        'selt': dict.fromkeys(['CETS', ], None),
    },
    'futures': {
        'forts': {},
    },
}
for e in settingISS:
    for m in settingISS[e]:
        for b in settingISS[e][m]:
            if b in instrument.instrument:
                settingISS[e][m][b] = instrument.instrument[b]

def pack(dataIn, infoPB, boardidFilter=None, instrumentFilter=False, statistic=None):
    for blk in infoPB:
        if len(dataIn[blk]['data']) == 0:
            continue
        data = dataIn[blk]['data']
        columns = dataIn[blk]['columns']
        item = infoPB[blk][0]()
        pack = infoPB[blk][1]()
        statistic.update({ blk: {} })
        stat = statistic[blk]
        tm = [0,0,0,0,0,0,0,0,0]
        i=1
        tm[0] = time_ns()
        for row in data:
            tm[1] = time_ns()
            item.Clear()
            bi = {}
            for col in range(len(columns)):
                colName = columns[col].upper()
                if row[col] is not None:
                    if colName in ('BOARDID', 'SECID'):
                        bi[colName] = row[col]
                    try:
                        getattr(item, colName).value = row[col]
                    except:
                        pass

            if boardidFilter is not None and len(boardidFilter) > 0:
                if bi['BOARDID'] not in boardidFilter or (instrumentFilter == True and bi['SECID'] in boardidFilter[bi['BOARDID']]):
                    continue
            tm[2] = time_ns()

            pack.pack.append(item)

            tm[3] = time_ns()
            s = item.SerializeToString()
            tm[4] = time_ns()

            if stat is not None:
                updateStat(stat, 'oneparse', tm[2] - tm[1], len(str(item)))
                updateStat(stat, 'oneser', tm[4] - tm[3], len(s))
                updateStat(stat, 'one', tm[4] - tm[1])
            i += 1

        '''
        print('Serialize     1 {0:18s}: Len ({1:>3d}, {2:>6.2f}, {3:>3d}) b. Time ({4:8.3f}, {5:8.3f}, {6:8.3f}) ms'.format(
            blk, st['minL'], st['sumL'] / st['cnt'], st['maxL'],
            st['minT'] / 1e6, (st['sumT'] / st['cnt']) / 1e6, st['maxT'] / 1e6
        ))
        '''
        tm[5] = time_ns()
        s = pack.SerializeToString()
        tm[6] = time_ns()

        if stat is not None:
            updateStat(stat, 'allparse', tm[5] - tm[0], len(str(pack)))
            updateStat(stat, 'allser', tm[6] - tm[5], len(s))
            updateStat(stat, 'all', tm[6] - tm[0])
        i += 1

        #print('Serialize {0:>5d} {1:18s}: Len {2:>10,d} bytes. Time {3:8.3f} ms'.format(i - 1, blk, len(s), (eT - bT) / 1e6))

URL_PREF = 'http://iss.moex.com/'
statistic = {}
for e in settingISS:
    for m in settingISS[e]:
        statistic['%s-%s' %(e, m)] = {}
        stat = statistic['%s-%s' %(e, m)]
        Info = readURLorFile(URL_PREF + 'iss/engines/%s/markets/%s/securities.json' %(e, m),
                                '%s-%s.json' %(e, m), dirName='data',
                                params={'iss.meta': 'off'}, stat=stat, useCache=True)
        #print('\nGet {0:s} - {1:s}. Read {2:>10,d} bytes. Time: read {3:>8.3f} ms, toJSON {4:>8.3f} ms.'.format(e, m, stat['len'], stat['get'] / 1e6, stat['json'] / 1e6))

        moduleName = e + m + '_pb2'
        structPrefix = e.capitalize() + m.capitalize()
        moduleImp = importlib.import_module(moduleName)

        pack(dataIn=Info, boardidFilter=settingISS[e][m], statistic=stat,
             infoPB={
                'securities': (universal_pb2.UniversalSecurity, universal_pb2.UniversalSecuritySet),
                'marketdata': (universal_pb2.UniversalMarketdata, universal_pb2.UniversalMarketdataSet),
                'marketdata_yields': (universal_pb2.UniversalYield, universal_pb2.UniversalYieldSet),
            }
        )
        pack(dataIn=Info, boardidFilter=settingISS[e][m], statistic=stat,
             infoPB={
                'securities': (getattr(moduleImp, structPrefix + 'Security'), getattr(moduleImp, structPrefix + 'SecuritySet')),
                'marketdata': (getattr(moduleImp, structPrefix + 'Marketdata'), getattr(moduleImp, structPrefix + 'MarketdataSet')),
                'marketdata_yields': (getattr(moduleImp, structPrefix + 'Yield'), getattr(moduleImp, structPrefix + 'YieldSet')),
            }
        )

