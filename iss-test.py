from tools import readURLorFile
from time import time_ns

import securities_pb2
import stockbonds_security_pb2
import stockbonds_marketdata_pb2
import stockbonds_yield_pb2

def pack(name, columns, data, itemPB2, packPB2):
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

    i=1
    for row in data:
        item = itemPB2()
        for col in range(len(columns)):
            if row[col] is not None and hasattr(item, columns[col]):
                setattr(item, columns[col], row[col])

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

    print('Serialize 1 %s: Len(b) (%d, %.2f, %d). Time(ms) (%.3f, %.3f, %.3f)'%(
        name, st['minL'], st['sumL'] / st['cnt'], st['maxL'],
        st['minT'] / 1e6, (st['sumT'] / st['cnt']) / 1e6, st['maxT'] / 1e6
    ))
    bT = time_ns()
    s = pack.SerializeToString()
    eT = time_ns()

    print('Serialize %d %s (pack): Len(b) %d. Time(ms) %.3f'%(
        i - 1, name, len(s), (eT - bT) / 1e6
    ))

URL_PREF = 'http://iss.moex.com/'
Info = readURLorFile(URL_PREF + 'iss/engines/stock/markets/bonds/securities.json',
                        'stock-bonds.json', dirName='data',
                        params={'iss.meta': 'off'})

pack('securities', Info['securities']['columns'], Info['securities']['data'],
     stockbonds_security_pb2.StockBondsSecurity, securities_pb2.StockBondsSecurities)
pack('marketdata', Info['marketdata']['columns'], Info['marketdata']['data'],
     stockbonds_marketdata_pb2.StockBondsMarketdata, securities_pb2.StockBondsMarketdatas)
pack('marketdata_yields', Info['marketdata_yields']['columns'], Info['marketdata_yields']['data'],
     stockbonds_yield_pb2.StockBondsYield, securities_pb2.StockBondsYields)