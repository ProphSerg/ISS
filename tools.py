import requests
import json
import os.path
from time import time_ns


def readURLorFile(url, file, dirName='dump', useCache=True, stat=None, **kwargs):
    file = os.path.join(dirName, file)
    if useCache:
        if not os.path.isdir(dirName):
            os.mkdir(dirName)
        if os.path.isfile(file):
            with open(file, 'r') as f:
                return json.load(f)

    t1 = time_ns()
    req = requests.get(url, **kwargs)
    t2 = time_ns()
    jreq = req.json()
    t3 = time_ns()
    if stat is not None:
        stat.update({
            'get': t2 - t1,
            'json': t3 - t2,
            'len': len(req.content),
        })
    with open(file, 'w') as f:
        json.dump(jreq, f)
    return jreq

mapType = {
    'string': 'google.protobuf.StringValue',
    'int32': 'google.protobuf.Int32Value',
    'int64': 'google.protobuf.Int64Value',
    'double': 'google.protobuf.DoubleValue',
    'time': 'issDataTime',
    'date': 'issDataTime',
    'datetime': 'issDataTime',
}
priorityType = (
    'google.protobuf.StringValue',
    'google.protobuf.Int32Value',
    'google.protobuf.Int64Value',
    'google.protobuf.FloatValue',
    'google.protobuf.DoubleValue',
    'issDataTime',
    'time',
    'date',
    'datetime',
    'BoardsEnum',
    'CurrencyEnum'
)

def protoType(inName, inType):
    if inName in ('FACEUNIT', 'CURRENCYID'):
        return 'CurrencyEnum'
    elif inName in ('BOARDID', ):
        return 'BoardsEnum'
    return mapType[inType] if inType in mapType else inType

def getMaxType(types):
    t = 0
    for i in types:
        t = max(t, priorityType.index(i))
    return priorityType[t]

def getAccum(accum, block, name, type, row):
    if block not in accum:
        accum[block] = {}
        accum[block]['accum'] = {}
    if name not in accum[block]['accum']:
        accum[block]['accum'][name] = {
            'type': set([type]),
            'row': row,
            'cnt': 1
        }
    else:
        accum[block]['accum'][name]['type'].add(type)
        accum[block]['accum'][name]['row'] = accum[block]['accum'][name]['row'] + row
        accum[block]['accum'][name]['cnt'] = accum[block]['accum'][name]['cnt'] + 1

    return accum

def genProtoFile(fileName, blocks, info, dirName='proto', genSet=False, accum=None):
    '''
    :param fileName:
    :param blocks: ((messagePrefix, messageName, BlockName), (), ...)
    :param info:
    :param dirName:
    :return:
    '''
    if not os.path.isdir(dirName):
        os.mkdir(dirName)

    with open(os.path.join(dirName, fileName + '.proto'), 'w') as f_p:
        f_p.write('syntax = "proto3";\n\n')
        f_p.write('import "google/protobuf/wrappers.proto";\n')
        f_p.write('import "iss-types.proto";\n')

        for blk in blocks:
            f_p.write('\nmessage %s%s {\n' % (blk[0], blk[1]))

            colNum = 1
            for col in info[blk[2]]['metadata']:
                if isinstance(col, tuple):
                    colType = getMaxType(col[1]['type'])
                    col = col[0]
                else:
                    colType = info[blk[2]]['metadata'][col]['type']
                col = col.upper()
                if colType == 'undefined':
                    continue
                colType = protoType(col, colType)
                if accum is not None:
                    accum = getAccum(accum, blk[2], col, colType, colNum)
                f_p.write('\t%s %s = %d;\n' % (colType, col, colNum))
                colNum += 1
            f_p.write('}\n')

            if genSet:
                f_p.write('\nmessage %s%sSet {\n\trepeated %s%s pack = 1;\n}\n' % (blk[0], blk[1], blk[0], blk[1]))

    if accum is not None:
        return accum