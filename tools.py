import requests
import json
import os.path


def readURLorFile(url, file, dirName='dump', useCache=True, **kwargs):
    if useCache:
        if not os.path.isdir(dirName):
            os.mkdir(dirName)
        file = os.path.join(dirName, file)
        if os.path.isfile(file):
            with open(file, 'r') as f:
                return json.load(f)

    req = requests.get(url, **kwargs)
    jreq = req.json()
    with open(file, 'w') as f:
        json.dump(jreq, f)
    return jreq

def protoType(inName, inType):
    if inType in ('time', 'date', 'datetime'):
        return 'string'
    elif inName in ('FACEUNIT', 'CURRENCYID'):
        return 'CurrencyEnum'
    elif inName in ('BOARDID', ):
        return 'BoardsEnum'
    return inType

def getMaxType(types):
    t = 'string'
    if 'int32' in types:
        t = 'int32'
    if 'int64' in types:
        t = 'int64'
    if 'double' in types:
        t = 'double'
    if 'time' in types:
        t = 'time'
    if 'date' in types:
        t = 'date'
    if 'datetime' in types:
        t = 'datetime'
    return t

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
        f_p.write('syntax = "proto3";\n')
        f_p.write('\nimport "iss-types.proto";\n')

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