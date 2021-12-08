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


def genProtoFile(fileName, blocks, info, dirName='proto', genSet=False):
    '''
    :param fileName:
    :param blocks: ((messagePrefix, messageName, BlockName), (), ...)
    :param info:
    :param dirName:
    :return:
    '''
    if not os.path.isdir(dirName):
        os.mkdir(dirName)

    with open(os.path.join(dirName, fileName) + '.proto', 'w') as f_p:
        f_p.write('syntax = "proto3";\n')

        for blk in blocks:
            f_p.write('\nmessage %s%s {\n' % (blk[0], blk[1]))

            colNum = 1
            for col in info[blk[2]]['metadata']:
                meta = info[blk[2]]['metadata'][col]
                colType = meta['type']
                if colType == 'undefined':
                    continue
                f_p.write('\t%s %s = %d;\n' % (
                    'string' if colType in ('time', 'date', 'datetime') else colType,
                    col, colNum)
                          )
                colNum += 1
            f_p.write('}\n')

            if genSet:
                f_p.write('\nmessage %s%sSet {\n\trepeated %s%s pack = 1;\n}\n' % (blk[0], blk[1], blk[0], blk[1]))
