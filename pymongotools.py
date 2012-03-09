
import json
import re

def shardingStatus(m):
    db = m['config']
    version = db.version.find_one()
    if not version:
        return 'ShardingStatus: not a shard db!\n'
    status_string = '--- Sharding Status ---\n'
    status_string += '  sharding version:' + json.dumps(version) + '\n'
    shards = db.shards.find().sort('_id')
    status_string += '  shards:\n      ' + '\n      '.join([json.dumps(i) for i in shards]) + '\n'
    status_string += '  databases:\n'
    dbnames = db.databases.find().sort('name')
    for d in dbnames:
        status_string += ' '*6 + json.dumps(d) + '\n'
        if not d['partitioned']:
            continue
        cols = db.collections.find({'_id':{ '$regex' : re.compile("^" + d['_id'] + ".")},  "dropped":False}).sort('_id')
        for c in cols:
            status_string += ' '*10 + c['_id'] + ' chunks:\n'
            chunks = db.chunks.group( key=['shard'], condition={ 'ns' : c['_id'] }, 
                initial={ "nChunks" : 0 }, 
                reduce="function (doc, out) { out.nChunks++; }") 
            for ck in chunks:
                status_string += ' '*14 + ck['shard'] + ' ' + str(ck['nChunks']) + '\n'
    return status_string


def printShardingStatus(m):
    print shardingStatus(m)


if __name__ == '__main__':
    import pymongo
    printShardingStatus(pymongo.Connection(port=30000))
    

    
