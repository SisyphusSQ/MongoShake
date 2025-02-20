import getopt
import sys
import time
import traceback
from typing import Mapping, Any

import pymongo
from loguru import logger
from pymongo import MongoClient
from pymongo.synchronous.collection import Collection
from pymongo.synchronous.command_cursor import CommandCursor
from pymongo.synchronous.cursor import Cursor
from pymongo.synchronous.database import Database

# constant
COMPARISION_COUNT: str = "comparison_count"
COMPARISION_MODE: str = "comparisonMode"
EXCLUDE_DBS: str = "excludeDbs"
EXCLUDE_COLLS: str = "excludeColls"
SAMPLE: str = "sample"

# we don't check collections and index here because sharding collection(`db.stats`) is split.
configure: dict[str, any] = {}
check_list: dict[str, int] = {"objects": 1, "numExtents": 1, "ok": 1}


class MongoCluster:
    # pymongo connection
    conn: MongoClient = None

    # connection string
    url: str = ""

    def __init__(self, url):
        self.url = url

    def connect(self) -> None:
        self.conn = pymongo.MongoClient(self.url)

    def close(self) -> None:
        self.conn.close()


def filter_check(m) -> dict[str, int]:
    new_m: dict[str, int] = {}
    for k in check_list:
        new_m[k] = m[k]
    return new_m


"""
    check meta data. include db.collection names and stats()
"""


def check(src_cluster: MongoCluster, dst_cluster: MongoCluster) -> bool:
    result: dict[str, int] = {}

    # check metadata
    src_db_names: list[str] = src_cluster.conn.list_database_names()
    dst_db_names: list[str] = dst_cluster.conn.list_database_names()
    src_db_names = [db for db in src_db_names if db not in configure[EXCLUDE_DBS]]
    dst_db_names = [db for db in dst_db_names if db not in configure[EXCLUDE_DBS]]
    if len(src_db_names) != len(dst_db_names):
        logger.error("DIFF => database count not equals src[{}] != dst[{}].\nsrc: {}\ndst: {}",
                     len(src_db_names), len(dst_db_names), src_db_names, dst_db_names)
        return False
    else:
        logger.info("EQUAL => database count equals")

    # check database names and collections
    for db in src_db_names:
        if db in configure[EXCLUDE_DBS]:
            logger.info("IGNR => ignore database [{}]", db)
            continue

        if dst_db_names.count(db) == 0:
            logger.error("DIFF => database [{}] only in srcDb", db)
            return False

        # db.stats() comparison
        src_db: Database = src_cluster.conn[db]
        dst_db: Database = dst_cluster.conn[db]

        # for collections in db
        src_colls: list[str] = src_db.list_collection_names()
        dst_colls: list[str] = dst_db.list_collection_names()
        src_colls = [coll for coll in src_colls if coll not in configure[EXCLUDE_COLLS] and src_colls.count(coll) > 0]
        dst_colls = [coll for coll in dst_colls if coll not in configure[EXCLUDE_COLLS] and dst_colls.count(coll) > 0]
        if len(src_colls) != len(dst_colls):
            logger.error(
                "DIFF => database [{}] collections count not equals, src[{}], dst[{}]", db, src_colls, dst_colls)
            return False
        else:
            logger.info("EQUAL => database [{}] collections count equals", db)

        for coll in src_colls:
            if coll in configure[EXCLUDE_COLLS]:
                logger.info("IGNR => ignore collection [{}]", coll)
                continue

            if dst_colls.count(coll) == 0:
                logger.error("DIFF => collection only in source [{}]", coll)
                return False

            src_coll: Collection = src_db[coll]
            dst_coll: Collection = dst_db[coll]

            logger.info("compare count for collection [{}]", coll)
            # comparison collection records number
            if src_coll.estimated_document_count() != dst_coll.estimated_document_count():
                logger.error("DIFF => collection [{}] record count not equals", coll)
                return False
            else:
                logger.info("EQUAL => collection [{}] record count equals", coll)

            logger.info("compare index for collection [{}]", coll)

            """
            # comparison collection index number
            src_index_length = len(src_coll.index_information())
            dst_index_length = len(dst_coll.index_information())
            if src_index_length != dst_index_length:
                logger.error("DIFF => collection [{}] index number not equals: src[{}], dst[{}]",
                             coll, src_index_length, dst_index_length)
                return False
            else:
                logger.info("EQUAL => collection [{}] index number equals", coll)

            logger.info("compare data sample for collection [{}]", coll)
            """

            # check sample data
            ns: str = f"{db}.{coll}"
            result = data_comparison(src_coll, dst_coll, configure[COMPARISION_MODE], ns, result)
            if result[ns] == 0:
                logger.info("EQUAL => collection [{}] data data comparison exactly equals", coll)
            else:
                logger.error("DIFF => collection [{} data comparison not equals", coll)

    for k, v in result:
        if v != 0:
            return False
    return True


"""
    check sample data. comparison every entry
"""


def data_comparison(src_coll: Collection, dst_coll: Collection, mode: str, ns: str,
                    result: dict[str, int]) -> dict[str, int]:
    result[ns] = 0
    if mode == "no":
        return result
    elif mode == "sample":
        # srcColl.count() must equals to dstColl.count()
        count = configure[COMPARISION_COUNT] \
            if configure[COMPARISION_COUNT] <= src_coll.estimated_document_count() \
            else src_coll.estimated_document_count()
    else:
        # all
        count = src_coll.count_documents({})

    if count == 0:
        return result

    rec_count: int = count
    batch: int = 16
    show_progress: int = (batch * 64)
    total: int = 0
    if mode == "sample":
        while count > 0:
            # sample a bunch of docs
            docs: CommandCursor[Mapping[str, Any]] = src_coll.aggregate([{"$sample": {"size": batch}}])
            while docs.alive:
                doc = docs.next()
                migrated = dst_coll.find_one(doc["_id"])
                # both origin and migrated bson is Map . so use ==
                if doc != migrated:
                    logger.error("DIFF => ns[{}] src_record[{}], dst_record[{}]", ns, doc, migrated)
                    result[ns] += 1
            total += batch
            count -= batch
            print_progress(total, rec_count, show_progress)
            time.sleep(0.001)
    else:
        # mode must be all
        docs: Cursor = src_coll.find({})
        for doc in docs:
            migrated = dst_coll.find_one(doc["_id"])
            if doc != migrated:
                logger.error("DIFF => ns[{}] src_record[{}], dst_record[{}]", ns, doc, migrated)
                result[ns] += 1
            total += 1
            print_progress(total, rec_count, show_progress)
        docs.close()
    return result


def print_progress(total: int, actual: int, show_progress: int) -> None:
    if total % show_progress == 0:
        logger.info("  ... process %d docs, %.2f %% !" % (total, actual * 100.0 / total))


def usage() -> None:
    print(
        '|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|')
    print(
        "| Usage: ./comparison.py --src=localhost:27017/db? --dest=localhost:27018/db? --count=10000 (the sample number) --excludeDbs=admin,local --excludeCollections=system.profile --comparisonMode=sample/all/no (sample: comparison sample number, default; all: comparison all data; no: only comparison outline without data)  |")
    print(
        '|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|')
    print(
        '| Like : ./comparison.py --src="localhost:3001" --dest=localhost:3100  --count=1000  --excludeDbs=admin,local,mongoshake --excludeCollections=system.profile --comparisonMode=sample  |')
    print(
        '|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|')
    exit(0)


if __name__ == "__main__":
    opts, args = getopt.getopt(sys.argv[1:], "hs:d:n:e:x:",
                               ["help", "src=", "dest=", "count=", "excludeDbs=", "excludeCollections=",
                                "comparisonMode="])

    configure[SAMPLE] = True
    configure[EXCLUDE_DBS] = []
    configure[EXCLUDE_COLLS] = []
    srcUrl: str = ""
    dstUrl: str = ""

    for key, value in opts:
        if key in ("-h", "--help"):
            usage()
        if key in ("-s", "--src"):
            srcUrl = value
        if key in ("-d", "--dest"):
            dstUrl = value
        if key in ("-n", "--count"):
            configure[COMPARISION_COUNT] = int(value)
        if key in ("-e", "--excludeDbs"):
            configure[EXCLUDE_DBS] = value.split(",")
        if key in ("-x", "--excludeCollections"):
            configure[EXCLUDE_COLLS] = value.split(",")
        if key in "--comparisonMode":
            print(value)
            if value != "all" and value != "no" and value != "sample":
                logger.info("comparisonMode[{}}] illegal", value)
                exit(1)
            configure[COMPARISION_MODE] = value
    if COMPARISION_MODE not in configure:
        configure[COMPARISION_MODE] = "sample"

    # params verify
    if len(srcUrl) == 0 or len(dstUrl) == 0:
        usage()

    # default count is 10000
    if configure.get(COMPARISION_COUNT) is None or configure.get(COMPARISION_COUNT) <= 0:
        configure[COMPARISION_COUNT] = 10000

    # ignore databases
    configure[EXCLUDE_DBS] += ["admin", "local"]
    configure[EXCLUDE_COLLS] += ["system.profile"]

    # dump configuration
    logger.info("Configuration [sample={}, count={}, excludeDbs={}, excludeColls={}]",
                configure[SAMPLE], configure[COMPARISION_COUNT], configure[EXCLUDE_DBS], configure[EXCLUDE_COLLS])

    try:
        src: MongoCluster = MongoCluster(srcUrl)
        dst: MongoCluster = MongoCluster(dstUrl)
        logger.info("[src = {}]", srcUrl)
        logger.info("[dst = {}]", dstUrl)
        src.connect()
        dst.connect()
    except Exception as e:
        traceback.print_exc()
        logger.error("create mongo connection failed {}|{}", srcUrl, dstUrl)
        exit()

    if check(src, dst):
        logger.info("SUCCESS")
    else:
        logger.warning("FAIL")

    src.close()
    dst.close()
