import struct
import decimal
import json
import logging
logger = logging.getLogger(__name__)
D = decimal.Decimal

from counterpartylib.lib import (config, util, exceptions, util, message_type)

def initialise(db):
    cursor = db.cursor()

    with db:
        cursor.execute('''CREATE TABLE IF NOT EXISTS assetgroups(
                        tx_index INTEGER,
                        tx_hash TEXT,
                        msg_index INTEGER DEFAULT 0,
                        block_index INTEGER,
                        asset_group TEXT,
                        owner TEXT,
                        status TEXT,
                                PRIMARY KEY (tx_index, msg_index),
                                FOREIGN KEY (tx_index, tx_hash, block_index) REFERENCES transactions(tx_index, tx_hash, block_index),
                                UNIQUE (tx_hash, msg_index))
                            ''')
        cursor.execute('''CREATE INDEX IF NOT EXISTS
                        asset_group_idx ON issuances (asset_group)
                    ''')

def validate (db, asset_group, source):
    problems = []
    cursor = db.cursor()

    with db:
        cursor.execute('''SELECT * FROM assetgroups
                            WHERE (status = ? AND asset_group = ?)
                            ORDER BY tx_index ASC''', ('valid', asset_group))
        asset_groups = cursor.fetchall()
        if asset_groups and asset_groups[-1]['owner'] != source:
            problems.append('asset group owned by another address')
    return problems

def create(db, tx_index, tx_hash, block_index, asset_group, owner, status):
    cursor = db.cursor()

    with db:
        bindings = {
            'tx_index': tx_index,
            'tx_hash': tx_hash,
            'msg_index': 0,
            'block_index': block_index,
            'asset_group': asset_group,
            'owner': owner,
            'status': status
        }
        cursor.execute('''INSERT INTO assetgroups VALUES (
            :tx_index,
            :tx_hash,
            :msg_index,
            :block_index,
            :asset_group,
            :owner,
            :status)''', bindings)
