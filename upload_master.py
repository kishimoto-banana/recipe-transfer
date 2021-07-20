import json
import os
import re
from dataclasses import asdict, dataclass
from typing import List

import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore

BATCH_SIZE = 500
WRITE_UPPER_LIMIT = 10000
FIREBASE_CRED_PATH = os.environ.get("firebase_cred_path", "firebase-admin.json")


cred = credentials.Certificate(FIREBASE_CRED_PATH)
firebase_admin.initialize_app(cred)

db = firestore.client()
collection = db.collection("seasonings")


@dataclass
class Seasoning:
    id: int
    name: str
    category: str


def upload():
    seasoning_df = pd.read_csv("data/seasoning.csv")

    seasonings = []
    for id, name, category in zip(
        seasoning_df["id"], seasoning_df["name"], seasoning_df["category"]
    ):
        seasonings.append(Seasoning(id, name, category))

    batch = db.batch()
    for seasoning in seasonings:
        batch.set(collection.document(), asdict(seasoning))
    batch.commit()


if __name__ == "__main__":
    upload()
