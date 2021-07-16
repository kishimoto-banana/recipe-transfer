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
collection = db.collection("recipes")
batch = db.batch()


@dataclass
class Recipe:
    url: str
    title: str
    seasonings: List[str]


def write_recipe():
    with open("data/recipes.json") as f:
        raw_recipes = json.load(f)

    with open("data/writed_urls.txt") as f:
        writed_urls = set(f.read().splitlines())

    # 調味料リスト
    seasoning_df = pd.read_csv("data/seasoning.csv")
    seasonings = set(seasoning_df["name"].tolist())

    # 名寄せ
    norm_df = pd.read_csv("data/word_normalize.csv")
    norm_dict = norm_df.set_index("前").to_dict()["後"]

    # TODO: 名寄せと調味料の抽出どうにかする
    norm_recipes = []
    for raw_recipe in raw_recipes:
        raw_seasonings = [
            re.sub(" \(.+\)$", "", ingredient)
            for ingredient in raw_recipe["ingredients"]
        ]
        raw_seasonings = [
            norm_dict[raw_seasoning] if raw_seasoning in norm_dict else raw_seasoning
            for raw_seasoning in raw_seasonings
        ]
        raw_seasonings = [
            raw_seasoning
            for raw_seasoning in raw_seasonings
            if raw_seasoning in seasonings
        ]
        raw_recipe["seasonings"] = raw_seasonings
        norm_recipes.append(raw_recipe)

    recipes = [
        Recipe(
            recipe["url"],
            recipe["title"],
            [seasoning for seasoning in recipe["seasonings"]],
        )
        for recipe in raw_recipes
        if recipe["url"] not in writed_urls
    ]

    for counter, idx in enumerate(range(0, len(recipes), BATCH_SIZE)):
        batch_num = (len(recipes) - 1) // BATCH_SIZE + 1
        print(f"process [{counter+1}/{batch_num}]")
        for recipe in recipes[idx : idx + BATCH_SIZE]:
            batch.set(
                collection.document(),
                asdict(recipe),
            )
            writed_urls.add(recipe.url)
        batch.commit()

        with open("data/writed_urls.txt", "w") as f:
            f.write("\n".join(writed_urls))

        # 一日の書き込み上限回数に達する前に止める
        if (counter + 1) * BATCH_SIZE >= WRITE_UPPER_LIMIT:
            break


if __name__ == "__main__":
    write_recipe()
