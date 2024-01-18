import os

FACTORY_ARGS = [
    {"bucket_url": os.path.abspath("tests/filesystem/samples")},
    # Ooginal:
    # {
    #     "bucket_url": "s3://dlt-ci-test-bucket/standard_source/samples",
    #     "kwargs": {"use_ssl": True}
    # },
    # deanja dev:
    {
        "bucket_url": "s3://flyingfish-dlt-ci-test-bucket/standard_source/samples",
        "kwargs": {"use_ssl": True},
    },
    # {"bucket_url": "gs://ci-test-bucket/standard_source/samples"},
    # {"bucket_url": "az://dlt-ci-test-bucket/standard_source/samples"},
    # gitpythonfs variations:
    # For dlt.common.storages with no support for params in url netloc. If no
    # function args provided it defaults to repo in working directory and ref HEAD
    {
        "bucket_url": "gitpythonfs://samples",
        "kwargs": {
            "repo_path": "tests/filesystem/cases/git",
            "ref": "unmodified-samples",
        },
    }
    # with dedicated test repo in `cases` and repo_path and ref specified in url netloc:
    # ["bucket_url":"gitpythonfs://tests/filesystem/cases/git:unmodified-samples@samples"],
]

GLOB_RESULTS = [
    {
        "glob": None,
        "file_names": ["sample.txt"],
    },
    {
        "glob": "*/*",
        "file_names": [
            "csv/freshman_kgs.csv",
            "csv/freshman_lbs.csv",
            "csv/mlb_players.csv",
            "csv/mlb_teams_2012.csv",
            "jsonl/mlb_players.jsonl",
            "parquet/mlb_players.parquet",
        ],
    },
    {
        "glob": "**/*.csv",
        "file_names": [
            "csv/freshman_kgs.csv",
            "csv/freshman_lbs.csv",
            "csv/mlb_players.csv",
            "csv/mlb_teams_2012.csv",
            "met_csv/A801/A881_20230920.csv",
            "met_csv/A803/A803_20230919.csv",
            "met_csv/A803/A803_20230920.csv",
        ],
    },
    {
        "glob": "*/*.csv",
        "file_names": [
            "csv/freshman_kgs.csv",
            "csv/freshman_lbs.csv",
            "csv/mlb_players.csv",
            "csv/mlb_teams_2012.csv",
        ],
    },
    {
        "glob": "csv/*",
        "file_names": [
            "csv/freshman_kgs.csv",
            "csv/freshman_lbs.csv",
            "csv/mlb_players.csv",
            "csv/mlb_teams_2012.csv",
        ],
    },
    {
        "glob": "csv/mlb*",
        "file_names": [
            "csv/mlb_players.csv",
            "csv/mlb_teams_2012.csv",
        ],
    },
    {
        "glob": "*",
        "file_names": ["sample.txt"],
    },
]
