[![Production Deployment](https://github.com/smu-chile/unidata_advanced_analytics/actions/workflows/pipeline.yaml/badge.svg)](https://github.com/smu-chile/unidata_advanced_analytics/actions/workflows/pipeline.yaml)

# Advanced Analytics Projects.
This repository contains all the projects developed by SMU Advanced Analytics. The projects inside the repo are organized in a monorepo standard, this is: 

```
unidata_advanced_analytics
├── docs
├── src
|   ├── project_a
|   │   ├── gbq_objects
|   │   │   └── table_a.json
|   │   ├── dependencies
|   │   │   ├── Dockerfile
|   │   │   └── requirements.txt
|   │   ├── scripts
|   │   │   ├── process_file_a.py
|   │   │   └── process_file_b.py
|   │   └── project_a_dag.py
|   ├── project_b
|   │   ├── dependencies
|   │   │   ├── Dockerfile
|   │   │   └── requirements.txt
|   │   ├── scripts
|   │   │   └── process_file.py
|   │   └── project_b_dag.py
|   ├── project_c
|   │   └── ...
|   └── ...
├── lint
│   ├── dev_ruff.toml
│   └── main_ruff.toml
├── .githooks
│   ├── post-checkout
│   └── ...
├── .gitignore
└── README.md
```

So every project is self-contained in its directory inside `src`. The projects contain:
- A `scripts` directory with processes executed by a `DataprocCreateBatchOperator` and the project DAGs scripts in its root directory.
- A `gbq_objects` directory with Google BigQuery tables to be created in its execution.
- A `dependencies` directory the Dockerfile and requirements needed to create the Docker image used by the Google Dataproc executor.

This repo uses git hooks to select automatically the ruff configuration for every branch. To activate this you'll need to run:

```
git config --local core.hooksPath .githooks/
```

and have installed Ruff in your current conda environment.
