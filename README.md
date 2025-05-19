[![Production Deployment](https://github.com/smu-chile/unidata_advanced_analytics/actions/workflows/pipeline.yaml/badge.svg?branch=main)](https://github.com/smu-chile/unidata_advanced_analytics/actions/workflows/pipeline.yaml)

[![Development Deployment](https://github.com/smu-chile/unidata_advanced_analytics/actions/workflows/pipeline.yaml/badge.svg?branch=main)](https://github.com/smu-chile/unidata_advanced_analytics/actions/workflows/pipeline.yaml)


# Advanced Analytics Projects.
This repository contains all the projects developed by SMU Advanced Analytics. The projects inside the repo are organized in a monorepo standard, this is: 

```
src
├── project_a
│   ├── scripts
│   │   ├── process_file_a.py
│   │   └── process_file_b.py
│   └── project_a_dag.py
├── project_b
│   ├── scripts
│   │   └── process_file.py
│   └── project_b_dag.py
├── project_c
│   └── ...
├── ...
├── .gitignore
├── Advanced Analytics.code-profile
├── README.md
└── ruff.toml
```

So every project is self-contained in its directory inside src. Usually the projects contain a **scripts** directory with processes executed by a `BashOperator` and the project DAGs scripts in its root directory.

Its **recommended** to add to the src:
1. A `credentials.py` file with the following structure:
   ```python
    """Credentials file.

    Contains a dict with credentials used for local testing
    """
    credentials={
        'aws_access_key_id': 'AWS_ACCESS_KEY_ID',
        'aws_secret_access_key': 'AWS_SECRET_ACCESS_KEY',
        'db_driver': 'NetezzaSQL',
        'db_ip': 'NETEZZA_SERVER_IP',
        'db_port': '5480',
        'db_name': 'SYSTEM',
        'db_user': 'USER',
        'db_password': 'PASSWORD'
    }
   ```

2. A `tmp_tests.ipynb`, for local development. Here, you can integrate the credentials adding the following codeblock:
   ```python
    from boto3 import Session

    from credentials import credentials


    session = Session(
        aws_access_key_id=credentials['aws_access_key_id'],
        aws_secret_access_key=credentials['aws_secret_access_key'],
        region_name='us-east-1'
    )
    credentials_string=f"""
            DRIVER={credentials['db_driver']};
            SERVER={credentials['db_ip']};
            PORT={credentials['db_port']};
            DATABASE={credentials['db_name']};
            UID={credentials['db_user']};
            PWD={credentials['db_password']};
    """
   ```

   Note that this file **will not be tracked by git**.

All files in the root directory are configuration files:
- `Advanced Analytics.code-profile`: Contains a **Visual Studio Code** profile with all the base configuration and extensions.
- `ruff.toml`: Contains the configuration to be used by the **Ruff lint**. The lint will only work after you provide it with a valid Python interpreter.
- `.gitignore`: Contains the git untracked files. It uses as a base the [Python development standard list](https://github.com/github/gitignore/blob/main/Python.gitignore) but adds:
   - `credentials.py` file.
   - Any file or directory that starts with `tmp`.
   - A `data` directory, where files used for local development can be downloaded.
   - A `backup` directory.
