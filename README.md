# fireflow

[![PyPI](https://img.shields.io/pypi/v/fireflow?label=PyPI&logo=pypi&style=social)](https://pypi.org/project/fireflow/)

A simple async workflow engine for interfacing with [FirecREST](https://products.cscs.ch/firecrest/).

The workflow engine uses [pyfirecrest](https://github.com/eth-cscs/pyfirecrest)
to interface with the [REST API](https://firecrest-api.cscs.ch).


See also the <https://github.com/eth-cscs/firecrest> demo server.

## Usage

### Installation

```console
$ pip install fireflow
```

### Basic interaction with fireflow

The simplest way to use `fireflow` is via the command line interface.

```console
$ fireflow --help
```

To initialise a project, simply run:

```console
$ fireflow init
```

This will create a `.fireflow_project` folder.

The `fireflow` package also provides a Python API, for example, to access a project:

```python
from fireflow import storage, orm
store = storage.Storage.from_path(".fireflow_project")
client = store.get_row(orm.Client, 1)
```

### The outline of a fireflow project

A `fireflow` project consists of:

- `objects`: binary file contents, which are stored in the project, and access via their SHA256 hash
- `tables`: which contain rows with the main data structures of the project
  - `clients`: a list of clients, which are the remote machines where the jobs will be run
  - `codes`: a list of codes, which are the executables that will be run on the remote machines
  - `calcjobs`: a list of calcjobs, which are the jobs that will be run on the remote machines

### Adding items to a fireflow project

You can use `fireflow add` to add items to a project, or directly when initialising a project, with `fireflow init -a`.

These commands take as input a ath to a YAML file, which contains a declarative list of items to be added, for example:

```yaml
objects:
  label1: {"content": "test", "extension": "txt"}
clients:
  - label: test-client
    client_url: "http://localhost:8000/"
    client_id: "firecrest-sample"
    client_secret: "b391e177-fa50-4987-beaf-e6d33ca93571"
    token_uri: "http://localhost:8080/auth/realms/kcrealm/protocol/openid-connect/token"
    machine_name: "cluster"
    work_dir: "/home/service-account-firecrest-sample"
    small_file_size_mb: 5
codes:
  - label: test-code1
    client_label: test-client
    script: |
      #!/bin/bash
      #SBATCH --job-name={{ calc.uuid }}
      mkdir -p output
      echo '{{ calc.parameters.echo_string }}' > output.txt
    upload_paths:
      "input1.txt": {"label": "label1"}
calcjobs:
  - label: test-calcjob1
    code_label: test-code1
    parameters:
      echo_string: "Hello world!"
    upload_paths:
      "input2.txt": {"label": "label1"}
    download_globs:
    - "**/*"
```

You can also use the YAML [anchors and aliases](https://www.linode.com/docs/guides/yaml-anchors-aliases-overrides-extensions/) feature to avoid repeating the same information, for example:

```yaml
objects:
  label1: {"content": "test", "extension": "txt"}
clients:
  - label: test-client
    client_url: "http://localhost:8000/"
    client_id: "firecrest-sample"
    client_secret: "b391e177-fa50-4987-beaf-e6d33ca93571"
    token_uri: "http://localhost:8080/auth/realms/kcrealm/protocol/openid-connect/token"
    machine_name: "cluster"
    work_dir: "/home/service-account-firecrest-sample"
    small_file_size_mb: 5
codes:
  - label: test-code1
    client_label: test-client
    script: |
      #!/bin/bash
      #SBATCH --job-name={{ calc.uuid }}
      mkdir -p output
      echo '{{ calc.parameters.echo_string }}' > output.txt
    upload_paths:
      "input1.txt": {"label": "label1"}
calcjob_templates:
  template1: &cj_template1
    code_label: test-code1
    parameters:
      echo_string: "Hello world!"
    upload_paths:
      "input2.txt": {"label": "label1"}
    download_globs:
    - "**/*"
calcjobs:
  - <<: *cj_template1
    label: test-calcjob1
  - <<: *cj_template1
    label: test-calcjob2
  - <<: *cj_template1
    label: test-calcjob3
  - <<: *cj_template1
    label: test-calcjob4
  - <<: *cj_template1
    label: test-calcjob5
  - <<: *cj_template1
    label: test-calcjob6
```

### Inspecting a fireflow project

You can use `fireflow status` to provide basic information about a project.

```console
$ fireflow status
Object Store:
- 1 object
Database:
- 1 client
- 1 code
- 6 calcjobs
  - 6 playing
```

You can also use `fireflow calcjob tree` to list all the calcjobs in a project, grouped by their client and code.

```console
Calcjobs 1-6 of 6
└── 1 - test-client
    └── 1 - test-code1
        ├── 1 - test-calcjob1 ▶
        ├── 2 - test-calcjob2 ▶
        ├── 3 - test-calcjob3 ▶
        ├── 4 - test-calcjob4 ▶
        ├── 5 - test-calcjob5 ▶
        └── 6 - test-calcjob6 ▶
```

You can also filter this tree, or the list from `fireflow calcjob list`, by using the `--where` option, which takes an [SQL-like WHERE statement](https://www.sqlite.org/lang_select.html).
For example:

```console
$ fireflow calcjob list --where "pk < 2 OR label like '%job5'"
                              CalcJob 1-2 of 2
╭────┬───────────────┬────────────────┬─────────────────┬─────────┬─────────╮
│ PK │ Label         │ Code           │ Client          │ State   │ Step    │
├────┼───────────────┼────────────────┼─────────────────┼─────────┼─────────┤
│ 1  │ test-calcjob1 │ 1 (test-code1) │ 1 (test-client) │ playing │ created │
│ 5  │ test-calcjob5 │ 1 (test-code1) │ 1 (test-client) │ playing │ created │
╰────┴───────────────┴────────────────┴─────────────────┴─────────┴─────────╯
```

You can inspect a specific calcjob with `fireflow calcjob show <PK>`, for example:

```console
$ fireflow calcjob show --script 1
CalcJob(
    pk=1,
    code_pk=1,
    label='test-calcjob1',
    uuid=UUID('6a2a7e2e-05d2-46a4-a565-1caaed088a9b'),
    parameters=frozen({'echo_string': 'Hello world!'}),
    upload_paths=frozen({'input2.txt': '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08'}),
    download_globs=('**/*',),
    state='playing'
)

Job script:
#!/bin/bash
#SBATCH --job-name=6a2a7e2e-05d2-46a4-a565-1caaed088a9b
mkdir -p output
echo 'Hello world!' > output.txt

Processing(pk=1, calcjob_pk=1, state='playing', step='created', job_id=None, exception=None, retrieved_paths=frozen({}))
```

### Running a fireflow project

The `fireflow run --number 2` command will run a maximum of 2 calcjobs in a project asynchronously.

The calcjobs run asynchronously, with the steps:

- `copying to remote folder`: copy the job script and input files to the remote folder (based on `Code.uoload_paths` and `Calcjob.upload_paths`)
- `submitting on remote`: submit the job script to the scheduler
- `polling job until finished`: poll the job status until it is finished
- `copying from remote folder`: copy the output files from the remote folder (based on `Calcjob.download_globs`)

You can then run all the calcjobs with:

```console
$ fireflow run --number 2
2023-02-10 10:55:16:fireflow.process:REPORT: PK-1: Uploading files to remote
2023-02-10 10:55:21:fireflow.process:REPORT: PK-2: Uploading files to remote
2023-02-10 10:55:23:fireflow.process:REPORT: PK-1: submitting on remote
2023-02-10 10:55:25:fireflow.process:REPORT: PK-2: submitting on remote
2023-02-10 10:55:27:fireflow.process:REPORT: PK-1: polling job until finished
2023-02-10 10:55:29:fireflow.process:REPORT: PK-2: polling job until finished
2023-02-10 10:55:30:fireflow.process:REPORT: PK-1: downloading files from remote
2023-02-10 10:55:33:fireflow.process:REPORT: PK-2: downloading files from remote
```

Inspecting the calcjobs again, you can see that they have been updated:

```console
$ fireflow calcjob tree
Calcjobs 1-6 of 6
└── 1 - test-client
    └── 1 - test-code1
        ├── 1 - test-calcjob1 ✅
        ├── 2 - test-calcjob2 ✅
        ├── 3 - test-calcjob3 ▶
        ├── 4 - test-calcjob4 ▶
        ├── 5 - test-calcjob5 ▶
        └── 6 - test-calcjob6 ▶
```

and the output files have been downloaded:

```console
$ fireflow calcjob show 1
CalcJob(
    pk=1,
    code_pk=1,
    label='test-calcjob1',
    uuid=UUID('6a2a7e2e-05d2-46a4-a565-1caaed088a9b'),
    parameters=frozen({'echo_string': 'Hello world!'}),
    upload_paths=frozen({'input2.txt': '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08'}),
    download_globs=('**/*',),
    state='finished'
)
Processing(
    pk=1,
    calcjob_pk=1,
    state='finished',
    step='finalised',
    job_id='214',
    exception=None,
    retrieved_paths=frozen({'input1.txt': '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08', 'input2.txt': '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08', 'output.txt':
'0ba904eae8773b70c75333db4de2f3ac45a8ad4ddba1b242f0b3cfc199391dd8', 'slurm-214.out': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', 'output': None})
)
```

## Design notes

- Minimise the number of requests to the server

## TODO

- share client requests: https://chat.openai.com/chat/e453e10b-19fd-46a8-9cfb-0c8d31d1a60d

## Potential aspects to upstream to AiiDA

- Using typer (built on top of click) for the CLI
- Updating to sqlalchemy v2, and using its declaratively mapped dataclasses
- Use the `--where` flag syntax for filtering
- Using <https://github.com/chrisjsewell/virtual-glob>, and the idea of `download_globs`
- Downloading files, from the remote, directly (and asynchronously) to the object store
- Print the job script, for debugging
- Add customisation of `sqlalchemy.engine` logging <https://docs.sqlalchemy.org/en/20/core/engines.html#configuring-logging>
