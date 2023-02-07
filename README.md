# firecrest-workflows

A simple async workflow engine for interfacing with [FirecREST](https://products.cscs.ch/firecrest/).

The workflow engine uses [pyfirecrest](https://github.com/eth-cscs/pyfirecrest)
to interface with the [REST API](https://firecrest-api.cscs.ch).


See also the <https://github.com/eth-cscs/firecrest> demo server.

## Usage

After installing the package, the `fireflow` command is available.

You can then create a workflow database from a YAML file, such as `example_setup.yml`:

```yaml
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
calcjobs:
  - label: test-calcjob1
    code_label: test-code1
    parameters:
      echo_string: "Hello world!"
    download_globs:
    - output.txt
  - label: test-calcjob2
    code_label: test-code1
    parameters:
      echo_string: "Hello world 2!"
    download_globs:
    - output.txt
```

Then run `fireflow init -a example_setup.yaml`, which will create a database in a `.fireflow_project` folder.

You can list all the calcjobs with:

```console
$ fireflow calcjob tree
Calcjobs 1-2 of 2
└── 1 - test-client
    └── 1 - test-code1
        ├── 1 - test-calcjob1 ▶
        └── 2 - test-calcjob2 ▶
```

You can then run all the calcjobs with:

```console
$ fireflow run
2023-02-07 20:10:58:fireflow.process:REPORT: PK-1: Uploading files to remote
2023-02-07 20:11:00:fireflow.process:REPORT: PK-2: Uploading files to remote
2023-02-07 20:11:01:fireflow.process:REPORT: PK-1: submitting on remote
2023-02-07 20:11:03:fireflow.process:REPORT: PK-2: submitting on remote
2023-02-07 20:11:05:fireflow.process:REPORT: PK-1: polling job until finished
2023-02-07 20:11:06:fireflow.process:REPORT: PK-2: polling job until finished
2023-02-07 20:11:08:fireflow.process:REPORT: PK-1: copying from remote folder
2023-02-07 20:11:09:fireflow.process:REPORT: PK-2: copying from remote folder
2023-02-07 20:11:13:fireflow.process:REPORT: PK-1: parsing output files
2023-02-07 20:11:13:fireflow.process:REPORT: PK-1: paths: ['job.sh', 'output.txt', 'slurm-147.out']
2023-02-07 20:11:13:fireflow.process:REPORT: PK-2: parsing output files
2023-02-07 20:11:13:fireflow.process:REPORT: PK-2: paths: ['slurm-148.out', 'job.sh', 'output.txt']
```

The calcjobs run asynchronously, with the steps:

- `prepare for submission`: create the job script
- `copying to remote folder`: copy the job script and input files to the remote folder
- `submitting on remote`: submit the job script to the scheduler
- `polling job until finished`: poll the job status until it is finished
- `copying from remote folder`: copy the output files from the remote folder
- `parsing output files`: parse the output files and store the results in the database

## Aims

- Minimise the number of requests to the server

## Upstream o AiiDA

- Print the job script, for debugging
- Add customisation of `sqlalchemy.engine` logging https://docs.sqlalchemy.org/en/20/core/engines.html#configuring-logging

https://chat.openai.com/chat/e453e10b-19fd-46a8-9cfb-0c8d31d1a60d
