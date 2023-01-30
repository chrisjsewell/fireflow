# firecrest-workflows

A simple async workflow engine for interfacing with [FirecREST](https://products.cscs.ch/firecrest/).

The workflow engine uses [pyfirecrest](https://github.com/eth-cscs/pyfirecrest)
to interface with the [REST API](https://firecrest-api.cscs.ch).


See also the <https://github.com/eth-cscs/firecrest> demo server.

## Usage

After installing the package, the `fc-wkflow` command is available.

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
    script: |
      #!/bin/bash
      #SBATCH --job-name={{ calc.uuid }}
      mkdir -p output
      echo '{{ calc.parameters.echo_string }}' > output.txt
    calcjobs:
    - label: test-calcjob1
      parameters:
        echo_string: "Hello world!"
      download_globs:
      - output.txt
    - label: test-calcjob2
      parameters:
        echo_string: "Hello world 2!"
      download_globs:
      - output.txt
```

Then run `fc-wkflow create example_setup.yaml`, which will create a database in a `wkflow_storage` folder.

You can list all the calcjobs with:

```console
$ fc-wkflow calcjob tree
Calcjobs 1-2 of 2
└── 1 - test-client
    └── 1 - test-code1
        ├── 1 - test-calcjob1
        └── 2 - test-calcjob2
```

You can then run all the calcjobs with:

```console
$ fc-wkflow run
2023-01-30 09:22:32:firecrest_wflow.process:INFO: prepare for submission: 31c02ae1-062e-4f03-a77f-b128aa31c744
2023-01-30 09:22:32:firecrest_wflow.process:INFO: copying to remote folder: /home/service-account-firecrest-sample/workflows/31c02ae1-062e-4f03-a77f-b128aa31c744
2023-01-30 09:22:33:firecrest_wflow.process:INFO: prepare for submission: 3c118af8-3b30-4e2d-b788-e33920a068e9
2023-01-30 09:22:33:firecrest_wflow.process:INFO: copying to remote folder: /home/service-account-firecrest-sample/workflows/3c118af8-3b30-4e2d-b788-e33920a068e9
2023-01-30 09:22:33:firecrest_wflow.process:INFO: submitting on remote: /home/service-account-firecrest-sample/workflows/31c02ae1-062e-4f03-a77f-b128aa31c744/job.sh
2023-01-30 09:22:35:firecrest_wflow.process:INFO: submitting on remote: /home/service-account-firecrest-sample/workflows/3c118af8-3b30-4e2d-b788-e33920a068e9/job.sh
2023-01-30 09:22:37:firecrest_wflow.process:INFO: polling job until finished: 31c02ae1-062e-4f03-a77f-b128aa31c744
2023-01-30 09:22:38:firecrest_wflow.process:INFO: polling job until finished: 3c118af8-3b30-4e2d-b788-e33920a068e9
2023-01-30 09:22:39:firecrest_wflow.process:INFO: copying from remote folder: /home/service-account-firecrest-sample/workflows/31c02ae1-062e-4f03-a77f-b128aa31c744
2023-01-30 09:22:40:firecrest_wflow.process:INFO: copying from remote folder: /home/service-account-firecrest-sample/workflows/3c118af8-3b30-4e2d-b788-e33920a068e9
2023-01-30 09:22:43:firecrest_wflow.process:INFO: parsing output files: /var/folders/t2/xbl15_3n4tsb1vr_ccmmtmbr0000gn/T/tmp2gcuydfw
2023-01-30 09:22:43:firecrest_wflow.process:INFO: parsing output files: /var/folders/t2/xbl15_3n4tsb1vr_ccmmtmbr0000gn/T/tmp9gcqzae4
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
