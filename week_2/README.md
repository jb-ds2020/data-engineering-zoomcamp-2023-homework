## Week 2 Homework

The goal of this homework is to familiarise users with workflow orchestration and observation. 

All code has been developed in the following repo: https://github.com/jb-ds2020/prefect-code.git


## Question 1. Load January 2020 data

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

* 447,770

After preparing the code I used the following command:
`python flows/02_gcp/etl_web_to_gcs_homework.py`


## Question 2. Scheduling with Cron

Cron is a common scheduling specification for workflows. 

Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

- `0 5 1 * *`
 1. minutes
 2. hours in UTC
 3. day of the month

`prefect deployment build flows/02_gcp/etl_web_to_gcs_homework.py:etl_web_to_gcs -n "Parametrized ETL Homework cronjob"  --cron "0 5 1 * *"` 

and finally apply the deployment:

`prefect deployment apply etl_web_to_gcs-deployment.yaml`

And finally activate the default work queue with `prefect agent start --work-queue "default"`

## Question 3. Loading data to BigQuery 

Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color. 

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

- 14,851,920

After setting up the python file for web to gcs and gcs to bigquery, the deployment was done with:

`prefect deployment build flows/02_gcp/ETL_web_gcs_bq_homework.py:etl_parent_flow -n "Parametrized ETL Homework"`

set the parameter to: `"year":2019, "months":[2,3], "color":"yello"` in the .yaml file

and finally apply the deployment:

`prefect deployment apply etl_parent_flow-deployment.yaml`

use the prefect gui to shedule a rum for Friday 16:26 European Time with cron string `26 15 * * 5`

And finally activate the default work queue with `prefect agent start --work-queue "default"`

## Question 4. Github Storage Block

Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image. 

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

- 88,605

Following code was used to deploy the code with a github repo:

```python
from prefect.deployments import Deployment
from ETL_web_gcs_bq_homework import etl_parent_flow

from prefect.filesystems import GitHub

storage = GitHub.load("github-prefect-code")

deployment = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="github-deployment",
    storage=storage,
    entrypoint="flows/02_gcp/ETL_web_gcs_bq_homework.py:etl_parent_flow",
)

if __name__ == "__main__":
    deployment.apply()
```

with the following command this deployment is set:

`python flows/03_deployments/git_deploy.py`

`prefect deployment run etl-parent-flow/github-deployment  -p "months=[11]" -p "year=2020" -p "color=green"`

## Question 5. Email or Slack notifications

Q5. It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur. 

Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up. 

Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.

Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook. 

How many rows were processed by the script?

- `514,392`

First it was necessary to connect to prefect cloud with an API key.

After setting up the notification, I set up all the block from the local orion server to the prefect cloud to have all the credentials available there.

I created a python file for the cloud deployment:
```python
from prefect.deployments import Deployment
from ETL_web_gcs_bq_homework import etl_parent_flow

from prefect_github.repository import GitHubRepository

storage = GitHubRepository.load("github-prefect-code")

deployment = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="github-deployment",
    storage=storage,
    entrypoint="flows/02_gcp/ETL_web_gcs_bq_homework.py:etl_parent_flow",
)

if __name__ == "__main__":
    deployment.apply()

```
Setup the deployment:
`python flows/03_deployments/git_deploy_prefect_cloud.py`

Run the deployment:
`prefect deployment run etl-parent-flow/github-deployment  -p "months=[4]" -p "year=2019" -p "color=green"`

## Question 6. Secrets

Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

- 8

Uses the code in a script to add the secrets block since it was not available in my UI:

```python
from prefect import flow
from prefect.blocks.system import Secret


@flow
def add_secret_block():
    Secret(value="passwordexample").save(name="jonassecret")


if __name__ == "__main__":
    add_secret_block()
```

`python flows/03_deployments/Secret.py`

Afterwards I could check the blocks under the Prefect cloud UI

Link: https://discourse.prefect.io/t/how-to-securely-store-secrets-in-prefect-2-0/1209
