from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service.jobs import Task, SparkPythonTask
from databricks.sdk.service.compute import ClusterSpec


def main():
    # Initialize the workspace client
    w = WorkspaceClient()

    # Open the job.py file and read its content
    with open("job.py", "r") as f:
        content = f.read()

    # Copy to a new location in the workspace
    w.workspace.mkdirs("/Shared/Scripts")
    w.workspace.upload(
        path="/Shared/Scripts/job.py",
        content=content,
        format=ImportFormat.RAW,
        language=Language.PYTHON,
        overwrite=True,
    )

    # Specify the job cluster specification
    cluster_spec = ClusterSpec(
        num_workers=1,
        spark_version="15.4.x-scala2.12",
        node_type_id="Standard_D4ds_v5",
    )

    # Specify the job task
    spark_python_task = SparkPythonTask(
        python_file="/Shared/Scripts/job.py",
        parameters=[],
    )
    task = Task(
        task_key="test",
        new_cluster=cluster_spec,
        spark_python_task=spark_python_task,
    )

    # Create or update the job
    job_name = "Test Job via SDK"
    job_id = None
    for job in w.jobs.list():
        if job_name == job.settings.name:
            job_id = job.job_id
            break

    if job_id:
        job_settings = w.jobs.get(job_id).settings
        job_settings.tasks = [task]
        w.jobs.reset(job_id, job_settings)
    else:
        w.jobs.create(name=job_name, tasks=[task])


if __name__ == "__main__":
    main()
