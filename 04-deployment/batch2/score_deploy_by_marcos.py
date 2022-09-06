from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import CronSchedule
from prefect.flow_runners import SubprocessFlowRunner


DeploymentSpec(
    flow_location="score_by_marcos.py",
    name="ride_duration_prediction",
    parameters={
        "taxi_type": "green",
        "run_id": "6574d0d7c4b044f585c08de11b3582c4",
    },
    flow_storage="649d00e3-37ea-4b0b-bb28-57e2b4001f07",
    schedule=CronSchedule(cron="0 3 2 * *"),
    flow_runner=SubprocessFlowRunner(),
    tags=["ml"]
)