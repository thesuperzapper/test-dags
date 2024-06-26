import asyncio
import os
from datetime import timedelta
from typing import Any, AsyncIterator, Dict, Optional

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.settings import Session
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils import timezone
from airflow.utils.context import Context
from airflow.utils.dates import days_ago
from airflow.utils.session import provide_session
from airflow.utils.state import TaskInstanceState
from pendulum.datetime import datetime


# define a trigger that sleeps until a given datetime, and then sends an event
# and "handles" `asyncio.CancelledError` by writing to a file
class DateTimeTriggerWithCancel(BaseTrigger):
    def __init__(
        self,
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: int,
        job_id: int,
        statement_name: str,
        moment: datetime.datetime,
    ):
        super().__init__()
        self.dag_id = dag_id
        self.task_id = task_id
        self.run_id = run_id
        self.map_index = map_index
        self.job_id = job_id
        self.statement_name = statement_name

        # set and validate the moment
        if not isinstance(moment, datetime.datetime):
            raise TypeError(
                f"Expected 'datetime.datetime' type for moment. Got '{type(moment)}'"
            )
        elif moment.tzinfo is None:
            raise ValueError("You cannot pass naive datetime")
        else:
            self.moment: pendulum.DateTime = timezone.convert_to_utc(moment)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        #
        # TODO: for Airflow 2.6.0+ you can get the `TaskInstance` from `self.task_instance` which removes
        #       the need to store `dag_id`, `task_id`, `run_id`, `map_index`, and `job_id` in the trigger
        #       serialization. However, you STILL NEED TO QUERY for the latest TaskInstance state.
        #
        return (
            "test_on_kill_deferred.DateTimeTriggerWithCancel",
            {
                "dag_id": self.dag_id,
                "task_id": self.task_id,
                "run_id": self.run_id,
                "map_index": self.map_index,
                "job_id": self.job_id,
                "statement_name": self.statement_name,
                "moment": self.moment,
            },
        )

    @provide_session
    def get_task_instance(self, session: Session) -> TaskInstance:
        query = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id == self.task_id,
            TaskInstance.run_id == self.run_id,
            TaskInstance.map_index == self.map_index,
        )
        task_instance = query.one_or_none()
        if task_instance is None:
            raise AirflowException(
                "TaskInstance with dag_id: %s, task_id: %s, run_id: %s and map_index: %s is not found",
                self.dag_id,
                self.task_id,
                self.run_id,
                self.map_index,
            )
        return task_instance

    def safe_to_cancel(self) -> bool:
        """
        Whether it is safe to cancel the external job which is being executed by this trigger.

        This is to avoid the case that `asyncio.CancelledError` is called because the trigger
        itself is stopped. Because in those cases, we should NOT cancel the external job.
        """
        # Database query is needed to get the latest state of the task instance.
        task_instance = self.get_task_instance()

        # If the current job_id is different from when the trigger was created,
        # then we should cancel the external job we are waiting on because the task has been
        # cleared and a new job has been created.
        if int(task_instance.job_id) != int(self.job_id):
            return True

        # If the task is not in a deferred state, then something else has happened to the task
        # since we were deferred (e.g. a manual state change), so we should cancel the external
        # job we are waiting on.
        return task_instance.state != TaskInstanceState.DEFERRED

    async def run(self) -> AsyncIterator[TriggerEvent]:
        self.log.info("trigger starting")
        try:
            # Sleep a second at a time
            while self.moment > pendulum.instance(timezone.utcnow()):
                self.log.info("sleeping 1 second...")
                await asyncio.sleep(1)

            # Send our single event and then we're done
            self.log.info("yielding event with payload %r", self.moment)
            yield TriggerEvent(
                {
                    "statement_name": self.statement_name,
                    "status": "success",
                    "moment": self.moment,
                }
            )

        except asyncio.CancelledError:
            self.log.info("asyncio.CancelledError was called")
            if self.statement_name:
                if self.safe_to_cancel():
                    self.log.warning("Cancelling query: %s", self.statement_name)

                    # Cancel the query (mock by writing to a file)
                    output_folder = (
                        f"/tmp/testing/on_kill_deferred/{self.dag_id}/{self.task_id}"
                    )
                    os.makedirs(output_folder, exist_ok=True)
                    with open(f"{output_folder}/log_trigger.txt", "a") as f:
                        f.write(
                            f"asyncio.CancelledError was called: {self.statement_name}\n"
                        )
                    yield TriggerEvent({"status": "cancelled"})
                else:
                    self.log.warning("Triggerer probably stopped, not cancelling query")
            else:
                self.log.error("self.statement_name is None")
        except Exception as e:
            self.log.exception("Exception occurred while checking for query completion")
            yield TriggerEvent({"status": "error", "message": str(e)})


# an operator that sleeps for a given number of seconds using a deferred trigger
class TestDeferredOperator(BaseOperator):
    statement_name: Optional[str]
    wait_seconds: int
    moment: Optional[datetime.datetime]

    def __init__(self, wait_seconds: int = 120, **kwargs):
        super().__init__(**kwargs)
        self.wait_seconds = wait_seconds
        self.statement_name = None
        self.moment = None

    def execute(self, context: Context) -> None:
        self.statement_name = (
            f"airflow"
            f"::{self.dag.dag_id}"
            f"::{self.task_id}"
            f"::{pendulum.now(timezone.utc).isoformat()}"
        )
        self.moment = pendulum.instance(timezone.utcnow()).add(
            seconds=self.wait_seconds
        )
        self.defer(
            trigger=DateTimeTriggerWithCancel(
                dag_id=self.dag.dag_id,
                task_id=self.task_id,
                run_id=context["run_id"],
                map_index=context["task_instance"].map_index,
                job_id=context["task_instance"].job_id,
                statement_name=self.statement_name,
                moment=self.moment,
            ),
            method_name="execute_complete",
        )

    def execute_complete(
        self,
        context: Context,
        event: Optional[Dict[str, Any]] = None,
    ) -> None:
        if event is None:
            raise AirflowException("Trigger event is None")

        if event["status"] == "error":
            msg = f"context: {context}, error message: {event['message']}"
            raise AirflowException(msg)

        if event["status"] == "cancelled":
            self.log.info(f"external job was cancelled: {self.statement_name}")
            return

        self.log.info("%s completed successfully.", self.task_id)

    def on_kill(self):
        output_folder = (
            f"/tmp/testing/on_kill_deferred/{self.dag.dag_id}/{self.task_id}"
        )
        os.makedirs(output_folder, exist_ok=True)
        with open(f"{output_folder}/log_operator.txt", "a") as f:
            f.write(f"on_kill was called: {self.statement_name}\n")


with DAG(
    dag_id="test_on_kill_deferred",
    schedule_interval="0 0 * * *",
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
) as dag:

    # task 1
    task_1 = TestDeferredOperator(
        task_id="task_1",
        wait_seconds=60,
    )
