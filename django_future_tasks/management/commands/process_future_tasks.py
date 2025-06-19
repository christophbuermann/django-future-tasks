import logging
import signal
import sys
import time
import timeit
import traceback
from datetime import timedelta
from sys import intern

from django import db
from django.core.management.base import BaseCommand
from django.utils import timezone

from django_future_tasks.handlers import future_task_signal
from django_future_tasks.models import FutureTask

logger = logging.getLogger("process_future_tasks")


class Command(BaseCommand):
    help = "Process future tasks from database"

    current_task_pk = None

    def add_arguments(self, parser):
        parser.add_argument(
            "--onetimerun",
            action="append",
            type=bool,
            default=False,
            help="Process tasks that are open at the time of running the command and exit.",
        )
        parser.add_argument(
            "--wait-for-tasks-duration-seconds",
            action="append",
            type=float,
            default=1.0,
            help="If there are no open tasks the command waits this amount of time until it checks for open tasks again.",
        )

    def _handle_termination(self, *args, **kwargs):
        try:
            current_task = FutureTask.objects.get(pk=self.current_task_pk)
            current_task.status = FutureTask.FUTURE_TASK_STATUS_INTERRUPTED
            current_task.save()
        except FutureTask.DoesNotExist:
            pass
        self._running = False

    def _handle_options(self, options):
        self.one_time_run = options["onetimerun"]
        self.wait_for_tasks_duration_seconds = options["wait_for_tasks_duration_seconds"]

    def get_open_tasks(self):
        return FutureTask.objects.filter(
            eta__lte=timezone.now(),
            status=FutureTask.FUTURE_TASK_STATUS_OPEN,
        ).order_by("eta")

    def endless_task_iterator(self):
        while self._running:
            tasks = self.get_open_tasks()
            yield from tasks
            if not tasks:
                time.sleep(self.wait_for_tasks_duration_seconds)

    @staticmethod
    def _convert_exception_args(args):
        return [str(arg) for arg in args]

    def handle_task(self, task):
        task.status = FutureTask.FUTURE_TASK_STATUS_IN_PROGRESS
        task.save()
        self.current_task_pk = task.pk
        try:
            start_time = timeit.default_timer()
            future_task_signal.send(sender=intern(task.type), instance=task)
            task.execution_time = timeit.default_timer() - start_time
            task.status = FutureTask.FUTURE_TASK_STATUS_DONE
        except Exception as exc:
            task.status = FutureTask.FUTURE_TASK_STATUS_ERROR
            task.result = {
                "exception": "An exception of type {} occurred.".format(
                    type(exc).__name__,
                ),
                "args": self._convert_exception_args(exc.args),
                "traceback": traceback.format_exception(
                    *sys.exc_info(),
                    limit=None,
                    chain=None,
                ),
            }
            logger.exception(exc)
        self.current_task_pk = None
        task.save()


    def handle(self, *args, **options):
        # Load given options.
        self._handle_options(options)
        tasks = iter(self.get_open_tasks()) if self.one_time_run else self.endless_task_iterator()
        while self._running:
            try:
                self.handle_task(next(tasks))
            except StopIteration:
                break
            except Exception as exc:
                logger.exception(
                    f"{exc.__class__.__name__} exception occurred...",
                )
                # As the database connection might have failed, we discard it here, so django will
                # create a new one on the next database access.
                db.close_old_connections()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # The command will run as long as the `_running` attribute is
        # set to `True`. To safely quit the command, just set this attribute to `False` and the
        # command will finish a running tick and quit afterwards.
        self._running = True

        # Register system signal handler to gracefully quit the service when
        # getting a `SIGINT` or `SIGTERM` signal (e.g. by CTRL+C).
        signal.signal(signal.SIGINT, self._handle_termination)
        signal.signal(signal.SIGTERM, self._handle_termination)
