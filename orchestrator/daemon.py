import click
import logging
import asyncio
from grpc import aio

from aapis.orchestrator.v1 import (
    orchestrator_pb2_grpc,
    orchestrator_pb2
)

from orchestrator.click_types import LogLevel

# https://stackoverflow.com/questions/38387443/how-to-implement-a-async-grpc-python-server/63020796#63020796

INSECURE_PORT = 40040

class Job(object):
    def __init__(self, id, priority, blockers, exec):
        self.id = id
        self.blockers = blockers
        if len(self.blockers) > 0:
            self.status = orchestrator_pb2.JobStatus.JOB_STATUS_BLOCKED
        else:
            self.status = orchestrator_pb2.JobStatus.JOB_STATUS_QUEUED
        self.prev_status = self.status
        self.priority = priority
        self.exec = exec
    
    async def pause(self):
        self.prev_status = self.status
        self.status = orchestrator_pb2.JobStatus.JOB_STATUS_PAUSED
    
    async def resume(self):
        self.status = self.prev_status

    async def removeBlocker(self, id):
        if id in self.blockers:
            self.blockers.remove(id)
        if len(self.blockers) == 0 and self.status == orchestrator_pb2.JobStatus.JOB_STATUS_BLOCKED:
            self.status = orchestrator_pb2.JobStatus.JOB_STATUS_QUEUED

class Mp4Job(Job):
    def __init__(self, id, priority, blockers, input_path, output_path):
        super(Mp4Job, self).__init__(id, priority, blockers, ["mp4", input_path, output_path])
        self.output_path = output_path

class Mp4UniteJob(Job):
    def __init__(self, id, priority, blockers, input_paths, output_path):
        super(Mp4UniteJob, self).__init__(id, priority, blockers, ["mp4unite", *input_paths, output_path])
        self.output_path = output_path

class ScrapeJob(Job):
    def __init__(self, id, priority, blockers, url, xpath, output_path):
        super(ScrapeJob, self).__init__(id, priority, blockers, ["TODO"])

class Orchestrator(orchestrator_pb2_grpc.OrchestratorServiceServicer):
    def __init__(self, num_allowed_threads):
        self._max_threads = num_allowed_threads
        self._num_threads = 0
        self._job_counter = 0

        self._jobs = {}
        self._completed_jobs = []
        self._errored_jobs = []
        self._canceled_jobs = []

        self._paused = False

        self._current_jobs = [None for _ in range(num_allowed_threads)]
        for i in range(num_allowed_threads):
            asyncio.get_event_loop().create_task(self.executor_thread(i))
    
        asyncio.get_event_loop().create_task(self.update_thread())
    
    async def update_thread(self):
        while True:
            # TODO handle blocks, PAUSED status as well
            await asyncio.sleep(5.0)
    
    async def executor_thread(self, idx):
        while True:
            if not self._paused:
                if self._current_jobs[idx] is not None:
                    job_id = self._current_jobs[idx]
                    self._jobs[job_id].status = orchestrator_pb2.JobStatus.JOB_STATUS_ACTIVE
                    proc = await asyncio.create_subprocess_exec(
                        *self._jobs[job_id].exec,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    _, stderr = await proc.communicate()
                    if stderr:
                        logging.warn(f"Job ID {job_id} ended with an error: {stderr.decode()}")
                        self._errored_jobs.append(job_id)
                        del self._jobs[job_id]
                    else:
                        logging.info(f"Job ID {job_id} completed successfully")
                        self._completed_jobs.append(job_id)
                        del self._jobs[job_id]
                    self._current_jobs[idx] = None
            await asyncio.sleep(5.0)
    
    async def KickoffJob(self, request, context):
        if request.WhichOneof("job") == "mp4":
            if request.mp4.WhichOneof("input") == "input_path":
                job = Mp4Job(
                    self._job_counter,
                    request.priority,
                    request.blocking_job_ids,
                    request.mp4.input_path,
                    request.mp4.output_path
                )
            elif request.mp4.WhichOneof("input") == "job_id_input":
                if request.mp4.job_id_input in self._jobs and ( \
                    isinstance(self._jobs[request.mp4.job_id_input], Mp4Job) or \
                    isinstance(self._jobs[request.mp4.job_id_input], Mp4UniteJob)):
                    blocking_job_ids = request.blocking_job_ids
                    if request.mp4.job_id_input not in blocking_job_ids:
                        blocking_job_ids.append(request.mp4.job_id_input)
                    job = Mp4Job(
                        self._job_counter,
                        request.priority,
                        blocking_job_ids,
                        self._jobs[request.mp4.job_id_input].output_path,
                        request.mp4.output_path
                    )
                else:
                    return orchestrator_pb2.KickoffJobResponse(
                        success=False,
                        message="Invalid job id input",
                        job_id=-1
                    )
            else:
                return orchestrator_pb2.KickoffJobResponse(
                    success=False,
                    message="Input path not specified",
                    job_id=-1
                )
        elif request.WhichOneof("job") == "mp4_unite":
            if len(request.mp4_unite.job_id_inputs) > 0:
                input_paths = request.mp4_unite.input_paths
                blocking_job_ids = request.blocking_job_ids
                for job_id in request.mp4_unite.job_id_inputs:
                    if job_id not in blocking_job_ids:
                        blocking_job_ids.append(job_id)
                    if job_id in self._jobs and ( \
                        isinstance(self._jobs[job_id], Mp4Job) or \
                        isinstance(self._jobs[job_id], Mp4UniteJob)):
                        input_paths.append(self._jobs[job_id].output_path)
                    else:
                        return orchestrator_pb2.KickoffJobResponse(
                            success=False,
                            message="Invalid job id input",
                            job_id=-1
                        )
                job = Mp4UniteJob(
                    self._job_counter,
                    request.priority,
                    blocking_job_ids,
                    input_paths,
                    request.mp4_unite.output_path
                )
            elif len(request.mp4_unite.input_paths) > 0:
                job = Mp4UniteJob(
                    self._job_counter,
                    request.priority,
                    request.blocking_job_ids,
                    request.mp4_unite.input_paths,
                    request.mp4_unite.output_path
                )
            else:
                return orchestrator_pb2.KickoffJobResponse(
                    success=False,
                    message="No input paths specified",
                    job_id=-1
                )
        elif request.WhiteOneof("job") == "scrape":
            return orchestrator_pb2.KickoffJobResponse(
                success=False,
                message="Scrape jobs not yet supported",
                job_id=-1
            )
        else:                               
            return orchestrator_pb2.KickoffJobResponse(
                success=False,
                message="Job type not specified",
                job_id=-1
            )
        self._jobs[self._job_counter] = job
        self._job_counter += 1
        return orchestrator_pb2.KickoffJobResponse(
            success=True,
            message="",
            job_id=job.id
        )
    
    async def JobStatus(self, request, context):
        if request.job_id in self._jobs:
            return orchestrator_pb2.JobStatusResponse(
                status=self._jobs[request.job_id].status,
                message=""
            )
        elif request.job_id in self._completed_jobs:
            return orchestrator_pb2.JobStatusResponse(
                status=orchestrator_pb2.JobStatus.JOB_STATUS_COMPLETE,
                message=""
            )
        elif request.job_id in self._errored_jobs:
            return orchestrator_pb2.JobStatusResponse(
                status=orchestrator_pb2.JobStatus.JOB_STATUS_ERROR,
                message=""
            )
        elif request.job_id in self._canceled_jobs:
            return orchestrator_pb2.JobStatusResponse(
                status=orchestrator_pb2.JobStatus.JOB_STATUS_CANCELED,
                message=""
            )
        else:
            return orchestrator_pb2.JobStatusResponse(
                status=orchestrator_pb2.JobStatus.JOB_STATUS_INVALID,
                message=f"Job ID {request.job_id} not found."
            )
    
    async def JobsSummaryStatus(self, request, context):
        queued_jobs = [job.id for job in self._jobs.values() if job.status == orchestrator_pb2.JobStatus.JOB_STATUS_QUEUED]
        active_jobs = [job.id for job in self._jobs.values() if job.status == orchestrator_pb2.JobStatus.JOB_STATUS_ACTIVE]
        blocked_jobs = [job.id for job in self._jobs.values() if job.status == orchestrator_pb2.JobStatus.JOB_STATUS_BLOCKED]
        paused_jobs = [job.id for job in self._jobs.values() if job.status == orchestrator_pb2.JobStatus.JOB_STATUS_PAUSED]
        discarded_jobs = self._errored_jobs + self._canceled_jobs
        return orchestrator_pb2.JobsSummaryStatusResponse(
            num_completed_jobs=len(self._completed_jobs),
            completed_jobs=self._completed_jobs,
            num_queued_jobs=len(queued_jobs),
            queued_jobs=queued_jobs,
            num_active_jobs=len(active_jobs),
            active_jobs=active_jobs,
            num_blocked_jobs=len(blocked_jobs),
            blocked_jobs=blocked_jobs,
            num_paused_jobs=len(paused_jobs),
            paused_jobs=paused_jobs,
            num_discarded_jobs=len(discarded_jobs),
            discarded_jobs=discarded_jobs
        )
    
    async def pause(self):
        if self._paused:
            return False
        self._paused = True
        return True
    
    async def resume(self):
        if not self._paused:
            return False
        self._paused = False
        return True

    async def PauseJobs(self, request, context):
        success = await self.pause()
        if success:
            return orchestrator_pb2.PauseJobsResponse(
                success=True,
                message=""
            )
        else:
            return orchestrator_pb2.PauseJobsResponse(
                success=False,
                message="Jobs already paused"
            )
    
    async def ResumeJobs(self, request, context):
        success = await self.resume()
        if success:
            return orchestrator_pb2.ResumeJobsResponse(
                success=True,
                message=""
            )
        else:
            return orchestrator_pb2.ResumeJobsResponse(
                success=False,
                message="Jobs already active"
            )
    
    async def cancel(self, id):
        if id in self._jobs:
            if self._jobs[id].status == orchestrator_pb2.JobStatus.JOB_STATUS_ACTIVE:
                return False, "Cannot cancel an active job"
            else:
                del self._jobs[id]
                self._canceled_jobs.append(id)
                return True, ""
        else:
            return False, "Job already completed, discarded, or non-existent"
    
    async def CancelJob(self, request, context):
        success, message = await self.cancel(request.job_id)
        return orchestrator_pb2.CancelJobResponse(
            success=success,
            message=message
        )

async def serve(num_allowed_threads):
    server = aio.server()
    orchestrator_pb2_grpc.add_OrchestratorServiceServicer_to_server(
        Orchestrator(num_allowed_threads), server)
    listen_addr = f"[::]:{INSECURE_PORT}"
    server.add_insecure_port(listen_addr)
    logging.info(f"Starting orchestrator server on {listen_addr}")
    await server.start()
    await server.wait_for_termination()

@click.command()
@click.option(
    "-n",
    "--num-allowed-threads",
    type=int,
    default=1,
)
@click.option(
    "-l",
    "--log-level",
    type=LogLevel(),
    default=logging.INFO,
)
def cli(num_allowed_threads, log_level):
    """Spawn the Orchestrator daemon."""
    logging.basicConfig(level=log_level)
    logging.info(f"Log level set to {log_level}")
    asyncio.run(serve(num_allowed_threads))

def main():
    cli()

if __name__ == "__main__":
    main()
