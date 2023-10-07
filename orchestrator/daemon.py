import click
import logging
import asyncio
from grpc import aio

from aapis.orchestrator.v1 import (
    orchestrator_pb2_grpc,
    orchestrator_pb2
)

from orchestrator.click_types import LogLevel
from orchestrator.jobs import jobFromProto


DEFAULT_INSECURE_PORT = 40040

class Orchestrator(orchestrator_pb2_grpc.OrchestratorServiceServicer):
    def __init__(self, num_allowed_threads):
        self._max_threads = num_allowed_threads
        self._num_threads = 0
        self._job_counter = 0

        self._jobs = {}
        self._completed_jobs = {}
        self._errored_jobs = []
        self._canceled_jobs = []

        self._paused = False

        self._current_jobs = [None for _ in range(num_allowed_threads)]
        for i in range(num_allowed_threads):
            asyncio.get_event_loop().create_task(self.executor_thread(i))
    
        asyncio.get_event_loop().create_task(self.update_thread())

    async def free_thread(self):
        for i, cj in enumerate(self._current_jobs):
            if cj is None:
                return i
        return -1
    
    async def update_thread(self):
        while True:
            if not self._paused:
                i = await self.free_thread()
                if i != -1 and len(self._jobs.values()) > 0:
                    # Determine the next job to slot into free thread i TODO make sorted_queued_candidates a function that JobsStatusResponse calls into as well
                    candidates = [(job.priority, job.id) for job in self._jobs.values() if job.status == orchestrator_pb2.JobStatus.JOB_STATUS_QUEUED]
                    sorted_candidates = sorted(candidates, key=lambda x: (x[0], x[1]))
                    self._current_jobs[i] = sorted_candidates[0][1]
            await asyncio.sleep(1.0)
    
    async def executor_thread(self, idx):
        while True:
            if not self._paused:
                if self._current_jobs[idx] is not None:
                    job_id = self._current_jobs[idx]
                    self._jobs[job_id].status = orchestrator_pb2.JobStatus.JOB_STATUS_ACTIVE
                    stdout, stderr = await self._jobs[job_id].execute()
                    if stderr:
                        logging.warn(f"Job ID {job_id} ended with an error: {stderr.decode()}")
                        self._errored_jobs.append(job_id)
                        del self._jobs[job_id]
                        for jid, job in self._jobs.items():
                            if job_id in job.blockers:
                                logging.warn(f"Canceling Job ID {jid}, which was dependent on Job ID {job_id}")
                                del self._jobs[jid]
                                self._canceled_jobs.append(jid)
                    else:
                        logging.info(f"Job ID {job_id} completed successfully")
                        self._completed_jobs[job_id] = self._jobs[job_id]
                        self._completed_jobs[job_id].outputs = await self._jobs[job_id].getOutputs(stdout)
                        del self._jobs[job_id]
                        for job in self._jobs.values():
                            for completed_job in self._completed_jobs.values():
                                await job.removeBlocker(completed_job)
                            if job.hasChildren() and job.status == orchestrator_pb2.JobStatus.JOB_STATUS_COMPLETE:
                                for child in job.getChildren():
                                    await self.add_job(child)
                                self._completed_jobs[job.id] = job
                                del self._jobs[job.id]
                    self._current_jobs[idx] = None
            await asyncio.sleep(1.0)
    
    async def add_job(self, job):
        job.id = self._job_counter
        self._jobs[self._job_counter] = job
        self._job_counter += 1
    
    async def KickoffJob(self, request, context):
        job, msg = jobFromProto(request)
        if job is None:
            return orchestrator_pb2.KickoffJobResponse(
                success=False,
                message=msg,
                job_id=-1
            )
        await self.add_job(job)
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
        elif request.job_id in self._completed_jobs.keys():
            return orchestrator_pb2.JobStatusResponse(
                status=orchestrator_pb2.JobStatus.JOB_STATUS_COMPLETE,
                message="",
                spawned_children=[job.id for job in self._completed_jobs[request.job_id].getChildren()]
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
            num_completed_jobs=len(self._completed_jobs.keys()),
            completed_jobs=self._completed_jobs.keys(),
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
        for job in self._jobs.values():
            await job.pause()
        self._paused = True
        return True
    
    async def resume(self):
        if not self._paused:
            return False
        for job in self._jobs.values():
            await job.resume()
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

async def serve(port, num_allowed_threads):
    server = aio.server()
    orchestrator_pb2_grpc.add_OrchestratorServiceServicer_to_server(
        Orchestrator(num_allowed_threads), server)
    listen_addr = f"[::]:{port}"
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
    "-p",
    "--port",
    type=int,
    default=DEFAULT_INSECURE_PORT,
)
@click.option(
    "-l",
    "--log-level",
    type=LogLevel(),
    default=logging.INFO,
)
def cli(num_allowed_threads, port, log_level):
    """Spawn the Orchestrator daemon."""
    logging.basicConfig(level=log_level)
    logging.info(f"Log level set to {log_level}")
    asyncio.run(serve(port, num_allowed_threads))

def main():
    cli()

if __name__ == "__main__":
    main()
