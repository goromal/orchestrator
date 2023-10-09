import click
import logging
import asyncio
import queue
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
        self._errored_jobs = {}
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
                    if len(candidates) > 0:
                        sorted_candidates = sorted(candidates, key=lambda x: (x[0], x[1]))
                        selected_job_id = sorted_candidates[0][1]
                        logging.info(f"Activating Job ID {selected_job_id} on Thread {i}")
                        self._current_jobs[i] = selected_job_id
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
                        self._errored_jobs[job_id] = self._jobs[job_id]
                        self._errored_jobs[job_id].msg = stderr.decode()
                        del self._jobs[job_id]
                        job_ids_to_delete = []
                        for jid, job in self._jobs.items():
                            if job_id in job.blockers:
                                logging.warn(f"Canceling Job ID {jid}, which was dependent on Job ID {job_id}")
                                job_ids_to_delete.append(jid)
                                self._canceled_jobs.append(jid)
                        for job_id_to_delete in job_ids_to_delete:
                            del self._jobs[job_id_to_delete]
                    else:
                        logging.info(f"Job ID {job_id} completed successfully")
                        
                        # Capture output for the original completed job
                        job = self._jobs[job_id]
                        job.status = orchestrator_pb2.JobStatus.JOB_STATUS_COMPLETE
                        job.outputs = await job.getOutputs(stdout.decode())
                        
                        # Completed job queue
                        completed_queue = queue.Queue()
                        completed_queue.put(job)

                        # Not sure if there's a better way to do this given how removeBlocker works;
                        # add all previously completed jobs
                        for completed_job in self._completed_jobs.values():
                            completed_queue.put(completed_job)

                        # Process all queued completed jobs
                        while not completed_queue.empty():
                            popped_job = completed_queue.get()
                            
                            # Add completed job children if they exist and give them IDs (if they don't have them)
                            if popped_job.hasChildren():
                                for child in popped_job.getChildren():
                                    if child.id == -1:
                                        await self.add_job(child)
                            
                            # Cycle through all dependent jobs
                            dependent_jobs = [dep_job for dep_job in self._jobs.values() if popped_job.id in dep_job.blockers]
                            for dependent_job in dependent_jobs:
                                transitioned = await dependent_job.removeBlocker(popped_job)
                                if dependent_job.hasChildren():
                                    for child in dependent_job.getChildren():
                                        if child.id == -1:
                                            await self.add_job(child)
                                if transitioned and dependent_job.status == orchestrator_pb2.JobStatus.JOB_STATUS_COMPLETE:
                                    completed_queue.put(dependent_job)

                            # Transfer the completed job
                            if popped_job.id not in self._completed_jobs:
                                self._completed_jobs[popped_job.id] = popped_job
                                del self._jobs[popped_job.id]

                    self._current_jobs[idx] = None
            await asyncio.sleep(1.0)
    
    async def add_job(self, job):
        job.id = self._job_counter
        logging.debug(f"Adding Job ID {job.id} to the queue")
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
            job = self._jobs[request.job_id]
            return orchestrator_pb2.JobStatusResponse(
                status=job.status,
                exec=" ".join(job.exec),
                priority=job.priority,
                blockers=job.blockers,
                outputs=job.outputs if job.outputs is not None else "",
                spawned_children=[],
                message=""
            )
        elif request.job_id in self._completed_jobs:
            job = self._completed_jobs[request.job_id]
            return orchestrator_pb2.JobStatusResponse(
                status=orchestrator_pb2.JobStatus.JOB_STATUS_COMPLETE,
                exec=" ".join(job.exec),
                priority=job.priority,
                blockers=[],
                outputs=job.outputs if job.outputs is not None else "",
                spawned_children=[child.id for child in job.getChildren()],
                message=""
            )
        elif request.job_id in self._errored_jobs:
            return orchestrator_pb2.JobStatusResponse(
                status=orchestrator_pb2.JobStatus.JOB_STATUS_ERROR,
                exec=" ".join(job.exec),
                priority=job.priority,
                blockers=job.blockers,
                outputs="",
                spawned_children=[],
                message=job.msg
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
        discarded_jobs = list(self._errored_jobs.keys()) + self._canceled_jobs
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
