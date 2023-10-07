import asyncio
import re

from aapis.orchestrator.v1 import orchestrator_pb2


class Job(object):
    def __init__(self, priority, blockers, exec):
        self.id = -1
        self.priority = priority

        self.blockers = blockers
        if len(self.blockers) > 0:
            self.status = orchestrator_pb2.JobStatus.JOB_STATUS_BLOCKED
        else:
            self.status = orchestrator_pb2.JobStatus.JOB_STATUS_QUEUED
        self.prev_status = self.status

        self.exec = exec

        self.outputs = None
        self.children = [] # job objects

    async def execute(self):
        proc = await asyncio.create_subprocess_exec(
            *self.exec,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        # TODO handle sudo
        stdout, stderr = await proc.communicate()
        return stdout, stderr
    
    async def getOutputs(self, stdout):
        return [] # can be overridden
    
    async def pause(self):
        self.prev_status = self.status
        self.status = orchestrator_pb2.JobStatus.JOB_STATUS_PAUSED
    
    async def resume(self):
        self.status = self.prev_status

    async def removeBlocker(self, job):
        if job.id in self.blockers:
            self.blockers.remove(job.id)
            await self.processBlockingJobOutputs(job.id, job.outputs)
        if len(self.blockers) == 0 and self.status == orchestrator_pb2.JobStatus.JOB_STATUS_BLOCKED:
            if self.hasChildren():
                self.status = orchestrator_pb2.JobStatus.JOB_STATUS_COMPLETE
            else:
                self.status = orchestrator_pb2.JobStatus.JOB_STATUS_QUEUED

    async def processBlockingJobOutputs(self, id, outputs):
        pass # can be overridden

    def hasChildren(self):
        return len(self.children) > 0

    def getChildren(self):
        return self.children

class Mp4Job(Job):
    def __init__(self, priority, blockers, input_path, input_id, output_path):
        self.input_id = input_id
        self.output_path = output_path
        if self.input_id is None:
            exec = ["mp4", input_path, self.output_path]
        else:
            # not ready
            exec = ["mp4__uninitialized"]
        super(Mp4Job, self).__init__(priority, blockers, exec)
        self.child_counter = 0

    async def getOutputs(self, stdout):
        return [self.output_path]
    
    async def processBlockingJobOutputs(self, id, outputs):
        if id == self.input_id:
            if len(outputs) == 1:
                self.exec = ["mp4", outputs[0], self.output_path]
            elif len(outputs) > 1:
                for output in outputs:
                    self.children.append(Mp4Job(
                        self.priority,
                        self.blockers,
                        output,
                        None,
                        f"{self.output_path.replace('.mp4', '')}_{self.child_counter}.mp4"
                    ))
                    self.child_counter += 1

class Mp4UniteJob(Job):
    def __init__(self, priority, blockers, input_paths, input_ids, output_path):
        self.input_paths = input_paths
        self.input_ids = input_ids
        self.output_path = output_path
        if len(self.input_ids) == 0:
            exec = ["mp4unite", *self.input_paths, self.output_path]
        else:
            # not ready
            exec = ["mp4unite__uninitialized"]
        super(Mp4UniteJob, self).__init__(priority, blockers, exec)
    
    async def getOutputs(self, stdout):
        return [self.output_path]
    
    async def processBlockingJobOutputs(self, id, outputs):
        if id in self.input_ids:
            self.input_paths += outputs
            for input_id in self.input_ids:
                if input_id in self.blockers:
                    # not finished collecting input_id outputs
                    return
            self.exec = ["mp4unite", *self.input_paths, self.output_path]

class ScrapeJob(Job):
    def __init__(self, priority, blockers, url, xpath, output_path, file_ext):
        super(ScrapeJob, self).__init__(priority, blockers, ["scrape", "simple-link-scraper", "--xpath", xpath, "--ext", file_ext, "-o", output_path, url])

    async def getOutputs(self, stdout):
        return re.findall(r"-> (\S+)\.\.\.", stdout)

def jobFromProto(proto):
    if proto.WhichOneof("job") == "mp4":
        if proto.mp4.WhichOneof("input") == "input_path":
            return Mp4Job(
                proto.priority,
                proto.blocking_job_ids,
                proto.mp4.input_path,
                None,
                proto.mp4.output_path
            ), ""
        elif proto.mp4.WhichOneof("input") == "job_id_input":
            blocking_job_ids = proto.blocking_job_ids
            if proto.mp4.job_id_input not in blocking_job_ids:
                blocking_job_ids.append(proto.mp4.job_id_input)
            return Mp4Job(
                proto.priority,
                blocking_job_ids,
                None,
                proto.mp4.job_id_input,
                proto.mp4.output_path
            )
        else:
            return None, "Input not specified"
    elif proto.WhichOneof("job") == "mp4_unite":
        if len(proto.mp4_unite.job_id_inputs) + len(proto.mp4_unite.input_paths) == 0:
            return None, "No inputs specified"
        blocking_job_ids = proto.blocking_job_ids
        input_paths = proto.mp4_unite.input_paths
        input_ids = []
        for job_id in proto.mp4_unite.job_id_inputs:
            input_ids.append(job_id)
            if job_id not in blocking_job_ids:
                blocking_job_ids.append(job_id)
        return Mp4UniteJob(
            proto.priority,
            blocking_job_ids,
            input_paths,
            input_ids,
            proto.mp4_unite.output_path
        ), ""
    elif proto.WhichOneof("job") == "scrape":
        return ScrapeJob(
            proto.priority,
            proto.blocking_job_ids,
            proto.scrape.url,
            proto.scrape.xpath,
            proto.scrape.output_path,
            proto.scrape.file_extension
        ), ""
    else:
        return None, f"Unsupported job type: {proto.Whichoneof('job')}"
