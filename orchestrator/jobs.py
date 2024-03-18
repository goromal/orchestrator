import asyncio
import glob
import logging
import re

from aapis.orchestrator.v1 import orchestrator_pb2


class Job(object):
    def __init__(self, priority, blockers, child_blockers, exec):
        logging.debug(
            f"Constructing Job with exec `{exec}`, blockers {blockers}, and child blockers {child_blockers}"
        )

        self.id = -1
        self.priority = priority

        self.blockers = blockers
        self.child_blockers = child_blockers
        if len(self.blockers) + len(self.child_blockers) > 0:
            self.status = orchestrator_pb2.JobStatus.JOB_STATUS_BLOCKED
        else:
            self.status = orchestrator_pb2.JobStatus.JOB_STATUS_QUEUED
        self.prev_status = self.status

        self.exec = exec

        self.outputs = None
        self.children = []  # job objects
        self.msg = ""

    async def execute(self):
        logging.debug(f"Executing command `{' '.join(self.exec)}`")
        try:
            proc = await asyncio.create_subprocess_exec(
                *self.exec,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            # TODO handle sudo
            stdout, stderr = await proc.communicate()
        except Exception as e:
            stdout = None
            stderr = f"{e}".encode()
        return stdout, stderr

    async def getOutputs(self, stdout):
        return []  # can be overridden

    async def pause(self):
        self.prev_status = self.status
        self.status = orchestrator_pb2.JobStatus.JOB_STATUS_PAUSED

    async def resume(self):
        self.status = self.prev_status

    async def removeBlocker(self, job):
        if job.id in self.blockers:
            logging.debug(f"Job ID {self.id}: Removing blocker {job.id}")
            self.blockers.remove(job.id)
            if job.hasChildren():
                for child in job.getChildren():
                    logging.debug(
                        f"Job ID {self.id}: Adding Job ID {child.id} as a blocker"
                    )
                    self.blockers.append(child.id)
        if job.id in self.child_blockers:
            logging.debug(f"Job ID {self.id}: Removing child blocker {job.id}")
            self.child_blockers.remove(job.id)
            await self.processBlockingChildOutputs(job.id, job.outputs)
            if job.hasChildren():
                for child in job.getChildren():
                    logging.debug(
                        f"Job ID {self.id}: Adding Job ID {child.id} as a child blocker"
                    )
                    self.child_blockers.append(child.id)
        if (
            len(self.blockers) + len(self.child_blockers) == 0
            and self.status == orchestrator_pb2.JobStatus.JOB_STATUS_BLOCKED
        ):
            await self.processUnblocked()
            if self.hasChildren():
                logging.debug(
                    f"Job ID {self.id}: Unblocked with status complete with {len(self.children)} children"
                )
                self.status = orchestrator_pb2.JobStatus.JOB_STATUS_COMPLETE
            else:
                logging.debug(f"Job ID {self.id}: Unblocked with status queued")
                self.status = orchestrator_pb2.JobStatus.JOB_STATUS_QUEUED
            return True
        else:
            return False

    async def processBlockingChildOutputs(self, id, outputs):
        pass  # can be overridden

    async def processUnblocked(self):
        pass  # can be overriden

    def hasChildren(self):
        return len(self.children) > 0

    def getChildren(self):
        return self.children


class Mp4Job(Job):
    def __init__(
        self, priority, blockers, input_path, input_id, output_path, mute=False
    ):
        self.output_path = output_path
        self.child_counter = 0
        self.mute = mute
        if input_id is None:
            if self.mute:
                exec = ["mp4", "-m", input_path, self.output_path]
            else:
                exec = ["mp4", input_path, self.output_path]
        else:
            # not ready
            exec = ["mp4__uninitialized"]
        super(Mp4Job, self).__init__(
            priority, blockers, [input_id] if input_id is not None else [], exec
        )

    async def getOutputs(self, stdout):
        return [self.output_path]

    async def processBlockingChildOutputs(self, id, outputs):
        if outputs is not None:
            if len(outputs) > 0:
                for output in outputs:
                    logging.debug(
                        f"Job ID {self.id} spawning child #{self.child_counter}"
                    )
                    self.children.append(
                        Mp4Job(
                            self.priority,
                            self.blockers[:],
                            output,
                            None,
                            f"{self.output_path.replace('.mp4', '')}_{self.child_counter}.mp4",
                            self.mute,
                        )
                    )
                    self.child_counter += 1


class Mp4UniteJob(Job):
    def __init__(self, priority, blockers, input_paths, input_ids, output_path):
        self.input_paths = list(input_paths)
        self.output_path = output_path
        if len(input_ids) == 0:
            exec = ["mp4unite", *self.input_paths, self.output_path]
        else:
            # not ready
            exec = ["mp4unite__uninitialized"]
        super(Mp4UniteJob, self).__init__(priority, blockers, list(input_ids), exec)

    async def getOutputs(self, stdout):
        return [self.output_path]

    async def processBlockingChildOutputs(self, id, outputs):
        if outputs is not None:
            if len(outputs) > 0:
                self.input_paths += outputs
                self.exec = ["mp4unite", *self.input_paths, self.output_path]


class ScrapeJob(Job):
    def __init__(self, priority, blockers, url, xpath, output_path, file_ext):
        super(ScrapeJob, self).__init__(
            priority,
            blockers,
            [],
            [
                "scrape",
                "simple-link-scraper",
                "--xpath",
                xpath,
                "--ext",
                file_ext,
                "-o",
                output_path,
                url,
            ],
        )

    async def getOutputs(self, stdout):
        return re.findall(r"-> (\S+)\.\.\.", stdout)


class BookNotesSyncJob(Job):
    def __init__(self, priority, blockers, docs_id, wiki_id):
        super(BookNotesSyncJob, self).__init__(
            priority,
            blockers,
            [],
            [
                "book-notes-sync",
                "sync",
                "--docs-id",
                docs_id,
                "--page-id",
                wiki_id,
                "--enable-logging",
                "0"
            ],
        )


class ListJob(Job):
    def __init__(self, priority, blockers, path, file_ext):
        self.path = path
        self.file_ext = file_ext
        super(ListJob, self).__init__(priority, blockers, [], ["ls", path])

    async def getOutputs(self, stdout):
        if self.file_ext:
            files = re.findall(rf"(\S+\.{self.file_ext})", stdout)
        else:
            files = re.findall(r"(\S+)", stdout)
        return [f"{self.path}/{file}" for file in files]


class RemoveJob(Job):
    def __init__(self, priority, blockers, input_path, input_id):
        self.input_path = input_path
        if input_id is None and len(blockers) == 0:
            rmfilelist = glob.glob(self.input_path)
            exec = ["rm"] + rmfilelist
        else:
            exec = ["rm__uninitialized"]
        super(RemoveJob, self).__init__(
            priority, blockers, [input_id] if input_id is not None else [], exec
        )

    async def processUnblocked(self):
        if (
            self.exec == ["rm__uninitialized"]
            and len(self.child_blockers) == 0
            and len(self.children) == 0
        ):
            rmfilelist = glob.glob(self.input_path)
            self.exec = ["rm"] + rmfilelist

    async def processBlockingChildOutputs(self, id, outputs):
        if outputs is not None:
            if len(outputs) > 0:
                for output in outputs:
                    logging.debug(
                        f"Job ID {self.id} spawning child #{len(self.children)}"
                    )
                    self.children.append(
                        RemoveJob(self.priority, self.blockers[:], output, None)
                    )

class BashJob(Job):
    def __init__(self, priority, blockers, bash_command):
        super(BashJob, self).__init__(priority, blockers, [], bash_command.split())

def jobFromProto(proto):
    if proto.WhichOneof("job") == "mp4":
        if proto.mp4.WhichOneof("input") == "input_path":
            return (
                Mp4Job(
                    proto.priority,
                    list(proto.blocking_job_ids),
                    proto.mp4.input_path,
                    None,
                    proto.mp4.output_path,
                    proto.mp4.mute,
                ),
                "",
            )
        elif proto.mp4.WhichOneof("input") == "job_id_input":
            return (
                Mp4Job(
                    proto.priority,
                    list(proto.blocking_job_ids),
                    None,
                    proto.mp4.job_id_input,
                    proto.mp4.output_path,
                    proto.mp4.mute,
                ),
                "",
            )
        else:
            return None, "Input not specified"
    elif proto.WhichOneof("job") == "mp4_unite":
        if len(proto.mp4_unite.job_id_inputs) + len(proto.mp4_unite.input_paths) == 0:
            return None, "No inputs specified"
        return (
            Mp4UniteJob(
                proto.priority,
                list(proto.blocking_job_ids),
                list(proto.mp4_unite.input_paths),
                list(proto.mp4_unite.job_id_inputs),
                proto.mp4_unite.output_path,
            ),
            "",
        )
    elif proto.WhichOneof("job") == "scrape":
        return (
            ScrapeJob(
                proto.priority,
                list(proto.blocking_job_ids),
                proto.scrape.url,
                proto.scrape.xpath,
                proto.scrape.output_path,
                proto.scrape.file_extension,
            ),
            "",
        )
    elif proto.WhichOneof("job") == "list":
        return (
            ListJob(
                proto.priority,
                list(proto.blocking_job_ids),
                proto.list.path,
                proto.list.file_extension,
            ),
            "",
        )
    elif proto.WhichOneof("job") == "remove":
        if proto.remove.WhichOneof("input") == "input_path":
            return (
                RemoveJob(
                    proto.priority,
                    list(proto.blocking_job_ids),
                    proto.remove.input_path,
                    None,
                ),
                "",
            )
        elif proto.remove.WhichOneof("input") == "job_id_input":
            return (
                RemoveJob(
                    proto.priority,
                    list(proto.blocking_job_ids),
                    None,
                    proto.remove.job_id_input,
                ),
                "",
            )
        else:
            return None, "Input not specified"
    elif proto.WhichOneof("job") == "bash":
        return (
            BashJob(
                proto.priority,
                list(proto.blocking_job_ids),
                proto.bash.bash_command,
            ),
            "",
        )
    elif proto.WhichOneof("job") == "book_notes_sync":
        return (
            BookNotesSyncJob(
                proto.priority,
                list(proto.blocking_job_ids),
                proto.book_notes_sync.docs_id,
                proto.book_notes_sync.wiki_page_id,
            ),
            "",
        )
    else:
        return None, f"Unsupported job type: {proto.Whichoneof('job')}"
