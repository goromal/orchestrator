import click
from colorama import Fore, Style
import asyncio
from grpc import aio

from aapis.orchestrator.v1 import (
    orchestrator_pb2_grpc,
    orchestrator_pb2
)

DEFAULT_INSECURE_PORT = 40040

class NotRequiredIf(click.Option):
    def __init__(self, *args, **kwargs):
        self.not_required_if = kwargs.pop('not_required_if')
        assert self.not_required_if, "'not_required_if' parameter required"
        kwargs['help'] = (kwargs.get('help', '') +
            ' NOTE: This argument is mutually exclusive with %s' %
            self.not_required_if
        ).strip()
        super(NotRequiredIf, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        we_are_present = self.name in opts
        other_present = self.not_required_if in opts

        if other_present:
            if we_are_present:
                raise click.UsageError(
                    "Illegal usage: `%s` is mutually exclusive with `%s`" % (
                        self.name, self.not_required_if))
            else:
                self.prompt = None

        return super(NotRequiredIf, self).handle_parse_result(
            ctx, opts, args)

@click.group()
@click.pass_context
@click.option(
    "-p",
    "--port",
    type=int,
    default=DEFAULT_INSECURE_PORT,
)
def cli(ctx: click.Context, port):
    """Orchestrate background jobs."""
    ctx.obj = {"insecure_port": port}

@cli.command()
@click.pass_context
@click.option(
    "--id",
    "id",
    type=int,
    default=None,
    help="Job ID"
)
@click.option(
    "--all",
    "all",
    is_flag=True,
    cls=NotRequiredIf,
    not_required_if="id",
    help="Get status for all jobs"
)
def status(ctx: click.Context, id, all):
    """Get the status of orchestrated jobs"""
    async def status_impl(id):
        async with aio.insecure_channel(f"localhost:{ctx.obj['insecure_port']}") as channel:
            stub = orchestrator_pb2_grpc.OrchestratorServiceStub(channel)
            response = await stub.JobStatus(orchestrator_pb2.JobStatusRequest(job_id=id))
        if response.status == orchestrator_pb2.JOB_STATUS_PAUSED:
            print(Fore.YELLOW + "PAUSED" + Style.RESET_ALL)
        elif response.status == orchestrator_pb2.JOB_STATUS_QUEUED:
            print(Fore.YELLOW + "QUEUED" + Style.RESET_ALL)
        elif response.status == orchestrator_pb2.JOB_STATUS_BLOCKED:
            print(Fore.YELLOW + "BLOCKED" + Style.RESET_ALL)
        elif response.status == orchestrator_pb2.JOB_STATUS_COMPLETE:
            print(Fore.GREEN + "COMPLETE" + Style.RESET_ALL)
        elif response.status == orchestrator_pb2.JOB_STATUS_ERROR:
            print(Fore.RED + "ERROR" + Style.RESET_ALL)
        elif response.status == orchestrator_pb2.JOB_STATUS_CANCELED:
            print(Fore.RED + "CANCELED" + Style.RESET_ALL)
        elif response.status == orchestrator_pb2.JOB_STATUS_INVALID:
            print(Fore.RED + "INVALID" + Style.RESET_ALL)
        if response.message:
            print(response.message)
    async def status_all_impl():
        async with aio.insecure_channel(f"localhost:{ctx.obj['insecure_port']}") as channel:
            stub = orchestrator_pb2_grpc.OrchestratorServiceStub(channel)
            response = await stub.JobsSummaryStatus(orchestrator_pb2.JobsSummaryStatusRequest())
        print("JOB STATUSES")
        print(Fore.GREEN + "  Completed" + Style.RESET_ALL + f":\t{response.num_completed_jobs}")
        if response.num_completed_jobs > 0:
            print("    [" +  " ".join([str(job) for job in response.completed_jobs]) + "]")
        print(f"  Active:\t{response.num_active_jobs}")
        if response.num_active_jobs > 0:
            print("    [" +  " ".join([str(job) for job in response.active_jobs]) + "]")
        print(Fore.YELLOW + "  Queued" + Style.RESET_ALL + f":\t{response.num_queued_jobs}")
        if response.num_queued_jobs > 0:
            print("    [" + " ".join([str(job) for job in response.queued_jobs]) + "]")
        print(Fore.YELLOW + "  Blocked" + Style.RESET_ALL + f":\t{response.num_blocked_jobs}")
        if response.num_blocked_jobs > 0:
            print("    [" +  " ".join([str(job) for job in response.blocked_jobs]) + "]")
        print(Fore.YELLOW + "  Paused" + Style.RESET_ALL + f":\t{response.num_paused_jobs}")
        if response.num_paused_jobs > 0:
            print("    [" +  " ".join([str(job) for job in response.paused_jobs]) + "]")
        print(Fore.RED + "  Discarded" + Style.RESET_ALL + f":\t{response.num_discarded_jobs}")
        if response.num_discarded_jobs > 0:
            print("    [" +  " ".join([str(job) for job in response.discarded_jobs]) + "]")
    if id is not None:
        asyncio.run(status_impl(id))
    else:
        asyncio.run(status_all_impl())

@cli.command()
@click.pass_context
@click.argument(
    "input"
)
@click.argument(
    "output"
)
@click.option(
    "-b",
    "--blocker",
    type=int,
    multiple=True,
    help="Job ID(s) to block on"
)
@click.option(
    "--priority",
    "priority",
    type=int,
    default=0,
    show_default=True,
    help="Priority level for the job"
)
def mp4(ctx: click.Context, input, output, blocker, priority):
    """Kickoff an mp4 job"""
    async def cmd_impl(inp, out, pri, blk):
        async with aio.insecure_channel(f"localhost:{ctx.obj['insecure_port']}") as channel:
            stub = orchestrator_pb2_grpc.OrchestratorServiceStub(channel)
            if inp.isnumeric():
                response = await stub.KickoffJob(orchestrator_pb2.KickoffJobRequest(
                    priority=pri,
                    blocking_job_ids=blk + [int(inp)],
                    mp4=orchestrator_pb2.Mp4Job(
                        job_id_input=int(inp),
                        output_path=out
                    )
                ))
            else:
                response = await stub.KickoffJob(orchestrator_pb2.KickoffJobRequest(
                    priority=pri,
                    blocking_job_ids=blk,
                    mp4=orchestrator_pb2.Mp4Job(
                        input_path=inp,
                        output_path=out
                    )
                ))
        if response.success:
            print(Fore.GREEN + "Job ID" + Style.RESET_ALL + f": {response.job_id}")
        else:
            print(Fore.RED + "Failed" + Style.RESET_ALL + f": {response.message}")
    asyncio.run(cmd_impl(input, output, priority, blocker))

@cli.command()
@click.pass_context
@click.argument(
    "input",
    nargs=-1
)
@click.argument(
    "output"
)
@click.option(
    "-b",
    "--blocker",
    type=int,
    multiple=True,
    help="Job ID(s) to block on"
)
@click.option(
    "--priority",
    "priority",
    type=int,
    default=0,
    show_default=True,
    help="Priority level for the job"
)
def mp4_unite(ctx: click.Context, input, output, blocker, priority):
    """Kickoff an mp4 unite job"""
    async def cmd_impl(inp, out, pri, blk):
        async with aio.insecure_channel(f"localhost:{ctx.obj['insecure_port']}") as channel:
            stub = orchestrator_pb2_grpc.OrchestratorServiceStub(channel)
            input_paths = []
            input_job_ids = []
            for cmd_inp in inp:
                if cmd_inp.isnumeric():
                    input_job_ids.append(int(cmd_inp))
                else:
                    input_paths.append(cmd_inp)
            response = await stub.KickoffJob(orchestrator_pb2.KickoffJobRequest(
                priority=pri,
                blocking_job_ids=blk + input_job_ids,
                mp4_unite=orchestrator_pb2.Mp4UniteJob(
                    input_paths=input_paths,
                    job_id_inputs=input_job_ids,
                    output_path=out
                )
            ))
        if response.success:
            print(Fore.GREEN + "Job ID" + Style.RESET_ALL + f": {response.job_id}")
        else:
            print(Fore.RED + "Failed" + Style.RESET_ALL + f": {response.message}")
    asyncio.run(cmd_impl(input, output, priority, blocker))

@cli.command()
@click.pass_context
@click.argument(
    "url"
)
@click.argument(
    "xpath"
)
@click.argument(
    "ext"
)
@click.argument(
    "output"
)
@click.option(
    "-b",
    "--blocker",
    type=int,
    multiple=True,
    help="Job ID(s) to block on"
)
@click.option(
    "--priority",
    "priority",
    type=int,
    default=0,
    show_default=True,
    help="Priority level for the job"
)
def scrape(ctx: click.Context, url, xpath, ext, output, blocker, priority):
    """Kickoff a scrape job"""
    async def cmd_impl(url, xpath, ext, output, pri, blk):
        async with aio.insecure_channel(f"localhost:{ctx.obj['insecure_port']}") as channel:
            stub = orchestrator_pb2_grpc.OrchestratorServiceStub(channel)
            response = await stub.KickoffJob(orchestrator_pb2.KickoffJobRequest(
                priority=pri,
                blocking_job_ids=blk,
                scrape=orchestrator_pb2.ScrapeJob(
                    url=url,
                    xpath=xpath,
                    output_path=output,
                    file_extension=ext
                )
            ))
        if response.success:
            print(Fore.GREEN + "Job ID" + Style.RESET_ALL + f": {response.job_id}")
        else:
            print(Fore.RED + "Failed" + Style.RESET_ALL + f": {response.message}")
    asyncio.run(cmd_impl(url, xpath, ext, output, priority, blocker))

def main():
    cli()

if __name__ == "__main__":
    main()
