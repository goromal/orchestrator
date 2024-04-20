import click
from colorama import Fore, Style
import asyncio
from grpc import aio

from aapis.orchestrator.v1 import orchestrator_pb2_grpc, orchestrator_pb2


DEFAULT_INSECURE_PORT = 40040


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
@click.argument("target")
def status(ctx: click.Context, target):
    """Get the status of orchestrated jobs (TARGET ∈ [all, [id], get-complete / gc, count-complete / cc, get-pending / gp, count-pending / cp, get-discarded / gd, count-discarded / cd])"""

    async def status_impl(id):
        async with aio.insecure_channel(
            f"localhost:{ctx.obj['insecure_port']}"
        ) as channel:
            stub = orchestrator_pb2_grpc.OrchestratorServiceStub(channel)
            try:
                response = await stub.JobStatus(
                    orchestrator_pb2.JobStatusRequest(job_id=id)
                )
            except:
                print(
                    Fore.RED
                    + f"orchestratord either is not running or is not listening on port {ctx.obj['insecure_port']}"
                    + Style.RESET_ALL
                )
                exit()
        detail_view = False
        if response.status == orchestrator_pb2.JOB_STATUS_ACTIVE:
            detail_view = True
            print(Fore.GREEN + "ACTIVE" + Style.RESET_ALL)
        elif response.status == orchestrator_pb2.JOB_STATUS_PAUSED:
            detail_view = True
            print(Fore.YELLOW + "PAUSED" + Style.RESET_ALL)
        elif response.status == orchestrator_pb2.JOB_STATUS_QUEUED:
            detail_view = True
            print(Fore.YELLOW + "QUEUED" + Style.RESET_ALL)
        elif response.status == orchestrator_pb2.JOB_STATUS_BLOCKED:
            detail_view = True
            print(Fore.YELLOW + "BLOCKED" + Style.RESET_ALL)
        elif response.status == orchestrator_pb2.JOB_STATUS_COMPLETE:
            detail_view = True
            print(Fore.GREEN + "COMPLETE" + Style.RESET_ALL)
        elif response.status == orchestrator_pb2.JOB_STATUS_ERROR:
            detail_view = True
            print(Fore.RED + "ERROR" + Style.RESET_ALL + f": {response.message}")
        elif response.status == orchestrator_pb2.JOB_STATUS_CANCELED:
            print(Fore.RED + "CANCELED" + Style.RESET_ALL)
        elif response.status == orchestrator_pb2.JOB_STATUS_INVALID:
            print(Fore.RED + "INVALID" + Style.RESET_ALL + f": {response.message}")
        if detail_view:
            print(Fore.CYAN + "  Command:  " + Style.RESET_ALL + response.exec)
            print(Fore.CYAN + "  Priority: " + Style.RESET_ALL + str(response.priority))
            print(Fore.CYAN + "  Blockers: " + Style.RESET_ALL + str(response.blockers))
            print(
                Fore.CYAN
                + "  Children: "
                + Style.RESET_ALL
                + str(response.spawned_children)
            )
            print(Fore.CYAN + "  Message:  " + Style.RESET_ALL + response.message)
            print(
                Fore.CYAN
                + "  Duration: "
                + Style.RESET_ALL
                + f"{response.exec_duration_secs:.2f}s"
            )
            print(Fore.CYAN + "  Output:  " + Style.RESET_ALL)
            print(response.program_output)
            print(Fore.CYAN + "  Outputs: " + Style.RESET_ALL)
            for output in response.outputs:
                print(Fore.CYAN + "    -> " + Style.RESET_ALL + output)

    async def status_all_impl():
        async with aio.insecure_channel(
            f"localhost:{ctx.obj['insecure_port']}"
        ) as channel:
            stub = orchestrator_pb2_grpc.OrchestratorServiceStub(channel)
            try:
                response = await stub.JobsSummaryStatus(
                    orchestrator_pb2.JobsSummaryStatusRequest()
                )
            except:
                print(
                    Fore.RED
                    + f"orchestratord either is not running or is not listening on port {ctx.obj['insecure_port']}"
                    + Style.RESET_ALL
                )
                exit()
        print("JOB STATUSES")
        print(
            Fore.GREEN
            + "  Completed"
            + Style.RESET_ALL
            + f":\t{response.num_completed_jobs}"
        )
        if response.num_completed_jobs > 0:
            print(
                "    [" + " ".join([str(job) for job in response.completed_jobs]) + "]"
            )
        print(f"  Active:\t{response.num_active_jobs}")
        if response.num_active_jobs > 0:
            print("    [" + " ".join([str(job) for job in response.active_jobs]) + "]")
        print(
            Fore.YELLOW
            + "  Queued"
            + Style.RESET_ALL
            + f":\t{response.num_queued_jobs}"
        )
        if response.num_queued_jobs > 0:
            print("    [" + " ".join([str(job) for job in response.queued_jobs]) + "]")
        print(
            Fore.YELLOW
            + "  Blocked"
            + Style.RESET_ALL
            + f":\t{response.num_blocked_jobs}"
        )
        if response.num_blocked_jobs > 0:
            print("    [" + " ".join([str(job) for job in response.blocked_jobs]) + "]")
        print(
            Fore.YELLOW
            + "  Paused"
            + Style.RESET_ALL
            + f":\t{response.num_paused_jobs}"
        )
        if response.num_paused_jobs > 0:
            print("    [" + " ".join([str(job) for job in response.paused_jobs]) + "]")
        print(
            Fore.RED
            + "  Discarded"
            + Style.RESET_ALL
            + f":\t{response.num_discarded_jobs}"
        )
        if response.num_discarded_jobs > 0:
            print(
                "    [" + " ".join([str(job) for job in response.discarded_jobs]) + "]"
            )

    async def complete_impl(count=True):
        async with aio.insecure_channel(
            f"localhost:{ctx.obj['insecure_port']}"
        ) as channel:
            stub = orchestrator_pb2_grpc.OrchestratorServiceStub(channel)
            try:
                response = await stub.JobsSummaryStatus(
                    orchestrator_pb2.JobsSummaryStatusRequest()
                )
            except:
                print(
                    Fore.RED
                    + f"orchestratord either is not running or is not listening on port {ctx.obj['insecure_port']}"
                    + Style.RESET_ALL
                )
                exit()
        if count:
            print(response.num_completed_jobs)
        else:
            print(" ".join(response.completed_jobs))

    async def pending_impl(count=True):
        async with aio.insecure_channel(
            f"localhost:{ctx.obj['insecure_port']}"
        ) as channel:
            stub = orchestrator_pb2_grpc.OrchestratorServiceStub(channel)
            try:
                response = await stub.JobsSummaryStatus(
                    orchestrator_pb2.JobsSummaryStatusRequest()
                )
            except:
                print(
                    Fore.RED
                    + f"orchestratord either is not running or is not listening on port {ctx.obj['insecure_port']}"
                    + Style.RESET_ALL
                )
                exit()
        if count:
            print(
                response.num_active_jobs
                + response.num_queued_jobs
                + response.num_blocked_jobs
                + response.num_paused_jobs
            )
        else:
            print(
                " ".join(
                    response.active_jobs
                    + response.queued_jobs
                    + response.blocked_jobs
                    + response.paused_jobs
                )
            )

    async def discarded_impl(count=True):
        async with aio.insecure_channel(
            f"localhost:{ctx.obj['insecure_port']}"
        ) as channel:
            stub = orchestrator_pb2_grpc.OrchestratorServiceStub(channel)
            try:
                response = await stub.JobsSummaryStatus(
                    orchestrator_pb2.JobsSummaryStatusRequest()
                )
            except:
                print(
                    Fore.RED
                    + f"orchestratord either is not running or is not listening on port {ctx.obj['insecure_port']}"
                    + Style.RESET_ALL
                )
                exit()
        if count:
            print(response.num_discarded_jobs)
        else:
            print(" ".join(response.discarded_jobs))

    if target.isnumeric():
        asyncio.run(status_impl(int(target)))
    elif target == "all":
        asyncio.run(status_all_impl())
    elif target == "get-complete" or target == "gc":
        asyncio.run(complete_impl(count=False))
    elif target == "count-complete" or target == "cc":
        asyncio.run(complete_impl(count=True))
    elif target == "get-pending" or target == "gp":
        asyncio.run(pending_impl(count=False))
    elif target == "count-pending" or target == "cp":
        asyncio.run(pending_impl(True))
    elif target == "get-discarded" or target == "gd":
        asyncio.run(discarded_impl(False))
    elif target == "count-discarded" or target == "cd":
        asyncio.run(discarded_impl(True))
    else:
        print(
            Fore.RED
            + "ERROR"
            + Style.RESET_ALL
            + f": undefined status target ({target})"
        )


@cli.command()
@click.pass_context
@click.argument("job_id")
def cancel(ctx: click.Context, job_id):
    """Cancel an active, queued, or blocked job"""

    async def cmd_impl(job_id):
        async with aio.insecure_channel(
            f"localhost:{ctx.obj['insecure_port']}"
        ) as channel:
            stub = orchestrator_pb2_grpc.OrchestratorServiceStub(channel)
            try:
                response = await stub.CancelJob(
                    orchestrator_pb2.CancelJobRequest(job_id=job_id)
                )
            except:
                print(
                    Fore.RED
                    + f"orchestratord either is not running or is not listening on port {ctx.obj['insecure_port']}"
                    + Style.RESET_ALL
                )
                exit()
        if response.success:
            print(Fore.GREEN + f"Success: job {job_id} canceled" + Style.RESET_ALL)
        else:
            print(
                Fore.RED
                + f"Commanded failed with message: {response.message}"
                + Style.RESET_ALL
            )

    try:
        job_id = int(job_id)
    except:
        print(Fore.RED + f"Mal-formed id: {job_id}" + Style.RESET_ALL)
        exit()
    asyncio.run(cmd_impl(job_id))


@cli.command()
@click.pass_context
@click.argument("input")
@click.argument("output")
@click.option(
    "--mute",
    "mute",
    type=bool,
    default=False,
    show_default=True,
    help="Whether to mute the output",
)
@click.option("-b", "--blocker", type=int, multiple=True, help="Job ID(s) to block on")
@click.option(
    "--priority",
    "priority",
    type=int,
    default=0,
    show_default=True,
    help="Priority level for the job",
)
def mp4(ctx: click.Context, input, output, mute, blocker, priority):
    """Kickoff an mp4 job"""

    async def cmd_impl(inp, out, pri, blk):
        async with aio.insecure_channel(
            f"localhost:{ctx.obj['insecure_port']}"
        ) as channel:
            stub = orchestrator_pb2_grpc.OrchestratorServiceStub(channel)
            if inp.isnumeric():
                try:
                    response = await stub.KickoffJob(
                        orchestrator_pb2.KickoffJobRequest(
                            priority=pri,
                            blocking_job_ids=blk,
                            mp4=orchestrator_pb2.Mp4Job(
                                job_id_input=int(inp), output_path=out, mute=mute
                            ),
                        )
                    )
                except:
                    print(
                        Fore.RED
                        + f"orchestratord either is not running or is not listening on port {ctx.obj['insecure_port']}"
                        + Style.RESET_ALL
                    )
                    exit()
            else:
                try:
                    response = await stub.KickoffJob(
                        orchestrator_pb2.KickoffJobRequest(
                            priority=pri,
                            blocking_job_ids=blk,
                            mp4=orchestrator_pb2.Mp4Job(
                                input_path=inp, output_path=out, mute=mute
                            ),
                        )
                    )
                except:
                    print(
                        Fore.RED
                        + f"orchestratord either is not running or is not listening on port {ctx.obj['insecure_port']}"
                        + Style.RESET_ALL
                    )
                    exit()
        if response.success:
            print(response.job_id)
        else:
            print(Fore.RED + "Failed" + Style.RESET_ALL + f": {response.message}")

    asyncio.run(cmd_impl(input, output, priority, list(blocker)))


@cli.command()
@click.pass_context
@click.argument("input", nargs=-1)
@click.argument("output")
@click.option("-b", "--blocker", type=int, multiple=True, help="Job ID(s) to block on")
@click.option(
    "--priority",
    "priority",
    type=int,
    default=0,
    show_default=True,
    help="Priority level for the job",
)
def mp4_unite(ctx: click.Context, input, output, blocker, priority):
    """Kickoff an mp4 unite job"""

    async def cmd_impl(inp, out, pri, blk):
        async with aio.insecure_channel(
            f"localhost:{ctx.obj['insecure_port']}"
        ) as channel:
            stub = orchestrator_pb2_grpc.OrchestratorServiceStub(channel)
            input_paths = []
            input_job_ids = []
            for cmd_inp in inp:
                if cmd_inp.isnumeric():
                    input_job_ids.append(int(cmd_inp))
                else:
                    input_paths.append(cmd_inp)
            try:
                response = await stub.KickoffJob(
                    orchestrator_pb2.KickoffJobRequest(
                        priority=pri,
                        blocking_job_ids=blk,
                        mp4_unite=orchestrator_pb2.Mp4UniteJob(
                            input_paths=input_paths,
                            job_id_inputs=input_job_ids,
                            output_path=out,
                        ),
                    )
                )
            except:
                print(
                    Fore.RED
                    + f"orchestratord either is not running or is not listening on port {ctx.obj['insecure_port']}"
                    + Style.RESET_ALL
                )
                exit()
        if response.success:
            print(response.job_id)
        else:
            print(Fore.RED + "Failed" + Style.RESET_ALL + f": {response.message}")

    asyncio.run(cmd_impl(input, output, priority, list(blocker)))


@cli.command()
@click.pass_context
@click.argument("url")
@click.argument("xpath")
@click.argument("ext")
@click.argument("output")
@click.option("-b", "--blocker", type=int, multiple=True, help="Job ID(s) to block on")
@click.option(
    "--priority",
    "priority",
    type=int,
    default=0,
    show_default=True,
    help="Priority level for the job",
)
def scrape(ctx: click.Context, url, xpath, ext, output, blocker, priority):
    """Kickoff a scrape job"""

    async def cmd_impl(url, xpath, ext, output, pri, blk):
        async with aio.insecure_channel(
            f"localhost:{ctx.obj['insecure_port']}"
        ) as channel:
            stub = orchestrator_pb2_grpc.OrchestratorServiceStub(channel)
            try:
                response = await stub.KickoffJob(
                    orchestrator_pb2.KickoffJobRequest(
                        priority=pri,
                        blocking_job_ids=blk,
                        scrape=orchestrator_pb2.ScrapeJob(
                            url=url, xpath=xpath, output_path=output, file_extension=ext
                        ),
                    )
                )
            except:
                print(
                    Fore.RED
                    + f"orchestratord either is not running or is not listening on port {ctx.obj['insecure_port']}"
                    + Style.RESET_ALL
                )
                exit()
        if response.success:
            print(response.job_id)
        else:
            print(Fore.RED + "Failed" + Style.RESET_ALL + f": {response.message}")

    asyncio.run(cmd_impl(url, xpath, ext, output, priority, list(blocker)))


@cli.command()
@click.pass_context
@click.argument("path")
@click.option("--ext", type=str, help="File extension to filter the listing")
@click.option("-b", "--blocker", type=int, multiple=True, help="Job ID(s) to block on")
@click.option(
    "--priority",
    "priority",
    type=int,
    default=0,
    show_default=True,
    help="Priority level for the job",
)
def listing(ctx: click.Context, path, ext, blocker, priority):
    """Kickoff a listing job"""

    async def cmd_impl(path, ext, pri, blk):
        async with aio.insecure_channel(
            f"localhost:{ctx.obj['insecure_port']}"
        ) as channel:
            stub = orchestrator_pb2_grpc.OrchestratorServiceStub(channel)
            try:
                response = await stub.KickoffJob(
                    orchestrator_pb2.KickoffJobRequest(
                        priority=pri,
                        blocking_job_ids=blk,
                        list=orchestrator_pb2.ListJob(path=path, file_extension=ext),
                    )
                )
            except:
                print(
                    Fore.RED
                    + f"orchestratord either is not running or is not listening on port {ctx.obj['insecure_port']}"
                    + Style.RESET_ALL
                )
                exit()
        if response.success:
            print(response.job_id)
        else:
            print(Fore.RED + "Failed" + Style.RESET_ALL + f": {response.message}")

    asyncio.run(cmd_impl(path, ext, priority, list(blocker)))


@cli.command()
@click.pass_context
@click.argument("input")
@click.option("-b", "--blocker", type=int, multiple=True, help="Job ID(s) to block on")
@click.option(
    "--priority",
    "priority",
    type=int,
    default=0,
    show_default=True,
    help="Priority level for the job",
)
def remove(ctx: click.Context, input, blocker, priority):
    """Kickoff a removal job"""

    async def cmd_impl(inp, pri, blk):
        async with aio.insecure_channel(
            f"localhost:{ctx.obj['insecure_port']}"
        ) as channel:
            stub = orchestrator_pb2_grpc.OrchestratorServiceStub(channel)
            if inp.isnumeric():
                try:
                    response = await stub.KickoffJob(
                        orchestrator_pb2.KickoffJobRequest(
                            priority=pri,
                            blocking_job_ids=blk,
                            remove=orchestrator_pb2.RemoveJob(job_id_input=int(inp)),
                        )
                    )
                except:
                    print(
                        Fore.RED
                        + f"orchestratord either is not running or is not listening on port {ctx.obj['insecure_port']}"
                        + Style.RESET_ALL
                    )
                    exit()
            else:
                try:
                    response = await stub.KickoffJob(
                        orchestrator_pb2.KickoffJobRequest(
                            priority=pri,
                            blocking_job_ids=blk,
                            remove=orchestrator_pb2.RemoveJob(input_path=inp),
                        )
                    )
                except:
                    print(
                        Fore.RED
                        + f"orchestratord either is not running or is not listening on port {ctx.obj['insecure_port']}"
                        + Style.RESET_ALL
                    )
                    exit()
        if response.success:
            print(response.job_id)
        else:
            print(Fore.RED + "Failed" + Style.RESET_ALL + f": {response.message}")

    asyncio.run(cmd_impl(input, priority, list(blocker)))


@cli.command()
@click.pass_context
@click.argument("command")
@click.option("-b", "--blocker", type=int, multiple=True, help="Job ID(s) to block on")
@click.option(
    "--priority",
    "priority",
    type=int,
    default=0,
    show_default=True,
    help="Priority level for the job",
)
def bash(ctx: click.Context, command, blocker, priority):
    """Kickoff a bash job"""

    async def cmd_impl(command, pri, blk):
        async with aio.insecure_channel(
            f"localhost:{ctx.obj['insecure_port']}"
        ) as channel:
            stub = orchestrator_pb2_grpc.OrchestratorServiceStub(channel)
            try:
                response = await stub.KickoffJob(
                    orchestrator_pb2.KickoffJobRequest(
                        priority=pri,
                        blocking_job_ids=blk,
                        bash=orchestrator_pb2.BashJob(bash_command=command),
                    )
                )
            except:
                print(
                    Fore.RED
                    + f"orchestratord either is not running or is not listening on port {ctx.obj['insecure_port']}"
                    + Style.RESET_ALL
                )
                exit()
        if response.success:
            print(response.job_id)
        else:
            print(Fore.RED + "Failed" + Style.RESET_ALL + f": {response.message}")

    asyncio.run(cmd_impl(command, priority, list(blocker)))


@cli.command()
@click.pass_context
@click.argument("cloud_dir")
@click.option("-b", "--blocker", type=int, multiple=True, help="Job ID(s) to block on")
@click.option(
    "--priority",
    "priority",
    type=int,
    default=0,
    show_default=True,
    help="Priority level for the job",
)
def sync(ctx: click.Context, cloud_dir, blocker, priority):
    """Kickoff a sync job"""

    async def cmd_impl(cloud_dir, pri, blk):
        async with aio.insecure_channel(
            f"localhost:{ctx.obj['insecure_port']}"
        ) as channel:
            stub = orchestrator_pb2_grpc.OrchestratorServiceStub(channel)
            try:
                response = await stub.KickoffJob(
                    orchestrator_pb2.KickoffJobRequest(
                        priority=pri,
                        blocking_job_ids=blk,
                        sync=orchestrator_pb2.SyncJob(cloud_dir=cloud_dir),
                    )
                )
            except:
                print(
                    Fore.RED
                    + f"orchestratord either is not running or is not listening on port {ctx.obj['insecure_port']}"
                    + Style.RESET_ALL
                )
                exit()
        if response.success:
            print(response.job_id)
        else:
            print(Fore.RED + "Failed" + Style.RESET_ALL + f": {response.message}")

    asyncio.run(cmd_impl(cloud_dir, priority, list(blocker)))


def main():
    cli()


if __name__ == "__main__":
    main()
