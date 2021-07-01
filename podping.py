import asyncio
import logging

from podping_hivewriter import config, run
from privex.steem import SteemAsync
import cProfile
import pstats


async def run_podping_hive():
    podping_hivewriter, _ = run.run()
    await podping_hivewriter.wait_startup()
    if podping_hivewriter._startup_done:
        while True:
            try:
                await asyncio.sleep(10)
            except KeyboardInterrupt:
                raise
    else:
        logging.error("Startup did not complete")
        loop.close()


async def process_block(b):
    print(f"doing stuff to block: {b!r}")


async def watch_hive_chain():

    l = logging.getLogger("privex.steem")
    l.setLevel(logging.DEBUG)

    hive = SteemAsync(network="hive")

    accounts = await hive.get_accounts("brianoflondon", "podping")
    print(accounts)
    props = await hive.get_props()
    headblock = int(props["head_block_number"])
    start_block = headblock - 5
    end_block = headblock

    blocks = await hive.get_blocks(start_block, end_block)
    loop = asyncio.get_running_loop()
    print("Queuing process_block asyncio tasks...")
    tasks = [loop.create_task(process_block(b)) for b in blocks]
    print("Waiting for blocks to finish processing")
    for t in tasks:
        await t
    print("Finished processing %d blocks", end_block - start_block)


if __name__ == "__main__":
    # with cProfile.Profile() as pr:

    if config.Config.url:
        run.run()
    else:
        loop = asyncio.get_event_loop()
        main_task = loop.create_task(watch_hive_chain())

        # main_task = loop.create_task(run_podping_hive())
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            raise
        except asyncio.CancelledError:
            raise
        finally:
            loop.close()
            # stats = pstats.Stats(pr)
            # stats.sort_stats(pstats.SortKey.TIME)
            # stats.print_stats()
            # stats.dump_stats(filename="/tmp/stats.prof")
