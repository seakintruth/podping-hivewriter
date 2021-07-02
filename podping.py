import asyncio
import logging

from podping_hivewriter import config, run

"""Stand alone command line front end for writing podpings"""


async def run_podping_hive():
    """Main function running the loop for Podping HiveWriter"""
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


if __name__ == "__main__":
    if config.Config.url:
        run.run()
    else:
        loop = asyncio.get_event_loop()
        main_task = loop.create_task(run_podping_hive())
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            raise
        except asyncio.CancelledError:
            raise
        finally:
            loop.close()
