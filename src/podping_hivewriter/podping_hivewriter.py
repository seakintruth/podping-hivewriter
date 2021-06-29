import asyncio
import sys
from datetime import datetime, timezone
import json
import logging
from sys import exc_info, getsizeof
from timeit import default_timer as timer
from typing import List, Set, Tuple, Iterable, Optional
import uuid

import beem
import rfc3987
import zmq
import zmq.asyncio
from beem.account import Account
from beem.exceptions import AccountDoesNotExistsException, MissingKeyError
from beemapi.exceptions import UnhandledRPCError
from beem.nodelist import NodeList
from podping_hivewriter.exceptions import PodpingCustomJsonPayloadExceeded
from podping_hivewriter.hive_wrapper import HiveWrapper
from podping_hivewriter.models.iri_batch import IRIBatch

from pydantic import ValidationError

from podping_hivewriter.config import Config
from podping_hivewriter.models.podping_settings import PodpingSettings
from podping_hivewriter.constants import (
    STARTUP_OPERATION_ID,
    STARTUP_FAILED_UNKNOWN_EXIT_CODE,
    STARTUP_FAILED_INVALID_POSTING_KEY_EXIT_CODE,
    STARTUP_FAILED_HIVE_API_ERROR_EXIT_CODE,
)
from podping_hivewriter.podping_config import (
    get_podping_settings,
    get_time_sorted_node_list,
)


def utc_date_str() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()


def size_of_dict_as_json(payload: dict):
    return len(json.dumps(payload, separators=(",", ":")).encode("UTF-8"))


class PodpingHivewriter:
    def __init__(
        self,
        server_account: str,
        posting_key: str,
        nodes: Iterable[str],
        operation_id="podping",
        resource_test=True,
        daemon=True,
        use_testnet=False,
    ):
        self._tasks: List[asyncio.Task] = []

        self.server_account: str = server_account
        self.required_posting_auths = [self.server_account]
        self.posting_key: str = posting_key
        self.operation_id: str = operation_id
        self.daemon = daemon
        self.use_testnet = use_testnet

        self.hive_wrapper = HiveWrapper(
            nodes, posting_key, daemon=daemon, use_testnet=use_testnet
        )

        self.total_iris_recv = 0
        self.total_iris_sent = 0
        self.total_iris_recv_deduped = 0

        self.iri_batch_queue: "asyncio.Queue[IRIBatch]" = asyncio.Queue()
        self.iri_queue: "asyncio.Queue[str]" = asyncio.Queue()

        self._startup_done = False
        asyncio.ensure_future(self._startup(resource_test=resource_test))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        try:
            for task in self._tasks:
                task.cancel()
        except RuntimeError:
            pass

    def _add_task(self, task):
        self._tasks.append(task)

    async def _startup(self, resource_test=True):
        logging.info(
            "Podping startup sequence initiated, please stand by, "
            "full bozo checks in operation..."
        )

        try:
            hive = await self.hive_wrapper.get_hive()
            account = Account(self.server_account, blockchain_instance=hive, lazy=True)
            allowed = get_allowed_accounts()
            if self.server_account not in allowed:
                logging.error(
                    f"Account @{self.server_account} not authorised to send Podpings"
                )
        except AccountDoesNotExistsException:
            logging.error(
                f"Hive account @{self.server_account} does not exist, "
                f"check ENV vars and try again AccountDoesNotExistsException",
                exc_info=True,
            )
            raise
        except Exception:
            logging.error("Unknown error occurred", exc_info=True)
            raise

        if resource_test:
            # noinspection PyBroadException
            try:  # Now post two custom json to test.
                manabar = account.get_rc_manabar()
                logging.info(
                    f"Testing Account Resource Credits"
                    f' - before {manabar.get("current_pct"):.2f}%'
                )
                custom_json = {
                    "server_account": self.server_account,
                    "USE_TEST_NODE": self.use_testnet,
                    "message": "Podping startup initiated",
                    "uuid": str(uuid.uuid4()),
                    "hive": repr(hive),
                }

                await self.send_notification(custom_json, STARTUP_OPERATION_ID)

                logging.info("Testing Account Resource Credits.... 5s")
                await asyncio.sleep(Config.podping_settings.hive_operation_period)
                manabar_after = account.get_rc_manabar()
                logging.info(
                    f"Testing Account Resource Credits"
                    f' - after {manabar_after.get("current_pct"):.2f}%'
                )
                cost = manabar.get("current_mana") - manabar_after.get("current_mana")
                if cost == 0:  # skip this test if we're going to get ZeroDivision
                    capacity = 1000000
                else:
                    capacity = manabar_after.get("current_mana") / cost
                logging.info(f"Capacity for further podpings : {capacity:.1f}")

                custom_json["v"] = Config.CURRENT_PODPING_VERSION
                custom_json["capacity"] = f"{capacity:.1f}"
                custom_json["message"] = "Podping startup complete"
                custom_json["hive"] = repr(hive)

                await self.send_notification(custom_json, STARTUP_OPERATION_ID)

            except MissingKeyError as e:
                logging.error(
                    "Startup of Podping status: FAILED!  Invalid posting key",
                    exc_info=True,
                )
                logging.error("Exiting")
                sys.exit(STARTUP_FAILED_INVALID_POSTING_KEY_EXIT_CODE)
            except UnhandledRPCError as e:
                if not Config.test:
                    logging.error(
                        "Startup of Podping status: FAILED!  API error",
                        exc_info=True,
                    )
                    logging.info("Exiting")
                    sys.exit(STARTUP_FAILED_HIVE_API_ERROR_EXIT_CODE)
                elif Config.test:
                    logging.warning("Ignoring unknown error in test mode")
            except Exception as e:
                if not Config.test:
                    logging.error(
                        "Startup of Podping status: FAILED!  Unknown error",
                        exc_info=True,
                    )
                    logging.error("Exiting")
                    sys.exit(STARTUP_FAILED_UNKNOWN_EXIT_CODE)
                elif Config.test:
                    logging.warning("Ignoring unknown error in test mode")

        logging.info("Startup of Podping status: SUCCESS! Hit the BOOST Button.")
        logging.info(
            f"---------------> {self.server_account} <- Hive Account will be used"
        )

        if self.daemon:
            self._add_task(asyncio.create_task(self._hive_status_loop()))
            self._add_task(asyncio.create_task(self._zmq_response_loop()))
            self._add_task(asyncio.create_task(self._iri_batch_loop()))
            self._add_task(asyncio.create_task(self._iri_batch_handler_loop()))

        self._startup_done = True

    async def wait_startup(self):
        while not self._startup_done:
            await asyncio.sleep(Config.podping_settings.hive_operation_period)

    async def _hive_status_loop(self):
        while True:
            try:
                self.output_hive_status()
                await asyncio.sleep(Config.podping_settings.diagnostic_report_period)
            except Exception as e:
                logging.error(e, exc_info=True)
            except asyncio.CancelledError:
                raise

    async def _iri_batch_handler_loop(self):
        """Opens and watches a queue and sends notifications to Hive one by one"""
        while True:
            try:
                iri_batch = await self.iri_batch_queue.get()

                start = timer()
                trx_id, failure_count = await self.failure_retry(iri_batch.iri_set)
                duration = timer() - start

                self.iri_batch_queue.task_done()

                logging.info(
                    f"Task time: {duration:0.2f} - trx_id: {trx_id} - "
                    f"Failures: {failure_count} - IRI batch_id {iri_batch.batch_id}"
                )
            except asyncio.CancelledError:
                raise

    async def _iri_batch_loop(self):
        async def get_from_queue():
            try:
                return await self.iri_queue.get()
            except RuntimeError:
                return

        while True:
            iri_set: Set[str] = set()
            start = timer()
            duration = 0
            iris_size_without_commas = 0
            iris_size_total = 0

            # Wait until we have enough IRIs to fit in the payload
            # or get into the current Hive block
            while (
                duration < Config.podping_settings.hive_operation_period
                and iris_size_total < Config.podping_settings.max_url_list_bytes
            ):
                #  get next URL from Q
                logging.debug(
                    f"Duration: {duration:.3f} - WAITING - Queue: {len(iri_set)}"
                )
                try:
                    iri = await asyncio.wait_for(
                        get_from_queue(),
                        timeout=Config.podping_settings.hive_operation_period,
                    )
                    iri_set.add(iri)
                    self.iri_queue.task_done()

                    logging.info(
                        f"Duration: {duration:.3f} - IRI in queue: {iri}"
                        f" - Num IRIs: {len(iri_set)}"
                    )

                    # byte size of IRI in JSON is IRI + 2 quotes
                    iris_size_without_commas += len(iri.encode("UTF-8")) + 2

                    # Size of payload in bytes is
                    # length of IRIs in bytes + the number of commas + 2 square brackets
                    # Assuming it's a JSON list eg ["https://...","https://"..."]
                    iris_size_total = iris_size_without_commas + len(iri_set) - 1 + 2
                except asyncio.TimeoutError:
                    pass
                except asyncio.CancelledError:
                    raise
                except Exception as ex:
                    logging.error(f"{ex} occurred", exc_info=True)
                finally:
                    # Always get the time of the loop
                    duration = timer() - start

            try:
                if len(iri_set):
                    batch_id = uuid.uuid4()
                    iri_batch = IRIBatch(batch_id=batch_id, iri_set=iri_set)
                    await self.iri_batch_queue.put(iri_batch)
                    self.total_iris_recv_deduped += len(iri_set)
                    logging.info(f"Size of IRIs: {iris_size_total}")
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                logging.error(f"{ex} occurred", exc_info=True)

    async def _zmq_response_loop(self):
        loop = asyncio.get_event_loop()

        context = zmq.asyncio.Context()
        socket = context.socket(zmq.REP, io_loop=loop)
        if Config.bind_all:
            socket.bind(f"tcp://*:{Config.zmq}")
        else:
            socket.bind(f"tcp://127.0.0.1:{Config.zmq}")

        while True:
            try:
                iri: str = await socket.recv_string()
                if rfc3987.match(iri, "IRI"):
                    await self.iri_queue.put(iri)
                    self.total_iris_recv += 1
                    await socket.send_string("OK")
                else:
                    await socket.send_string("Invalid IRI")
            except asyncio.CancelledError:
                socket.close()
                raise

    def output_hive_status(self) -> None:
        """Output the name of the current hive node
        on a regular basis"""
        up_time = datetime.utcnow() - Config.startup_datetime
        logging.info("--------------------------------------------------------")
        logging.info(f"Using Hive Node: {self.hive_wrapper.nodes[0]}")
        logging.info(f"Up Time: {up_time}")
        logging.info(
            f"Urls Received: {self.total_iris_recv} - "
            f"Urls Deduped: {self.total_iris_recv_deduped} - "
            f"Urls Sent: {self.total_iris_sent}"
        )
        logging.info("--------------------------------------------------------")

    async def send_notification(
        self, payload: dict, operation_id: Optional[str] = None
    ) -> str:
        try:
            # Assert Exception:o.json.length() <= HIVE_CUSTOM_OP_DATA_MAX_LENGTH:
            # Operation JSON must be less than or equal to 8192 bytes.
            size_of_json = size_of_dict_as_json(payload)
            if size_of_json > 8192:
                raise PodpingCustomJsonPayloadExceeded(
                    "Max custom_json payload exceeded"
                )
            tx = await self.hive_wrapper.custom_json(
                operation_id or self.operation_id, payload, self.required_posting_auths
            )

            tx_id = tx["trx_id"]

            logging.info(f"Transaction sent: {tx_id} - JSON size: {size_of_json}")

            return tx_id

        except MissingKeyError:
            logging.error(f"The provided key for @{self.server_account} is not valid")
            raise

        except Exception as ex:
            logging.error(repr(ex))
            logging.debug(ex, exc_info=True)
            raise ex

    async def send_notification_iri(self, iri: str, reason=1) -> str:
        payload = {
            "v": Config.CURRENT_PODPING_VERSION,
            "num_urls": 1,
            "r": reason,
            "urls": [iri],
        }
        return await self.send_notification(payload)

    async def send_notification_iris(self, iris: Set[str], reason=1) -> str:
        num_iris = len(iris)
        payload = {
            "v": Config.CURRENT_PODPING_VERSION,
            "num_urls": num_iris,
            "r": reason,
            "urls": list(iris),
        }

        tx_id = await self.send_notification(payload)

        logging.info(f"Transaction sent: {tx_id} - Num IRIs: {num_iris}")
        self.total_iris_sent += num_iris

        return tx_id

    async def failure_retry(
        self, iri_set: Set[str], failure_count=0
    ) -> Tuple[str, int]:
        await self.wait_startup()
        #TODO: #7 this should write to a file called failures that can be processed later.
        if failure_count >= len(Config.HALT_TIME):
            print(f"Failure at: {datetime.utcnow()}")
            logging.error(f"Failed to send: {len(iri_set)} iri - Printing to STDOUT")
            for iri in iri_set:
                print(iri)
            return "FAILED", failure_count

        if failure_count > 0:
            logging.warning(f"Waiting {Config.HALT_TIME[failure_count]}s before retry")
            await asyncio.sleep(Config.HALT_TIME[failure_count])
            logging.info(
                f"FAILURE COUNT: {failure_count} - RETRYING {len(iri_set)} IRIs"
            )
        else:
            logging.info(f"Received {len(iri_set)} IRIs")

        try:
            trx_id = await self.send_notification_iris(iris=iri_set)
            if failure_count > 0:
                logging.info(
                    f"----> FAILURE CLEARED after {failure_count} retries <-----"
                )
            return trx_id, failure_count
        except Exception:
            logging.warning(f"Failed to send {len(iri_set)} IRIs")
            if logging.DEBUG >= logging.root.level:
                for iri in iri_set:
                    logging.debug(iri)
            await self.hive_wrapper.rotate_nodes()

            # Since this is endless recursion, this could theoretically fail with
            # enough retries ... (python doesn't optimize tail recursion)
            return await self.failure_retry(iri_set, failure_count + 1)


def get_allowed_accounts(acc_name: str = "podping") -> Set[str]:
    """get a list of all accounts allowed to post by acc_name (podping)
    and only react to these accounts"""

    # Ignores test node.
    nodes = Config.podping_settings.main_nodes
    try:
        hive = beem.Hive(node=nodes)
        master_account = Account(acc_name, blockchain_instance=hive, lazy=True)
        return set(master_account.get_following())
    except Exception:
        logging.error(
            f"Allowed Account: {acc_name} - Failure on Node: {nodes[0]}", exc_info=True
        )