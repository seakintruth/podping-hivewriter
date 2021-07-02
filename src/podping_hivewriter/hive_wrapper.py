import asyncio
import logging
import sys
from collections import deque
from timeit import default_timer as timer
from typing import Iterable, List, Set

import beem
from beem.account import Account
from podping_hivewriter import config

from podping_hivewriter.config import Config
from podping_hivewriter.constants import SERVER_ACCOUNT_NOT_AUTHORISED_TO_PODPING


def get_hive(nodes: Iterable[str], posting_key: str, use_testnet=False) -> beem.Hive:
    nodes = tuple(nodes)
    # Beem's expected type for nodes
    # noinspection PyTypeChecker
    hive = beem.Hive(node=nodes, keys=posting_key, nobroadcast=Config.nobroadcast)

    if use_testnet:
        logging.info(f"---------------> Using Test Node: {nodes[:2]}")
    else:
        logging.info(f"---------------> Using Main Hive Chain: {nodes[:2]}")

    return hive


class HiveWrapper:
    def __init__(
        self,
        nodes: Iterable[str],
        posting_key: str,
        server_account: str,
        daemon=True,
        use_testnet=False,
    ):
        self._tasks: List[asyncio.Task] = []

        self.posting_key = posting_key
        self.server_account = server_account
        self.daemon = daemon
        self.use_testnet = use_testnet

        self.nodes = deque(nodes)
        self._hive: beem.Hive = get_hive(
            self.nodes, self.posting_key, use_testnet=self.use_testnet
        )

        self.allowed_accounts: Set[str] = set()

        self._hive_lock = asyncio.Lock()

        if daemon:
            self._add_task(asyncio.create_task(self._rotate_nodes_loop()))
            self._add_task(asyncio.create_task(self._recheck_allowed_accounts_loop()))

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

    async def _rotate_nodes_loop(self):
        while True:
            try:
                await asyncio.sleep(Config.podping_settings.diagnostic_report_period)
                await self.rotate_nodes()
            except Exception as e:
                logging.error(e, exc_info=True)
            except asyncio.CancelledError:
                raise

    async def rotate_nodes(self):
        async with self._hive_lock:
            self.nodes.rotate(1)
            self._hive = get_hive(
                self.nodes, self.posting_key, use_testnet=self.use_testnet
            )
            logging.info(f"New Hive Nodes in use: {self._hive}")

    async def _recheck_allowed_accounts_loop(self):
        while True:
            try:
                await asyncio.sleep(
                    10 * Config.podping_settings.diagnostic_report_period
                )
                await self.recheck_allowed_accounts()

            except Exception as e:
                logging.error(e, exc_info=True)
            except asyncio.CancelledError:
                raise

    async def recheck_allowed_accounts(self, acc_name: str = None) -> bool:
        """Returns True if valid podping account, Sys.exit if not"""
        if not acc_name:
            acc_name = config.Config.podping_settings.control_account
        if await self.get_allowed_accounts(acc_name):
            return True
        else:
            logging.error(f"FATAL: {acc_name} is not Podping! Contact @podping on Hive")
            logging.error("Exiting")
            sys.exit(SERVER_ACCOUNT_NOT_AUTHORISED_TO_PODPING)

    async def custom_json(
        self, operation_id: str, payload: dict, required_posting_auths: List[str]
    ):
        async with self._hive_lock:
            # noinspection PyTypeChecker
            return self._hive.custom_json(
                id=operation_id,
                json_data=payload,
                required_posting_auths=required_posting_auths,
            )

    async def get_hive(self, force_mainnet: bool = False):
        async with self._hive_lock:
            return self._hive

    async def get_allowed_accounts(self, acc_name: str = None) -> bool:
        """get a list of all accounts allowed to post by acc_name (podping)
        and only react to these accounts. Returns True if server_account is in the list"""
        if not acc_name:
            acc_name = config.Config.podping_settings.control_account
        # Ignores test node.
        previous_allowed = set()
        if self.allowed_accounts:
            previous_allowed = self.allowed_accounts
        nodes = Config.podping_settings.main_nodes
        try:
            hive = get_hive(
                nodes=config.Config.podping_settings.main_nodes,
                posting_key=[],
                use_testnet=False,
            )
            master_account = Account(acc_name, blockchain_instance=hive, lazy=True)
            self.allowed_accounts = set(master_account.get_following())
        except Exception as e:
            logging.error(e, exc_info=True)
            logging.error(
                f"Allowed Account: {acc_name} - Failure on Node: {nodes[0]}",
                exc_info=True,
            )
            self.allowed_accounts = previous_allowed
        finally:
            return self.server_account in self.allowed_accounts
